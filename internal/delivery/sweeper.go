package delivery

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m7jay/pigeon/internal/metrics"
	"github.com/m7jay/pigeon/internal/retention"
)

// sweeperLockID is the advisory-lock key that makes exactly one sweeper active
// per deployment (D-7). It is a fixed constant, not a hash of the database name,
// so every replica contends for the same lock and only one wins.
const sweeperLockID int64 = 0x70696765_6e5f7377 // "pigen_sw"

// MaxSweepInterval is D-6's ceiling on the recovery latency: a delivery whose lease
// has lapsed MUST be re-queued within 10 s, so the sweep cadence is a hard maximum,
// not a default. NewSweeper clamps to it and internal/config clamps
// PIGEON_SWEEPER_INTERVAL the same way, so no single setting can void D-6.
const MaxSweepInterval = 10 * time.Second

// SweeperOptions configures the sweeper.
type SweeperOptions struct {
	// URL is the DSN for the dedicated connection that holds the leadership lock.
	// It must stay open for the sweeper's lifetime; closing it releases the lock.
	URL string
	// Interval is the sweep cadence: it MUST be ≤ 10 s (D-6), so a crashed
	// worker's deliveries are re-queued within the window, and it is clamped to
	// MaxSweepInterval. Defaults to 5 s.
	Interval time.Duration
	// LockWait is how long to keep competing for the lock before giving up boot.
	// Defaults to 60 s.
	LockWait time.Duration
	Logger   *slog.Logger
	// Retention, when set, runs the partition retention pass on every tick. It is
	// driven here rather than in its own goroutine because D-7 says exactly one
	// sweeper is active and the lease that guarantees that is this one: the same
	// holder that re-queues leases also creates the days ahead and drops the
	// oldest partition under disk pressure. A second replica stays a standby and
	// therefore cannot race the drops (AC-42). Nil disables the retention pass.
	Retention RetentionSweeper
	// Metrics, when set, receives the partition-drop counter (SB-6). Nil is fine.
	Metrics *metrics.Metrics
}

// RetentionSweeper is the retention tick the sweeper drives under its leadership
// lease. *retention.Manager implements it; the interface keeps this package from
// importing the SQL layer's retention bindings.
type RetentionSweeper interface {
	RunOnce(ctx context.Context) (retention.Report, error)
}

// StoreRequeuer is the subset of *store.Store the sweeper needs, so the sweeper
// can be constructed against a real or fake store in tests.
type StoreRequeuer interface {
	SweeperRequeue(ctx context.Context) (int, error)
}

// Sweeper re-queues deliveries whose lease has lapsed (D-6) under a single
// leaseholder (D-7). It is the guaranteed path: the worker claim loop is
// opportunistic, so the sweeper runs on a fixed cadence ≤ the D-6 window and
// catches anything a crashed worker was holding.
type Sweeper struct {
	store StoreRequeuer
	opts  SweeperOptions
	log   *slog.Logger
}

// NewSweeper builds a Sweeper. A nil logger is replaced rather than panicked on.
func NewSweeper(st StoreRequeuer, opts SweeperOptions) *Sweeper {
	if opts.Interval <= 0 {
		opts.Interval = 5 * time.Second
	}
	if opts.Interval > MaxSweepInterval {
		// D-6 is a ceiling, not an aspiration (FR-802's idiom): a slower tick
		// would widen how long a crashed worker's delivery stays unclaimed.
		opts.Interval = MaxSweepInterval
	}
	if opts.LockWait <= 0 {
		opts.LockWait = 60 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Sweeper{store: st, opts: opts, log: opts.Logger}
}

// Run holds leadership (D-7) and sweeps until ctx is cancelled. It is a loop,
// not a single acquisition: a replica that cannot win the lock is a *standby*
// (D-7: "every other core replica runs as a standby that competes for the
// lock"), so it keeps competing instead of giving up. When the holder dies, its
// session ends, Postgres releases the advisory lock within the keepalive window
// (well under the D-7 30 s ceiling), and the standby here wins it and takes over
// — which is exactly AC-42's "standby takes leadership within 30 s".
//
// The advisory lock is held on one dedicated connection for as long as this
// replica leads. If that connection dies, the lock is gone; runAsLeader notices
// within a tick and stands down, so two sweepers never drop partitions at once.
func (s *Sweeper) Run(ctx context.Context) error {
	for {
		lease, err := s.acquireLeadership(ctx)
		if err != nil {
			return err
		}
		s.log.Info("sweeper: leadership acquired",
			"interval_ms", s.opts.Interval.Milliseconds(),
			"retention", s.opts.Retention != nil)
		s.runAsLeader(ctx, lease)
		lease.release()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Lost the lock (connection died) or never had it yet: pause briefly,
		// then compete again as a standby.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// runAsLeader sweeps on the fixed cadence until ctx is cancelled or the
// leadership lock is lost. Losing the lock is not an error the caller retries
// blindly: the sweeper goes back to competing as a standby so the replica that
// now holds the lock is the only one dropping partitions.
func (s *Sweeper) runAsLeader(ctx context.Context, lease *leadership) {
	// Sweep once immediately. Two reasons: a worker may have died during a
	// restart and its lease must not wait out an interval (D-6), and a cold boot
	// needs today's partitions to exist before the first publish lands (SB-1).
	s.sweepOnce(ctx)

	ticker := time.NewTicker(s.opts.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if ok, err := lease.held(ctx); err != nil || !ok {
				// The dedicated connection is gone or another session holds the
				// lock: sweeping on would be a second active sweeper (D-7).
				s.log.Warn("sweeper: lost leadership; standing by", "error", err)
				return
			}
			s.sweepOnce(ctx)
		}
	}
}

// sweepOnce is one tick. Recovery runs first: a crashed worker's delivery is the
// most urgent thing the sweeper owns (D-6), and a retention failure must never
// stop it. Retention runs second, under the same leadership lease (D-7), so the
// holder that drops partitions is unique and AC-42's "exactly one drop" holds
// across replicas without a second lock protocol.
func (s *Sweeper) sweepOnce(ctx context.Context) {
	if n, err := s.store.SweeperRequeue(ctx); err != nil {
		s.log.Warn("sweeper: requeue failed", "error", err)
	} else if n > 0 {
		s.log.Info("sweeper: re-queued expired leases", "requeued", n)
	}

	if s.opts.Retention == nil {
		return
	}
	rep, err := s.opts.Retention.RunOnce(ctx)
	if err != nil {
		// Retention failing is not a reason to stop the recovery sweep; the next
		// tick retries. It IS a reason to say so loudly, because a stuck retention
		// pass is how a disk fills.
		s.log.Warn("sweeper: retention tick failed", "error", err)
		return
	}
	if len(rep.Dropped) > 0 {
		if s.opts.Metrics != nil {
			s.opts.Metrics.RecordPartitionDrop(len(rep.Dropped))
		}
		s.log.Info("sweeper: dropped partitions", "dropped", len(rep.Dropped))
	}
	if rep.KeptOverBudget {
		// The manager already logged this at error level; the sweeper adds the
		// tick's own context so an operator sees it from both owners.
		s.log.Error("sweeper: disk over budget and no partition left to drop; ingest is shedding",
			"used_pct", rep.Status.Disk.UsedPct, "max_disk_pct", rep.Status.MaxDiskPct)
	}
}

// leadership is a held advisory-lock session. The lock lives on conn (a single
// dedicated connection from pool) and is released when that connection closes.
type leadership struct {
	conn *pgxpool.Conn
	pool *pgxpool.Pool
}

// release closes the dedicated connection, which ends the session and therefore
// drops the session-level advisory lock. Postgres then lets a standby win it.
func (l *leadership) release() {
	if l.conn != nil {
		l.conn.Release()
		l.conn = nil
	}
	if l.pool != nil {
		l.pool.Close()
		l.pool = nil
	}
}

// held reports whether this session still holds the leadership lock. It queries
// pg_locks over the *locked connection itself*: if that connection died, the
// query fails and the caller stands down, so a sweeper never keeps running after
// losing its lease (which would be a second active sweeper, D-7).
//
// It reads pg_locks rather than calling pg_try_advisory_lock again on purpose:
// session advisory locks are re-entrant, so a second successful try would not
// distinguish "still held" from "re-acquired", and unlocking to probe would
// briefly free the lock for a competitor.
func (l *leadership) held(ctx context.Context) (bool, error) {
	if l.conn == nil {
		return false, nil
	}
	high, low := advisoryLockParts(sweeperLockID)
	qctx, qcancel := context.WithTimeout(ctx, 2*time.Second)
	defer qcancel()
	var ok bool
	if err := l.conn.QueryRow(qctx, `
		select exists (
			select 1 from pg_locks
			 where locktype = 'advisory'
			   and pid = pg_backend_pid()
			   and classid::bigint = $1
			   and objid::bigint = $2
			   and objsubid = 1
			   and granted
		)`, high, low).Scan(&ok); err != nil {
		return false, err
	}
	return ok, nil
}

// advisoryLockParts splits a 64-bit advisory key into the (classid, objid) pair
// pg_locks uses for the single-key form. objsubid is 1 for that form.
func advisoryLockParts(id int64) (int64, int64) {
	return int64(uint32(id >> 32)), int64(uint32(id & 0xffffffff))
}

// acquireLeadership competes for the advisory lock with pg_try_advisory_lock
// (non-blocking) on a dedicated single connection, retrying until it wins or ctx
// is cancelled. A win is held for the caller by keeping its connection open; the
// returned lease's release closes it. Every other replica stays a standby
// competing for the lock (D-7) — it never gives up, because giving up is how a
// deployment ends up with no sweeper at all after the first holder dies (AC-42).
// LockWait is how long to compete before announcing, at info level, that this
// replica is standing by; it does not stop the competition.
func (s *Sweeper) acquireLeadership(ctx context.Context) (*leadership, error) {
	if s.opts.URL == "" {
		return nil, fmt.Errorf("sweeper: leadership lock needs a DSN (URL)")
	}
	cfg, err := pgxpool.ParseConfig(s.opts.URL)
	if err != nil {
		return nil, fmt.Errorf("sweeper: parse lock DSN: %w", err)
	}
	// One connection, held for the sweeper's lifetime: the session-level advisory
	// lock lives on it and is released only when it closes.
	cfg.MaxConns = 1
	// Bound how long Postgres takes to notice a dead client, so a crashed or
	// partitioned holder's lock is released within the D-7 30 s ceiling even if
	// its socket never sends a FIN. idle + interval*(count-1) ≈ 3 + 3*3 = 12 s.
	cfg.ConnConfig.RuntimeParams["tcp_keepalives_idle"] = "3"
	cfg.ConnConfig.RuntimeParams["tcp_keepalives_interval"] = "3"
	cfg.ConnConfig.RuntimeParams["tcp_keepalives_count"] = "4"

	lockPool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("sweeper: connect lock DSN: %w", err)
	}
	// Prove the connection works before competing; a dead DSN is a boot failure,
	// not a silent no-op sweeper.
	pingCtx, pingCancel := context.WithTimeout(ctx, 10*time.Second)
	err = lockPool.Ping(pingCtx)
	pingCancel()
	if err != nil {
		lockPool.Close()
		return nil, fmt.Errorf("sweeper: lock DSN unreachable: %w", err)
	}

	competingSince := time.Now()
	announced := false
	for {
		conn, err := lockPool.Acquire(ctx)
		if err != nil {
			if ctx.Err() != nil {
				lockPool.Close()
				return nil, ctx.Err()
			}
			lockPool.Close()
			return nil, fmt.Errorf("sweeper: acquire lock connection: %w", err)
		}
		var held bool
		qctx, qcancel := context.WithTimeout(ctx, 2*time.Second)
		err = conn.QueryRow(qctx, `select pg_try_advisory_lock($1)`, sweeperLockID).Scan(&held)
		qcancel()
		if err == nil && held {
			// Held: keep this connection (and its lock) until release runs.
			return &leadership{conn: conn, pool: lockPool}, nil
		}
		conn.Release() // not held; compete again
		if ctx.Err() != nil {
			lockPool.Close()
			return nil, ctx.Err()
		}
		if !announced && time.Since(competingSince) >= s.opts.LockWait {
			s.log.Info("sweeper: standing by; another replica holds the leadership lock")
			announced = true
		}
		select {
		case <-ctx.Done():
			lockPool.Close()
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}
