package delivery

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
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

// Run acquires leadership (D-7), then re-queues expired leases on a fixed cadence
// (D-6) until ctx is cancelled. Holding the advisory lock on a dedicated
// connection for the whole run means a SIGKILL or network partition releases it
// when that connection dies — bounded to well under the D-7 30 s ceiling by the
// low TCP keepalives set on it, so partition drops never run concurrently.
func (s *Sweeper) Run(ctx context.Context) error {
	release, err := s.acquireLeadership(ctx)
	if err != nil {
		return err
	}
	defer release()
	s.log.Info("sweeper started", "interval_ms", s.opts.Interval.Milliseconds())

	ticker := time.NewTicker(s.opts.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if n, err := s.store.SweeperRequeue(ctx); err != nil {
				s.log.Warn("sweeper: requeue failed", "error", err)
			} else if n > 0 {
				s.log.Info("sweeper: re-queued expired leases", "requeued", n)
			}
		}
	}
}

// acquireLeadership competes for the advisory lock with pg_try_advisory_lock
// (non-blocking) on a dedicated single connection, retrying until it wins or
// LockWait elapses. A win is held for the caller by keeping its connection open;
// the returned cleanup closes it, which releases the lock. Every other replica
// stays a standby competing for the lock (D-7).
func (s *Sweeper) acquireLeadership(ctx context.Context) (cleanup func(), err error) {
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

	deadline, cancel := context.WithTimeout(ctx, s.opts.LockWait)
	defer cancel()
	for {
		conn, err := lockPool.Acquire(ctx)
		if err != nil {
			if ctx.Err() != nil {
				lockPool.Close()
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("sweeper: acquire lock connection: %w", err)
		}
		var held bool
		qctx, qcancel := context.WithTimeout(deadline, 2*time.Second)
		err = conn.QueryRow(qctx, `select pg_try_advisory_lock($1)`, sweeperLockID).Scan(&held)
		qcancel()
		if err == nil && held {
			// Held: keep this connection (and its lock) until cleanup runs.
			return func() {
				conn.Release()
				lockPool.Close()
			}, nil
		}
		conn.Release() // not held; compete again on the next tick
		if ctx.Err() != nil {
			lockPool.Close()
			return nil, ctx.Err()
		}
		if deadline.Err() != nil {
			lockPool.Close()
			return nil, fmt.Errorf("sweeper: could not acquire leadership within %s", s.opts.LockWait)
		}
		select {
		case <-ctx.Done():
			lockPool.Close()
			return nil, ctx.Err()
		case <-deadline.Done():
			lockPool.Close()
			return nil, fmt.Errorf("sweeper: could not acquire leadership within %s", s.opts.LockWait)
		case <-time.After(1 * time.Second):
		}
	}
}
