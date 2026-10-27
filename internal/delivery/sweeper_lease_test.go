package delivery

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m7jay/pigeon/internal/metrics"
	"github.com/m7jay/pigeon/internal/retention"
	"github.com/m7jay/pigeon/internal/store"
)

// This file is AC-42: "start two sweepers; kill the lock holder — the standby
// takes leadership within 30 s; exactly one partition is dropped; no delivery is
// re-queued twice". It runs against a real Postgres because the property is
// carried by the advisory lock and the partition primitives; a fake would pass
// while the lock was wrong.

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// lockHolderPid returns the backend holding the sweeper leadership lock, or 0
// when no session holds it. It reads pg_locks directly, so it observes the real
// lock rather than the sweeper's own bookkeeping.
func lockHolderPid(t *testing.T, admin *pgxpool.Pool) int32 {
	t.Helper()
	high, low := advisoryLockParts(sweeperLockID)
	var pid int32
	err := admin.QueryRow(context.Background(), `
		select pid from pg_locks
		 where locktype = 'advisory'
		   and classid::bigint = $1 and objid::bigint = $2
		   and objsubid = 1 and granted`, high, low).Scan(&pid)
	if err != nil {
		return 0 // no holder
	}
	return pid
}

// waitForHolder blocks until some session holds the leadership lock, and returns
// that session's pid.
func waitForHolder(t *testing.T, admin *pgxpool.Pool, within time.Duration) int32 {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		if pid := lockHolderPid(t, admin); pid != 0 {
			return pid
		}
		if time.Now().After(deadline) {
			t.Fatalf("no sweeper acquired leadership within %s", within)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestSweeperStandbyTakesOverWhenTheHolderIsKilled is AC-42's first half. Two
// sweepers compete for the same advisory lock; the standby keeps competing rather
// than exiting (D-7), and when the holder's session is killed the standby takes
// leadership well inside the 30 s ceiling.
func TestSweeperStandbyTakesOverWhenTheHolderIsKilled(t *testing.T) {
	admin := testPool(t)
	st := store.New(admin)
	dsn := os.Getenv("PIGEON_TEST_DATABASE_URL")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := SweeperOptions{URL: dsn, Interval: 100 * time.Millisecond,
		LockWait: 500 * time.Millisecond, Logger: discardLogger()}

	leader := NewSweeper(st, opts)
	go func() { _ = leader.Run(ctx) }()
	holderPid := waitForHolder(t, admin, 5*time.Second)

	standby := NewSweeper(st, opts)
	go func() { _ = standby.Run(ctx) }()

	// While the holder is alive the standby must NOT take the lock: that is the
	// exclusivity D-7 promises (and half of what keeps drops from running twice).
	time.Sleep(700 * time.Millisecond)
	if got := lockHolderPid(t, admin); got != holderPid {
		t.Fatalf("leadership moved while the holder was alive: %d -> %d", holderPid, got)
	}

	// Kill the lock holder, as the acceptance criterion says: this ends the
	// session, which is what releases a session-level advisory lock.
	if _, err := admin.Exec(ctx, `select pg_terminate_backend($1)`, holderPid); err != nil {
		t.Fatalf("terminate holder %d: %v", holderPid, err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for {
		if pid := lockHolderPid(t, admin); pid != 0 && pid != holderPid {
			// A different session now holds the lock: the standby took over.
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no standby took leadership within 30 s of the holder being killed (AC-42)")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestTwoSweepersDropExactlyOnePartition is AC-42's second half: with two sweeper
// replicas over the same database and one droppable partition, exactly one drop
// happens. Each sweeper has its own retention manager (as separate processes
// would) and shares the metrics counter, so a second, concurrent drop would show
// up as a count of two.
func TestTwoSweepersDropExactlyOnePartition(t *testing.T) {
	admin := testPool(t)
	st := store.New(admin)
	dsn := os.Getenv("PIGEON_TEST_DATABASE_URL")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Exactly one droppable partition exists: 40 days ago, so its day has ended.
	past := time.Now().UTC().AddDate(0, 0, -40)
	name, err := st.EnsurePartition(ctx, "attempts", past)
	if err != nil {
		t.Fatalf("EnsurePartition: %v", err)
	}

	// The volume is over budget, so the leader drops the oldest partition first
	// (SB-3). One partition is droppable, so one drop is all any replica may do.
	over := retention.Usage{UsedBytes: 950, TotalBytes: 1000, UsedPct: 95, Source: "test"}
	newOverBudgetManager := func() *retention.Manager {
		return retention.NewManager(st, fakeGauge{over}, retention.Options{
			MaxDiskPct: 80, Logger: discardLogger(),
		})
	}

	shared := metrics.New()
	opts := func(m *retention.Manager) SweeperOptions {
		return SweeperOptions{URL: dsn, Interval: 100 * time.Millisecond,
			LockWait: 500 * time.Millisecond, Logger: discardLogger(),
			Retention: m, Metrics: shared}
	}

	a, b := NewSweeper(st, opts(newOverBudgetManager())), NewSweeper(st, opts(newOverBudgetManager()))
	go func() { _ = a.Run(ctx) }()
	go func() { _ = b.Run(ctx) }()

	// Let both sweepers tick many times: the leader drops, then finds nothing
	// droppable and sheds; the standby must not drop anything at all.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if shared.PartitionDropCount() > 1 {
			t.Fatalf("partitions dropped = %d, want at most 1: two sweepers dropped concurrently (D-7/AC-42)",
				shared.PartitionDropCount())
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got := shared.PartitionDropCount(); got != 1 {
		t.Fatalf("partitions dropped = %d, want exactly 1 (AC-42)", got)
	}

	parts, err := st.ListPartitions(ctx, "attempts")
	if err != nil {
		t.Fatalf("ListPartitions: %v", err)
	}
	for _, p := range parts {
		if p.Name == name {
			t.Fatalf("%s was not dropped", name)
		}
	}
}

// fakeGauge is a fixed disk reading; the sweepers only need the budget verdict.
type fakeGauge struct{ u retention.Usage }

func (g fakeGauge) Usage(context.Context) (retention.Usage, error) { return g.u, nil }
