package delivery

import (
	"context"
	"io"
	"log/slog"
	"os"
	"slices"
	"sort"
	"sync"
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
// replicas over the same database, exactly one replica drops partitions, and every
// partition it drops goes exactly once. Two design choices are what let this test
// fail when that stops being true:
//
//   - Two partitions have fully elapsed (41 and 40 days ago), not one. With a
//     single droppable partition the SQL guard's "already gone" answer caps the
//     drop count at one *whatever* the replicas do — a second concurrent dropper
//     gets `false` back and is not counted — so a guard on the count could never
//     fire. Two give a second active sweeper a partition of its own to reach for.
//   - Each replica carries its own recording store and retention manager, as
//     separate processes would. The shared metrics counter counts drops, not
//     droppers, so on its own it cannot say which replica dropped.
//
// The wait loop re-reads the partition catalogue every 50 ms while the holder is
// dropping, i.e. as the reader an operator's dashboard is (SB-6's usage endpoint
// lists the same catalogue). That is deliberate: it is how this test found that
// pg_total_relation_size answers NULL for a partition dropped between the query's
// snapshot and the size read, which made ListPartitions fail mid-drop. The reader
// now skips such a partition (internal/store/retention.go).
//
// A replica that sweeps without the leadership lock — exactly what D-7 forbids —
// is therefore visible here as a second replica reaching the drop path, and the
// per-replica books say so by name.
func TestTwoSweepersDropExactlyOnePartition(t *testing.T) {
	admin := testPool(t)
	st := store.New(admin)
	dsn := os.Getenv("PIGEON_TEST_DATABASE_URL")
	// ctx is cancelled to stop the replicas; the catalogue is read over its own
	// context, because a cancelled sweep context cannot query anything.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	qctx, qcancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer qcancel()

	// Two fully-elapsed partitions, so either replica has real work waiting the
	// moment it ticks.
	elapsed := map[string]bool{}
	for _, days := range []int{-41, -40} {
		day := time.Now().UTC().AddDate(0, 0, days)
		name, err := st.EnsurePartition(qctx, "attempts", day)
		if err != nil {
			t.Fatalf("EnsurePartition(%s): %v", day.Format("2006-01-02"), err)
		}
		elapsed[name] = true
	}

	tables := []string{"attempts", "deliveries"}
	before := map[string]map[string]bool{}
	for _, table := range tables {
		before[table] = partitionNames(t, qctx, st, table)
	}

	// The volume is over budget, so the holder drops the oldest partition first
	// (SB-3) and keeps dropping one per tick until nothing droppable is left.
	over := retention.Usage{UsedBytes: 950, TotalBytes: 1000, UsedPct: 95, Source: "test"}
	newOverBudgetManager := func(rs *replicaStore) *retention.Manager {
		return retention.NewManager(rs, fakeGauge{over}, retention.Options{
			MaxDiskPct: 80, Logger: discardLogger(),
		})
	}

	shared := metrics.New()
	replicas := []*replicaStore{{Store: st}, {Store: st}}
	var wg sync.WaitGroup
	for i, rs := range replicas {
		s := NewSweeper(st, SweeperOptions{URL: dsn, Interval: 100 * time.Millisecond,
			LockWait: 500 * time.Millisecond, Logger: discardLogger(),
			Retention: newOverBudgetManager(rs), Metrics: shared})
		wg.Add(1)
		go func(i int, s *Sweeper) {
			defer wg.Done()
			_ = s.Run(ctx)
		}(i, s)
	}

	// Wait for the elapsed partitions to go. The holder drops one per tick, so the
	// wait is bounded by how many partitions are droppable, not by a leadership
	// change (nothing kills the holder here; that is the other AC-42 test).
	deadline := time.Now().Add(20 * time.Second)
	for {
		live := partitionNames(t, qctx, st, "attempts")
		pending := 0
		for name := range elapsed {
			if live[name] {
				pending++
			}
		}
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d elapsed partitions are still live after 20 s: retention is not dropping under disk pressure (SB-3)", pending, len(elapsed))
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Stop both replicas before reading their books: a tick in flight must not land
	// a drop after the catalogue snapshot below.
	cancel()
	wg.Wait()

	// Exactly one replica may have reached the drop path at all. A standby never
	// holds the lease, so it never even asks for a drop. This is the assertion a
	// mutation that sweeps on every replica cannot survive.
	active, dropped := 0, 0
	byTable := map[string]map[string]int{}
	for i, rs := range replicas {
		attempts, drops := rs.snapshot()
		if attempts > 0 {
			active++
		}
		dropped += len(drops)
		for _, d := range drops {
			if byTable[d.table] == nil {
				byTable[d.table] = map[string]int{}
			}
			byTable[d.table][d.name]++
		}
		t.Logf("replica %d: drop attempts=%d, successful drops=%v", i, attempts, drops)
	}
	if active != 1 {
		t.Fatalf("%d of %d replicas reached the drop path, want exactly 1: a replica swept without the leadership lock (D-7/AC-42)", active, len(replicas))
	}

	// Every partition that left the catalogue was dropped exactly once, by that one
	// replica: no partition went twice (D-7) and none vanished unaccounted for.
	for _, table := range tables {
		for name, n := range byTable[table] {
			if n != 1 {
				t.Fatalf("%s partition %s was dropped %d times, want exactly 1 (D-7/AC-42)", table, name, n)
			}
		}
		live := partitionNames(t, qctx, st, table)
		var vanished []string
		for name := range before[table] {
			if !live[name] {
				vanished = append(vanished, name)
			}
		}
		recorded := sortedNames(byTable[table])
		sort.Strings(vanished)
		if !slices.Equal(vanished, recorded) {
			t.Fatalf("%s: dropped %v, but the partitions that left the catalogue are %v", table, recorded, vanished)
		}
	}

	// The elapsed partitions this test created are gone, and the shipped metrics
	// counter agrees with the per-replica books.
	live := partitionNames(t, qctx, st, "attempts")
	for name := range elapsed {
		if live[name] {
			t.Fatalf("%s was not dropped (AC-42: the elapsed partition must go)", name)
		}
	}
	if got := shared.PartitionDropCount(); got != int64(dropped) {
		t.Fatalf("metrics counter = %d, per-replica successful drops = %d", got, dropped)
	}
}

// replicaStore is one replica's view of the partition store. It records what
// *this* replica asked to drop — attempts as well as successes — which is the only
// way to tell "one sweeper dropped" from "two sweepers reached the drop path": the
// shared metrics counter counts drops, not droppers.
type replicaStore struct {
	retention.Store
	mu       sync.Mutex
	attempts int
	dropped  []dropRecord
}

// dropRecord is one successful drop, kept with its table so the test can compare
// the books against the live partition catalogue.
type dropRecord struct{ table, name string }

// DropPartition counts the attempt, then the drop if the SQL primitive removed the
// partition. The attempt is recorded even when the drop returns false, because
// "this replica reached the drop path without the leadership lock" is the failure
// AC-42 is about.
func (r *replicaStore) DropPartition(ctx context.Context, table, name string) (bool, error) {
	dropped, err := r.Store.DropPartition(ctx, table, name)
	r.mu.Lock()
	r.attempts++
	if dropped {
		r.dropped = append(r.dropped, dropRecord{table: table, name: name})
	}
	r.mu.Unlock()
	return dropped, err
}

// snapshot copies this replica's books under the lock.
func (r *replicaStore) snapshot() (int, []dropRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.attempts, append([]dropRecord(nil), r.dropped...)
}

// partitionNames returns the live leaf partitions of one table as a set.
func partitionNames(t *testing.T, ctx context.Context, st *store.Store, table string) map[string]bool {
	t.Helper()
	parts, err := st.ListPartitions(ctx, table)
	if err != nil {
		t.Fatalf("ListPartitions(%s): %v", table, err)
	}
	names := make(map[string]bool, len(parts))
	for _, p := range parts {
		names[p.Name] = true
	}
	return names
}

// sortedNames returns the keys of a name->count map, sorted for a stable message.
func sortedNames(counts map[string]int) []string {
	names := make([]string, 0, len(counts))
	for name := range counts {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// fakeGauge is a fixed disk reading; the sweepers only need the budget verdict.
type fakeGauge struct{ u retention.Usage }

func (g fakeGauge) Usage(context.Context) (retention.Usage, error) { return g.u, nil }
