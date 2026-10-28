package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/m7jay/pigeon/internal/retention"
)

// These tests exercise the SQL side of retention (SB-1/SB-2, D-7's "no drop runs
// concurrently with itself") through the runtime role, so the SECURITY DEFINER
// grants are on the hook exactly as in production.

// TestPartitionCatalogueAndDropGuard: partitions exist per day (SB-1), the
// catalogue reports them, and the drop primitive refuses anything that is not a
// fully-elapsed leaf partition of a managed table.
func TestPartitionCatalogueAndDropGuard(t *testing.T) {
	admin := testPool(t)
	st := newAppStore(t, admin) // pigeon_app: proves the definer grant reaches it
	ctx := context.Background()
	today := time.Now().UTC()

	name, err := st.EnsurePartition(ctx, "deliveries", today)
	if err != nil {
		t.Fatalf("EnsurePartition(today): %v", err)
	}
	if want := "deliveries_" + today.Format("20060102"); name != want {
		t.Fatalf("EnsurePartition = %q, want %q", name, want)
	}
	// SB-1: the primitives are idempotent, so the sweeper can call them every tick.
	again, err := st.EnsurePartition(ctx, "deliveries", today)
	if err != nil || again != name {
		t.Fatalf("second EnsurePartition = (%q, %v), want (%q, nil)", again, err, name)
	}

	parts, err := st.ListPartitions(ctx, "deliveries")
	if err != nil {
		t.Fatalf("ListPartitions: %v", err)
	}
	found := false
	for _, p := range parts {
		if p.Name != name {
			continue
		}
		found = true
		if got := p.Day.UTC().Format("2006-01-02"); got != today.Format("2006-01-02") {
			t.Fatalf("partition Day = %s, want %s", got, today.Format("2006-01-02"))
		}
		if p.Table != "deliveries" {
			t.Fatalf("partition Table = %q, want deliveries", p.Table)
		}
	}
	if !found {
		t.Fatalf("today's partition %s is missing from the catalogue (%d partitions)", name, len(parts))
	}

	// The rail: a partition whose day has not ended is never droppable, whatever
	// the disk pressure. The primitive refuses loudly; the manager never asks.
	if _, err := st.DropPartition(ctx, "deliveries", name, retention.DropByAge); err == nil {
		t.Fatalf("DropPartition(%s) accepted today's partition, want a refusal", name)
	}

	// A fully-elapsed day may be dropped, whole (SB-2: no DELETE anywhere).
	past := today.AddDate(0, 0, -30)
	pastName, err := st.EnsurePartition(ctx, "deliveries", past)
	if err != nil {
		t.Fatalf("EnsurePartition(past): %v", err)
	}
	dropped, err := st.DropPartition(ctx, "deliveries", pastName, retention.DropByAge)
	if err != nil {
		t.Fatalf("DropPartition(%s): %v", pastName, err)
	}
	if !dropped {
		t.Fatalf("DropPartition(%s) = false, want true", pastName)
	}
	parts, err = st.ListPartitions(ctx, "deliveries")
	if err != nil {
		t.Fatalf("ListPartitions after drop: %v", err)
	}
	for _, p := range parts {
		if p.Name == pastName {
			t.Fatalf("%s survived its drop", pastName)
		}
	}

	// Already gone is a no-op, not an error: a retry after a successful drop (or a
	// race between two replicas) must not fail the tick.
	if got, err := st.DropPartition(ctx, "deliveries", pastName, retention.DropByAge); err != nil || got {
		t.Fatalf("second DropPartition(%s) = (%v, %v), want (false, nil)", pastName, got, err)
	}

	// A name from another parent, and an unmanaged table, are both refused before
	// any DDL is built.
	if _, err := st.DropPartition(ctx, "deliveries", "attempts_20260101", retention.DropByAge); err == nil {
		t.Fatal("DropPartition accepted a partition of another table")
	}
	if _, err := st.ListPartitions(ctx, "api_keys"); err == nil {
		t.Fatal("ListPartitions accepted an unmanaged table")
	}
	if _, err := st.DropPartition(ctx, "api_keys", "api_keys_20260101", retention.DropByAge); err == nil {
		t.Fatal("DropPartition accepted an unmanaged table")
	}
}

// TestDropPartitionSerialisesWithAConcurrentDrop is D-7's second half at the SQL
// layer: no partition drop may run concurrently with itself. The primitive takes
// a transaction-scoped advisory lock, capped by lock_timeout, so a contended drop
// leaves the partition for the next tick instead of blocking the sweeper forever.
func TestDropPartitionSerialisesWithAConcurrentDrop(t *testing.T) {
	admin := testPool(t)
	st := newAppStore(t, admin)
	ctx := context.Background()

	past := time.Now().UTC().AddDate(0, 0, -40)
	name, err := st.EnsurePartition(ctx, "attempts", past)
	if err != nil {
		t.Fatalf("EnsurePartition: %v", err)
	}

	// Hold the drop lock in another transaction, exactly as a competing dropper
	// (another replica, or a second code path) would.
	holder, err := admin.Begin(ctx)
	if err != nil {
		t.Fatalf("begin holder: %v", err)
	}
	if _, err := holder.Exec(ctx, `select pg_advisory_xact_lock(pigeon_partition_drop_lock_id())`); err != nil {
		_ = holder.Rollback(ctx)
		t.Fatalf("hold drop lock: %v", err)
	}

	start := time.Now()
	dropped, err := st.DropPartition(ctx, "attempts", name, retention.DropByAge)
	if err != nil {
		_ = holder.Rollback(ctx)
		t.Fatalf("DropPartition under contention: %v", err)
	}
	if dropped {
		_ = holder.Rollback(ctx)
		t.Fatal("dropped a partition while another drop held the lock")
	}
	if waited := time.Since(start); waited > 15*time.Second {
		_ = holder.Rollback(ctx)
		t.Fatalf("the contended drop waited %s; lock_timeout must bound it", waited)
	}
	if err := holder.Rollback(ctx); err != nil {
		t.Fatalf("rollback holder: %v", err)
	}

	// The lock is free: the same drop now succeeds, so the refusal above was
	// contention rather than a broken primitive.
	dropped, err = st.DropPartition(ctx, "attempts", name, retention.DropByAge)
	if err != nil {
		t.Fatalf("DropPartition after release: %v", err)
	}
	if !dropped {
		t.Fatalf("DropPartition(%s) after the lock was released = false, want true", name)
	}
}

// TestDropPartitionOutcomeSurvivesCallerCancellation pins the contract that makes
// AC-42's books trustworthy: the outcome of a DROP this call has already sent is
// still reported to its caller when that caller's context is cancelled while the
// transaction is running.
//
// The reason is measured, not hypothetical. pgx answers a cancelled round trip with
// context.Canceled - it closes the connection - and that client-side abort does not
// stop the server, so a COMMIT the server already recorded stands while the caller
// holds an error. A two-replica AC-42 run caught exactly that: the sweeper's tick
// was cancelled just as the second DROP committed, and the test reported
//
//	attempts: dropped [attempts_20260917], but the partitions that left the catalogue are [attempts_20260917 attempts_20260918]
//
// with attempts_20260918 already gone from the unfiltered catalogue
// (pg_inherits joined to pg_class, no size filter). An instrumented run of the
// card's command on the tree this slice was cut from reproduced it 1 iteration in
// 20, printing, in the same millisecond the catalogue stopped listing the partition:
//
//	DropPartition(attempts, attempts_20260918) -> dropped=false, err=drop attempts partition attempts_20260918: context canceled
//
// The window is reproduced here deterministically instead of by timing: an ACCESS
// SHARE lock held in another session keeps the DROP inside the server, so the
// cancellation is guaranteed to land mid-transaction, and releasing that lock is
// what makes the outcome decidable.
func TestDropPartitionOutcomeSurvivesCallerCancellation(t *testing.T) {
	admin := testPool(t)
	st := newAppStore(t, admin)
	ctx := context.Background()

	past := time.Now().UTC().AddDate(0, 0, -40)
	name, err := st.EnsurePartition(ctx, "attempts", past)
	if err != nil {
		t.Fatalf("EnsurePartition: %v", err)
	}

	// Hold a lock the DROP's ACCESS EXCLUSIVE has to wait for, so the statement is
	// demonstrably with the server rather than still in the pool.
	holder, err := admin.Begin(ctx)
	if err != nil {
		t.Fatalf("begin holder: %v", err)
	}
	if _, err := holder.Exec(ctx, fmt.Sprintf("lock table %s in access share mode", name)); err != nil {
		_ = holder.Rollback(ctx)
		t.Fatalf("lock %s: %v", name, err)
	}

	dropCtx, cancelDrop := context.WithCancel(ctx)
	type outcome struct {
		dropped bool
		err     error
	}
	done := make(chan outcome, 1)
	go func() {
		dropped, err := st.DropPartition(dropCtx, "attempts", name, retention.DropByAge)
		done <- outcome{dropped: dropped, err: err}
	}()

	// Wait for the drop to reach the lock. The read is scoped to this database on
	// purpose: every pigeon worker shares one Postgres cluster, so an unscoped
	// count would be satisfied by another worker's suite.
	blocked := false
	deadline := time.Now().Add(4 * time.Second)
	for !blocked && time.Now().Before(deadline) {
		var waiting int
		if err := admin.QueryRow(ctx, `
			select count(*) from pg_stat_activity
			 where datname = current_database()
			   and wait_event_type = 'Lock'
			   and query like '%pigeon_drop_partition%'`).Scan(&waiting); err != nil {
			_ = holder.Rollback(ctx)
			t.Fatalf("pg_stat_activity: %v", err)
		}
		blocked = waiting > 0
		if !blocked {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if !blocked {
		_ = holder.Rollback(ctx)
		t.Fatal("the drop never waited on the holder's lock, so the cancellation below would not have landed mid-transaction")
	}

	cancelDrop()
	// Release the lock: the transaction was not cancelled, so it now finishes. Its
	// outcome is the only record of the drop, and AC-42's books are built from it.
	if err := holder.Rollback(ctx); err != nil {
		t.Fatalf("rollback holder: %v", err)
	}

	var got outcome
	select {
	case got = <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("DropPartition did not return within 20 s of the lock it waited on being released")
	}
	if got.err != nil {
		t.Fatalf("DropPartition reported %v: the caller's cancellation revoked the outcome of a DROP that was already with the server, so a committed drop can leave the catalogue with no record of it (AC-42)", got.err)
	}
	if !got.dropped {
		t.Fatalf("DropPartition(%s) = false, want true: the drop runs to completion once its lock is released", name)
	}

	// Report and catalogue agree, which is the property AC-42 asserts.
	var live bool
	if err := admin.QueryRow(ctx, `select to_regclass($1) is not null`, name).Scan(&live); err != nil {
		t.Fatalf("to_regclass(%s): %v", name, err)
	}
	if live {
		t.Fatalf("%s is still in the catalogue although its drop was reported as done", name)
	}

	// The detach is deliberately narrow: a context that is already dead when the
	// call starts never sends the DROP, so nothing is dropped behind the caller's
	// back. The partition is recreated to prove exactly that.
	if _, err := st.EnsurePartition(ctx, "attempts", past); err != nil {
		t.Fatalf("recreate %s: %v", name, err)
	}
	deadCtx, cancelDead := context.WithCancel(ctx)
	cancelDead()
	if dropped, err := st.DropPartition(deadCtx, "attempts", name, retention.DropByAge); err == nil {
		t.Fatalf("DropPartition(%s) with an already-cancelled context = (%v, nil), want an error before any DDL is sent", name, dropped)
	}
	if err := admin.QueryRow(ctx, `select to_regclass($1) is not null`, name).Scan(&live); err != nil {
		t.Fatalf("to_regclass(%s): %v", name, err)
	}
	if !live {
		t.Fatalf("%s was dropped by a call whose context was already cancelled", name)
	}
}
