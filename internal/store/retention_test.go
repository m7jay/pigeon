package store

import (
	"context"
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
