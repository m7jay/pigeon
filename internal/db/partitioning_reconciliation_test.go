package db

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// TestMigrationReconcilesPartitioningWithS6AndS7 is S8's cross-slice proof.
//
// S8 turns deliveries and attempts into daily range partitions (SB-1) by swapping
// the tables, which recreates them from an explicit column, index and policy list.
// S6's 0010_attempt_buffer.sql and S7's 0011_s7_inspect_replay.sql landed on the
// base while this slice was open, and both attach objects to those two tables, so
// the swap has to carry them or the pair cannot coexist in one tree — which is
// what made a fresh migrate fail three ways before 0012 existed:
//
//   - SQLSTATE 42830: 0011's self-referencing foreign key on
//     deliveries.replay_of_delivery_id, because a partitioned table's referenced
//     unique key must contain the partition key.
//   - SQLSTATE 0A000: 0011's partial unique index, because a unique index on a
//     partitioned table must contain the partition key.
//   - SQLSTATE 42P10: fan-out's ON CONFLICT inference against the full unique
//     constraint, which is not the predicate A-4 records.
//
// This asserts the reconciled catalogue rather than the SQL text, after a fresh
// Migrate over every migration file in order — the path CI takes, and the only
// path that can catch the ordering problem at all. The two behavioural halves are
// covered where they belong and run against this same schema: a replay creating a
// second delivery for a pair (FR-1106) in internal/store and internal/httpapi's
// inspect tests, and a re-fan-out being a no-op (FR-704) in the relay and
// fan-out tests, where the ON CONFLICT inference is exercised for real.
func TestMigrationReconcilesPartitioningWithS6AndS7(t *testing.T) {
	dsn := os.Getenv("PIGEON_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PIGEON_TEST_DATABASE_URL not set; skipping the S6/S7 partitioning reconciliation proof")
	}
	ctx := context.Background()
	pool, err := Connect(ctx, dsn, 10)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer pool.Close()

	// A fresh migrate over 0001…0012 in ascending order. Idempotent, so this also
	// holds when another package in the same run migrated the shared database
	// first — but the ordering failure this test exists for only shows up when the
	// files apply in sequence, which is what this call does.
	if err := Migrate(ctx, pool, 30*time.Second); err != nil {
		t.Fatalf("Migrate over every migration file (0001…0012, ascending, as CI applies them): %v", err)
	}

	var recorded int
	if err := pool.QueryRow(ctx,
		`select count(*) from schema_migrations where version = '0012'`).Scan(&recorded); err != nil {
		t.Fatalf("read schema_migrations: %v", err)
	}
	if recorded != 1 {
		t.Errorf("schema_migrations has %d rows for 0012, want 1", recorded)
	}

	// SB-1: both tables are daily range partitions on their own day column.
	for _, tc := range []struct{ table, key string }{
		{"deliveries", "created_at"},
		{"attempts", "occurred_at"},
	} {
		var relkind, partkey string
		if err := pool.QueryRow(ctx, `
			select c.relkind::text, pg_get_partkeydef(c.oid)
			  from pg_class c
			  join pg_namespace n on n.oid = c.relnamespace
			 where n.nspname = 'public' and c.relname = $1`, tc.table).Scan(&relkind, &partkey); err != nil {
			t.Fatalf("read %s: %v", tc.table, err)
		}
		if relkind != "p" {
			t.Errorf("%s has relkind %q, want \"p\": a partitioned table (SB-1)", tc.table, relkind)
		}
		if want := "RANGE (" + tc.key + ")"; partkey != want {
			t.Errorf("%s is partitioned by %q, want %q", tc.table, partkey, want)
		}
	}

	// A-4's arbiter, as the swap had to leave it: partitioned, so the key contains
	// the partition key, with 0011's predicate — the invariant — intact.
	var indexdef string
	if err := pool.QueryRow(ctx, `
		select pg_get_indexdef(i.indexrelid)
		  from pg_index i
		  join pg_class c on c.oid = i.indrelid
		 where c.relname = 'deliveries'
		   and i.indexrelid = to_regclass('deliveries_first_fanout_key')`).Scan(&indexdef); err != nil {
		t.Fatalf("read deliveries_first_fanout_key (0011's first-fan-out arbiter, recreated by 0012): %v", err)
	}
	for _, want := range []string{
		"UNIQUE INDEX",
		"(event_id, endpoint_id, created_at)",
		"WHERE (replay_of_delivery_id IS NULL)",
	} {
		if !strings.Contains(indexdef, want) {
			t.Errorf("deliveries_first_fanout_key is %s; it must contain %q (A-4's predicate on the key the partition forces)", indexdef, want)
		}
	}

	// The two foreign keys the swap had to drop: neither can exist on a partitioned
	// table, and leaving the first one in place is what made 0011 fail with 42830.
	for _, tc := range []struct{ table, what, query string }{
		{"deliveries", "self-referencing replay FK", `
			select count(*) from pg_constraint
			 where conrelid = 'deliveries'::regclass and contype = 'f'
			   and confrelid = 'deliveries'::regclass`},
		{"attempts", "FK to deliveries", `
			select count(*) from pg_constraint
			 where conrelid = 'attempts'::regclass and contype = 'f'`},
	} {
		var n int
		if err := pool.QueryRow(ctx, tc.query).Scan(&n); err != nil {
			t.Fatalf("count %s %s: %v", tc.table, tc.what, err)
		}
		if n != 0 {
			t.Errorf("%s still has %d %s; a foreign key to a partitioned table must include the partition key on both sides, so the link is provenance (a plain bigint), not ownership", tc.table, n, tc.what)
		}
	}

	// The replay link itself survives the swap: FR-1106 needs the column even though
	// nothing may enforce it.
	var hasReplayColumn bool
	if err := pool.QueryRow(ctx, `
		select count(*) = 1 from information_schema.columns
		 where table_schema = 'public' and table_name = 'deliveries'
		   and column_name = 'replay_of_delivery_id'`).Scan(&hasReplayColumn); err != nil {
		t.Fatalf("read information_schema.columns: %v", err)
	}
	if !hasReplayColumn {
		t.Errorf("deliveries.replay_of_delivery_id is gone: 0011's FR-1106 link did not survive the swap")
	}

	// 0011's other objects, recreated on the replacements. FR-1109/FR-1111 read the
	// event index, FR-1102/FR-1103 the attempt policy, FR-903/FR-905 the view.
	exists := func(t *testing.T, what, query string, args ...any) bool {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", what, err)
		}
		if n == 0 {
			t.Errorf("%s is missing: the swap dropped it and did not recreate it", what)
			return false
		}
		return true
	}
	exists(t, "deliveries_event_idx (FR-1109/FR-1111)", `
		select count(*) from pg_class c join pg_namespace n on n.oid = c.relnamespace
		 where n.nspname = 'public' and c.relname = 'deliveries_event_idx' and c.relkind in ('i', 'I')`)
	exists(t, "attempts_tenant_read policy (FR-1102/FR-1103)", `
		select count(*) from pg_policy p
		 join pg_class c on c.oid = p.polrelid
		where c.relname = 'attempts' and p.polname = 'attempts_tenant_read'`)

	// The view must keep security_invoker, or it would answer past the caller's
	// policies and become a way around tenant isolation (MT-1).
	var viewOK bool
	if err := pool.QueryRow(ctx, `
		select count(*) = 1 from pg_class c
		 join pg_namespace n on n.oid = c.relnamespace
		 where n.nspname = 'public' and c.relname = 'delivery_dedupe_all'
		   and c.relkind = 'v'
		   and coalesce(c.reloptions::text, '') like '%security_invoker=true%'`).Scan(&viewOK); err != nil {
		t.Fatalf("read delivery_dedupe_all: %v", err)
	}
	if !viewOK {
		t.Errorf("delivery_dedupe_all is missing or no longer security_invoker: FR-903/FR-905 read it and MT-1 depends on the caller's policies still applying")
	}
}
