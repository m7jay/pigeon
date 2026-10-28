package db

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// TestDedupeArchiveMigrationCatalog is FR-905/AC-46's catalogue proof: after a
// fresh Migrate over every migration in order, the archive 0013 creates is
// partitioned by day (SB-1), carries the tenant policy on itself AND on the leaves
// the primitive makes later (MT-1), has A-4's primary key and no surrogate id, and
// is reachable through the one UNION view the support path reads (FR-903).
//
// It asserts the catalogue rather than the SQL text, because the catalogue is what
// the running system answers from - and it is the same fresh-Migrate path CI takes.
func TestDedupeArchiveMigrationCatalog(t *testing.T) {
	dsn := os.Getenv("PIGEON_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PIGEON_TEST_DATABASE_URL not set; skipping the FR-905 archive catalogue proof")
	}
	ctx := context.Background()
	pool, err := Connect(ctx, dsn, 10)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer pool.Close()

	if err := Migrate(ctx, pool, 30*time.Second); err != nil {
		t.Fatalf("Migrate over every migration file (0001…0013, ascending, as CI applies them): %v", err)
	}

	// The appended migration is recorded exactly once: 0012 is S8's and may not be
	// edited (db.Migrate refuses a changed checksum), so FR-905 has to arrive as its
	// own file.
	var recorded int
	if err := pool.QueryRow(ctx,
		`select count(*) from schema_migrations where version = '0013'`).Scan(&recorded); err != nil {
		t.Fatalf("read schema_migrations: %v", err)
	}
	if recorded != 1 {
		t.Fatalf("schema_migrations has %d rows for 0013, want 1", recorded)
	}

	// SB-1: daily range partitions on the day column the copy stamps.
	var relkind, partkey string
	if err := pool.QueryRow(ctx, `
		select c.relkind::text, pg_get_partkeydef(c.oid)
		  from pg_class c join pg_namespace n on n.oid = c.relnamespace
		 where n.nspname = 'public' and c.relname = 'delivery_dedupe'`).Scan(&relkind, &partkey); err != nil {
		t.Fatalf("read delivery_dedupe: %v", err)
	}
	if relkind != "p" || partkey != "RANGE (first_seen_at)" {
		t.Fatalf("delivery_dedupe is relkind=%q partkey=%q, want a range partitioned table on first_seen_at (FR-905)",
			relkind, partkey)
	}

	// A-4's shape: four columns, the pair carrying the identity, no surrogate id.
	// The primary key is the pair PLUS the partition key, and that is PostgreSQL's
	// rule rather than a choice: a unique key on a partitioned table must contain
	// the partition key (S8's swap hit exactly this on `deliveries`, which is why
	// its own key is (event_id, endpoint_id, created_at) and not the pair alone).
	// The pair is still the identity within a day, the copy's ON CONFLICT DO
	// NOTHING is what makes a repeat copy a no-op, and delivery_dedupe_all's
	// min()+GROUP BY is what folds a pair that appears in two days back into one
	// answer - asserted end to end in internal/store.
	for _, want := range []struct{ column, dataType string }{
		{"endpoint_id", "text"},
		{"event_id", "text"},
		{"first_delivery_id", "bigint"},
		{"first_seen_at", "timestamp with time zone"},
	} {
		var got string
		if err := pool.QueryRow(ctx, `
			select data_type from information_schema.columns
			 where table_schema = 'public' and table_name = 'delivery_dedupe' and column_name = $1`,
			want.column).Scan(&got); err != nil {
			t.Fatalf("read delivery_dedupe.%s: %v", want.column, err)
		}
		if got != want.dataType {
			t.Errorf("delivery_dedupe.%s is %s, want %s (A-4)", want.column, got, want.dataType)
		}
	}
	var pk, hasID string
	if err := pool.QueryRow(ctx, `
		select pg_get_constraintdef(c.oid) from pg_constraint c
		 where c.conrelid = 'delivery_dedupe'::regclass and c.contype = 'p'`).Scan(&pk); err != nil {
		t.Fatalf("read the archive's primary key: %v", err)
	}
	if !strings.Contains(pk, "PRIMARY KEY (endpoint_id, event_id") || !strings.Contains(pk, "first_seen_at") {
		t.Errorf("archive primary key = %s, want the pair plus the partition key (endpoint_id, event_id, first_seen_at)", pk)
	}
	if err := pool.QueryRow(ctx, `
		select count(*)::text from information_schema.columns
		 where table_schema = 'public' and table_name = 'delivery_dedupe' and column_name = 'id'`).Scan(&hasID); err != nil {
		t.Fatalf("look for a surrogate id: %v", err)
	}
	if hasID != "0" {
		t.Error("delivery_dedupe has a surrogate id column; A-4 records the pair as the identity")
	}

	// MT-1 on the parent, and (below) on what the partition primitive makes.
	var rls bool
	var policies int
	if err := pool.QueryRow(ctx, `
		select c.relrowsecurity, (select count(*) from pg_policy p where p.polrelid = c.oid)
		  from pg_class c where c.oid = 'delivery_dedupe'::regclass`).Scan(&rls, &policies); err != nil {
		t.Fatalf("read the archive's RLS state: %v", err)
	}
	if !rls || policies == 0 {
		t.Fatalf("delivery_dedupe relrowsecurity=%v policies=%d, want the flag and a tenant policy: the archive holds tenant data", rls, policies)
	}

	// 0012's primitives must exist ONCE each: a leftover 2-argument
	// pigeon_drop_partition would make the 2-argument call ambiguous, and the
	// reason argument is what decides whether the copy may be skipped (FR-905).
	var n int
	var args string
	if err := pool.QueryRow(ctx, `
		select count(*) from pg_proc p join pg_namespace n on n.oid = p.pronamespace
		 where n.nspname = 'public' and p.proname = 'pigeon_drop_partition'`).Scan(&n); err != nil {
		t.Fatalf("count pigeon_drop_partition: %v", err)
	}
	if n != 1 {
		t.Fatalf("pigeon_drop_partition has %d overloads, want exactly 1 (the reason-bearing one)", n)
	}
	if err := pool.QueryRow(ctx, `
		select pg_get_function_arguments(p.oid) from pg_proc p join pg_namespace n on n.oid = p.pronamespace
		 where n.nspname = 'public' and p.proname = 'pigeon_drop_partition'`).Scan(&args); err != nil {
		t.Fatalf("read pigeon_drop_partition's signature: %v", err)
	}
	if !strings.Contains(args, "p_reason text DEFAULT 'age'") {
		t.Fatalf("pigeon_drop_partition(%s), want a defaulted p_reason so an ordinary drop is the default (FR-905)", args)
	}

	// The partition primitive must repair the archive's leaves too: S8's guard is
	// what enforces RLS+policy on a leaf, and it has to know the new table, or the
	// copy lands in an unisolated partition.
	day := time.Now().UTC().AddDate(0, 0, -40)
	var leaf string
	if err := pool.QueryRow(ctx,
		`select pigeon_ensure_partition('delivery_dedupe', ($1)::date)`, day.Format("2006-01-02")).Scan(&leaf); err != nil {
		t.Fatalf("pigeon_ensure_partition('delivery_dedupe'): %v", err)
	}
	var leafRLS bool
	var leafPolicies int
	if err := pool.QueryRow(ctx, `
		select c.relrowsecurity, (select count(*) from pg_policy p where p.polrelid = c.oid)
		  from pg_class c where c.oid = to_regclass($1)`, leaf).Scan(&leafRLS, &leafPolicies); err != nil {
		t.Fatalf("read %s: %v", leaf, err)
	}
	if !leafRLS || leafPolicies == 0 {
		t.Fatalf("%s: relrowsecurity=%v policies=%d, want the flag and the policy the parent has (MT-1)",
			leaf, leafRLS, leafPolicies)
	}

	// FR-903/FR-905's one view: a UNION, not a UNION ALL, so a pair answerable from
	// both the live side and the archive still counts once.
	var viewdef string
	var invoker bool
	if err := pool.QueryRow(ctx, `
		select pg_get_viewdef('delivery_dedupe_all'::regclass, true),
		       coalesce(c.reloptions::text, '') like '%security_invoker=true%'
		  from pg_class c where c.oid = 'delivery_dedupe_all'::regclass`).Scan(&viewdef, &invoker); err != nil {
		t.Fatalf("read delivery_dedupe_all: %v", err)
	}
	if !invoker {
		t.Error("delivery_dedupe_all is not security_invoker: it would answer past the caller's policies (MT-1)")
	}
	lower := strings.ToLower(viewdef)
	if !strings.Contains(lower, "union") || strings.Contains(lower, "union all") {
		t.Errorf("delivery_dedupe_all = %s; want UNION (a key answerable from both sides must count once)", viewdef)
	}
	for _, want := range []string{"deliveries", "delivery_dedupe"} {
		if !strings.Contains(lower, want) {
			t.Errorf("delivery_dedupe_all does not read %s (FR-905: live while the partition exists, archive after)", want)
		}
	}
}
