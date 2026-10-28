package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m7jay/pigeon/internal/db"
)

// seedLeafEvent gives one tenant the event type and the event a probe delivery
// needs, written through the admin pool (seeding is the platform's own privileged
// path, and RLS forbids a tenant role from inventing event types).
func seedLeafEvent(t *testing.T, admin *pgxpool.Pool, producerID, name, eventID string, at time.Time) {
	t.Helper()
	ctx := context.Background()
	if _, err := admin.Exec(ctx, `
		insert into event_types (id, producer_id, env, name, routing_mode, event_class)
		values ($1, $2, 'live', $3, 'consumer', 'normal')
		on conflict (id) do nothing`, "evttype_"+name, producerID, name); err != nil {
		t.Fatalf("seed event type %s: %v", name, err)
	}
	if _, err := admin.Exec(ctx, `
		insert into events (id, producer_id, env, event_type, payload_version, routing_mode,
		                    event_class, consumer_ref, occurred_at, payload)
		values ($1, $2, 'live', $3, 1, 'consumer', 'normal', 'cref_leafiso', $4, '{}'::jsonb)`,
		eventID, producerID, name, at); err != nil {
		t.Fatalf("seed event %s: %v", eventID, err)
	}
}

// leafFixtureEvent is one event this fixture seeds, and the tenant that owns it.
type leafFixtureEvent struct {
	eventID  string
	tenant   string // "a" or "b"
	typeName string
}

// leafFixtureEvents is the fixture's whole event set, and the only source of the
// events it seeds: the two probe deliveries, plus the three the write probes use
// (a cross-tenant insert must fail on the policy, not on the unique arbiter).
// resetLeafFixture deletes exactly these and their trails, so the seed list and the
// reset cannot drift apart.
var leafFixtureEvents = []leafFixtureEvent{
	{"evt_leafiso_a", "a", "leafiso.a"},
	{"evt_leafiso_b", "b", "leafiso.b"},
	{"evt_leafiso_a_w1", "a", "leafiso.a"},
	{"evt_leafiso_a_w2", "a", "leafiso.a"},
	{"evt_leafiso_b_w", "b", "leafiso.b"},
}

// resetLeafFixture makes the fixture own the state it asserts on. The row counts
// below are exact ("one per tenant"), and this database is reused across local runs,
// so rows a previous run left in the day would either fail that guard or, worse,
// leave the isolation assertions vacuous.
//
// The attempt trail is cleared by the whole UTC day rather than by this fixture's
// delivery ids, and that is deliberate: 0012 dropped attempts' foreign key to
// deliveries, so another test's `truncate table ... deliveries cascade` removes the
// delivery and leaves its trail behind: a predicate over the surviving deliveries
// never sees those orphaned rows, which is how a second run accumulated four rows in
// this leaf. The fixture already claims the day exclusively (it asserts the leaf
// holds exactly its own two rows), so clearing the day is clearing its own state.
func resetLeafFixture(t *testing.T, admin *pgxpool.Pool, day time.Time, events []leafFixtureEvent) {
	t.Helper()
	ctx := context.Background()
	dayStart := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, time.UTC)
	dayEnd := dayStart.AddDate(0, 0, 1)

	ids := make([]string, 0, len(events))
	for _, ev := range events {
		ids = append(ids, ev.eventID)
	}

	// Order matters: the trail's predicate reads the deliveries, and deleting the
	// events cascades into them (deliveries_event_id_fkey).
	for _, stmt := range []struct {
		what string
		sql  string
		args []any
	}{
		{"attempt trail", `delete from attempts where occurred_at >= $1 and occurred_at < $2`,
			[]any{dayStart, dayEnd}},
		{"deliveries", `delete from deliveries where event_id = any($1)`, []any{ids}},
		{"events", `delete from events where id = any($1)`, []any{ids}},
	} {
		if _, err := admin.Exec(ctx, stmt.sql, stmt.args...); err != nil {
			t.Fatalf("reset %s for the leaf fixture: %v", stmt.what, err)
		}
	}
}

// TestPartitionLeavesEnforceTenantIsolation is the MT-1 half of SB-1.
//
// 0012 replaces one protected relation with a parent plus daily children, and
// PostgreSQL does not propagate row level security to a partition: the flag and the
// policies belong to each relation (measured on 16.2, `deliveries.relrowsecurity`
// true while `deliveries_YYYYMMDD` was false). Enabling RLS on the parent alone
// would therefore have handed the runtime role every tenant's rows to any query that
// names the day — a repository bug, a fixture, psql — which the table being replaced
// refused. So this test names a leaf directly, on both partitioned tables, and
// asserts the runtime role reads and writes only its own tenant; it also sweeps the
// catalogue, because one unisolated day is a day of every tenant's rows.
func TestPartitionLeavesEnforceTenantIsolation(t *testing.T) {
	admin := testPool(t)
	app := appPool(t, admin)
	st := New(app)
	ctx := context.Background()

	aID, _ := seedProducer(t, admin, "leafiso-a")
	bID, _ := seedProducer(t, admin, "leafiso-b")

	// A day the migration's cutover window does not cover, so both leaves here are
	// created by pigeon_ensure_partition after 0012 ran: this is the "leaves created
	// later" half of the requirement, and it is the half fan-out and the sweeper
	// exercise every day in production.
	day := time.Now().UTC().AddDate(0, 0, -20)
	at := time.Date(day.Year(), day.Month(), day.Day(), 12, 0, 0, 0, time.UTC)

	deliveryLeaf, err := st.EnsurePartition(ctx, "deliveries", day)
	if err != nil {
		t.Fatalf("EnsurePartition(deliveries, %s): %v", day.Format("2006-01-02"), err)
	}
	attemptLeaf, err := st.EnsurePartition(ctx, "attempts", day)
	if err != nil {
		t.Fatalf("EnsurePartition(attempts, %s): %v", day.Format("2006-01-02"), err)
	}

	// The leaves exist; clear whatever a previous run left in them before seeding, so
	// the exact counts below are this run's rows.
	resetLeafFixture(t, admin, day, leafFixtureEvents)

	// The runtime role's privileges on those two leaves. In a deployment they come
	// from the default privileges in deploy/postgres/01-roles.sql, which cover
	// partitions created after the grant; a test database has no such default
	// privileges, so the fixture hands them over explicitly. The property under test
	// here is the policy, not the grant - but without the grant the reads below would
	// fail with 42501 for the wrong reason and prove nothing.
	for _, leaf := range []string{deliveryLeaf, attemptLeaf} {
		if _, err := admin.Exec(ctx, `grant select, insert, update, delete on `+leaf+` to `+appRole); err != nil {
			t.Fatalf("grant on %s: %v", leaf, err)
		}
	}

	// Two tenants' rows in ONE day's partition: a delivery each, plus the attempt
	// trail the read policy is about. The last three events are reserved for the
	// write probes, so a cross-tenant insert cannot fail on the unique arbiter
	// instead of on the policy.
	for _, ev := range leafFixtureEvents {
		owner := aID
		if ev.tenant == "b" {
			owner = bID
		}
		seedLeafEvent(t, admin, owner, ev.typeName, ev.eventID, at)
	}

	for _, d := range []struct{ eventID, producerID, consumerID string }{
		{"evt_leafiso_a", aID, "cns_leafiso_a"},
		{"evt_leafiso_b", bID, "cns_leafiso_b"},
	} {
		if _, err := admin.Exec(ctx, `
			insert into deliveries (event_id, endpoint_id, producer_id, env, consumer_id, status, created_at)
			values ($1, 'ept_leafiso', $2, 'live', $3, 'delivered', $4)`,
			d.eventID, d.producerID, d.consumerID, at); err != nil {
			t.Fatalf("seed delivery for %s: %v", d.producerID, err)
		}
	}
	if _, err := admin.Exec(ctx, `
		insert into attempts (delivery_id, attempt, outcome, error_class, duration_ms, occurred_at)
		select d.id, 1, 'retryable', 'timeout', 5, $3 from deliveries d
		 where d.event_id in ($1, $2)`,
		"evt_leafiso_a", "evt_leafiso_b", at); err != nil {
		t.Fatalf("seed attempts: %v", err)
	}

	// The fixture has to be non-vacuous: the leaf really holds both tenants' rows,
	// which is what the runtime role must not be able to see whole.
	for _, tc := range []struct {
		relation string
		want     int
	}{
		{deliveryLeaf, 2},
		{attemptLeaf, 2},
	} {
		var all int
		if err := admin.QueryRow(ctx, `select count(*) from `+tc.relation).Scan(&all); err != nil {
			t.Fatalf("admin count of %s: %v", tc.relation, err)
		}
		if all != tc.want {
			t.Fatalf("fixture: %s holds %d rows, want %d (one per tenant); the isolation assertions below would be vacuous", tc.relation, all, tc.want)
		}
	}

	// Read as the runtime role, scoped to producer A: through the parent, and by
	// naming the partition. Both have to answer with A's row alone.
	scopedCount := func(relation string) int {
		t.Helper()
		var n int
		if err := db.ControlTx(ctx, app, func(tx pgx.Tx) error {
			if err := db.SetProducerScope(ctx, tx, aID); err != nil {
				return err
			}
			if err := db.SetEnvScope(ctx, tx, "live"); err != nil {
				return err
			}
			return tx.QueryRow(ctx, `select count(*) from `+relation).Scan(&n)
		}); err != nil {
			t.Fatalf("runtime-role read of %s: %v", relation, err)
		}
		return n
	}

	for _, tc := range []struct {
		relation string
		what     string
	}{
		{"deliveries", "the parent"},
		{deliveryLeaf, "a leaf named directly"},
		{"attempts", "the parent"},
		{attemptLeaf, "a leaf named directly"},
	} {
		if got := scopedCount(tc.relation); got != 1 {
			t.Errorf("the runtime role sees %d rows in %s (%s), want 1 (its own tenant's); PostgreSQL does not propagate RLS to partitions, so every leaf needs its parent's flag and policy", got, tc.relation, tc.what)
		}
	}

	// The write side, with the same scope. A cross-tenant row must be refused by the
	// policy (42501), not by a missing privilege and not by the unique arbiter; the
	// tenant's own row must still be accepted, through the parent and through the
	// leaf, or the policy would be blocking rather than filtering.
	scopedInsert := func(relation, eventID, producerID, consumerID string) error {
		return db.ControlTx(ctx, app, func(tx pgx.Tx) error {
			if err := db.SetProducerScope(ctx, tx, aID); err != nil {
				return err
			}
			if err := db.SetEnvScope(ctx, tx, "live"); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `
				insert into `+relation+` (event_id, endpoint_id, producer_id, env, consumer_id, status, created_at)
				values ($1, 'ept_leafiso', $2, 'live', $3, 'pending', $4)`,
				eventID, producerID, consumerID, at)
			return err
		})
	}

	for _, relation := range []string{"deliveries", deliveryLeaf} {
		err := scopedInsert(relation, "evt_leafiso_b_w", bID, "cns_leafiso_b")
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
			t.Errorf("inserting another tenant's row into %s returned %v; want SQLSTATE 42501 from the row level security policy", relation, err)
		}
	}

	for _, own := range []struct{ relation, eventID string }{
		{"deliveries", "evt_leafiso_a_w1"},
		{deliveryLeaf, "evt_leafiso_a_w2"},
	} {
		if err := scopedInsert(own.relation, own.eventID, aID, "cns_leafiso_a"); err != nil {
			t.Errorf("inserting its own row into %s failed: %v", own.relation, err)
		}
	}
	if got := scopedCount(deliveryLeaf); got != 3 {
		t.Errorf("after two own-tenant inserts the runtime role sees %d rows in %s, want 3 (the seeded row plus both writes)", got, deliveryLeaf)
	}

	// attempts carries a read-only policy (0011), so an insert is refused there too
	// — on the parent and, since it is the relation being named, on the leaf.
	var bDelivery int64
	if err := admin.QueryRow(ctx,
		`select id from deliveries where event_id = 'evt_leafiso_b'`).Scan(&bDelivery); err != nil {
		t.Fatalf("read tenant B's delivery id: %v", err)
	}
	for _, relation := range []string{"attempts", attemptLeaf} {
		err := db.ControlTx(ctx, app, func(tx pgx.Tx) error {
			if err := db.SetProducerScope(ctx, tx, aID); err != nil {
				return err
			}
			if err := db.SetEnvScope(ctx, tx, "live"); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `
				insert into `+relation+` (delivery_id, attempt, outcome, error_class, duration_ms, occurred_at)
				values ($1, 99, 'retryable', 'timeout', 1, $2)`, bDelivery, at)
			return err
		})
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
			t.Errorf("writing an attempt into %s returned %v; want SQLSTATE 42501 (the parent has no write policy and neither may its leaves)", relation, err)
		}
	}

	// It is not only the leaves this test created. Every leaf of both partitioned
	// tables — the days the migration's cutover loop created and every day created
	// since, by fan-out, the sweeper or a fixture — carries the parent's flag and
	// the parent's policy. One leaf without them is a day of every tenant's rows.
	wantPolicy := map[string]string{
		"deliveries": "deliveries_tenant",
		"attempts":   "attempts_tenant_read",
	}
	rows, err := admin.Query(ctx, `
		select parent.relname, leaf.relname, leaf.relrowsecurity,
		       coalesce((select string_agg(p.polname, ',' order by p.polname)
		                   from pg_policy p where p.polrelid = leaf.oid), '')
		  from pg_class leaf
		  join pg_inherits i on i.inhrelid = leaf.oid
		  join pg_class parent on parent.oid = i.inhparent
		 where parent.relname in ('deliveries', 'attempts')
		 order by 1, 2`)
	if err != nil {
		t.Fatalf("read the partition catalogue: %v", err)
	}
	defer rows.Close()

	leaves := 0
	seen := map[string]int{}
	for rows.Next() {
		var parent, leaf string
		var rls bool
		var policies string
		if err := rows.Scan(&parent, &leaf, &rls, &policies); err != nil {
			t.Fatalf("scan the partition catalogue: %v", err)
		}
		leaves++
		seen[parent]++
		if !rls {
			t.Errorf("leaf %s has row level security disabled; a partition the runtime role can name is readable across tenants (MT-1)", leaf)
		}
		if policies != wantPolicy[parent] {
			t.Errorf("leaf %s carries policies %q, want %q (its parent's)", leaf, policies, wantPolicy[parent])
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("partition catalogue rows: %v", err)
	}
	if leaves < 2 || seen["deliveries"] == 0 || seen["attempts"] == 0 {
		t.Fatalf("the catalogue sweep saw %d leaves (%v); it must cover both partitioned tables or it proves nothing", leaves, seen)
	}
}
