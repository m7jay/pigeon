package store

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m7jay/pigeon/internal/db"
	"github.com/m7jay/pigeon/internal/retention"
)

// FR-905/AC-46 at the SQL layer: the dedupe archive `delivery_dedupe`, its copy
// out of a `deliveries` partition in the same transaction as the drop, the two
// SB-3 pressure paths, the drop order, and the tenant isolation of an archive
// leaf. The policy half (which partition goes first, under which reason) is
// unit-tested in internal/retention; everything here needs a real Postgres
// because it is the SQL primitive's behaviour that is under test.

// dedupeDay is one day's worth of dedupe fixture: a producer, a consumer, a
// verified endpoint, an event and a delivery for that endpoint, written into a
// PAST day's `deliveries` partition so the drop primitive will accept it. The
// second delivery of the pair is a replay (FR-1106), which is what proves the
// archive records the pair's FIRST delivery rather than whichever row came last.
type dedupeDay struct {
	producerID string
	scope      Scope
	consumerID string
	endpointID string
	eventID    string
	firstID    int64
	replayID   int64
	seenAt     time.Time
	day        time.Time
	leaf       string
}

// seedDedupeDay builds that fixture for one UTC day. admin writes the event and
// the deliveries (the platform's own privileged path: fan-out is not what this
// test is about, and it stamps created_at from now()); st is the runtime-role
// store, so the endpoint and consumer are created through the real API paths.
func seedDedupeDay(t *testing.T, admin *pgxpool.Pool, st *Store, slug string, day time.Time) dedupeDay {
	t.Helper()
	ctx := context.Background()
	day = day.UTC().Truncate(24 * time.Hour)

	pid, _ := seedProducer(t, admin, slug)
	sc := Scope{ProducerID: pid, Env: "live"}
	etName := strings.ReplaceAll(slug, "-", "") + ".order"
	con := mustConsumer(t, st, sc, "cref-"+slug, "Dedupe "+slug)
	ep := mustVerifiedEndpoint(t, st, sc, con.ID, "e-"+slug, "https://8.8.4.4/hook")

	leaf, err := st.EnsurePartition(ctx, "deliveries", day)
	if err != nil {
		t.Fatalf("EnsurePartition(deliveries, %s): %v", day.Format("2006-01-02"), err)
	}

	d := dedupeDay{
		producerID: pid, scope: sc, consumerID: con.ID, endpointID: ep.ID,
		eventID: "evt_" + slug,
		seenAt:  day.Add(12 * time.Hour),
		day:     day,
		leaf:    leaf,
	}
	seedLeafEvent(t, admin, pid, etName, d.eventID, d.seenAt)

	if err := admin.QueryRow(ctx, `
		insert into deliveries (event_id, endpoint_id, producer_id, env, consumer_id, status, created_at)
		values ($1, $2, $3, 'live', $4, 'delivered', $5)
		returning id`,
		d.eventID, ep.ID, pid, con.ID, d.seenAt).Scan(&d.firstID); err != nil {
		t.Fatalf("seed first delivery: %v", err)
	}
	if err := admin.QueryRow(ctx, `
		insert into deliveries (event_id, endpoint_id, producer_id, env, consumer_id, status, created_at, replay_of_delivery_id)
		values ($1, $2, $3, 'live', $4, 'delivered', $5, $6)
		returning id`,
		d.eventID, ep.ID, pid, con.ID, d.seenAt, d.firstID).Scan(&d.replayID); err != nil {
		t.Fatalf("seed replay delivery: %v", err)
	}
	return d
}

// archiveRows is the admin-side read of the archive for one pair: the id and the
// timestamp the support answer is made of. Admin, because this asserts what the
// archive HOLDS - a tenant-scoped read is asserted separately.
func archiveRows(t *testing.T, admin *pgxpool.Pool, endpointID, eventID string) (int64, time.Time, int) {
	t.Helper()
	var firstID int64
	var seen time.Time
	var n int
	err := admin.QueryRow(context.Background(), `
		select coalesce(min(first_delivery_id), 0), count(*)
		  from delivery_dedupe where endpoint_id = $1 and event_id = $2`,
		endpointID, eventID).Scan(&firstID, &n)
	if err != nil {
		t.Fatalf("read the archive for %s/%s: %v", endpointID, eventID, err)
	}
	if n > 0 {
		if err := admin.QueryRow(context.Background(), `
			select min(first_seen_at) from delivery_dedupe
			 where endpoint_id = $1 and event_id = $2`, endpointID, eventID).Scan(&seen); err != nil {
			t.Fatalf("read the archive timestamp: %v", err)
		}
	}
	return firstID, seen, n
}

// resetArchiveDay clears the archive day a fixture is about to seed into, so the
// exact counts it asserts belong to this run.
//
// `delivery_dedupe` is not part of the routing schema `resetRoutingTables` truncates
// and only a partition drop removes its rows, so a second run against the same
// database added its two pairs to the day partition the previous run had already
// filled: 2 -> 4 -> ... and the `want 2` non-vacuity guard failed before any of the
// isolation assertions below it ran. The caller asserts the day holds exactly its
// own rows, so clearing the day is clearing its own state. `first_seen_at` is the
// archive's partition key (0014), so the window is the day partition itself; the
// bounds are rebuilt from midnight UTC rather than from the incoming time, which
// carries the current clock time and would miss rows earlier in the day.
func resetArchiveDay(t *testing.T, admin *pgxpool.Pool, day time.Time) {
	t.Helper()
	dayStart := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, time.UTC)
	if _, err := admin.Exec(context.Background(),
		`delete from delivery_dedupe where first_seen_at >= $1 and first_seen_at < $2`,
		dayStart, dayStart.AddDate(0, 0, 1)); err != nil {
		t.Fatalf("reset the archive day %s: %v", dayStart.Format("2006-01-02"), err)
	}
}

// TestOrdinaryDropCopiesTheDedupeKeysInOneTransaction is FR-905's centre: an
// ordinary drop copies the partition's dedupe keys into `delivery_dedupe` and
// drops the partition, so at every instant either the `deliveries` row or the
// archive row answers a key - and the answer the support path reads
// (`delivery_dedupe_all`) keeps working with the live side gone.
func TestOrdinaryDropCopiesTheDedupeKeysInOneTransaction(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	st := New(appPool(t, admin))
	ctx := context.Background()

	now := time.Now().UTC()
	dropped := seedDedupeDay(t, admin, st, "dedupecopy", now.AddDate(0, 0, -20))
	kept := seedDedupeDay(t, admin, st, "dedupecopyb", now.AddDate(0, 0, -19))

	// The fixture is real before anything is dropped: the live rows answer the
	// question, counting the replay (FR-1106) as a second delivery of the pair.
	before, err := st.DedupeAnswer(ctx, dropped.scope, dropped.endpointID, dropped.eventID)
	if err != nil {
		t.Fatalf("DedupeAnswer before the drop: %v", err)
	}
	if !before.Duplicate || before.FirstDeliveryID != dropped.firstID || before.Deliveries != 2 {
		t.Fatalf("live answer = %+v, want duplicate with first delivery %d and 2 deliveries",
			before, dropped.firstID)
	}

	if got, err := st.DropPartition(ctx, "deliveries", dropped.leaf, retention.DropByAge); err != nil || !got {
		t.Fatalf("DropPartition(%s, age) = (%v, %v), want (true, nil)", dropped.leaf, got, err)
	}

	// The day is gone from `deliveries`...
	var leafGone bool
	if err := admin.QueryRow(ctx, `select to_regclass($1) is null`, dropped.leaf).Scan(&leafGone); err != nil {
		t.Fatalf("read the dropped leaf: %v", err)
	}
	if !leafGone {
		t.Fatalf("%s still exists after its drop", dropped.leaf)
	}
	var live int
	if err := admin.QueryRow(ctx, `select count(*) from deliveries`).Scan(&live); err != nil {
		t.Fatalf("count deliveries: %v", err)
	}

	// ...and the archive holds the pair with its FIRST delivery and its ORIGINAL
	// timestamp, not the replay's.
	firstID, seenAt, n := archiveRows(t, admin, dropped.endpointID, dropped.eventID)
	if n != 1 {
		t.Fatalf("archive rows for the dropped pair = %d, want 1 (the pair counts once)", n)
	}
	if firstID != dropped.firstID {
		t.Fatalf("archived first_delivery_id = %d, want the first delivery %d (not the replay %d)",
			firstID, dropped.firstID, dropped.replayID)
	}
	if !seenAt.UTC().Equal(dropped.seenAt) {
		t.Fatalf("archived first_seen_at = %s, want the original %s",
			seenAt.UTC().Format(time.RFC3339), dropped.seenAt.Format(time.RFC3339))
	}

	// The archive's partition is the day's partition, which is what lets
	// retention drop the archive by day (SB-2, FR-905).
	if _, _, n := archiveRowsInLeaf(t, admin, "delivery_dedupe_"+dropped.day.Format("20060102"),
		dropped.endpointID, dropped.eventID); n != 1 {
		t.Fatalf("the copy did not land in the day's archive partition (%s)", "delivery_dedupe_"+dropped.day.Format("20060102"))
	}

	// The other day is untouched: the copy is per-partition, and so is the drop.
	if _, _, n := archiveRows(t, admin, kept.endpointID, kept.eventID); n != 0 {
		t.Fatalf("the archive holds %d rows for a pair whose partition was never dropped", n)
	}
	var keptLive int
	if err := admin.QueryRow(ctx, `select count(*) from deliveries where event_id = $1`, kept.eventID).Scan(&keptLive); err != nil {
		t.Fatalf("count the kept day's deliveries: %v", err)
	}
	if keptLive != 2 {
		t.Fatalf("the kept day has %d deliveries, want 2: dropping one partition must not touch another", keptLive)
	}

	// AC-46: the question is still answerable - duplicate, with the first delivery
	// and the original timestamp - and the answer can only have come from the
	// archive, because `deliveries` reported 0 rows for the pair.
	after, err := st.DedupeAnswer(ctx, dropped.scope, dropped.endpointID, dropped.eventID)
	if err != nil {
		t.Fatalf("DedupeAnswer after the drop: %v", err)
	}
	if !after.Duplicate || after.FirstDeliveryID != dropped.firstID {
		t.Fatalf("archived answer = %+v, want duplicate %v with first delivery %d",
			after, true, dropped.firstID)
	}
	if after.FirstSeenAt == nil || !after.FirstSeenAt.UTC().Equal(dropped.seenAt) {
		t.Fatalf("archived answer first_seen_at = %v, want the original %s", after.FirstSeenAt, dropped.seenAt)
	}
	if after.Deliveries != 0 {
		t.Fatalf("archived answer reports %d live deliveries, want 0: the partition was dropped", after.Deliveries)
	}
}

// archiveRowsInLeaf is the same read, naming one archive partition directly, so a
// test can show WHICH day partition the copy went into.
func archiveRowsInLeaf(t *testing.T, admin *pgxpool.Pool, leaf, endpointID, eventID string) (int64, time.Time, int) {
	t.Helper()
	var firstID *int64
	var seen *time.Time
	var n int
	if err := admin.QueryRow(context.Background(),
		`select min(first_delivery_id), min(first_seen_at), count(*) from `+leaf+
			` where endpoint_id = $1 and event_id = $2`, endpointID, eventID).Scan(&firstID, &seen, &n); err != nil {
		t.Fatalf("read %s: %v", leaf, err)
	}
	if firstID == nil || seen == nil {
		return 0, time.Time{}, n
	}
	return *firstID, *seen, n
}

// TestPairArchivedInTwoDaysCountsOnceWithTheOriginalTimestamp covers the case the
// archive's key makes possible: a replay (FR-1106) lands on the NEXT day, so one
// pair ends up in two day partitions and - after both are dropped - in two archive
// rows. The support answer must still be one answer, the pair's first, because
// that is what delivery_dedupe_all's min()+GROUP BY is for (A-4: "a key answerable
// from both sides still counts once").
func TestPairArchivedInTwoDaysCountsOnceWithTheOriginalTimestamp(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	st := New(appPool(t, admin))
	ctx := context.Background()

	d := seedDedupeDay(t, admin, st, "dedupecrossday", time.Now().UTC().AddDate(0, 0, -40))

	// The same pair again the next day, as a replay of the first delivery.
	nextDay := d.day.AddDate(0, 0, 1)
	nextLeaf, err := st.EnsurePartition(ctx, "deliveries", nextDay)
	if err != nil {
		t.Fatalf("EnsurePartition(%s): %v", nextDay.Format("2006-01-02"), err)
	}
	atNext := nextDay.Add(3 * time.Hour)
	var replayID int64
	if err := admin.QueryRow(ctx, `
		insert into deliveries (event_id, endpoint_id, producer_id, env, consumer_id, status, created_at, replay_of_delivery_id)
		values ($1, $2, $3, 'live', $4, 'delivered', $5, $6) returning id`,
		d.eventID, d.endpointID, d.producerID, d.consumerID, atNext, d.firstID).Scan(&replayID); err != nil {
		t.Fatalf("seed the next day's replay: %v", err)
	}

	for _, leaf := range []string{d.leaf, nextLeaf} {
		if got, err := st.DropPartition(ctx, "deliveries", leaf, retention.DropByAge); err != nil || !got {
			t.Fatalf("DropPartition(%s) = (%v, %v), want (true, nil)", leaf, got, err)
		}
	}

	// Two days were copied, so the archive holds the pair twice - once per day.
	var rows int
	if err := admin.QueryRow(ctx, `
		select count(*) from delivery_dedupe where endpoint_id = $1 and event_id = $2`,
		d.endpointID, d.eventID).Scan(&rows); err != nil {
		t.Fatalf("count the archive: %v", err)
	}
	if rows != 2 {
		t.Fatalf("the archive holds %d rows for a pair that lived in two day partitions, want 2", rows)
	}

	// The view still answers once, with the ORIGINAL delivery and timestamp: a
	// second row for the same pair must not turn the answer into "first seen
	// yesterday" or into a second entry.
	var viewRows int
	var viewFirstID int64
	var viewSeen time.Time
	if err := admin.QueryRow(ctx, `
		select count(*), min(first_delivery_id), min(first_seen_at)
		  from delivery_dedupe_all where endpoint_id = $1 and event_id = $2`,
		d.endpointID, d.eventID).Scan(&viewRows, &viewFirstID, &viewSeen); err != nil {
		t.Fatalf("read the view: %v", err)
	}
	if viewRows != 1 {
		t.Fatalf("delivery_dedupe_all returns %d rows for the pair, want 1 (UNION + GROUP BY)", viewRows)
	}
	if viewFirstID != d.firstID || !viewSeen.UTC().Equal(d.seenAt) {
		t.Fatalf("the collapsed answer is (%d, %s), want the original (%d, %s)",
			viewFirstID, viewSeen.UTC().Format(time.RFC3339), d.firstID, d.seenAt.Format(time.RFC3339))
	}

	answer, err := st.DedupeAnswer(ctx, d.scope, d.endpointID, d.eventID)
	if err != nil {
		t.Fatalf("DedupeAnswer: %v", err)
	}
	if !answer.Duplicate || answer.FirstDeliveryID != d.firstID {
		t.Fatalf("answer = %+v, want the pair with first delivery %d", answer, d.firstID)
	}
	if answer.FirstSeenAt == nil || !answer.FirstSeenAt.UTC().Equal(d.seenAt) {
		t.Fatalf("answer first_seen_at = %v, want the original %s", answer.FirstSeenAt, d.seenAt.Format(time.RFC3339))
	}
}

// TestDropAbortsCopyWithTheDropWhenTheCopyFails is FR-905's "must abort that
// transaction - copy and drop together - if the copy fails". The failure is
// injected where a real one would happen (the insert into the archive), and the
// assertion is that BOTH halves are undone: the partition is still there and the
// archive holds nothing, so the next tick can retry the whole thing.
func TestDropAbortsCopyWithTheDropWhenTheCopyFails(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	st := New(appPool(t, admin))
	ctx := context.Background()

	d := seedDedupeDay(t, admin, st, "dedupeabort", time.Now().UTC().AddDate(0, 0, -30))

	if _, err := admin.Exec(ctx, `
		create function pigeon_test_block_archive() returns trigger language plpgsql as $$
		begin
			raise exception 'FR-905 probe: archive insert blocked';
		end $$`); err != nil {
		t.Fatalf("create the failing trigger function: %v", err)
	}
	if _, err := admin.Exec(ctx, `
		create trigger pigeon_test_block_archive before insert on delivery_dedupe
		for each row execute function pigeon_test_block_archive()`); err != nil {
		t.Fatalf("create the failing trigger: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), `drop trigger if exists pigeon_test_block_archive on delivery_dedupe`)
		_, _ = admin.Exec(context.Background(), `drop function if exists pigeon_test_block_archive()`)
	})

	got, err := st.DropPartition(ctx, "deliveries", d.leaf, retention.DropByAge)
	if err == nil {
		t.Fatalf("DropPartition succeeded with the copy failing (dropped=%v): the copy and the drop must commit together or not at all", got)
	}
	if got {
		t.Fatal("DropPartition reported a drop while its copy failed")
	}
	if !strings.Contains(err.Error(), "FR-905 probe") {
		t.Fatalf("DropPartition error = %v, want the injected copy failure (otherwise this test proves nothing about the copy)", err)
	}

	// Neither half committed: the partition is still there, with its rows, and the
	// archive has nothing for it.
	var leafThere bool
	if err := admin.QueryRow(ctx, `select to_regclass($1) is not null`, d.leaf).Scan(&leafThere); err != nil {
		t.Fatalf("read the leaf: %v", err)
	}
	if !leafThere {
		t.Fatalf("%s was dropped even though its copy failed (FR-905)", d.leaf)
	}
	var liveRows int
	if err := admin.QueryRow(ctx, `select count(*) from deliveries where event_id = $1`, d.eventID).Scan(&liveRows); err != nil {
		t.Fatalf("count the day's deliveries: %v", err)
	}
	if liveRows != 2 {
		t.Fatalf("the day holds %d deliveries after the aborted drop, want 2", liveRows)
	}
	if _, _, n := archiveRows(t, admin, d.endpointID, d.eventID); n != 0 {
		t.Fatalf("the archive holds %d rows after a failed copy, want 0: the copy must roll back with the drop", n)
	}
}

// TestDropCopiesNothingWhenAWriterHoldsThePartition is the other half of "both
// commit or neither": when the drop cannot happen because a writer holds the
// partition, nothing may be copied either. The primitive takes the partition lock
// BEFORE it copies, so a contended drop leaves the archive empty and the partition
// in place for the next tick.
func TestDropCopiesNothingWhenAWriterHoldsThePartition(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	st := New(appPool(t, admin))
	ctx := context.Background()

	d := seedDedupeDay(t, admin, st, "dedupeheld", time.Now().UTC().AddDate(0, 0, -31))

	// A reader (any open transaction on the partition holds ACCESS SHARE, and the
	// drop needs ACCESS EXCLUSIVE) stands in for the worker that is mid-write.
	holder, err := admin.Begin(ctx)
	if err != nil {
		t.Fatalf("begin the holder: %v", err)
	}
	var n int
	if err := holder.QueryRow(ctx, `select count(*) from `+d.leaf).Scan(&n); err != nil {
		_ = holder.Rollback(ctx)
		t.Fatalf("read the held partition: %v", err)
	}

	start := time.Now()
	got, err := st.DropPartition(ctx, "deliveries", d.leaf, retention.DropByAge)
	if err != nil {
		_ = holder.Rollback(ctx)
		t.Fatalf("DropPartition while the partition was held: %v", err)
	}
	if got {
		_ = holder.Rollback(ctx)
		t.Fatal("dropped a partition a writer held")
	}
	if waited := time.Since(start); waited > 20*time.Second {
		_ = holder.Rollback(ctx)
		t.Fatalf("the held drop waited %s; lock_timeout must bound it", waited)
	}
	if _, _, n := archiveRows(t, admin, d.endpointID, d.eventID); n != 0 {
		_ = holder.Rollback(ctx)
		t.Fatalf("the archive holds %d rows after a drop that did not happen: the copy must not outlive the drop", n)
	}
	if err := holder.Rollback(ctx); err != nil {
		t.Fatalf("rollback the holder: %v", err)
	}

	// The holder is gone: the same drop now copies AND drops, which is what makes
	// the refusal above contention rather than a broken primitive.
	got, err = st.DropPartition(ctx, "deliveries", d.leaf, retention.DropByAge)
	if err != nil || !got {
		t.Fatalf("DropPartition after the holder released = (%v, %v), want (true, nil)", got, err)
	}
	if _, _, n := archiveRows(t, admin, d.endpointID, d.eventID); n != 1 {
		t.Fatalf("archive rows after the retry = %d, want 1", n)
	}
}

// TestPressureDropSkipsTheCopyAndWarnsWithTheDayRange is FR-903's first loss path:
// an SB-3 disk-pressure drop may skip the copy, because the space is what is being
// reclaimed - and it MUST log the resulting gap at warn with the partition and the
// day range, so the operator knows which support answers just disappeared.
//
// The warning is asserted on the server side (the primitive's RAISE WARNING, read
// back through the connection's notice handler) and the behaviour on the ordinary
// path (the store's own call), because a test that only saw the Go-side log would
// not prove the SQL primitive said anything.
func TestPressureDropSkipsTheCopyAndWarnsWithTheDayRange(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	st := New(appPool(t, admin))
	ctx := context.Background()

	d := seedDedupeDay(t, admin, st, "dedupepressure", time.Now().UTC().AddDate(0, 0, -32))
	other := seedDedupeDay(t, admin, st, "dedupepressureb", time.Now().UTC().AddDate(0, 0, -33))

	// The pressure path, through the store: the partition goes and nothing is
	// copied (there is no space to copy into - that is the point).
	got, err := st.DropPartition(ctx, "deliveries", d.leaf, retention.DropByPressure)
	if err != nil || !got {
		t.Fatalf("DropPartition(%s, pressure) = (%v, %v), want (true, nil)", d.leaf, got, err)
	}
	if _, _, n := archiveRows(t, admin, d.endpointID, d.eventID); n != 0 {
		t.Fatalf("a pressure drop copied %d rows into the archive, want 0 (FR-905: only an ordinary drop copies)", n)
	}
	after, err := st.DedupeAnswer(ctx, d.scope, d.endpointID, d.eventID)
	if err != nil {
		t.Fatalf("DedupeAnswer after a pressure drop: %v", err)
	}
	if after.Duplicate {
		t.Fatalf("the answer survived a pressure drop that skipped the copy: %+v", after)
	}

	// The warning, from the primitive itself. It is read off a connection built
	// with a notice handler, because a RAISE WARNING lands in the connection's
	// notice stream and nowhere else - asserting only the Go-side log would not
	// prove the SQL said anything.
	cfg, err := pgx.ParseConfig(os.Getenv("PIGEON_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatalf("parse the test DSN: %v", err)
	}
	var notices []string
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		notices = append(notices, n.Message)
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect with a notice handler: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	var pressureDropped bool
	if err := conn.QueryRow(ctx, `select pigeon_drop_partition($1, $2, $3)`,
		"deliveries", other.leaf, "pressure").Scan(&pressureDropped); err != nil {
		t.Fatalf("pressure drop on the notice connection: %v", err)
	}
	if !pressureDropped {
		t.Fatalf("%s was not dropped", other.leaf)
	}

	wantRange := other.day.Format("2006-01-02") + " to " + other.day.AddDate(0, 0, 1).Format("2006-01-02")
	var found bool
	for _, m := range notices {
		if strings.Contains(m, other.leaf) && strings.Contains(m, wantRange) {
			found = true
		}
	}
	if !found {
		t.Fatalf("the pressure drop raised no warning naming the partition and the day range %q; notices: %v",
			wantRange, notices)
	}
}

// TestArchiveLeavesAreTenantIsolated is MT-1 for FR-905's table. The archive holds
// tenant data - endpoint ids, event ids, delivery ids - and PostgreSQL propagates
// neither the RLS flag nor a policy to a partition, so the runtime role must read
// only its own tenant through the parent AND through a leaf named directly.
// Without the leaf policy a query that names a day (a repository bug, a fixture,
// psql) reads every tenant's archive rows.
func TestArchiveLeavesAreTenantIsolated(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	app := appPool(t, admin)
	st := New(app)
	ctx := context.Background()

	// Two tenants in ONE deliveries day, so the copy puts both into one archive
	// day partition and the leaf really is shared.
	day := time.Now().UTC().AddDate(0, 0, -34)
	// The archive day is the one relation resetRoutingTables does not reset, so the
	// fixture owns it before it seeds: without this the copy of a previous run's two
	// pairs is still in the day partition and the non-vacuity guard below reads 4.
	resetArchiveDay(t, admin, day)
	a := seedDedupeDay(t, admin, st, "dedupeiso-a", day)
	b := seedDedupeDay(t, admin, st, "dedupeiso-b", day)
	if a.leaf != b.leaf {
		t.Fatalf("the fixture did not share a day partition: %s vs %s", a.leaf, b.leaf)
	}
	if got, err := st.DropPartition(ctx, "deliveries", a.leaf, retention.DropByAge); err != nil || !got {
		t.Fatalf("DropPartition(%s) = (%v, %v), want (true, nil)", a.leaf, got, err)
	}

	leaf := "delivery_dedupe_" + day.Format("20060102")
	var relrowsecurity bool
	var policies int
	if err := admin.QueryRow(ctx, `
		select c.relrowsecurity, (select count(*) from pg_policy p where p.polrelid = c.oid)
		  from pg_class c where c.oid = to_regclass($1)`, leaf).Scan(&relrowsecurity, &policies); err != nil {
		t.Fatalf("read %s: %v", leaf, err)
	}
	if !relrowsecurity || policies == 0 {
		t.Fatalf("%s: relrowsecurity=%v policies=%d, want the flag and the parent's policy on the leaf",
			leaf, relrowsecurity, policies)
	}

	// The fixture is non-vacuous: the leaf holds both tenants' pairs.
	var all int
	if err := admin.QueryRow(ctx, `select count(*) from `+leaf).Scan(&all); err != nil {
		t.Fatalf("count the leaf: %v", err)
	}
	if all != 2 {
		t.Fatalf("%s holds %d rows, want 2 (one per tenant)", leaf, all)
	}

	// A test database has no default privileges, so the runtime role's grants on a
	// leaf created after the harness's blanket grant are explicit here (the
	// property under test is the policy, not the grant).
	if _, err := admin.Exec(ctx, `grant select on `+leaf+` to `+appRole); err != nil {
		t.Fatalf("grant on %s: %v", leaf, err)
	}

	counts := func(producerID, endpointID string) (parent, leafRows int) {
		err := db.ControlTx(ctx, app, func(tx pgx.Tx) error {
			if err := db.SetProducerScope(ctx, tx, producerID); err != nil {
				return err
			}
			if err := db.SetEnvScope(ctx, tx, "live"); err != nil {
				return err
			}
			if err := tx.QueryRow(ctx,
				`select count(*) from delivery_dedupe where endpoint_id = $1`, endpointID).Scan(&parent); err != nil {
				return err
			}
			return tx.QueryRow(ctx,
				`select count(*) from `+leaf+` where endpoint_id = $1`, endpointID).Scan(&leafRows)
		})
		if err != nil {
			t.Fatalf("scoped archive read for %s: %v", producerID, err)
		}
		return parent, leafRows
	}

	if parent, leafRows := counts(a.producerID, a.endpointID); parent != 1 || leafRows != 1 {
		t.Fatalf("tenant A reads its own archive as parent=%d leaf=%d, want 1 and 1", parent, leafRows)
	}
	if parent, leafRows := counts(a.producerID, b.endpointID); parent != 0 || leafRows != 0 {
		t.Fatalf("tenant A reads tenant B's archived pair as parent=%d leaf=%d, want 0 and 0 (MT-1)", parent, leafRows)
	}
	// The same through the view the support path reads, which is security_invoker
	// and must not be a way around the archive's policy.
	var viaView int
	if err := db.ControlTx(ctx, app, func(tx pgx.Tx) error {
		if err := db.SetProducerScope(ctx, tx, a.producerID); err != nil {
			return err
		}
		if err := db.SetEnvScope(ctx, tx, "live"); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `select count(*) from delivery_dedupe_all where endpoint_id = $1`,
			b.endpointID).Scan(&viaView)
	}); err != nil {
		t.Fatalf("scoped view read: %v", err)
	}
	if viaView != 0 {
		t.Fatalf("delivery_dedupe_all returned %d of another tenant's archived pairs (MT-1)", viaView)
	}
}

// TestRuntimeRoleCannotWriteTheArchive is MT-1 applied to what the archive is FOR.
// Production grants the runtime role every table privilege through
// deploy/postgres/01-roles.sql's default privileges, so what has to stop the
// application from writing `delivery_dedupe` is the policy - and it exists FOR
// SELECT only, which is deliberate: the copy is the SECURITY DEFINER function's
// job, and a role that could insert could forge an "already seen" answer, while a
// role that could delete could erase one (SB-2's "never by DELETE", as a property
// of the role rather than only of the code).
func TestRuntimeRoleCannotWriteTheArchive(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	app := appPool(t, admin)
	st := New(app)
	ctx := context.Background()

	d := seedDedupeDay(t, admin, st, "dedupeforge", time.Now().UTC().AddDate(0, 0, -41))
	if got, err := st.DropPartition(ctx, "deliveries", d.leaf, retention.DropByAge); err != nil || !got {
		t.Fatalf("DropPartition(%s) = (%v, %v), want (true, nil)", d.leaf, got, err)
	}

	// The grant the production roles file would have applied. Granting it here is
	// what makes the refusals below about the policy rather than about a missing
	// privilege (a test database has no default privileges).
	if _, err := admin.Exec(ctx,
		`grant select, insert, update, delete on delivery_dedupe to `+appRole); err != nil {
		t.Fatalf("grant the archive to the runtime role: %v", err)
	}

	// The role can read its own pair, so the grant and the policy are both in force
	// and a later refusal cannot be read as "no privilege".
	var readable int
	if err := db.ControlTx(ctx, app, func(tx pgx.Tx) error {
		if err := db.SetProducerScope(ctx, tx, d.producerID); err != nil {
			return err
		}
		if err := db.SetEnvScope(ctx, tx, "live"); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `select count(*) from delivery_dedupe where endpoint_id = $1`,
			d.endpointID).Scan(&readable)
	}); err != nil {
		t.Fatalf("scoped read as the runtime role: %v", err)
	}
	if readable != 1 {
		t.Fatalf("the runtime role reads %d of its own archived rows, want 1", readable)
	}

	// A forged answer: refused, by the policy rather than by the grant. The scope is
	// set to the caller's own tenant here, so this cannot pass merely because the
	// writer was scoped out of the row it is trying to touch.
	err := db.ControlTx(ctx, app, func(tx pgx.Tx) error {
		if err := db.SetProducerScope(ctx, tx, d.producerID); err != nil {
			return err
		}
		if err := db.SetEnvScope(ctx, tx, "live"); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			insert into delivery_dedupe (endpoint_id, event_id, first_delivery_id, first_seen_at)
			values ($1, $2, 999, $3)`, d.endpointID, "evt_forged_by_the_app_role", d.seenAt)
		return err
	})
	if err == nil {
		t.Fatal("the runtime role inserted into the dedupe archive: an answer can be forged (FR-905)")
	}
	if !strings.Contains(err.Error(), "row-level security") {
		t.Fatalf("the insert was refused for the wrong reason: %v", err)
	}

	// Erasure: no policy covers DELETE or UPDATE, so either the statement is refused
	// or it sees no rows - it cannot touch what the support path reads.
	if tag, err := app.Exec(ctx, `delete from delivery_dedupe`); err == nil && tag.RowsAffected() != 0 {
		t.Fatalf("the runtime role deleted %d archived rows, want 0 (SB-2, FR-905)", tag.RowsAffected())
	}
	if tag, err := app.Exec(ctx, `update delivery_dedupe set first_delivery_id = 1`); err == nil && tag.RowsAffected() != 0 {
		t.Fatalf("the runtime role updated %d archived rows, want 0 (FR-905)", tag.RowsAffected())
	}
	var after int64
	if err := admin.QueryRow(ctx, `
		select count(*) from delivery_dedupe where endpoint_id = $1 and event_id = $2`,
		d.endpointID, d.eventID).Scan(&after); err != nil {
		t.Fatalf("count the archive: %v", err)
	}
	if after != 1 {
		t.Fatalf("the archive holds %d rows for the pair after a delete by the runtime role, want 1 (SB-2, FR-905)", after)
	}
	firstID, seenAt, n := archiveRows(t, admin, d.endpointID, d.eventID)
	if n != 1 || firstID != d.firstID || !seenAt.UTC().Equal(d.seenAt) {
		t.Fatalf("the archived answer is now (%d, %s, %d rows), want the original %d / %s / 1 row",
			firstID, seenAt.UTC().Format(time.RFC3339), n, d.firstID, d.seenAt.Format(time.RFC3339))
	}
}

// TestDedupeArchiveIsDroppedByPartitionOnly is SB-2 on FR-905's table, as a static
// guard because no runtime assertion can cover a statement that does not exist
// yet: there is no DELETE against the archive in the tree, so the archive leaves
// whole partitions like every other retained table (bloat and vacuum pressure on
// the disk the delivery path needs are what SB-2 exists to avoid).
//
// Test files are skipped: this file names the statement it is guarding against.
func TestDedupeArchiveIsDroppedByPartitionOnly(t *testing.T) {
	root := filepath.Join("..", "..")
	needle := "delete" + " from delivery_dedupe"
	var offenders []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".git" || d.Name() == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".sql") && !strings.HasSuffix(path, ".go") {
			return nil
		}
		if strings.HasSuffix(path, "_test.go") {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(strings.ToLower(string(body)), needle) {
			offenders = append(offenders, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk the tree: %v", err)
	}
	if len(offenders) > 0 {
		t.Fatalf("DELETE against the dedupe archive (SB-2 forbids it; drop the partition instead): %v", offenders)
	}
}

// TestDedupeAnswerNeverReachesForRedis is FR-905's "nothing about dedupe may live
// in Redis", enforced where it can be: the two files the support question travels
// through must not import or mention the broker at all. AC-46 flushes Redis and
// asks again; this guard is what keeps that property from being re-broken by a
// later cache, and it is cheap enough to run on every change.
func TestDedupeAnswerNeverReachesForRedis(t *testing.T) {
	for _, path := range []string{
		filepath.Join("..", "store", "inspect.go"),   // DedupeAnswer: one SQL query
		filepath.Join("..", "httpapi", "inspect.go"), // handleDedupeAnswer
	} {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		lower := strings.ToLower(string(body))
		for _, word := range []string{"redis", "broker"} {
			if strings.Contains(lower, word) {
				t.Fatalf("%s mentions %q: nothing about dedupe may live outside Postgres (FR-905)", path, word)
			}
		}
	}
}
