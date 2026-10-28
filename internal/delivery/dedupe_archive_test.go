package delivery

import (
	"context"
	"testing"
	"time"

	"github.com/m7jay/pigeon/internal/metrics"
	"github.com/m7jay/pigeon/internal/retention"
	"github.com/m7jay/pigeon/internal/store"
)

// AC-46's sizing and gauge half, on a real Postgres: after 100,000 pairs have been
// copied into the archive, the measured per-pair cost must be within 50% of the
// documented ~200 B, and the gauge the operator scrapes must agree with
// pg_total_relation_size rather than with a row count.
//
// The chain under test is the whole one, deliberately: one sweep tick drops an
// expired `deliveries` partition by age (FR-905's copy), the manager measures the
// archive with pg_total_relation_size (FR-905's gauge), and the sweeper hands that
// number to the metrics the /metrics scrape renders (SB-6, OBS-11). Each step is
// asserted against the database's own answer, so a constant or a stale value
// cannot pass.
func TestDedupeArchiveSizingAndGaugeAgree(t *testing.T) {
	const pairs = 100_000

	admin := testPool(t)
	st := store.New(admin)
	ctx := context.Background()

	if _, err := admin.Exec(ctx, `truncate table events, deliveries cascade`); err != nil {
		t.Fatalf("reset the event/delivery tables: %v", err)
	}

	// The archive's day, three days back, so an hour-long deliveries window has
	// expired it and the day itself has ended (the drop primitive refuses today).
	day := time.Now().UTC().AddDate(0, 0, -3).Truncate(24 * time.Hour)
	at := day.Add(12 * time.Hour)
	leaf, err := st.EnsurePartition(ctx, "deliveries", day)
	if err != nil {
		t.Fatalf("EnsurePartition(deliveries, %s): %v", day.Format("2006-01-02"), err)
	}

	if _, err := admin.Exec(ctx, `
		insert into producers (id, slug, name) values ('prd_archsize', 'archsize', 'Archive sizing')
		on conflict (id) do nothing`); err != nil {
		t.Fatalf("seed producer: %v", err)
	}
	if _, err := admin.Exec(ctx, `
		insert into event_types (id, producer_id, env, name, routing_mode, event_class)
		values ('evttype_archsize', 'prd_archsize', 'live', 'archsize.order', 'consumer', 'normal')
		on conflict (id) do nothing`); err != nil {
		t.Fatalf("seed event type: %v", err)
	}
	// 100,000 pairs of 26-character text ids, which is the shape A-4's ~200 B is
	// published for: `endpoint_id` is one endpoint, `event_id` is a ULID per pair.
	if _, err := admin.Exec(ctx, `
		insert into events (id, producer_id, env, event_type, payload_version, routing_mode,
		                    event_class, consumer_ref, occurred_at, payload)
		select 'evt_archsize_' || lpad(i::text, 6, '0'), 'prd_archsize', 'live', 'archsize.order',
		       1, 'consumer', 'normal', 'cref_archsize', $1, '{}'::jsonb
		  from generate_series(1, $2) i`, at, pairs); err != nil {
		t.Fatalf("seed events: %v", err)
	}
	if _, err := admin.Exec(ctx, `
		insert into deliveries (event_id, endpoint_id, producer_id, env, consumer_id, status, created_at)
		select 'evt_archsize_' || lpad(i::text, 6, '0'), 'ept_archsize_0000000000000000', 'prd_archsize',
		       'live', 'cns_archsize', 'delivered', $1
		  from generate_series(1, $2) i`, at, pairs); err != nil {
		t.Fatalf("seed deliveries: %v", err)
	}

	// One sweep tick, with the archive window at its default and the deliveries
	// window shrunk to an hour (AC-46's setting).
	m := metrics.New()
	rm := retention.NewManager(st, nil, retention.Options{
		DeliveriesRetention: time.Hour,
		Logger:              discardLogger(),
	})
	sw := NewSweeper(st, SweeperOptions{Retention: rm, Metrics: m, Logger: discardLogger()})
	sw.sweepOnce(ctx)

	var leafGone bool
	if err := admin.QueryRow(ctx, `select to_regclass($1) is null`, leaf).Scan(&leafGone); err != nil {
		t.Fatalf("read the dropped leaf: %v", err)
	}
	if !leafGone {
		t.Fatalf("%s was not dropped by the tick (a 1 h window and a 3 day old day)", leaf)
	}

	var archived int64
	if err := admin.QueryRow(ctx, `select count(*) from delivery_dedupe`).Scan(&archived); err != nil {
		t.Fatalf("count the archive: %v", err)
	}
	if archived != pairs {
		t.Fatalf("the archive holds %d pairs after the drop, want %d (every pair of the dropped day, once)", archived, pairs)
	}

	// The database's own answer, which is what the documented figure has to match.
	// Summed over the archive's partitions: a partitioned parent has no storage of
	// its own, so pg_total_relation_size on `delivery_dedupe` itself answers 0 (it
	// is the sum an operator means by "how big is the archive").
	var size int64
	if err := admin.QueryRow(ctx, `
		select coalesce(sum(pg_total_relation_size(c.oid)), 0)
		  from pg_inherits i
		  join pg_class c on c.oid = i.inhrelid
		 where i.inhparent = to_regclass('delivery_dedupe') and c.relkind = 'r'`).Scan(&size); err != nil {
		t.Fatalf("pg_total_relation_size over the archive's partitions: %v", err)
	}
	perPair := size / pairs
	t.Logf("measured %d bytes for %d pairs = %d B per pair (A-4 publishes ~200 B)", size, pairs, perPair)
	// A-4 publishes ~200 B per pair (heap tuple header, three text values, the
	// timestamp, the primary-key btree entry). The acceptance criterion allows 50%
	// either way, which is what makes this a check on the design rather than on the
	// exact page layout.
	if perPair < 100 || perPair > 300 {
		t.Fatalf("measured %d B per archived pair, want within 50%% of the documented ~200 B (100..300)", perPair)
	}

	// FR-905's gauge, as the sweeper exported it to GET /metrics.
	if got := m.Counter(metrics.DedupeArchiveBytes); got != size {
		t.Fatalf("the exported gauge = %d, want the measured %d (pg_total_relation_size)", got, size)
	}
	if !metrics.IsGauge(metrics.DedupeArchiveBytes) {
		t.Fatal("the archive size is exported as a counter; FR-905 publishes a level, so /metrics must type it gauge")
	}
}
