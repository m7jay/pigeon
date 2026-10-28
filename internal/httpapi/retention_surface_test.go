package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/m7jay/pigeon/internal/metrics"
	"github.com/m7jay/pigeon/internal/ratelimit"
	"github.com/m7jay/pigeon/internal/retention"
)

// The storage-budget surface: SB-7's publish gate (429) and SB-6's
// GET /v1/meta/usage. Both are driven through the Retention interface, so "the
// disk is at 95%" is a one-line fixture instead of a filled volume (AC-18).

// fakeRetention is the read side of the budget the handlers see.
type fakeRetention struct {
	over   bool
	status retention.Status
	err    error
}

func (f *fakeRetention) IngestGate(context.Context) bool { return f.over }
func (f *fakeRetention) Status(context.Context) (retention.Status, error) {
	return f.status, f.err
}

// withRetention builds a second API over the same database with a retention
// surface attached, the way main.go attaches the real manager.
func (e *env) withRetention(t *testing.T, r Retention, m *metrics.Metrics) *env {
	t.Helper()
	api := New(Deps{
		Lookup:          e.st.LookupAPIKey,
		Sessions:        e.st.LookupSession,
		Data:            e.st,
		Version:         "test-retention",
		SessionTTL:      time.Hour,
		KeyLimiter:      ratelimit.New(1000, 1000),
		ProducerLimiter: ratelimit.New(1000, 1000),
		Retention:       r,
		Metrics:         m,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	out := &env{admin: e.admin, app: e.app, st: e.st, api: api, adminToken: e.adminToken}
	out.srv = httptest.NewServer(api.Handler())
	t.Cleanup(out.srv.Close)
	return out
}

// TestPublishShedsWith429OverDiskBudget is AC-18's ingest half: past the budget,
// publish is refused with 429 and nothing is written, and the shed is counted.
func TestPublishShedsWith429OverDiskBudget(t *testing.T) {
	e := setup(t)
	tn := e.seedTenant(t, "shed-over")
	createType(t, e, tn, "payment.succeeded")

	m := metrics.New()
	re := e.withRetention(t, &fakeRetention{over: true}, m)

	code, raw := publish(t, re, tn.KeyPlain, map[string]any{
		"event_type": "payment.succeeded",
		"payload":    map[string]any{"amount": 100},
	})
	mustStatus(t, code, raw, http.StatusTooManyRequests)
	if got := errorCode(t, raw); got != "over_budget" {
		t.Fatalf("error code = %q, want over_budget; body: %s", got, raw)
	}
	if got := re.lastRetryAfter(); got != "30" {
		t.Fatalf("Retry-After = %q, want 30 (SB-7 tells the producer when to return)", got)
	}
	if got := m.ShedCount(); got != 1 {
		t.Fatalf("shed count = %d, want 1", got)
	}

	// Shedding means nothing was accepted: no event row for this producer. The
	// count is scoped to the tenant because other tests use the same type name.
	var n int
	if err := re.admin.QueryRow(ctx(),
		`select count(*) from events where producer_id = $1`, tn.ID).Scan(&n); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if n != 0 {
		t.Fatalf("events = %d, want 0: a shed publish must not write (SB-7)", n)
	}

	// SB-6: the shed counter is visible to a scrape even before any drop.
	code, raw = re.do(http.MethodGet, "/metrics", re.adminToken, nil)
	mustStatus(t, code, raw, http.StatusOK)
	if !strings.Contains(string(raw), "pigeon_publishes_shed_total 1") {
		t.Fatalf("/metrics is missing pigeon_publishes_shed_total 1:\n%s", raw)
	}
}

// TestPublishProceedsUnderDiskBudget is the other side of the gate: below the
// budget the same publish is accepted.
func TestPublishProceedsUnderDiskBudget(t *testing.T) {
	e := setup(t)
	tn := e.seedTenant(t, "shed-under")
	createType(t, e, tn, "payment.succeeded")

	re := e.withRetention(t, &fakeRetention{over: false}, metrics.New())
	code, raw := publish(t, re, tn.KeyPlain, map[string]any{
		"event_type": "payment.succeeded",
		"payload":    map[string]any{"amount": 100},
	})
	mustStatus(t, code, raw, http.StatusAccepted)
}

// TestUsageEndpointReportsDiskAgainstBudget is SB-6: an operator can see the
// volume against the budget before history starts disappearing.
func TestUsageEndpointReportsDiskAgainstBudget(t *testing.T) {
	e := setup(t)
	st := retention.Status{
		Disk:          retention.Usage{UsedBytes: 900, TotalBytes: 1000, UsedPct: 90, Source: "test"},
		MaxDiskPct:    80,
		OverBudget:    true,
		RetentionDays: map[string]int{"deliveries": 14, "attempts": 7, "delivery_dedupe": 30},
		// FR-905: the archive's measured size travels in the same payload, so the
		// pressure the operator acts on includes the space the archive holds.
		DedupeArchiveBytes: 21757952,
		Partitions: map[string][]retention.Partition{
			"delivery_dedupe": {{Table: "delivery_dedupe", Name: "delivery_dedupe_20261018", Bytes: 21757952}},
		},
	}
	re := e.withRetention(t, &fakeRetention{status: st}, nil)

	code, raw := re.do(http.MethodGet, "/v1/meta/usage", re.adminToken, nil)
	mustStatus(t, code, raw, http.StatusOK)
	var got retention.Status
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode usage payload: %v; body: %s", err, raw)
	}
	if !got.OverBudget || got.MaxDiskPct != 80 {
		t.Fatalf("payload = %+v, want over_budget at max_disk_pct 80", got)
	}
	if got.Disk.UsedPct != 90 || got.Disk.Source != "test" {
		t.Fatalf("disk = %+v, want the 90%% test reading", got.Disk)
	}
	if got.RetentionDays["deliveries"] != 14 || got.RetentionDays["attempts"] != 7 {
		t.Fatalf("retention_days = %v, want the configured windows", got.RetentionDays)
	}
	if got.RetentionDays["delivery_dedupe"] != 30 || got.DedupeArchiveBytes != 21757952 {
		t.Fatalf("payload = %+v, want the dedupe archive's 30 d window and its measured size (FR-905)",
			got)
	}
	if len(got.Partitions["delivery_dedupe"]) != 1 {
		t.Fatalf("partitions = %v, want the archive's day partition listed like the others",
			got.Partitions)
	}
}

// TestMetricsExportsTheDedupeArchiveGauge is FR-905's "the archive's size MUST be
// exposed as a gauge read from pg_total_relation_size": the scrape carries the
// value the sweeper measured, and it is typed gauge, so a Prometheus reader does
// not rate() a level. The measurement itself is exercised against a real archive
// in internal/delivery (TestDedupeArchiveSizingAndGaugeAgree).
func TestMetricsExportsTheDedupeArchiveGauge(t *testing.T) {
	e := setup(t)
	m := metrics.New()
	m.Set(metrics.DedupeArchiveBytes, 21757952)
	re := e.withRetention(t, &fakeRetention{}, m)

	code, raw := re.do(http.MethodGet, "/metrics", re.adminToken, nil)
	mustStatus(t, code, raw, http.StatusOK)
	body := string(raw)
	if !strings.Contains(body, "# TYPE pigeon_dedupe_archive_bytes gauge") {
		t.Fatalf("/metrics does not type the archive size as a gauge (FR-905):\n%s", body)
	}
	if !strings.Contains(body, "pigeon_dedupe_archive_bytes 21757952") {
		t.Fatalf("/metrics is missing the measured archive size:\n%s", body)
	}
}

// TestDedupeAnswerSurvivesTheSweep is AC-46 end to end at the API: with
// `deliveries` retention set to an hour, a delivery is written into a day the
// sweeper may drop, the sweep drops that partition (copying its dedupe keys into
// the archive on the way), and the support question is then asked over HTTP. The
// answer must be there, carrying the FIRST delivery and its ORIGINAL timestamp,
// and it can only have come from Postgres: the live rows are gone (the answer
// reports 0 deliveries) and nothing on this path touches Redis at all - the
// handler issues one scoped query against delivery_dedupe_all, asserted by
// TestDedupeAnswerNeverReachesForRedis in internal/store.
func TestDedupeAnswerSurvivesTheSweep(t *testing.T) {
	e, fx := setupS7(t)
	tn := fx.Tenant

	// A published event with no fan-out: this test writes the one delivery itself,
	// because fan-out stamps created_at from now() and AC-46 needs a day that has
	// ended (the drop primitive refuses today).
	eventID := e.publishFor(t, fx, fx.Ref)
	// Two whole days back, truncated to midnight: one day back is still not
	// expired between 00:00 and 01:00 UTC (the age rule drops a day D only when
	// D+1d <= now - 1h), and a `day` that carries the wall clock puts noon inside
	// TODAY's partition on any run at or after 12:00 UTC. Either way the sweep
	// below drops nothing and the archive assertion reads 0 rows. Two days back is
	// expired at every hour of the day.
	day := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -2)
	at := time.Date(day.Year(), day.Month(), day.Day(), 12, 0, 0, 0, time.UTC)
	leaf, err := e.st.EnsurePartition(ctx(), "deliveries", day)
	if err != nil {
		t.Fatalf("EnsurePartition: %v", err)
	}
	var deliveryID int64
	if err := e.admin.QueryRow(ctx(), `
		insert into deliveries (event_id, endpoint_id, producer_id, env, consumer_id, status, created_at)
		values ($1, $2, $3, 'live', $4, 'delivered', $5) returning id`,
		eventID, fx.EndpointID, tn.ID, fx.ConsumerID, at).Scan(&deliveryID); err != nil {
		t.Fatalf("seed the delivery: %v", err)
	}

	ask := func() (int, []byte) {
		return e.do(http.MethodGet,
			"/v1/producer/deliveries/dedupe?env=live&endpoint_id="+fx.EndpointID+"&event_id="+eventID,
			tn.OwnerToken, nil)
	}

	// Live first, so the fixture is known to answer before the drop.
	code, raw := ask()
	mustStatus(t, code, raw, http.StatusOK)
	if live := decode[dedupeJSONBody](t, raw); !live.Duplicate || live.Deliveries != 1 {
		t.Fatalf("the pre-sweep answer = %+v, want a duplicate of the live delivery", live)
	}

	// AC-46's setting: an hour of deliveries retention, then a sweep.
	rm := retention.NewManager(e.st, nil, retention.Options{
		DeliveriesRetention: time.Hour,
		Logger:              slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	rep, err := rm.RunOnce(ctx())
	if err != nil {
		t.Fatalf("retention RunOnce: %v", err)
	}
	if len(rep.Dropped) == 0 {
		t.Fatalf("the sweep dropped nothing; the day %s must be expired under a 1 h window", leaf)
	}

	var leafGone bool
	if err := e.admin.QueryRow(ctx(), `select to_regclass($1) is null`, leaf).Scan(&leafGone); err != nil {
		t.Fatalf("read the dropped leaf: %v", err)
	}
	if !leafGone {
		t.Fatalf("%s is still there after the sweep", leaf)
	}
	var archived int
	if err := e.admin.QueryRow(ctx(),
		`select count(*) from delivery_dedupe where endpoint_id = $1 and event_id = $2`,
		fx.EndpointID, eventID).Scan(&archived); err != nil {
		t.Fatalf("read the archive: %v", err)
	}
	if archived != 1 {
		t.Fatalf("the archive holds %d rows for the pair, want 1 (FR-905's copy at the drop)", archived)
	}

	// The question again, with the cache state a Redis wipe would leave: the answer
	// is unchanged, and the 0 live deliveries are what say it came from the archive.
	code, raw = ask()
	mustStatus(t, code, raw, http.StatusOK)
	after := decode[dedupeJSONBody](t, raw)
	if !after.Duplicate {
		t.Fatalf("the answer after the sweep = %+v, want the archived duplicate (FR-905)", after)
	}
	if after.FirstDeliveryID != deliveryID {
		t.Fatalf("archived first_delivery_id = %d, want %d", after.FirstDeliveryID, deliveryID)
	}
	if after.FirstSeenAt == nil || *after.FirstSeenAt != at.Format(time.RFC3339) {
		t.Fatalf("archived first_seen_at = %v, want the original %s", after.FirstSeenAt, at.Format(time.RFC3339))
	}
	if after.Deliveries != 0 {
		t.Fatalf("the archived answer reports %d live deliveries, want 0: the partition was dropped", after.Deliveries)
	}
}

// TestUsageEndpointIs503WithoutRetention: with no retention signal configured the
// endpoint says so rather than reporting a calm zero.
func TestUsageEndpointIs503WithoutRetention(t *testing.T) {
	e := setup(t)
	re := e.withRetention(t, nil, nil)
	code, raw := re.do(http.MethodGet, "/v1/meta/usage", re.adminToken, nil)
	mustStatus(t, code, raw, http.StatusServiceUnavailable)
	if got := errorCode(t, raw); got != "retention_unavailable" {
		t.Fatalf("error code = %q, want retention_unavailable; body: %s", got, raw)
	}
}

// TestUsageEndpointIs503WhenStatusFails: a Status error means "we cannot tell how
// full the disk is", which is a 503, not a payload full of zeros.
func TestUsageEndpointIs503WhenStatusFails(t *testing.T) {
	e := setup(t)
	re := e.withRetention(t, &fakeRetention{err: errors.New("statfs: no such file or directory")}, nil)
	code, raw := re.do(http.MethodGet, "/v1/meta/usage", re.adminToken, nil)
	mustStatus(t, code, raw, http.StatusServiceUnavailable)
	if got := errorCode(t, raw); got != "disk_usage_unavailable" {
		t.Fatalf("error code = %q, want disk_usage_unavailable; body: %s", got, raw)
	}
}
