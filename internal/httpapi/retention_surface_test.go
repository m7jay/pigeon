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
		RetentionDays: map[string]int{"deliveries": 14, "attempts": 7},
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
