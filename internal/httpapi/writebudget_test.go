package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/m7jay/pigeon/internal/metrics"
	"github.com/m7jay/pigeon/internal/ratelimit"
	"github.com/m7jay/pigeon/internal/store"
	"github.com/m7jay/pigeon/internal/writeq"
)

// This file is FR-818's ingest half (SPEC.md A-11) against the real handler over
// a real database: the publish path is admitted by the process's write budget or
// shed with 429 `over_capacity`, and it never grows latency silently instead.
//
// The budget here is a real *writeq.Queue, not a stub: the saturation is produced
// by holding its slots, exactly the state a full write path is in, and the
// handler reaches it through the same Deps field main.go wires.

// withWriteBudget builds a second API over the same database with FR-818's write
// budget attached, the way main.go attaches the one instance the store's workers
// submit to.
func (e *env) withWriteBudget(t *testing.T, q *writeq.Queue, m *metrics.Metrics) *env {
	t.Helper()
	api := New(Deps{
		Lookup:          e.st.LookupAPIKey,
		Sessions:        e.st.LookupSession,
		Data:            e.st,
		Version:         "test-write-budget",
		SessionTTL:      time.Hour,
		KeyLimiter:      ratelimit.New(1000, 1000),
		ProducerLimiter: ratelimit.New(1000, 1000),
		Metrics:         m,
		Writes:          q,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	out := &env{admin: e.admin, app: e.app, st: e.st, api: api, adminToken: e.adminToken}
	out.srv = httptest.NewServer(api.Handler())
	t.Cleanup(out.srv.Close)
	return out
}

// countEvents and countOutbox are scoped to one producer, because every test in
// this package shares one database and the same event-type names.
func countEvents(t *testing.T, e *env, producerID string) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(ctx(), `select count(*) from events where producer_id = $1`, producerID).Scan(&n); err != nil {
		t.Fatalf("count events: %v", err)
	}
	return n
}

func countOutbox(t *testing.T, e *env, producerID string) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(ctx(),
		`select count(*) from outbox o join events ev on ev.id = o.event_id where ev.producer_id = $1`,
		producerID).Scan(&n); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	return n
}

// TestPublishShedsWith429WhenTheWritePathIsSaturated is AC-1. With every slot of
// the budget held by a writer that has not committed, a publish is refused with
// 429 `over_capacity` (not SB-7's `over_budget`), carries a Retry-After, advances
// the shed counter, and writes NOTHING - no event row, no outbox row. A publish
// that returns 202 always has both.
func TestPublishShedsWith429WhenTheWritePathIsSaturated(t *testing.T) {
	e := setup(t)
	tn := e.seedTenant(t, "wq-shed")
	createType(t, e, tn, "payment.succeeded")

	m := metrics.New()
	q := store.NewWriteQueue(e.app, writeq.Options{
		Depth: 1, Workers: 1, Pending: 4, AdmitTimeout: 100 * time.Millisecond,
	})
	t.Cleanup(q.Close)
	// Every slot held by a writer inside its transaction: the saturation itself.
	held, err := q.Acquire(ctx())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	re := e.withWriteBudget(t, q, m)

	eventsBefore, outboxBefore := countEvents(t, re, tn.ID), countOutbox(t, re, tn.ID)
	body := map[string]any{"event_type": "payment.succeeded", "payload": map[string]any{"amount": 100}}

	code, raw := publish(t, re, tn.KeyPlain, body)
	mustStatus(t, code, raw, http.StatusTooManyRequests)
	if got := errorCode(t, raw); got != "over_capacity" {
		t.Fatalf("error code = %q, want over_capacity; an operator must be able to tell write pressure from disk pressure (over_budget); body: %s", got, raw)
	}
	if got := re.lastRetryAfter(); got == "" {
		t.Fatal("the shed response carries no Retry-After: a producer has no way to know when to come back")
	}
	if got := m.ShedCount(); got != 1 {
		t.Fatalf("shed count = %d, want 1 (SB-6's counter is the alert rule's signal)", got)
	}
	if n := countEvents(t, re, tn.ID); n != eventsBefore {
		t.Fatalf("events = %d, want %d: a shed publish must not write (FR-818)", n, eventsBefore)
	}
	if n := countOutbox(t, re, tn.ID); n != outboxBefore {
		t.Fatalf("outbox = %d, want %d: a shed publish must not write (FR-604/FR-818)", n, outboxBefore)
	}

	// And the same request, once a slot frees, is accepted with both rows: the
	// shed is the budget, not the request.
	held()
	code, raw = publish(t, re, tn.KeyPlain, body)
	mustStatus(t, code, raw, http.StatusAccepted)
	if n := countEvents(t, re, tn.ID); n != eventsBefore+1 {
		t.Fatalf("events = %d, want %d: a 202 must have its event row", n, eventsBefore+1)
	}
	if n := countOutbox(t, re, tn.ID); n != outboxBefore+1 {
		t.Fatalf("outbox = %d, want %d: a 202 must have its outbox row (FR-604)", n, outboxBefore+1)
	}
	if got := m.ShedCount(); got != 1 {
		t.Fatalf("shed count = %d after a successful publish, want 1", got)
	}
}

// TestPublishLatencyIsBoundedUnderSaturation is AC-4, which is AC-30's "no silent
// latency growth" clause as a deterministic in-process test rather than a load
// run: under saturation every response is a 202 within the admission deadline or
// a 429, and the worst latency observed is the deadline - never the unbounded
// wait on the pool that S10 measured as 10.2 s at pool 10 and as a p99 growing
// from 3,593.4 ms to 4,281.6 ms over a 30-minute run.
func TestPublishLatencyIsBoundedUnderSaturation(t *testing.T) {
	e := setup(t)
	tn := e.seedTenant(t, "wq-latency")
	createType(t, e, tn, "payment.succeeded")

	const admit = 100 * time.Millisecond
	// A slow box adds scheduling overhead to a timer that was set for `admit`; the
	// bound below still separates "waited its deadline" from "parked on the pool",
	// which is two orders of magnitude apart in the measurement this exists for.
	const epsilon = 200 * time.Millisecond

	m := metrics.New()
	q := store.NewWriteQueue(e.app, writeq.Options{
		Depth: 2, Workers: 1, Pending: 4, AdmitTimeout: admit,
	})
	t.Cleanup(q.Close)
	re := e.withWriteBudget(t, q, m)

	post := func() (int, time.Duration) {
		t.Helper()
		raw, err := json.Marshal(map[string]any{
			"event_type": "payment.succeeded",
			"payload":    map[string]any{"amount": 1},
		})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		req, err := http.NewRequest(http.MethodPost, re.srv.URL+"/v1/events", bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer "+tn.KeyPlain)
		req.Header.Set("Content-Type", "application/json")
		start := time.Now()
		resp, err := re.srv.Client().Do(req)
		if err != nil {
			t.Fatalf("publish: %v", err)
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode, time.Since(start)
	}

	// Phase 1: every slot held, so a publish can only be shed.
	var held []writeq.Release
	for i := 0; i < 2; i++ {
		rel, err := q.Acquire(ctx())
		if err != nil {
			t.Fatalf("Acquire: %v", err)
		}
		held = append(held, rel)
	}
	type observed struct {
		code    int
		latency time.Duration
		phase   string
	}
	var results []observed
	for i := 0; i < 3; i++ {
		code, d := post()
		results = append(results, observed{code, d, "saturated"})
	}

	// Phase 2: the budget frees and the same request is served, so the test also
	// proves the shed was the budget's state and not a permanently broken path.
	for _, rel := range held {
		rel()
	}
	for i := 0; i < 2; i++ {
		code, d := post()
		results = append(results, observed{code, d, "admitted"})
	}

	var maxAdmitted, maxShed time.Duration
	admitted, shed := 0, 0
	for _, r := range results {
		switch r.code {
		case http.StatusAccepted:
			admitted++
			if r.latency > maxAdmitted {
				maxAdmitted = r.latency
			}
		case http.StatusTooManyRequests:
			shed++
			if r.latency > maxShed {
				maxShed = r.latency
			}
		default:
			t.Fatalf("a publish under saturation answered %d; every response must be 202 or 429", r.code)
		}
		if r.latency > admit+epsilon {
			t.Fatalf("a %s publish took %s, past the %s admission deadline (+%s): this is the silent latency growth FR-818 exists to remove",
				r.phase, r.latency, admit, epsilon)
		}
	}
	if shed == 0 {
		t.Fatal("a saturated write path shed nothing: the budget is not refusing ingest")
	}
	if admitted == 0 {
		t.Fatal("nothing was admitted once the budget freed: the shed is not a budget state, it is a broken path")
	}
	if got := m.ShedCount(); int64(got) != int64(shed) {
		t.Fatalf("shed counter = %d, want %d", got, shed)
	}
	t.Logf("publish latency under a %s admission deadline: max admitted %s, max shed %s (%d admitted, %d shed)",
		admit, maxAdmitted, maxShed, admitted, shed)
}

// TestDiskGateTakesPrecedenceOverWritePressure keeps the two 429 codes
// distinguishable, which is the whole reason `over_capacity` is not
// `over_budget`: when the volume is over its budget AND the write path is full,
// the producer is told about the disk, because that is the condition it can do
// nothing about but wait out.
func TestDiskGateTakesPrecedenceOverWritePressure(t *testing.T) {
	e := setup(t)
	tn := e.seedTenant(t, "wq-precedence")
	createType(t, e, tn, "payment.succeeded")

	m := metrics.New()
	q := store.NewWriteQueue(e.app, writeq.Options{
		Depth: 1, Workers: 1, Pending: 4, AdmitTimeout: 50 * time.Millisecond,
	})
	t.Cleanup(q.Close)
	held, err := q.Acquire(ctx())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer held()

	re := e.withRetention(t, &fakeRetention{over: true}, m)
	re = withWrites(t, re, q)

	code, raw := publish(t, re, tn.KeyPlain, map[string]any{
		"event_type": "payment.succeeded",
		"payload":    map[string]any{"amount": 1},
	})
	mustStatus(t, code, raw, http.StatusTooManyRequests)
	if got := errorCode(t, raw); got != "over_budget" {
		t.Fatalf("error code = %q, want over_budget: SB-7's disk gate is checked first, and the two causes must stay tellable apart", got)
	}
}

// withWrites attaches a write budget to an already-built API by republishing it
// over the same database. It exists for the precedence test, which needs the
// retention surface and the budget on the same server.
func withWrites(t *testing.T, e *env, q *writeq.Queue) *env {
	t.Helper()
	deps := e.api.deps
	deps.Writes = q
	api := New(deps, slog.New(slog.NewTextHandler(io.Discard, nil)))
	out := &env{admin: e.admin, app: e.app, st: e.st, api: api, adminToken: e.adminToken}
	out.srv = httptest.NewServer(api.Handler())
	t.Cleanup(out.srv.Close)
	return out
}
