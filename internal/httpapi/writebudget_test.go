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
// latency growth" clause as a deterministic in-process test rather than a load run.
//
// The criterion is the recorded admission DECISION and its accounting, not an
// elapsed time. For the whole saturated phase this test holds every slot of the
// budget itself and releases none until phase 2, so a publish that answered 202
// there could only have waited for a slot the test had not freed - the parking on
// the pgx pool S10 measured as 10.2 s at pool 10, and as a p99 growing from
// 3,593.4 ms to 4,281.6 ms over a 30-minute run. So while the budget is full every
// response must be 429 `over_capacity` with Retry-After: 1, the shed counter must
// advance by exactly one per shed, a shed must write neither row, and the budget
// must still report every slot held; once the slots are freed every response must
// be 202 and commit BOTH rows (FR-604), with the shed counter frozen and no slot
// left in flight. A path that parked instead of deciding cannot satisfy that, and
// nothing here depends on how fast (or how busy) the box is.
//
// This replaced an elapsed bound (100 ms admission deadline + 200 ms of slack) that
// measured the box rather than the mechanism: on a 2 vCPU host sharing a second
// full `-race` suite it reddened with `a admitted publish took 377.477348ms, past
// the 100ms admission deadline (+200ms)` on a tree whose httpapi/writeq files were
// byte-identical to the ones CI ran green in 0.45 s (runs 40955358597,
// 40963661270), while the same test in isolation passed at 74.8 ms. The pair that
// settled it (card t_2dbef0fb, 24 fresh-process runs per side, one load window, one
// 2 vCPU box): the old bound 18 red / 24; the criterion below 0 red / 24 - and 17 of
// those 24 runs logged a leg over the old 300 ms ceiling (worst 1387.5 ms), so on
// that box the old bound's verdict came from throughput, not from the mechanism.
// Widening the constant was not an option (§12: a timing flake becomes a state
// assertion, never a looser threshold); the latencies are still measured and logged
// below as diagnostics - a reading of the box, never a pass/fail criterion.
//
// Not repeatable in place, like every test in this package: setup() provisions ONE
// shared env per process (`sharedOnce`) and the fixtures use fixed slugs, so a
// second invocation of this name in the same process re-seeds `wq-latency` into the
// rows the first left behind and fails inside `seedTenant` with
// `409 ... "that resource already exists"`. One invocation per fresh database is
// the supported shape for internal/httpapi; that red is a property of the harness,
// not of the criterion below.
func TestPublishLatencyIsBoundedUnderSaturation(t *testing.T) {
	e := setup(t)
	tn := e.seedTenant(t, "wq-latency")
	createType(t, e, tn, "payment.succeeded")

	const admit = 100 * time.Millisecond

	m := metrics.New()
	q := store.NewWriteQueue(e.app, writeq.Options{
		Depth: 2, Workers: 1, Pending: 4, AdmitTimeout: admit,
	})
	t.Cleanup(q.Close)
	re := e.withWriteBudget(t, q, m)

	// The client's timeout is a liveness guard, NOT a latency criterion: the
	// admission deadline is asserted through the decision (a 429 while the budget is
	// full), and this exists only so that a path which never decides fails with a
	// message in seconds instead of hanging the package until Go's aggregate
	// -timeout. It is 300x the deadline and fires only when no decision is returned
	// at all, which is why it cannot red on a merely busy box the way the bound it
	// replaces did.
	client := &http.Client{Timeout: 30 * time.Second}
	post := func() (code int, raw []byte, retryAfter string, took time.Duration) {
		t.Helper()
		body, err := json.Marshal(map[string]any{
			"event_type": "payment.succeeded",
			"payload":    map[string]any{"amount": 1},
		})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		req, err := http.NewRequest(http.MethodPost, re.srv.URL+"/v1/events", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer "+tn.KeyPlain)
		req.Header.Set("Content-Type", "application/json")
		start := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("a publish on a saturated write path returned no decision: %v (every response must be 202 or 429, never a park on the pool)", err)
		}
		defer resp.Body.Close()
		raw, err = io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read response: %v", err)
		}
		return resp.StatusCode, raw, resp.Header.Get("Retry-After"), time.Since(start)
	}

	// Phase 1: the whole budget is held for the entire phase, and only phase 2
	// releases it - so the only decision this path may record is "shed".
	full := q.Stats().Depth
	var held []writeq.Release
	for i := 0; i < full; i++ {
		rel, err := q.Acquire(ctx())
		if err != nil {
			t.Fatalf("Acquire: %v", err)
		}
		held = append(held, rel)
	}
	eventsBefore, outboxBefore := countEvents(t, re, tn.ID), countOutbox(t, re, tn.ID)

	const saturated = 3
	var maxShed time.Duration
	for i := 0; i < saturated; i++ {
		code, raw, retryAfter, took := post()
		if took > maxShed {
			maxShed = took
		}
		if code != http.StatusTooManyRequests {
			t.Fatalf("saturated publish %d answered %d, want 429: with every slot of the budget already held, a shed is the only decision this path may record - serving it is the parking on the pool FR-818 removes; body: %s", i+1, code, raw)
		}
		if got := errorCode(t, raw); got != "over_capacity" {
			t.Fatalf("saturated publish %d answered code %q, want over_capacity: a full write budget is neither disk pressure (over_budget) nor a rate limit (rate_limited); body: %s", i+1, got, raw)
		}
		if retryAfter != "1" {
			t.Fatalf("saturated publish %d carries Retry-After %q, want \"1\": write pressure clears in the time a transaction takes", i+1, retryAfter)
		}
	}
	// The budget is still full after every decision, so no publish was admitted
	// behind the test's back and no slot leaked; the sheds above are the budget's
	// state and nothing else.
	if got := q.Stats().InFlight; got != full {
		t.Fatalf("the budget reports %d slot(s) in flight after %d sheds with %d held, want %d: a shed publish took a slot", got, saturated, full, full)
	}
	if got := m.ShedCount(); got != saturated {
		t.Fatalf("shed counter = %d after %d saturated publishes, want %d: the recorded decision and its accounting must agree", got, saturated, saturated)
	}
	if n := countEvents(t, re, tn.ID); n != eventsBefore {
		t.Fatalf("events = %d, want %d: a shed publish must not write (FR-818)", n, eventsBefore)
	}
	if n := countOutbox(t, re, tn.ID); n != outboxBefore {
		t.Fatalf("outbox = %d, want %d: a shed publish must not write (FR-604/FR-818)", n, outboxBefore)
	}

	// Phase 2: the budget frees and the same request is served, so the test also
	// proves the shed was the budget's state and not a permanently broken path.
	for _, rel := range held {
		rel()
	}
	const admitted = 2
	var maxAdmitted time.Duration
	for i := 0; i < admitted; i++ {
		code, raw, _, took := post()
		if took > maxAdmitted {
			maxAdmitted = took
		}
		mustStatus(t, code, raw, http.StatusAccepted)
	}
	if n := countEvents(t, re, tn.ID); n != eventsBefore+admitted {
		t.Fatalf("events = %d, want %d: every admitted publish has its event row", n, eventsBefore+admitted)
	}
	if n := countOutbox(t, re, tn.ID); n != outboxBefore+admitted {
		t.Fatalf("outbox = %d, want %d: every admitted publish has its outbox row (FR-604)", n, outboxBefore+admitted)
	}
	if got := m.ShedCount(); got != saturated {
		t.Fatalf("shed counter = %d after %d admitted publishes, want %d: admission must not shed", got, admitted, saturated)
	}
	// The handler releases its slot on the way out (`defer release()`), which is
	// after it wrote the response the client has already read - so wait (bounded)
	// for a post-condition that is guaranteed to arrive rather than racing it. That
	// is a bounded wait for a state, not a tolerance on how long a write takes.
	released := time.Now().Add(30 * time.Second)
	for q.Stats().InFlight != 0 {
		if time.Now().After(released) {
			t.Fatalf("the budget still reports %d slot(s) in flight after every admitted publish returned: an admitted publish leaked its slot", q.Stats().InFlight)
		}
		time.Sleep(time.Millisecond)
	}

	t.Logf("publish decisions under a %s admission deadline: %d shed (max %s), %d admitted (max %s) - diagnostics of the box; the criterion is the decisions and their accounting",
		admit, saturated, maxShed, admitted, maxAdmitted)
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
