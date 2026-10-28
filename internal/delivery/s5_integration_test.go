package delivery

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/m7jay/pigeon/internal/config"
	"github.com/m7jay/pigeon/internal/limit"
	"github.com/m7jay/pigeon/internal/pause"
	"github.com/m7jay/pigeon/internal/store"
)

// S5's integration coverage: the pause thresholds on a real Postgres (AC-14), the
// retry window that ends a delivery (AC-36), the per-endpoint window that governs
// over the global default (AC-37), and the cluster-wide concurrency cap under two
// replicas (AC-41).
//
// The pause COUNTERS are Redis, so these run against miniredis rather than a
// fixture that fakes the tracker: the Lua that trims the sliding hour and derives
// breadth membership is the mechanism under test, and a stub would test the stub.
// The durable pause flag, the notification record and the retry window are all
// Postgres, which is the real thing.

// s5Fixture is one seeded endpoint with its subscriptions, plus the Redis the
// pause counters and the per-endpoint limits share.
type s5Fixture struct {
	st          *store.Store
	admin       *pgxpool.Pool
	sc          store.Scope
	endpointID  string
	consumerRef string
	counters    *pause.Tracker
	rdb         *redis.Client
	limits      *limit.Redis
}

// newS5Fixture truncates the routing tables and builds a producer, a consumer, a
// verified endpoint pointing at url, and one subscription per event type.
func newS5Fixture(t *testing.T, url string, eventTypes ...string) *s5Fixture {
	t.Helper()
	ctx := context.Background()
	st, admin := harness(t)
	if _, err := admin.Exec(ctx,
		`truncate table events, outbox, event_types, consumers, endpoints, subscriptions, deliveries cascade`); err != nil {
		t.Fatalf("reset routing tables: %v", err)
	}
	const slug = "s5del"
	pid := "prd_" + slug
	if _, err := admin.Exec(ctx,
		`insert into producers (id, slug, name) values ($1,$2,$3) on conflict do nothing`,
		pid, slug, "Producer "+slug); err != nil {
		t.Fatalf("insert producer: %v", err)
	}
	sc := store.Scope{ProducerID: pid, Env: "test"}
	con, err := st.CreateConsumer(ctx, sc, "school_1", "School 1")
	if err != nil {
		t.Fatalf("CreateConsumer: %v", err)
	}
	created, err := st.CreateEndpoint(ctx, sc, store.NewEndpoint{
		ConsumerID: con.ID, Name: "e1", URL: url, Internal: true,
	})
	if err != nil {
		t.Fatalf("CreateEndpoint: %v", err)
	}
	if err := st.VerifyEndpoint(ctx, sc, created.Endpoint.ID, created.VerificationToken); err != nil {
		t.Fatalf("VerifyEndpoint: %v", err)
	}
	for _, et := range eventTypes {
		if _, err := st.CreateEventType(ctx, sc, store.NewEventType{
			Name: et, RoutingMode: store.RoutingConsumer, PayloadSchema: json.RawMessage(`{}`),
		}); err != nil {
			t.Fatalf("CreateEventType(%s): %v", et, err)
		}
		if _, err := st.CreateSubscription(ctx, sc, store.NewSubscription{
			EndpointID: created.Endpoint.ID, EventType: et,
		}); err != nil {
			t.Fatalf("CreateSubscription(%s): %v", et, err)
		}
	}

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	lim, err := limit.NewRedisFrom(rdb)
	if err != nil {
		t.Fatalf("limit.NewRedisFrom: %v", err)
	}
	return &s5Fixture{
		st: st, admin: admin, sc: sc, endpointID: created.Endpoint.ID,
		consumerRef: con.Ref,
		counters:    pause.NewTracker(rdb, pause.DefaultThresholds()),
		rdb:         rdb, limits: lim,
	}
}

// options builds worker options on the fast-but-real defaults, with the tracker
// wired in and the caller's adjustments applied.
func (f *s5Fixture) options(mutate func(*Options)) Options {
	opts := testOptions(5*time.Second, time.Millisecond, 4*time.Millisecond)
	opts.Pauses = f.counters
	// Jitter off: the schedule is then exactly §9's, which is what makes an
	// assertion about "the next attempt landed inside the window" deterministic.
	opts.Jitter = 0
	if mutate != nil {
		mutate(&opts)
	}
	return opts
}

// publish registers one event of eventType and fans it out, returning the event id
// and the delivery it produced for the fixture's endpoint.
func (f *s5Fixture) publish(t *testing.T, eventType, payload string) (string, int64) {
	t.Helper()
	ctx := context.Background()
	ref := f.consumerRef
	pub, err := f.st.PublishEvent(ctx, f.sc, store.PublishInput{
		EventType: eventType, Payload: json.RawMessage(payload), ConsumerRef: &ref,
	})
	if err != nil {
		t.Fatalf("PublishEvent(%s): %v", eventType, err)
	}
	if _, err := f.st.FanOutEvent(ctx, f.sc, pub.ID); err != nil {
		t.Fatalf("FanOutEvent(%s): %v", eventType, err)
	}
	var deliveryID int64
	if err := f.admin.QueryRow(ctx,
		`select id from deliveries where event_id=$1 and endpoint_id=$2`,
		pub.ID, f.endpointID).Scan(&deliveryID); err != nil {
		t.Fatalf("resolve delivery for %s: %v", eventType, err)
	}
	return pub.ID, deliveryID
}

func (f *s5Fixture) deliveryStatus(t *testing.T, deliveryID int64) string {
	t.Helper()
	var status string
	if err := f.admin.QueryRow(context.Background(),
		`select status from deliveries where id=$1`, deliveryID).Scan(&status); err != nil {
		t.Fatalf("read delivery status: %v", err)
	}
	return status
}

func (f *s5Fixture) attemptCount(t *testing.T, deliveryID int64) int {
	t.Helper()
	var n int
	if err := f.admin.QueryRow(context.Background(),
		`select count(*) from attempts where delivery_id=$1`, deliveryID).Scan(&n); err != nil {
		t.Fatalf("count attempts: %v", err)
	}
	return n
}

func (f *s5Fixture) deliveryCount(t *testing.T) int {
	t.Helper()
	var n int
	if err := f.admin.QueryRow(context.Background(),
		`select count(*) from deliveries`).Scan(&n); err != nil {
		t.Fatalf("count deliveries: %v", err)
	}
	return n
}

// nextAttemptAt reports whether the delivery has a next attempt and no retry
// deadline left behind it (FR-1208).
func (f *s5Fixture) nextAttemptAt(t *testing.T, deliveryID int64) sql.NullTime {
	t.Helper()
	var next sql.NullTime
	if err := f.admin.QueryRow(context.Background(),
		`select next_attempt_at from deliveries where id=$1`, deliveryID).Scan(&next); err != nil {
		t.Fatalf("read next_attempt_at: %v", err)
	}
	return next
}

// retryWindowUsed is retry_deadline - created_at: the window the platform actually
// granted this delivery, which is how AC-37's "the per-endpoint window governs" is
// observable rather than inferred.
func (f *s5Fixture) retryWindowUsed(t *testing.T, deliveryID int64) time.Duration {
	t.Helper()
	var secs float64
	if err := f.admin.QueryRow(context.Background(),
		`select (extract(epoch from (retry_deadline - created_at)))::double precision
		 from deliveries where id=$1`, deliveryID).Scan(&secs); err != nil {
		t.Fatalf("read retry window: %v", err)
	}
	return time.Duration(secs * float64(time.Second))
}

// createdAt is the instant the delivery row was written, which the claim measures
// the granted retry window from.
func (f *s5Fixture) createdAt(t *testing.T, deliveryID int64) time.Time {
	t.Helper()
	var at time.Time
	if err := f.admin.QueryRow(context.Background(),
		`select created_at from deliveries where id=$1`, deliveryID).Scan(&at); err != nil {
		t.Fatalf("read created_at: %v", err)
	}
	return at
}

// retryDeadline is the durable deadline FR-1201/FR-1207/FR-1208 expose. It is
// stamped on the first failure, so a delivery that has never failed has none.
func (f *s5Fixture) retryDeadline(t *testing.T, deliveryID int64) time.Time {
	t.Helper()
	var at sql.NullTime
	if err := f.admin.QueryRow(context.Background(),
		`select retry_deadline from deliveries where id=$1`, deliveryID).Scan(&at); err != nil {
		t.Fatalf("read retry_deadline: %v", err)
	}
	if !at.Valid {
		t.Fatalf("delivery %d has no retry_deadline; a failure should have stamped it", deliveryID)
	}
	return at.Time
}

func (f *s5Fixture) subscriptionStatus(t *testing.T, eventType string) (string, string) {
	t.Helper()
	var status string
	var reason sql.NullString
	if err := f.admin.QueryRow(context.Background(),
		`select status, pause_reason from subscriptions where endpoint_id=$1 and event_type=$2`,
		f.endpointID, eventType).Scan(&status, &reason); err != nil {
		t.Fatalf("read subscription %s: %v", eventType, err)
	}
	return status, reason.String
}

func (f *s5Fixture) endpointStatus(t *testing.T) (string, string) {
	t.Helper()
	var status string
	var reason sql.NullString
	if err := f.admin.QueryRow(context.Background(),
		`select status, pause_reason from endpoints where id=$1`, f.endpointID).Scan(&status, &reason); err != nil {
		t.Fatalf("read endpoint: %v", err)
	}
	return status, reason.String
}

func (f *s5Fixture) setEndpoint(t *testing.T, assignment string) {
	t.Helper()
	if _, err := f.admin.Exec(context.Background(),
		`update endpoints set `+assignment+` where id=$1`, f.endpointID); err != nil {
		t.Fatalf("update endpoint (%s): %v", assignment, err)
	}
}

// drive runs the worker until the condition holds or the budget is spent, which is
// how the sequential tests advance a retry schedule without guessing at sleeps.
func drive(t *testing.T, w *Worker, budget int, until func() bool) bool {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < budget; i++ {
		if _, err := w.ProcessOne(ctx); err != nil {
			t.Fatalf("ProcessOne: %v", err)
		}
		if until() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return until()
}

// ---------------------------------------------------------------------------
// AC-14 / FR-1202 / FR-1204 / FR-1205 / FR-1211
// ---------------------------------------------------------------------------

// TestAC14SubscriptionPauseAndResume: ten failed attempts for one
// (endpoint, event_type) inside the hour pause that subscription, notify once
// naming endpoint, event type and the first and last failing response, leave the
// endpoint's other subscriptions alone - and a resume clears the window so the
// next matching event delivers.
func TestAC14SubscriptionPauseAndResume(t *testing.T) {
	const failing, sibling = "order.created", "order.shipped"
	var healthy atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var env struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal(body, &env)
		if env.Type == failing && !healthy.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, "receiver down")
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	f := newS5Fixture(t, srv.URL, failing, sibling)
	_, initial := f.publish(t, failing, `{"id":"o1"}`)
	w := New(f.st, f.options(nil))
	ctx := context.Background()

	// 10 failures at §9's shape but a millisecond scale: the depth threshold is
	// what is under test, not the wall clock.
	if !drive(t, w, 200, func() bool {
		status, _ := f.subscriptionStatus(t, failing)
		return status == "paused"
	}) {
		status, _ := f.subscriptionStatus(t, failing)
		t.Fatalf("subscription %s = %q after %d attempts; FR-1202 wants it paused at 10 failures",
			failing, status, f.attemptCount(t, initial))
	}

	attempts := f.attemptCount(t, initial)
	if attempts < 10 {
		t.Fatalf("%d failed attempts before the pause, want at least 10 (FR-1202)", attempts)
	}
	if status, reason := f.subscriptionStatus(t, failing); status != "paused" || reason != "failure_threshold" {
		t.Fatalf("subscription %s = (%q, %q), want (paused, failure_threshold)", failing, status, reason)
	}
	// The sibling subscription on the same endpoint is untouched: the pause is
	// per (endpoint, event_type), not per endpoint (AC-14).
	if status, _ := f.subscriptionStatus(t, sibling); status != "active" {
		t.Errorf("subscription %s = %q, want active: one bad event type must not pause its siblings", sibling, status)
	}
	// FR-1203's boundary: one subscription is below `active >= 2`, so the endpoint
	// level does not apply and the endpoint is NOT paused.
	if status, _ := f.endpointStatus(t); status != "active" {
		t.Errorf("endpoint = %q, want active: breadth needs at least two subscriptions (FR-1203)", status)
	}

	// FR-1204: exactly one notification, naming the endpoint, the event type and
	// the first and last failing response.
	notes, err := f.st.PauseNotifications(ctx, f.endpointID, 10)
	if err != nil {
		t.Fatalf("PauseNotifications: %v", err)
	}
	if len(notes) != 1 {
		t.Fatalf("%d pause notifications, want exactly 1 (FR-1204): %+v", len(notes), notes)
	}
	n := notes[0]
	if n.Kind != "subscription" || n.EventType != failing || n.EndpointID != f.endpointID {
		t.Errorf("notification = %+v, want a subscription pause for %s on %s", n, failing, f.endpointID)
	}
	if n.FirstResponse == "" || n.LastResponse == "" {
		t.Errorf("notification quotes first=%q last=%q; FR-1204 requires both failing responses", n.FirstResponse, n.LastResponse)
	}
	// A second failure inside the same episode must not produce a second message:
	// the transition is one-shot and the notification row is written with it.
	if p, err := f.st.PauseSubscriptionForFailures(ctx, f.endpointID, failing); err != nil {
		t.Fatalf("second PauseSubscriptionForFailures: %v", err)
	} else if p.Transitioned {
		t.Error("a second pause inside the same episode reported a transition; FR-1204 says notify once")
	}
	if notes, _ := f.st.PauseNotifications(ctx, f.endpointID, 10); len(notes) != 1 {
		t.Errorf("%d notifications after a repeat call, want 1 (FR-1204)", len(notes))
	}

	// A paused subscription collects no new attempts: the delivery is held, not
	// dropped (FR-1202), which is what makes the resume below meaningful.
	deliveriesBefore := f.deliveryCount(t)
	if did, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne while paused: %v", err)
	} else if did {
		t.Error("the worker claimed a delivery for a paused subscription; FR-1202 says it collects none")
	}

	// FR-1207: the pause must not consume the delivery's remaining window, so the
	// held delivery's deadline is pushed forward by exactly the paused period.
	deadlineBefore := f.retryDeadline(t, initial)
	time.Sleep(25 * time.Millisecond)

	// FR-1205/FR-1211: one action, no replay - the resume clears the flag and the
	// counter state, and creates nothing.
	resumer := pause.NewResumer(f.st, f.counters, 0)
	out, err := resumer.ResumeSubscription(ctx, f.endpointID, failing)
	if err != nil {
		t.Fatalf("ResumeSubscription: %v", err)
	}
	if !out.Resumed {
		t.Fatal("resume reported no transition on a paused subscription (FR-1205)")
	}
	if got := f.deliveryCount(t); got != deliveriesBefore {
		t.Errorf("delivery count %d -> %d across a resume; FR-1205 forbids auto-replay", deliveriesBefore, got)
	}
	if pausedFor := out.PausedFor; pausedFor < 20*time.Millisecond {
		t.Errorf("resume reported a paused period of %s, want the ~25 ms the scope was paused", pausedFor)
	}
	deadlineAfter := f.retryDeadline(t, initial)
	if !deadlineAfter.After(deadlineBefore) {
		t.Errorf("retry_deadline %s -> %s across a resume; FR-1207 extends it by the paused period",
			deadlineBefore, deadlineAfter)
	}
	if status, reason := f.subscriptionStatus(t, failing); status != "active" || reason != "" {
		t.Fatalf("subscription after resume = (%q, %q), want (active, none)", status, reason)
	}
	// FR-1211: the window and the breadth membership are gone, so the subscription
	// re-qualifies only after ten fresh failures.
	if v, err := f.counters.FailingSubscriptions(ctx, f.endpointID, time.Now()); err != nil || v != 0 {
		t.Errorf("failing subscriptions after resume = %d (err %v), want 0 (FR-1211)", v, err)
	}
	depth, err := f.counters.Observe(ctx, pause.Observation{
		EndpointID: f.endpointID, EventType: failing, At: time.Now()})
	if err != nil {
		t.Fatalf("Observe after resume: %v", err)
	}
	if depth.Depth != 1 {
		t.Errorf("depth of the first failure after a resume = %d, want 1: the window was cleared (FR-1211)", depth.Depth)
	}

	// And the next matching event delivers, as does the delivery that was held.
	healthy.Store(true)
	_, next := f.publish(t, failing, `{"id":"o2"}`)
	for _, id := range []int64{initial, next} {
		if !drive(t, w, 100, func() bool { return f.deliveryStatus(t, id) == "delivered" }) {
			t.Fatalf("delivery %d = %q after the resume, want delivered", id, f.deliveryStatus(t, id))
		}
	}
}

// ---------------------------------------------------------------------------
// AC-36 / FR-1201: the retry window ends the delivery
// ---------------------------------------------------------------------------

// TestAC36RetryWindowEndsTheDelivery: a delivery whose schedule would land beyond
// its retry window becomes failed_terminal there, with retries left, and keeps its
// row for replay.
//
// The window is 250 ms rather than 72 h so the test is a test; the arithmetic that
// makes 72 h terminate at 16 attempts is verified at the real numbers in
// retry_test.go's TestAC36WindowBoundaryTerminalWithinSixteenAttempts. What this
// adds is the durable half: the deadline on the row, the terminal transition, and
// the absence of a next attempt.
func TestAC36RetryWindowEndsTheDelivery(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	f := newS5Fixture(t, srv.URL, "order.created")
	_, deliveryID := f.publish(t, "order.created", `{"id":"o1"}`)

	const window = 250 * time.Millisecond
	w := New(f.st, f.options(func(o *Options) {
		o.RetryWindow = window
		o.BackoffBase = 100 * time.Millisecond
		o.BackoffMax = 100 * time.Millisecond
	}))

	if !drive(t, w, 100, func() bool { return f.deliveryStatus(t, deliveryID) == "failed_terminal" }) {
		t.Fatalf("delivery = %q, want failed_terminal at the window boundary (FR-1201)",
			f.deliveryStatus(t, deliveryID))
	}
	attempts := f.attemptCount(t, deliveryID)
	if attempts < 2 {
		t.Errorf("%d attempts before the window closed, want at least 2: the first schedule fits and the second does not", attempts)
	}
	if attempts > 16 {
		t.Errorf("%d attempts, want <= 16 (AC-36)", attempts)
	}
	if attempts == w.opts.MaxRetries+1 {
		t.Errorf("%d attempts means the retry COUNT ended this, not the window; FR-1201 gives the window precedence", attempts)
	}
	// FR-1208: a terminal delivery has no next attempt.
	if next := f.nextAttemptAt(t, deliveryID); next.Valid {
		t.Errorf("next_attempt_at = %v on a terminal delivery, want null", next.Time)
	}
	// The deadline the platform granted is the one it was configured with, and the
	// row still exists, so the delivery stays replayable (FR-1201, FR-1206).
	if used := f.retryWindowUsed(t, deliveryID); used < window-time.Second || used > window+time.Second {
		t.Errorf("retry window used = %s, want ~%s (§9's window governs the delivery)", used, window)
	}
	if f.deliveryStatus(t, deliveryID) == "deleted" {
		t.Error("a terminally failed delivery must stay in the table for replay (FR-1201)")
	}
}

// TestAC37ClaimUsesTheEndpointWindow is AC-37's governance claim made observable:
// the deadline the claim hands the worker comes from the ENDPOINT's retry_window
// when the endpoint sets one, and from the deployment default when it does not.
func TestAC37ClaimUsesTheEndpointWindow(t *testing.T) {
	f := newS5Fixture(t, "http://127.0.0.1:1/unused", "order.created")
	ctx := context.Background()
	const deploymentWindow = 72 * time.Hour

	_, byDefault := f.publish(t, "order.created", `{"id":"o1"}`)
	d, got, err := f.st.ClaimDelivery(ctx, 40*time.Second, deploymentWindow)
	if err != nil || !got {
		t.Fatalf("ClaimDelivery: (got=%v, err=%v)", got, err)
	}
	// The claim's own deadline is what the worker schedules against, so it is what
	// AC-37 is about; the row's column is stamped from it on the first failure
	// (TestAC36RetryWindowEndsTheDelivery asserts that half).
	created := f.createdAt(t, byDefault)
	if got := d.RetryDeadline.Sub(created); got < deploymentWindow-time.Second || got > deploymentWindow+time.Second {
		t.Errorf("with no endpoint override the claimed deadline is %s out, want the deployment's %s", got, deploymentWindow)
	}

	// Now the endpoint's own window, which AC-37 says governs.
	f.setEndpoint(t, `retry_window = interval '1 hour', max_retries = 3, retry_backoff_base = interval '5 seconds'`)
	_, withOverride := f.publish(t, "order.created", `{"id":"o2"}`)
	// The first delivery is leased for 40 s by the claim above, so the next claim
	// is the new one.
	d2, got, err := f.st.ClaimDelivery(ctx, 40*time.Second, deploymentWindow)
	if err != nil || !got {
		t.Fatalf("ClaimDelivery for the override: (got=%v, err=%v)", got, err)
	}
	if d2.DeliveryID != withOverride {
		t.Fatalf("claimed delivery %d, want %d", d2.DeliveryID, withOverride)
	}
	if got := d2.RetryDeadline.Sub(f.createdAt(t, withOverride)); got < time.Hour-time.Second || got > time.Hour+time.Second {
		t.Errorf("claimed deadline with a 1 h endpoint override = %s out, want ~1h: the endpoint governs (AC-37)", got)
	}
	// §9's other per-endpoint knobs travel with the claim.
	if d2.Endpoint.MaxRetries != 3 {
		t.Errorf("claimed max_retries = %d, want the endpoint's 3 (§9)", d2.Endpoint.MaxRetries)
	}
	if d2.Endpoint.BackoffBase != 5*time.Second {
		t.Errorf("claimed backoff base = %s, want the endpoint's 5s (§9)", d2.Endpoint.BackoffBase)
	}
	// And an endpoint with no override reports 0, which the worker reads as "use
	// the deployment default" rather than as a policy of zeros.
	if d.Endpoint.MaxRetries != 0 || d.Endpoint.MaxConcurrency != 0 || d.Endpoint.MaxRPS != 0 {
		t.Errorf("an endpoint with no overrides reported %+v; NULL must travel as 0 (inherit)", d.Endpoint)
	}
}

// ---------------------------------------------------------------------------
// FR-1203 / FR-1210 / FR-1211: the endpoint pauses on breadth, and an endpoint
// resume forgives exactly its former members
// ---------------------------------------------------------------------------

// TestEndpointBreadthPauseDoesNotTouchSubscriptions: three subscriptions that each
// accumulate three failures inside the hour trip N = min(3, max(3, ceil(3/4))) = 3
// and pause the ENDPOINT, before any of them reaches its own depth of ten. The
// endpoint pause writes no per-subscription rows (FR-1210), so resuming it restores
// all three and clears their windows (FR-1211) - and the deliveries were held, not
// dropped (FR-1205).
func TestEndpointBreadthPauseDoesNotTouchSubscriptions(t *testing.T) {
	types := []string{"order.created", "order.shipped", "order.delivered"}
	var healthy atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if !healthy.Load() {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = io.WriteString(w, "receiver down")
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	f := newS5Fixture(t, srv.URL, types...)
	held := make([]int64, 0, len(types))
	for _, et := range types {
		_, id := f.publish(t, et, `{"id":"b1"}`)
		held = append(held, id)
	}
	w := New(f.st, f.options(func(o *Options) {
		// A failed attempt's retry is pushed out far enough that the claim loop
		// reaches the endpoint's other subscriptions before it comes back to this
		// one, so the three accumulate three failures each (AC-44's 3 x 3 = 9)
		// instead of one subscription racing to its own depth of ten.
		o.BackoffBase = 60 * time.Millisecond
		o.BackoffMax = 60 * time.Millisecond
	}))
	ctx := context.Background()

	if !drive(t, w, 500, func() bool {
		status, _ := f.endpointStatus(t)
		return status == "paused"
	}) {
		status, reason := f.endpointStatus(t)
		t.Fatalf("endpoint = (%q, %q); FR-1203 wants it paused once %d subscriptions hold three failures each",
			status, reason, len(types))
	}
	if _, reason := f.endpointStatus(t); reason != "failure_threshold" {
		t.Errorf("endpoint pause_reason = %q, want failure_threshold", reason)
	}
	// The three subscriptions are untouched: the endpoint pause is not a fan of
	// per-subscription pauses (FR-1210).
	for _, et := range types {
		if status, _ := f.subscriptionStatus(t, et); status != "active" {
			t.Errorf("subscription %s = %q after an endpoint pause, want active (FR-1210 writes no subscription rows)",
				et, status)
		}
	}
	// No subscription reached ten: this trip was breadth, not depth.
	for _, id := range held {
		if n := f.attemptCount(t, id); n >= 10 {
			t.Errorf("delivery %d has %d attempts; the endpoint should have tripped at breadth before any depth of ten", id, n)
		}
	}

	notes, err := f.st.PauseNotifications(ctx, f.endpointID, 10)
	if err != nil {
		t.Fatalf("PauseNotifications: %v", err)
	}
	if len(notes) != 1 {
		t.Fatalf("%d notifications, want exactly 1 for the endpoint (FR-1204/FR-1210): %+v", len(notes), notes)
	}
	n := notes[0]
	if n.Kind != "endpoint" {
		t.Errorf("notification kind = %q, want endpoint", n.Kind)
	}
	if n.FailingSubscriptions != len(types) {
		t.Errorf("notification names %d failing subscriptions, want %d (FR-1204)", n.FailingSubscriptions, len(types))
	}
	if n.FirstResponse == "" || n.LastResponse == "" {
		t.Errorf("endpoint notification quotes first=%q last=%q; FR-1204 requires both", n.FirstResponse, n.LastResponse)
	}

	// While the endpoint is paused nothing on it is attempted.
	if did, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne while the endpoint is paused: %v", err)
	} else if did {
		t.Error("a delivery was claimed on a paused endpoint; FR-1202/FR-1203 hold its subscriptions")
	}

	// FR-1211: an endpoint resume deletes the breadth set AND every member's
	// window, so no member re-qualifies on one further failure.
	resumer := pause.NewResumer(f.st, f.counters, 0)
	out, err := resumer.ResumeEndpoint(ctx, f.endpointID)
	if err != nil {
		t.Fatalf("ResumeEndpoint: %v", err)
	}
	if !out.Resumed {
		t.Fatal("resume reported no transition on a paused endpoint (FR-1205)")
	}
	forgiven := append([]string(nil), out.Forgiven...)
	sort.Strings(forgiven)
	want := append([]string(nil), types...)
	sort.Strings(want)
	if !reflect.DeepEqual(forgiven, want) {
		t.Errorf("resume forgave %v, want every former member %v (FR-1211)", forgiven, want)
	}
	if v, err := f.counters.FailingSubscriptions(ctx, f.endpointID, time.Now()); err != nil || v != 0 {
		t.Errorf("failing subscriptions after an endpoint resume = %d (err %v), want 0 (FR-1211)", v, err)
	}
	if status, reason := f.endpointStatus(t); status != "active" || reason != "" {
		t.Errorf("endpoint after resume = (%q, %q), want (active, none)", status, reason)
	}

	// The held deliveries were never replayed, and they now deliver.
	healthy.Store(true)
	for _, id := range held {
		if !drive(t, w, 200, func() bool { return f.deliveryStatus(t, id) == "delivered" }) {
			t.Fatalf("delivery %d = %q after the resume, want delivered", id, f.deliveryStatus(t, id))
		}
	}
}

// ---------------------------------------------------------------------------
// AC-41 / FR-1302 / FR-1307: the concurrency cap is the endpoint's, not the
// replica's
// ---------------------------------------------------------------------------

// TestAC41ConcurrencyCapIsIdenticalAtOneAndTwoReplicas: 100 deliveries enqueued at
// once against one endpoint capped at 10 concurrent never observe more than 10
// in-flight requests, and the observed count is the same with one worker and two
// sharing one Redis.
func TestAC41ConcurrencyCapIsIdenticalAtOneAndTwoReplicas(t *testing.T) {
	const (
		concurrencyCap = 10
		deliveries     = 100
		eventType      = "order.created"
	)

	// The receiver takes a fixed 250 ms per request, so the number of handlers
	// running at once IS the number of requests the endpoint is serving at once -
	// which is what AC-41 measures. (A handler that blocks until the test releases
	// it would keep counting after a client timeout tore the request down, and
	// would then be measuring the test, not the limiter.)
	const serviceTime = 250 * time.Millisecond
	var counter inFlightCounter
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		counter.enter()
		time.Sleep(serviceTime)
		counter.leave()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	f := newS5Fixture(t, srv.URL, eventType)
	// The AC's own configuration: one endpoint capped at 10 concurrent. The RPS
	// bucket stays at its §9 default, which is separate by design (FR-1306).
	f.setEndpoint(t, `max_concurrency = 10`)
	for i := 0; i < deliveries; i++ {
		f.publish(t, eventType, `{"id":"bulk"}`)
	}
	if n := f.deliveryCount(t); n != deliveries {
		t.Fatalf("enqueued %d deliveries, want %d", n, deliveries)
	}

	observed := map[string]int{}

	// The deployment defaults, from the shipped config rather than from a number
	// written here: the third run below caps an endpoint that sets NO override, so
	// the assertion has to be about what the deployment actually ships
	// (PIGEON_DELIVERY_MAX_CONNS_PER_HOST, FR-1302's documented 10). Only the
	// three variables Validate() requires are set; nothing else is configured.
	t.Setenv("PIGEON_DATABASE_URL", "postgres://unused/unused?sslmode=disable")
	t.Setenv("PIGEON_REDIS_URL", "redis://unused:6379/0")
	t.Setenv("PIGEON_INSTANCE_ADMIN_PASSWORD", "not-the-documented-weak-default")
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("config.Load with the required variables set: %v", err)
	}
	if cfg.DeliveryMaxConnsPerHost != concurrencyCap {
		t.Errorf("the shipped default per-endpoint concurrency is %d, want AC-41's %d (FR-1302)",
			cfg.DeliveryMaxConnsPerHost, concurrencyCap)
	}

	run := func(name string, replicas int) {
		t.Helper()
		counter.reset()
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		for i := 0; i < replicas; i++ {
			w := New(f.st, f.options(func(o *Options) {
				// Backpressure well ABOVE the endpoint's cap: the point of the
				// assertion is that the ENDPOINT's limit binds, not the process's.
				o.Concurrency = 4 * concurrencyCap
				o.Limits = f.limits
				// The deployment default, as the process is configured: the
				// per-endpoint transport pool and the cluster-wide lease both read
				// this number, so the run below with no endpoint override is
				// capping on exactly what a replica would.
				o.MaxConnsPerHost = cfg.DeliveryMaxConnsPerHost
				o.PollInterval = time.Millisecond
			}))
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = w.Run(ctx)
			}()
		}
		// Wait for the cap to be reached, let the load run against it for a while,
		// then read the high-water mark (which no later traffic can lower).
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			if _, peak, _ := counter.snapshot(); peak >= concurrencyCap {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		time.Sleep(time.Second)
		_, peak, started := counter.snapshot()
		observed[name] = peak
		cancel()
		wg.Wait()
		// An aborted attempt tears down the client side while the handler is still
		// working, so let the receiver quiesce before the next run resets the count.
		drain := time.Now().Add(5 * time.Second)
		for time.Now().Before(drain) {
			if cur, _, _ := counter.snapshot(); cur == 0 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if cur, _, _ := counter.snapshot(); cur != 0 {
			t.Errorf("%s: %d requests still in flight after the workers drained", name, cur)
		}
		if peak > concurrencyCap {
			t.Errorf("%s: observed %d in-flight for an endpoint capped at %d (FR-1302)", name, peak, concurrencyCap)
		}
		if peak < concurrencyCap {
			t.Errorf("%s: peak in-flight was only %d of a %d cap; the load never reached the limit", name, peak, concurrencyCap)
		}
		if started < concurrencyCap {
			t.Errorf("%s: only %d requests started, want more than the cap so the cap was actually exercised", name, started)
		}
		t.Logf("%s: %d requests started, peak in-flight %d", name, started, peak)
	}

	run("one replica", 1)
	for i := 0; i < deliveries; i++ {
		f.publish(t, eventType, `{"id":"bulk2"}`)
	}
	run("two replicas", 2)
	// Third: no endpoint override at all, so FR-1302's documented default is what
	// does the capping rather than an explicit 10.
	f.setEndpoint(t, `max_concurrency = null`)
	for i := 0; i < deliveries; i++ {
		f.publish(t, eventType, `{"id":"bulk3"}`)
	}
	run("default cap, one replica", 1)

	if observed["one replica"] != observed["two replicas"] {
		t.Errorf("AC-41: peak in-flight was %d with one replica and %d with two; adding a replica must not add capacity (FR-1307)",
			observed["one replica"], observed["two replicas"])
	}
	if observed["default cap, one replica"] != observed["one replica"] {
		t.Errorf("peak in-flight with no endpoint override was %d, want FR-1302's default of %d",
			observed["default cap, one replica"], observed["one replica"])
	}
}

// TestDocumentedLimitDefaults pins the numbers FR-1302/FR-1306 publish, so an edit
// that quietly changes the documented default fails here. The behaviour behind them
// is asserted end to end by TestAC41 and by the bucket tests in internal/limit.
func TestDocumentedLimitDefaults(t *testing.T) {
	if defaultMaxConcurrency != 10 {
		t.Errorf("FR-1302's default concurrency = %d, want 10", defaultMaxConcurrency)
	}
	if defaultMaxRPS != 25 {
		t.Errorf("FR-1306's default RPS = %d, want 25", defaultMaxRPS)
	}
	if maxAttemptTimeout != 5*time.Second {
		t.Errorf("FR-802's attempt ceiling = %s, want 5s", maxAttemptTimeout)
	}
}

// inFlightCounter is the receiver's own observation of how many requests it is
// serving at once - the number AC-41 is about.
type inFlightCounter struct {
	mu      sync.Mutex
	cur     int
	peak    int
	started int
}

func (c *inFlightCounter) enter() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cur++
	c.started++
	if c.cur > c.peak {
		c.peak = c.cur
	}
}

func (c *inFlightCounter) leave() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cur--
}

func (c *inFlightCounter) snapshot() (cur, peak, started int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cur, c.peak, c.started
}

func (c *inFlightCounter) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cur, c.peak, c.started = 0, 0, 0
}

// ---------------------------------------------------------------------------
// AC-35 / FR-1207: the durable half of the retry window - an outage ridden out
// on scheduled retries, and a pause that EXTENDS the window instead of consuming
// ---------------------------------------------------------------------------

// TestAC35RecoversInsideTheWindow is AC-35's durable half. retry_test.go proves
// the arithmetic at §9's real numbers (a 20 h outage is ridden out inside 72 h);
// this proves the platform actually does it on the delivery row: a receiver that
// is down and then comes back is delivered on a scheduled retry, with no
// operator action, and the window is not what ended anything.
//
// The clock is compressed - base 50 ms, cap 100 ms, window 4 s - because the
// SHAPE is the claim: the receiver is unhealthy for the first two attempts, so
// only a retry can carry the delivery.
func TestAC35RecoversInsideTheWindow(t *testing.T) {
	// The first two attempts fail and the receiver is healthy from the third: the
	// recovery is driven by the attempts themselves rather than by a wall clock,
	// so a loaded machine cannot turn this into a flake.
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	f := newS5Fixture(t, srv.URL, "order.created")
	_, deliveryID := f.publish(t, "order.created", `{"id":"o1"}`)

	const window = 4 * time.Second
	w := New(f.st, f.options(func(o *Options) {
		o.RetryWindow = window
		o.BackoffBase = 50 * time.Millisecond
		o.BackoffMax = 100 * time.Millisecond
	}))

	if !drive(t, w, 5000, func() bool { return f.deliveryStatus(t, deliveryID) == "delivered" }) {
		t.Fatalf("delivery = %q after the receiver recovered, want delivered (AC-35): the retry schedule has to carry it",
			f.deliveryStatus(t, deliveryID))
	}
	failed := f.attemptCount(t, deliveryID)
	if failed < 2 {
		t.Errorf("%d failed attempts before the recovery, want >= 2: the delivery has to have ridden out the outage on retries", failed)
	}
	used := f.retryWindowUsed(t, deliveryID)
	if used < window-time.Second || used > window+time.Second {
		t.Errorf("the delivery ran on a %s window, want the configured %s: the window is the deployment's, not a fallback", used, window)
	}
	t.Logf("AC-35: %d failed attempts (%d carrying requests), then delivered inside the %s window", failed, hits.Load(), window)
}

// TestFR1207ResumeExtendsThePromisedWindow: a delivery that was fanned out but
// never attempted has no retry_deadline yet - the first failure's requeue stamps
// it - and those are exactly the rows a pause holds. FR-1207 says a pause
// extends the delivery's window, so the deadline a resume produces must be
// created_at + the window the delivery was PROMISED + the paused period.
//
// The shape it must not have is resume_time + paused_for: that hands a held
// delivery a window of one pause, so an endpoint paused for ten minutes would
// terminal everything it held after about two attempts - a pause consuming the
// window it exists to preserve.
//
// The endpoint's own 1 h window is in place (the smallest the column allows)
// while the resumer is given the deployment default, so this also pins that the
// ENDPOINT's window governs the pause arithmetic, as it does the claim (AC-37).
func TestFR1207ResumeExtendsThePromisedWindow(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	f := newS5Fixture(t, srv.URL, "order.created")
	f.setEndpoint(t, `retry_window = interval '1 hour'`)
	_, deliveryID := f.publish(t, "order.created", `{"id":"p1"}`)
	created := f.createdAt(t, deliveryID)

	// The case under test is the NULL one, so assert it is the one we have.
	var before sql.NullTime
	if err := f.admin.QueryRow(ctx, `select retry_deadline from deliveries where id=$1`, deliveryID).Scan(&before); err != nil {
		t.Fatalf("read retry_deadline: %v", err)
	}
	if before.Valid {
		t.Fatalf("the delivery already has retry_deadline %s; this test is about a delivery held by a pause that was never attempted",
			before.Time)
	}

	if _, err := f.st.PauseSubscriptionForFailures(ctx, f.endpointID, "order.created"); err != nil {
		t.Fatalf("PauseSubscriptionForFailures: %v", err)
	}
	const paused = 300 * time.Millisecond
	time.Sleep(paused)

	resumer := pause.NewResumer(f.st, f.counters, 0) // 0 = the documented deployment default (72 h)
	out, err := resumer.ResumeSubscription(ctx, f.endpointID, "order.created")
	if err != nil {
		t.Fatalf("ResumeSubscription: %v", err)
	}
	if !out.Resumed {
		t.Fatal("resume reported no transition on a paused subscription (FR-1205)")
	}
	resumedAt := time.Now()

	used := f.retryDeadline(t, deliveryID).Sub(created)
	if used < time.Hour+paused/2 {
		t.Errorf("the resume granted a %s window, want the promised 1 h plus the %s pause: extending from the resume instead of from the promise is what consumes a held delivery's window (FR-1207)",
			used, paused)
	}
	if used > time.Hour+30*time.Second {
		t.Errorf("the resume granted a %s window, want the promised 1 h plus the pause: the paused period must be added once, not twice (and the base must be the promise, not the resume)",
			used)
	}
	// The schedule a never-scheduled row does not have is not invented: the row is
	// claimable at the resume rather than one pause later.
	if next := f.nextAttemptAt(t, deliveryID); !next.Valid || next.Time.After(resumedAt.Add(time.Second)) {
		t.Errorf("next_attempt_at = %v, want an instant at or before the resume: a hold must not push a delivery into the future", next)
	}

	// And the claim hands the worker that deadline: a stamped deadline is what the
	// worker schedules against from here on.
	d, got, err := f.st.ClaimDelivery(ctx, 40*time.Second, 72*time.Hour)
	if err != nil || !got {
		t.Fatalf("ClaimDelivery after the resume: (got=%v, err=%v)", got, err)
	}
	if d.DeliveryID != deliveryID {
		t.Fatalf("claimed delivery %d, want %d", d.DeliveryID, deliveryID)
	}
	if got := d.RetryDeadline.Sub(created); got < used-time.Second || got > used+time.Second {
		t.Errorf("the claim reports a %s deadline, want the stored %s: the worker has to schedule against the window the pause preserved",
			got, used)
	}
}
