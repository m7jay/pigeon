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
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/m7jay/pigeon/internal/config"
	"github.com/m7jay/pigeon/internal/limit"
	"github.com/m7jay/pigeon/internal/metrics"
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
// and the delivery it produced for the fixture's endpoint (f.endpointID).
func (f *s5Fixture) publish(t *testing.T, eventType, payload string) (string, int64) {
	return f.publishTo(t, f.consumerRef, f.endpointID, eventType, payload)
}

// publishTo registers one event of eventType on a given endpoint, returning the
// event id and the delivery it produced for endpointID. The consumerRef must be
// the ref of a consumer that has a subscription for eventType on endpointID.
func (f *s5Fixture) publishTo(t *testing.T, consumerRef, endpointID string, eventType, payload string) (string, int64) {
	t.Helper()
	ctx := context.Background()
	pub, err := f.st.PublishEvent(ctx, f.sc, store.PublishInput{
		EventType: eventType, Payload: json.RawMessage(payload), ConsumerRef: &consumerRef,
	})
	if err != nil {
		t.Logf("payload=[%s] err=[%v]", payload, err)
		t.Fatalf("PublishEvent(%s): %v", eventType, err)
	}
	if _, err := f.st.FanOutEvent(ctx, f.sc, pub.ID); err != nil {
		t.Fatalf("FanOutEvent(%s): %v", eventType, err)
	}
	var deliveryID int64
	if err := f.admin.QueryRow(ctx,
		`select id from deliveries where event_id=$1 and endpoint_id=$2`,
		pub.ID, endpointID).Scan(&deliveryID); err != nil {
		t.Fatalf("resolve delivery for %s on %s: %v", eventType, endpointID, err)
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

// attemptsForType is the durable attempt count across every delivery of one event
// type. The breadth rule counts SUBSCRIPTION windows (FR-1203), so the three
// failures a subscription needs can be spread over three deliveries with one
// attempt each - a per-delivery count cannot see them.
func (f *s5Fixture) attemptsForType(t *testing.T, eventType string) int {
	t.Helper()
	var n int
	if err := f.admin.QueryRow(context.Background(),
		`select count(*) from attempts a
		   join deliveries d on d.id = a.delivery_id
		   join events e on e.id = d.event_id
		  where e.event_type = $1`, eventType).Scan(&n); err != nil {
		t.Fatalf("count attempts for %s: %v", eventType, err)
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

// makeDue makes the delivery claimable now, so the test's decision about the clock
// is not undone by the claim's own wall-clock check: the worker schedules the next
// attempt at an instant the test supplies, and the row must be due when the test
// says it is rather than when the box's clock catches up with that instant.
func (f *s5Fixture) makeDue(t *testing.T, deliveryID int64) {
	t.Helper()
	if _, err := f.admin.Exec(context.Background(),
		`update deliveries set lease_expires_at = now() - interval '1 second' where id=$1`, deliveryID); err != nil {
		t.Fatalf("make delivery %d due: %v", deliveryID, err)
	}
}

// steppedClock is a worker clock the test moves by hand (Options.Now), so a retry
// schedule is arithmetic the test drives instead of a race with how fast the box
// got around to starting an attempt. Reads and writes are both on the test's
// goroutine, but the mutex makes that a fact rather than an assumption.
type steppedClock struct {
	mu  sync.Mutex
	now time.Time
}

func newSteppedClock(at time.Time) *steppedClock { return &steppedClock{now: at} }

// at is the clock itself: Options.Now takes no arguments.
func (c *steppedClock) at() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// set moves the clock to an instant the test has decided on.
func (c *steppedClock) set(at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = at
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
//
// The clock is the test's (Options.Now), and so is "the delivery is due", because
// every observable below is derived from how much time has passed. Read off the
// wall clock, how many attempts fit inside a 250 ms window is a property of how
// fast the box got around to starting one, and on a busy runner the FIRST failure
// already landed past the window: the row was failed_terminal at the boundary,
// which is exactly FR-1201, while the test asserted a one-attempt delivery and a
// null deadline and reddened CI's required-name list (t_12e66b54). Stepping the
// clock to the instant the worker itself scheduled makes the count arithmetic:
// 100 ms steps inside a 250 ms window are attempts at 0, 100 and 200 ms, and the
// next one would land at 300 ms.
func TestAC36RetryWindowEndsTheDelivery(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	f := newS5Fixture(t, srv.URL, "order.created")
	_, deliveryID := f.publish(t, "order.created", `{"id":"o1"}`)

	const window = 250 * time.Millisecond
	// The clock starts where the row does: the claim measures the window from
	// deliveries.created_at, so the test's clock and the row's deadline share one
	// timeline.
	clock := newSteppedClock(f.createdAt(t, deliveryID))
	w := New(f.st, f.options(func(o *Options) {
		o.RetryWindow = window
		o.BackoffBase = 100 * time.Millisecond
		o.BackoffMax = 100 * time.Millisecond
		o.Now = clock.at
	}))

	ctx := context.Background()
	for i := 0; i < 16 && f.deliveryStatus(t, deliveryID) != "failed_terminal"; i++ {
		f.makeDue(t, deliveryID)
		if _, err := w.ProcessOne(ctx); err != nil {
			t.Fatalf("ProcessOne: %v", err)
		}
		// The worker scheduled the next attempt; be there for it, rather than
		// waiting for the wall clock to catch up with the schedule.
		if next := f.nextAttemptAt(t, deliveryID); next.Valid {
			clock.set(next.Time)
		}
	}

	attempts := f.attemptCount(t, deliveryID)
	if got := f.deliveryStatus(t, deliveryID); got != "failed_terminal" {
		t.Fatalf("delivery = %q after %d attempts and %s of the test's clock, want failed_terminal: the window must end it (FR-1201)",
			got, attempts, clock.at().Sub(f.createdAt(t, deliveryID)))
	}
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
	t.Logf("AC-36: %d attempts, failed_terminal at the %s window, retry_deadline preserved on the row", attempts, window)
}

// TestAC37ClaimUsesTheEndpointWindow is AC-37's governance claim made observable:
// the deadline the claim hands the worker comes from the ENDPOINT's retry_window
// when the endpoint sets one, and from the deployment default when it does not.
func TestAC37ClaimUsesTheEndpointWindow(t *testing.T) {
	f := newS5Fixture(t, "http://127.0.0.1:1/unused", "order.created")
	ctx := context.Background()
	const deploymentWindow = 72 * time.Hour

	_, byDefault := f.publish(t, "order.created", `{"id":"o1"}`)
	d, got, err := f.st.ClaimDelivery(ctx, 40*time.Second, deploymentWindow, store.AttemptClassAny)
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
	d2, got, err := f.st.ClaimDelivery(ctx, 40*time.Second, deploymentWindow, store.AttemptClassAny)
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
	w := New(f.st, f.options(func(o *Options) {
		// A failed delivery's next attempt is pushed past the rest of this test -
		// an hour out - so the worker cannot come back to a subscription it has
		// already failed: the 3 x 3 the breadth rule counts (AC-44) is placed by
		// the loop below, one fresh delivery per failure, rather than hoped for
		// from a retry cadence.
		//
		// A one-second cadence did not say that. FR-1209 counts EVERY failed
		// attempt in its subscription's sliding hour, so on a busy box the 1 s
		// lease expires while the placement loop below is still pumping; the claim
		// takes the lowest id first, so the fresh delivery queues behind its
		// predecessors' retries, three retries - one per subscription - cross the
		// breadth floor of three, and the endpoint pauses (FR-1203) BEFORE the
		// delivery this round is about has been attempted. That row then stays
		// "pending" for any number of ProcessOne calls, because the claim holds
		// every pending delivery of a paused endpoint (t_3218398e).
		o.BackoffBase = time.Hour
		o.BackoffMax = time.Hour
	}))
	ctx := context.Background()

	// Three failures for each of the three subscriptions, in rounds: nine
	// failures spread 3/3/3 is exactly what N = min(3, max(3, ceil(3/4))) = 3
	// wants, and nowhere near any subscription's own depth of ten.
	held := make([]int64, 0, len(types)*len(types))
	paused := false
	for round := 0; round < len(types) && !paused; round++ {
		for _, et := range types {
			_, id := f.publish(t, et, `{"id":"b1"}`)
			held = append(held, id)
			if !drive(t, w, 300, func() bool { return f.attemptCount(t, id) >= 1 }) {
				status, reason := f.endpointStatus(t)
				t.Fatalf("the %s delivery of round %d was never attempted (status %q, endpoint = (%q, %q))",
					et, round+1, f.deliveryStatus(t, id), status, reason)
			}
			if status, _ := f.endpointStatus(t); status == "paused" {
				paused = true
				break
			}
		}
	}
	if !paused {
		status, reason := f.endpointStatus(t)
		t.Fatalf("endpoint = (%q, %q) after %d failures spread over %d subscriptions; FR-1203 wants %d failing subscriptions to pause it",
			status, reason, len(held), len(types), len(types))
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
	// The breadth set is the three subscriptions, each with three failures: this
	// trip was breadth, not depth, and the count is of subscriptions rather than
	// of attempts (a subscription with nine failures is still one member).
	sortedTypes := append([]string(nil), types...)
	sort.Strings(sortedTypes)
	members, err := f.counters.FailingMembers(ctx, f.endpointID)
	if err != nil {
		t.Fatalf("FailingMembers: %v", err)
	}
	sort.Strings(members)
	if !reflect.DeepEqual(members, sortedTypes) {
		t.Errorf("breadth set = %v, want %v: the endpoint tripped on %d failing subscriptions",
			members, sortedTypes, len(types))
	}
	for _, et := range types {
		// The 3 x 3 came from the placements and nothing else (FR-1209 counts
		// every failed attempt in its subscription's window): each subscription
		// holds exactly the three failures the loop placed, so the endpoint tripped
		// on breadth AT the floor of three rather than on the way to any
		// subscription's own depth of ten (FR-1202, FR-1203). A per-delivery count
		// cannot see a retry cadence reaching that floor on its own, which is
		// exactly how this test used to race its own pause (t_3218398e).
		if n := f.attemptsForType(t, et); n != len(types) {
			t.Errorf("subscription %s has %d attempts, want exactly the %d the loop placed: the endpoint must trip on breadth at the floor, not on retries reaching it first",
				et, n, len(types))
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

	// The held deliveries were never replayed, and they now deliver. Their
	// schedule is where the placement's backoff left it - an hour out - so the test
	// supplies the dueness itself, the way TestAC36RetryWindowEndsTheDelivery does:
	// the retry cadence lives in the lease (0013's requeue sets lease_expires_at to
	// the next attempt), and the property under test is that FR-1205 held these
	// rows and FR-1207 kept their window - not that an hour can pass inside a test.
	healthy.Store(true)
	for _, id := range held {
		f.makeDue(t, id)
	}
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
//
// The receiver holds every request open on a barrier the test releases, so the peak
// is the number of requests the limiter ADMITTED at once - a property of the
// barrier - instead of the number the scheduler managed to overlap inside the
// receiver's 250 ms service time. It is that second number a busy box moves, and it
// used to move the verdict with it (t_12e66b54): the probe could not get the cap's
// worth of requests into the receiver at all, which was reported as "the load never
// reached the limit", as an apparent capacity difference between the one- and
// two-replica runs (FR-1307's claim, which nothing had broken), and as a default-cap
// run that "disagreed" with FR-1302's documented default. Measured on this box under
// an artificial load, the pre-fix test was red 5 of 5 runs with peaks from 4 to 9.
//
// What the barrier proves is stronger than the peak comparison it replaces: with the
// cap held open, an eleventh concurrent request is exactly what AC-41 forbids, and
// the receiver may not see one. When the box cannot generate the load at all - the
// cap is never reached inside probeDeadline - the run says so and asserts only the
// half it can still prove, that the cap was not exceeded.
func TestAC41ConcurrencyCapIsIdenticalAtOneAndTwoReplicas(t *testing.T) {
	const (
		concurrencyCap = 10
		deliveries     = 100
		eventType      = "order.created"
		// serviceTime is how long a released request occupies the receiver, so the
		// traffic the limiter admits after the release overlaps rather than arriving
		// one at a time.
		serviceTime = 250 * time.Millisecond
		// probeDeadline bounds the wait for the endpoint's cap to be reached before
		// the run declares its probe short (the !capped branch in run).
		probeDeadline = 8 * time.Second
		// holdTime is how long the cap stays held open: an eleventh request would
		// have to be admitted inside it.
		holdTime = 250 * time.Millisecond
		// settleTime lets the released wave finish against the cap, which is where a
		// limiter that admitted MORE than the cap would show up as a second wave.
		settleTime = time.Second
	)

	var counter inFlightCounter
	var barrier loadBarrier
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		counter.enter()
		// A held request counts for exactly as long as it is in the receiver, and it
		// leaves as soon as the barrier opens - far inside the attempt's own ceiling
		// (FR-802, 5 s) - so the count is the limiter's admission and not a request
		// the client already gave up on.
		barrier.wait()
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
	reached := map[string]bool{}

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
		barrier.arm()
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
		// Wait for the cap to be inside the receiver, then hold it open. Nothing
		// that has entered can leave until the test releases it, so reaching the cap
		// is the limiter's admission and not how fast the box started ten requests.
		capped := false
		probe := time.Now().Add(probeDeadline)
		for time.Now().Before(probe) {
			if cur, _, _ := counter.snapshot(); cur >= concurrencyCap {
				capped = true
				break
			}
			time.Sleep(time.Millisecond)
		}
		if capped {
			// The cap is spent, so an eleventh concurrent request must not get in -
			// with two replicas that is precisely FR-1307's claim, since the second
			// replica's own tenth-plus-one is what would show up here.
			hold := time.Now().Add(holdTime)
			for time.Now().Before(hold) {
				time.Sleep(5 * time.Millisecond)
			}
			if cur, peak, started := counter.snapshot(); cur != concurrencyCap || peak != concurrencyCap || started != concurrencyCap {
				t.Errorf("%s: with the endpoint's cap of %d held open the receiver saw %d in flight (peak %d, %d requests in total); no more than the cap may be admitted at once (FR-1302)",
					name, concurrencyCap, cur, peak, started)
			}
		}
		barrier.release()
		// Let the released wave finish, then read the high-water mark (which no
		// later traffic can lower). A limiter that admitted more than the cap would
		// show a second wave inside this window; one that honours the cap shows none,
		// because a saturated waiter is parked for the slot's TTL and the next wave
		// is that far away (measured at 14.5 s here - S11's card owns that behaviour,
		// this test only records it).
		time.Sleep(settleTime)
		_, peak, started := counter.snapshot()
		observed[name] = peak
		reached[name] = capped
		cancel()
		wg.Wait()
		// An aborted attempt tears down the client side while the handler is still
		// working, so let the receiver quiesce before the next run resets the count:
		// a straggler that entered afterwards would be charged to the next run's
		// peak.
		drain := time.Now().Add(5 * time.Second)
		for time.Now().Before(drain) {
			cur, _, before := counter.snapshot()
			time.Sleep(20 * time.Millisecond)
			cur2, _, after := counter.snapshot()
			if cur == 0 && cur2 == 0 && before == after {
				break
			}
		}
		if cur, _, _ := counter.snapshot(); cur != 0 {
			t.Errorf("%s: %d requests still in flight after the workers drained", name, cur)
		}
		if peak > concurrencyCap {
			t.Errorf("%s: observed %d in-flight for an endpoint capped at %d (FR-1302)", name, peak, concurrencyCap)
		}
		if !capped {
			// The probe never got the cap's worth of requests into the receiver, so
			// this run's number is the box's, not the limiter's: fail on what it can
			// still prove - that the cap was not exceeded - and record the shortfall
			// with its reason, rather than assert a peak the probe never reached.
			// (That assertion is what shipped, and a busy runner reddens it without
			// anything being wrong.)
			t.Logf("%s: PROBE SHORT - the %d cap was not reached inside %s (peak in-flight %d, %d requests served): a saturated box may not be able to start the cap's worth of requests at once, so this run asserts only that the cap was not exceeded",
				name, concurrencyCap, probeDeadline, peak, started)
			return
		}
		t.Logf("%s: %d requests served, the %d cap held in the receiver, peak %d", name, started, concurrencyCap, peak)
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

	// AC-41's claim is about capacity, not throughput: no configuration may exceed
	// the cap, and a second replica may not add any. The two-replica run's held-cap
	// check above is that claim - it is the run that would show a per-process cap -
	// and every run asserts "never more than the cap". These compare the peaks
	// across runs as a cross-check, and are skipped for any run whose probe came up
	// short (that run logged why, and asserting on a number the box did not produce
	// is the defect this test is being fixed for).
	if reached["one replica"] && reached["two replicas"] && observed["one replica"] != observed["two replicas"] {
		t.Errorf("AC-41: peak in-flight was %d with one replica and %d with two; adding a replica must not add capacity (FR-1307)",
			observed["one replica"], observed["two replicas"])
	}
	if reached["one replica"] && reached["default cap, one replica"] && observed["default cap, one replica"] != observed["one replica"] {
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

// loadBarrier holds every request the receiver gets until the test releases it, so
// how many are in flight at once is decided by the test rather than by how quickly
// the box schedules ten goroutines. A released barrier never blocks, so the traffic
// after the release is the same load the test always ran.
type loadBarrier struct {
	mu       sync.Mutex
	gate     chan struct{}
	released bool
}

// arm gives the next run a fresh closed gate.
func (b *loadBarrier) arm() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.gate = make(chan struct{})
	b.released = false
}

// release opens the barrier: every request already inside it, and every later one,
// proceeds. Releasing twice is a no-op rather than a panic, so the release cannot
// depend on the branch that reaches it.
func (b *loadBarrier) release() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.released {
		b.released = true
		close(b.gate)
	}
}

// wait blocks until the test releases the barrier.
func (b *loadBarrier) wait() {
	b.mu.Lock()
	gate := b.gate
	b.mu.Unlock()
	<-gate
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
	if out.PausedFor < paused {
		t.Errorf("the resume reported a %s pause, want at least the %s the scope was paused for: the period it added back has to be the one it recorded",
			out.PausedFor, paused)
	}
	resumedAt := time.Now()

	// The window is the promise plus the pause ONCE, measured against the period
	// the resume RECORDED. The 30 s ceiling this replaces could not see a pause
	// added twice (300 ms of it on this clock), which is one of the two shapes
	// FR-1207 forbids; the other, a deadline derived from the resume rather than
	// from the promise, lands about one pause short (t_3218398e).
	want := time.Hour + out.PausedFor
	used := f.retryDeadline(t, deliveryID).Sub(created)
	if used < want-2*time.Millisecond || used > want+2*time.Millisecond {
		t.Errorf("the resume granted a %s window, want the promised 1 h plus the %s pause it recorded, added once (FR-1207)",
			used, out.PausedFor)
	}
	// The schedule a never-scheduled row does not have is not invented: the row is
	// claimable at the resume rather than one pause later.
	if next := f.nextAttemptAt(t, deliveryID); !next.Valid || next.Time.After(resumedAt.Add(time.Second)) {
		t.Errorf("next_attempt_at = %v, want an instant at or before the resume: a hold must not push a delivery into the future", next)
	}

	// And the claim hands the worker that deadline: a stamped deadline is what the
	// worker schedules against from here on.
	d, got, err := f.st.ClaimDelivery(ctx, 40*time.Second, 72*time.Hour, store.AttemptClassAny)
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

// TestFR1207EndpointResumeExtendsThePromisedWindow: FR-1207 on the ENDPOINT
// scope. 0013 implements the requirement twice - pigeon_resume_endpoint and
// pigeon_resume_subscription are separate SQL bodies - so the subscription pin
// above leaves the endpoint half free to fall back to resume_time + paused_for,
// which is the shape the requirement forbids: an endpoint paused for ten minutes
// would terminal everything it held after about two attempts.
//
// The endpoint carries no retry_window override here, so the promise is the
// deployment's own window (the 72 h the resumer is handed), which is the
// coalesce's right-hand side; the endpoint's 1 h override is the case above.
func TestFR1207EndpointResumeExtendsThePromisedWindow(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	f := newS5Fixture(t, srv.URL, "order.created")
	_, deliveryID := f.publish(t, "order.created", `{"id":"e1"}`)
	created := f.createdAt(t, deliveryID)

	// The case under test is the never-attempted row, so assert it is the one we
	// have: a failure's requeue is what stamps retry_deadline, and a delivery
	// that was only fanned out is exactly what an endpoint pause holds.
	var before sql.NullTime
	if err := f.admin.QueryRow(ctx, `select retry_deadline from deliveries where id=$1`, deliveryID).Scan(&before); err != nil {
		t.Fatalf("read retry_deadline: %v", err)
	}
	if before.Valid {
		t.Fatalf("the delivery already has retry_deadline %s; this test is about a delivery held by a pause that was never attempted",
			before.Time)
	}

	if _, err := f.st.PauseEndpointForFailures(ctx, f.endpointID, 3); err != nil {
		t.Fatalf("PauseEndpointForFailures: %v", err)
	}
	const paused = time.Second
	time.Sleep(paused)

	resumer := pause.NewResumer(f.st, f.counters, 0) // 0 = the documented deployment default (72 h)
	out, err := resumer.ResumeEndpoint(ctx, f.endpointID)
	if err != nil {
		t.Fatalf("ResumeEndpoint: %v", err)
	}
	if !out.Resumed {
		t.Fatal("resume reported no transition on a paused endpoint (FR-1205)")
	}
	if out.PausedFor < paused {
		t.Errorf("the resume reported a %s pause, want at least the %s the scope was paused for: the period it added back has to be the one it recorded",
			out.PausedFor, paused)
	}
	resumedAt := time.Now()

	// The window is the promise plus the pause ONCE, and the pause is the period
	// the resume RECORDED rather than the constant slept above: the row holds 72 h
	// + the real elapsed pause, and on a loaded box - or under -race, where the
	// base's own suite reddened here with 72h0m1.981377s against the 1 s curl the
	// sleep intends - that overshoots any fixed ceiling (t_3218398e). Both shapes
	// the requirement forbids are a whole pause away from this bound: a resume that
	// derives the deadline from now() grants about one pause (the old
	// pigeon_resume_endpoint shape), and one that adds the paused period twice
	// lands a pause past it.
	want := 72*time.Hour + out.PausedFor
	used := f.retryDeadline(t, deliveryID).Sub(created)
	if used < want-2*time.Millisecond || used > want+2*time.Millisecond {
		t.Errorf("the endpoint resume granted a %s window, want the promised 72 h plus the %s pause it recorded, added once (FR-1207)",
			used, out.PausedFor)
	}
	// A hold must not push a delivery into the future: the schedule a
	// never-scheduled row does not have is not invented.
	if next := f.nextAttemptAt(t, deliveryID); !next.Valid || next.Time.After(resumedAt.Add(time.Second)) {
		t.Errorf("next_attempt_at = %v, want an instant at or before the resume", next)
	}

	// And the claim hands the worker that deadline: a stamped deadline is what
	// the worker schedules against from here on.
	d, got, err := f.st.ClaimDelivery(ctx, 40*time.Second, 72*time.Hour, store.AttemptClassAny)
	if err != nil || !got {
		t.Fatalf("ClaimDelivery after the endpoint resume: (got=%v, err=%v)", got, err)
	}
	if d.DeliveryID != deliveryID {
		t.Fatalf("claimed delivery %d, want %d", d.DeliveryID, deliveryID)
	}
	if got := d.RetryDeadline.Sub(created); got < used-time.Second || got > used+time.Second {
		t.Errorf("the claim reports a %s deadline, want the stored %s: the endpoint resume's window has to be the one the worker schedules against",
			got, used)
	}
}

// TestFR1207EndpointResumeKeepsAStampedDeadline: the other half of the same
// requirement on the same scope. A delivery that failed before the pause already
// has its own deadline, and FR-1207 extends THAT instant by the paused period
// rather than re-deriving it from the promise - which is the coalesce's
// left-hand side, and the case a test of the NULL row alone cannot see (there
// the promise and the resume are the same instant).
func TestFR1207EndpointResumeKeepsAStampedDeadline(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	f := newS5Fixture(t, srv.URL, "order.created")
	_, deliveryID := f.publish(t, "order.created", `{"id":"e2"}`)
	// The instant a first failure would have stamped: an hour after the promise,
	// so a resume that re-derived the deadline from created_at cannot be mistaken
	// for one that kept this row's own.
	if _, err := f.admin.Exec(ctx,
		`update deliveries set retry_deadline = created_at + interval '1 hour' where id=$1`, deliveryID); err != nil {
		t.Fatalf("stamp retry_deadline: %v", err)
	}
	stamped := f.retryDeadline(t, deliveryID)

	if _, err := f.st.PauseEndpointForFailures(ctx, f.endpointID, 3); err != nil {
		t.Fatalf("PauseEndpointForFailures: %v", err)
	}
	const paused = 500 * time.Millisecond
	time.Sleep(paused)

	resumer := pause.NewResumer(f.st, f.counters, 0)
	// FR-1207's arithmetic is the period the platform RECORDED, not the period this
	// test slept: the resumed deadline moves by now() - paused_at, which on a busy
	// box overshoots a fixed sleep by seconds, so a bound derived from the constant
	// above reddens CI for a box's throughput rather than for the code
	// (t_3218398e). The resume reports the period it added back, which is the only
	// value the assertion is entitled to compare against.
	resumed, err := resumer.ResumeEndpoint(ctx, f.endpointID)
	if err != nil {
		t.Fatalf("ResumeEndpoint: %v", err)
	}
	if !resumed.Resumed {
		t.Fatal("resume reported no transition on a paused endpoint (FR-1205)")
	}
	if resumed.PausedFor < paused {
		t.Errorf("the resume reported a %s pause, want at least the %s the scope was paused for: the period it added back has to be the one it recorded",
			resumed.PausedFor, paused)
	}

	// The stamped instant moves by exactly the paused period: not to
	// created_at + the window (which would re-promise an hour that has already
	// partly elapsed), not to the resume itself (which would hand a delivery
	// that has been failing for an hour a fresh window), and not by the pause
	// twice.
	delta := f.retryDeadline(t, deliveryID).Sub(stamped)
	if delta < resumed.PausedFor-2*time.Millisecond || delta > resumed.PausedFor+2*time.Millisecond {
		t.Errorf("the stamped deadline moved %s, want the %s pause the resume recorded, added once (FR-1207)",
			delta, resumed.PausedFor)
	}
}

// ---------------------------------------------------------------------------
// S11 (FR-1303, FR-1304; SPEC.md A-10): the attempt-class lanes and the visible
// hold.
//
// Three properties are pinned here. The two lanes PARTITION the process attempt
// budget, so retry work cannot consume the allowance a first attempt draws on
// while attempts in flight for one process still total at most C. An endpoint
// that cannot admit a request is HELD - recorded on the row, visible to the
// producer, consuming no attempt and moving no window - instead of parking the
// dispatch path on it. And the parking this card exists for is pinned as a
// regression: at the base the healthy endpoint's delivery waits behind the capped
// endpoint's backlog; here it does not.
// ---------------------------------------------------------------------------

// addEndpoint registers one more consumer (its ref must be unused under the
// fixture's producer) with a verified endpoint subscribed to every eventType, and
// returns the consumer's ref and the endpoint's id.
func (f *s5Fixture) addEndpoint(t *testing.T, consumerRef, name, url string, eventTypes ...string) (string, string) {
	t.Helper()
	ctx := context.Background()
	con, err := f.st.CreateConsumer(ctx, f.sc, consumerRef, "Consumer "+consumerRef)
	if err != nil {
		t.Fatalf("CreateConsumer(%s): %v", consumerRef, err)
	}
	created, err := f.st.CreateEndpoint(ctx, f.sc, store.NewEndpoint{
		ConsumerID: con.ID, Name: name, URL: url, Internal: true,
	})
	if err != nil {
		t.Fatalf("CreateEndpoint(%s): %v", name, err)
	}
	if err := f.st.VerifyEndpoint(ctx, f.sc, created.Endpoint.ID, created.VerificationToken); err != nil {
		t.Fatalf("VerifyEndpoint(%s): %v", name, err)
	}
	for _, et := range eventTypes {
		if _, err := f.st.CreateSubscription(ctx, f.sc, store.NewSubscription{
			EndpointID: created.Endpoint.ID, EventType: et,
		}); err != nil {
			t.Fatalf("CreateSubscription(%s on %s): %v", et, name, err)
		}
	}
	return con.Ref, created.Endpoint.ID
}

// capEndpoint writes limit columns on an endpoint by id: the fixture's setEndpoint
// only reaches the fixture's own endpoint, and these tests need a second one with
// its own cap (FR-1302) or rate (FR-1306).
func (f *s5Fixture) capEndpoint(t *testing.T, endpointID, assignment string) {
	t.Helper()
	if _, err := f.admin.Exec(context.Background(),
		`update endpoints set `+assignment+` where id=$1`, endpointID); err != nil {
		t.Fatalf("update endpoint %s (%s): %v", endpointID, assignment, err)
	}
}

// seedRetryBacklog flips every delivery on one endpoint to a RETRY
// (deliveries.attempts = 1), which is the only thing that puts a row on FR-1303's
// retry lane: a delivery's class is its own counter, never how it was published.
func seedRetryBacklog(t *testing.T, admin *pgxpool.Pool, endpointID string) {
	t.Helper()
	if _, err := admin.Exec(context.Background(),
		`update deliveries set attempts = 1 where endpoint_id = $1`, endpointID); err != nil {
		t.Fatalf("seed retry backlog on %s: %v", endpointID, err)
	}
}

// deliveryRow is the durable state FR-1304's contract turns on, read back from the
// row rather than inferred from what the worker did.
type deliveryRow struct {
	attempts       int
	status         string
	heldSince      sql.NullTime
	heldReason     sql.NullString
	retryDeadline  sql.NullTime
	gate           sql.NullTime // next_attempt_at, the instant the row is claimable again
	firstAttemptAt sql.NullTime
	attemptRows    int
}

func readDelivery(t *testing.T, f *s5Fixture, deliveryID int64) deliveryRow {
	t.Helper()
	ctx := context.Background()
	var d deliveryRow
	if err := f.admin.QueryRow(ctx,
		`select attempts, status, held_back_since, held_back_reason, retry_deadline,
		        next_attempt_at, first_attempt_at
		   from deliveries where id = $1`, deliveryID).
		Scan(&d.attempts, &d.status, &d.heldSince, &d.heldReason, &d.retryDeadline,
			&d.gate, &d.firstAttemptAt); err != nil {
		t.Fatalf("read delivery %d: %v", deliveryID, err)
	}
	if err := f.admin.QueryRow(ctx,
		`select count(*) from attempts where delivery_id = $1`, deliveryID).Scan(&d.attemptRows); err != nil {
		t.Fatalf("count attempt rows for %d: %v", deliveryID, err)
	}
	return d
}

// waitFor polls until cond holds, up to budget, and reports whether it did. The
// conditions here are durable-state reads (a hold on a row, an attempt that
// started), never a guess at a sleep.
func waitFor(budget time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// pendingOn counts the still-pending deliveries of one endpoint: the backlog that
// is either due or held, and so the work still standing in front of anything
// published after it.
func pendingOn(t *testing.T, admin *pgxpool.Pool, endpointID string) int {
	t.Helper()
	var n int
	if err := admin.QueryRow(context.Background(),
		`select count(*) from deliveries where endpoint_id = $1 and status = 'pending'`, endpointID).Scan(&n); err != nil {
		t.Fatalf("count pending deliveries on %s: %v", endpointID, err)
	}
	return n
}

// runWorker starts one worker on the fixture with the given options and stops it
// when the test ends.
func (f *s5Fixture) runWorker(t *testing.T, mutate func(*Options)) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = New(f.st, f.options(mutate)).Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
}

// TestAC52DeliveryLanesPartitionTheProcessBudget is AC-52's lane half (FR-1303):
// one endpoint at its cap holding a due RETRY backlog, a second endpoint healthy
// with due FIRST attempts, and one process serving both. The retry backlog keeps
// the retry lane busy for the whole window; the healthy endpoint's first attempts
// must keep being claimed and started alongside it; retries in flight never exceed
// R, first attempts never exceed C - R, and the two together never exceed C.
//
// "The first-attempt lane is never empty" is deliberately NOT the assertion: a
// lane's allowance is an allowance, not an occupancy (a lane is empty whenever
// nothing of its class is in flight, which a correct implementation does all the
// time). What is asserted is that the allowance is never consumed by the other
// class, and that first attempts make progress throughout the drain.
func TestAC52DeliveryLanesPartitionTheProcessBudget(t *testing.T) {
	const (
		processCap  = 4 // PIGEON_DELIVERY_CONCURRENCY, FR-816's per-process cap
		retryLane   = 2 // PIGEON_DELIVERY_RETRY_CONCURRENCY, FR-1303's R
		firstLane   = processCap - retryLane
		endpointCap = 2
		dueRetry    = 60
		dueFirst    = 20
		service     = 100 * time.Millisecond
	)

	// The shipped defaults come from the deployment's own loader rather than from
	// numbers written here: R is partitioned OUT of C, so the first-attempt lane
	// keeps C - R and neither allowance may be zero at a deployment's defaults.
	t.Setenv("PIGEON_DATABASE_URL", "postgres://unused/unused?sslmode=disable")
	t.Setenv("PIGEON_REDIS_URL", "redis://unused:6379/0")
	t.Setenv("PIGEON_INSTANCE_ADMIN_PASSWORD", "not-the-documented-weak-default")
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("config.Load with the required variables set: %v", err)
	}
	if cfg.DeliveryConcurrency != 64 || cfg.DeliveryRetryConcurrency != 16 {
		t.Errorf("shipped lane defaults are C=%d R=%d, want C=64 and R=16 (FR-1303)",
			cfg.DeliveryConcurrency, cfg.DeliveryRetryConcurrency)
	}
	if first := cfg.DeliveryConcurrency - cfg.DeliveryRetryConcurrency; first < 1 {
		t.Errorf("the shipped partition leaves the first-attempt lane %d slots (FR-1303)", first)
	}

	// Each receiver takes a fixed 100 ms, so the number of handlers running at once
	// IS the number of attempts in flight - the shape AC-41 measures with.
	var retrySide, firstSide inFlightCounter
	srvA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		retrySide.enter()
		time.Sleep(service)
		retrySide.leave()
		w.WriteHeader(http.StatusOK)
	}))
	defer srvA.Close()
	srvB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		firstSide.enter()
		time.Sleep(service)
		firstSide.leave()
		w.WriteHeader(http.StatusOK)
	}))
	defer srvB.Close()

	// The capped endpoint is the fixture's own, at FR-1302's cap, holding a due
	// retry backlog (FR-1303's class predicate is `deliveries.attempts > 0`).
	f := newS5Fixture(t, srvA.URL, "order.created")
	f.setEndpoint(t, `max_concurrency = `+strconv.Itoa(endpointCap))
	for i := 0; i < dueRetry; i++ {
		f.publish(t, "order.created", `{"id":"retry-`+strconv.Itoa(i)+`"}`)
	}
	seedRetryBacklog(t, f.admin, f.endpointID)

	// The healthy endpoint: a second one, capped well above the process budget so
	// that only FR-1303's lanes can bound this process, holding due first attempts.
	refB, idB := f.addEndpoint(t, "school_s11b", "eB", srvB.URL, "order.created")
	f.capEndpoint(t, idB, "max_concurrency = 20")
	for i := 0; i < dueFirst; i++ {
		f.publishTo(t, refB, idB, "order.created", `{"id":"first-`+strconv.Itoa(i)+`"}`)
	}

	f.runWorker(t, func(o *Options) {
		o.Concurrency = processCap
		o.RetryConcurrency = retryLane
		o.Limits = f.limits
		o.MaxConnsPerHost = 10
		o.PollInterval = time.Millisecond
	})

	// Observe the drain. The assertion is progress-based rather than rate-based, so
	// a slower box (or a busy shared Postgres) makes the test take longer rather
	// than fail: AC-52 is about one class not consuming the other's allowance, not
	// about a throughput number. Sampling stops once BOTH classes have made
	// progress - the retry backlog is being served AND first attempts keep being
	// claimed and started - or the budget runs out.
	const progress = 3
	var worstRetry, worstFirst, worstTotal, startedRetry, startedFirst int
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		curRetry, peakRetry, startRetry := retrySide.snapshot()
		curFirst, peakFirst, startFirst := firstSide.snapshot()
		worstRetry = max(worstRetry, peakRetry)
		worstFirst = max(worstFirst, peakFirst)
		worstTotal = max(worstTotal, curRetry+curFirst)
		startedRetry, startedFirst = startRetry, startFirst
		if startedRetry >= progress && startedFirst >= progress {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if startedRetry < progress {
		t.Errorf("the capped endpoint served %d of its %d due retries in 30s: the retry lane was idle, so its bound proves nothing (FR-1303)",
			startedRetry, dueRetry)
	}
	if startedFirst < progress {
		t.Errorf("the healthy endpoint started %d of its %d due first attempts while the retry backlog drained (FR-1303)",
			startedFirst, dueFirst)
	}
	// The two classes overlapped: first attempts were starting while the capped
	// endpoint still had a backlog due. Without that overlap the bounds below prove
	// nothing - a lane that is never busy cannot overrun its allowance.
	if stillDue := pendingOn(t, f.admin, f.endpointID); stillDue == 0 {
		t.Errorf("the retry backlog had already drained when the first-attempt progress was observed, so the two classes never overlapped (FR-1303)")
	} else {
		t.Logf("first attempts started (%d) with %d of the capped endpoint's %d due retries still pending",
			startedFirst, stillDue, dueRetry)
	}
	if worstFirst == 0 {
		t.Error("no first attempt was ever in flight; the first-attempt lane was starved by the retry backlog (FR-1303)")
	}
	if worstRetry > retryLane {
		t.Errorf("retry attempts in flight peaked at %d, want <= R = %d (FR-1303)", worstRetry, retryLane)
	}
	if worstFirst > firstLane {
		t.Errorf("first-attempt attempts in flight peaked at %d, want <= C - R = %d (FR-1303)", worstFirst, firstLane)
	}
	if worstTotal > processCap {
		t.Errorf("attempts in flight across both lanes peaked at %d, want <= C = %d (FR-816: the lanes partition the budget, they do not add to it)",
			worstTotal, processCap)
	}
}

// TestAC52SharedLaneAtOneProcessSlotRunsBothClasses is A-10's boundary: the clamp
// reaches R = 0 only at C = 1, where ONE lane serves both classes because a
// degenerate partition MUST NOT starve FR-1201's retry policy. The assertion is
// progress - both a first attempt and a retry get attempted, with at most one in
// flight - not the lane bounds, which are not claimed at that boundary.
func TestAC52SharedLaneAtOneProcessSlotRunsBothClasses(t *testing.T) {
	var served inFlightCounter
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		served.enter()
		defer served.leave()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	f := newS5Fixture(t, srv.URL, "order.created")
	_, firstID := f.publish(t, "order.created", `{"id":"first"}`)
	_, retryID := f.publish(t, "order.created", `{"id":"retry"}`)
	if _, err := f.admin.Exec(context.Background(),
		`update deliveries set attempts = 1 where id = $1`, retryID); err != nil {
		t.Fatalf("seed the retry: %v", err)
	}

	f.runWorker(t, func(o *Options) {
		// C = 1 is the only configuration that reaches R = 0.
		o.Concurrency = 1
		o.RetryConcurrency = 1
		o.Limits = f.limits
		o.MaxConnsPerHost = 10
		o.PollInterval = time.Millisecond
	})

	delivered := func() bool {
		var n int
		if err := f.admin.QueryRow(context.Background(),
			`select count(*) from deliveries where status = 'delivered'`).Scan(&n); err != nil {
			t.Fatalf("count delivered: %v", err)
		}
		return n == 2
	}
	if !waitFor(10*time.Second, delivered) {
		first, retry := readDelivery(t, f, firstID), readDelivery(t, f, retryID)
		t.Fatalf("at C = 1 one shared lane must still run both classes: first attempt is %s (attempts %d), retry is %s (attempts %d) (FR-1201, FR-1303)",
			first.status, first.attempts, retry.status, retry.attempts)
	}
	if _, peak, started := served.snapshot(); peak != 1 {
		t.Errorf("at C = 1 the process ran %d attempts at once (started %d), want exactly 1 (FR-816, FR-1303)", peak, started)
	}
	for _, tc := range []struct {
		name string
		id   int64
	}{{"first attempt", firstID}, {"retry", retryID}} {
		row := readDelivery(t, f, tc.id)
		if row.status != "delivered" || !row.firstAttemptAt.Valid {
			t.Errorf("%s: status %s, first_attempt_at %v; the shared lane did not attempt it (FR-1201)",
				tc.name, row.status, row.firstAttemptAt)
		}
	}
}

// TestAC53HoldOnTheEndpointConcurrencyCap is AC-53's concurrency leg (FR-1304): an
// endpoint at its cap with deliveries due. Nothing is attempted and no attempt row
// is written; each held delivery carries held_back_since and a reason, the delivery
// view returns them, endpoint_held_back_total advances, retry_deadline is unchanged
// and deliveries.attempts does not advance; and when the cap frees, the delivery's
// next attempt is the one it already owed.
func TestAC53HoldOnTheEndpointConcurrencyCap(t *testing.T) {
	const (
		processCap  = 4
		retryLane   = 2
		endpointCap = 1
		// holderTTL keeps the endpoint's one slot leased for the whole assertion
		// phase, so the cap cannot free itself underneath a read: a short lease
		// lapsing mid-test is what makes the worker attempt the delivery and end
		// the hold episode the assertions below are reading. It is not an interval
		// the test waits out - the cap is freed by the explicit Release at the end,
		// not by this TTL expiring. The worker's own SlotTTL stays at the shipped
		// 3 x timeout ratio.
		holderTTL = 60 * time.Second
	)

	var served atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		served.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	f := newS5Fixture(t, srv.URL, "order.created")
	f.setEndpoint(t, `max_concurrency = 1`)
	_, deliveryID := f.publish(t, "order.created", `{"id":"held"}`)
	before := readDelivery(t, f, deliveryID)
	if before.attempts != 0 || before.retryDeadline.Valid || before.heldSince.Valid {
		t.Fatalf("the delivery is not in its pristine state: %+v", before)
	}

	// Put the endpoint AT its cap through the real limiter (FR-1307): one slot
	// leased by a holder that is not this delivery, which is what another replica's
	// in-flight attempt looks like in the shared slot set.
	ctx := context.Background()
	holder, ok, _, err := f.limits.Acquire(ctx, f.endpointID, endpointCap, holderTTL, "s11-test-holder")
	if err != nil {
		t.Fatalf("lease the endpoint's slot: %v", err)
	}
	if !ok {
		t.Fatalf("the endpoint's slot set was not empty: ok=%v", ok)
	}

	m := metrics.New()
	f.runWorker(t, func(o *Options) {
		o.Concurrency = processCap
		o.RetryConcurrency = retryLane
		o.Limits = f.limits
		o.MaxConnsPerHost = 10
		o.PollInterval = time.Millisecond
		o.Metrics = m
	})

	if !waitFor(5*time.Second, func() bool { return readDelivery(t, f, deliveryID).heldSince.Valid }) {
		t.Fatal("a delivery due at a capped endpoint was never held back: it was either attempted (FR-1304) or never claimed")
	}
	held := readDelivery(t, f, deliveryID)
	if held.heldReason.String != HoldReasonConcurrency {
		t.Errorf("held_back_reason is %q, want %q (FR-1302's cap is what refused it)", held.heldReason.String, HoldReasonConcurrency)
	}
	if got := served.Load(); got != 0 {
		t.Errorf("%d requests reached the endpoint's receiver while the delivery was held; a held delivery MUST NOT be attempted (FR-1304)", got)
	}
	if held.attemptRows != 0 {
		t.Errorf("%d attempt rows were written for a held delivery; the producer must see the hold, not a slow attempt (FR-1304)", held.attemptRows)
	}
	if held.attempts != before.attempts {
		t.Errorf("deliveries.attempts advanced from %d to %d on a hold; a hold consumes no attempt (FR-1304)", before.attempts, held.attempts)
	}
	if held.retryDeadline != before.retryDeadline {
		t.Errorf("retry_deadline moved from %v to %v on a hold; only a pause extends the window (FR-1207, FR-1304)",
			before.retryDeadline, held.retryDeadline)
	}
	if !held.gate.Valid || !held.gate.Time.After(held.heldSince.Time) {
		t.Errorf("the claim gate is %v, not after the hold started at %v: a held delivery must not be re-claimed in the same instant (FR-1304)",
			held.gate, held.heldSince)
	}
	if got := m.HeldBackFor(f.endpointID); got.Total < 1 || got.Seconds() <= 0 {
		t.Errorf("endpoint_held_back_total is %d and the held seconds are %v, want both to advance (OBS-13, FR-1304)",
			got.Total, got.Seconds())
	}

	// The delivery view (FR-1102/FR-1103) is where the producer reads the hold.
	detail, err := f.st.GetDelivery(ctx, f.sc, deliveryID)
	if err != nil {
		t.Fatalf("GetDelivery: %v", err)
	}
	if detail.Delivery.HeldBackSince == nil || detail.Delivery.HeldBackReason == nil {
		t.Errorf("the delivery view returned held_back_since=%v held_back_reason=%v, want both set (FR-1102, FR-1103)",
			detail.Delivery.HeldBackSince, detail.Delivery.HeldBackReason)
	} else {
		if !detail.Delivery.HeldBackSince.Equal(held.heldSince.Time) {
			t.Errorf("the view reports the hold starting at %v, the row at %v", detail.Delivery.HeldBackSince, held.heldSince.Time)
		}
		if *detail.Delivery.HeldBackReason != HoldReasonConcurrency {
			t.Errorf("the view reports reason %q, want %q", *detail.Delivery.HeldBackReason, HoldReasonConcurrency)
		}
	}

	// A second refusal is the same episode: held_back_since is the START of the
	// hold, not the instant of the last refusal (FR-1304's "since when").
	again, err := f.st.HoldDelivery(ctx, deliveryID, HoldReasonConcurrency, held.gate.Time)
	if err != nil {
		t.Fatalf("re-record the hold: %v", err)
	}
	if !again {
		t.Fatal("re-recording the hold on a still-pending delivery reported that nothing was held")
	}
	if second := readDelivery(t, f, deliveryID); !second.heldSince.Time.Equal(held.heldSince.Time) {
		t.Errorf("held_back_since moved from %v to %v on a repeated refusal; it is the start of the episode",
			held.heldSince.Time, second.heldSince.Time)
	}
	// A hold on a row that is no longer pending is a no-op, not an error: another
	// worker may have finished the delivery while this one was refused admission.
	if held, err := f.st.HoldDelivery(ctx, 1<<62, HoldReasonConcurrency, time.Now().Add(time.Second)); err != nil || held {
		t.Errorf("holding a delivery that does not exist = (%v, %v), want (false, nil) (FR-1304)", held, err)
	}

	// Free the cap and drop the claim gate with it: the next attempt is the one
	// the delivery already owed - attempt 1, with no attempt consumed by the
	// hold. The gate is the hold's own backoff (the wait the limiter reported,
	// which is a whole holderTTL here), not part of FR-1304's requirement, so
	// waiting it out would only make the test slower without asserting more.
	holder.Release(ctx)
	if _, err := f.admin.Exec(ctx,
		`update deliveries set lease_expires_at = now(), next_attempt_at = now() where id = $1`,
		deliveryID); err != nil {
		t.Fatalf("release the hold's claim gate: %v", err)
	}
	if !waitFor(10*time.Second, func() bool { return readDelivery(t, f, deliveryID).firstAttemptAt.Valid }) {
		t.Fatal("the delivery was never attempted after its endpoint's cap freed (FR-1304)")
	}
	after := readDelivery(t, f, deliveryID)
	if after.status != "delivered" {
		t.Errorf("status is %s after the cap freed, want delivered (the attempt it owed)", after.status)
	}
	if after.attempts != 1 {
		t.Errorf("deliveries.attempts is %d after its first attempt, want 1: the hold consumed an attempt (FR-1304)", after.attempts)
	}
	if after.attemptRows != 0 {
		t.Errorf("%d attempt rows were written for a successful first attempt (FR-811, AC-28)", after.attemptRows)
	}
	if after.heldSince.Valid {
		t.Errorf("held_back_since is still %v after the attempt started; the hold ends when the attempt runs (FR-1304)", after.heldSince.Time)
	}
	if got := served.Load(); got != 1 {
		t.Errorf("the receiver served %d requests, want exactly the one attempt the delivery owed", got)
	}
}

// TestAC53HoldOnTheEndpointsEmptyRateBucket is AC-53's second leg (FR-1306): the
// endpoint's concurrency is free and its rate bucket is empty. The delivery is held
// for the same reason shape, with the OTHER limiter named in the reason, and no
// attempt is made or recorded.
func TestAC53HoldOnTheEndpointsEmptyRateBucket(t *testing.T) {
	var served atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		served.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	f := newS5Fixture(t, srv.URL, "order.created")
	// Cap high, rate 1/s: the concurrency step admits, the bucket is what refuses,
	// and the wait the limiter reports is long enough to observe (FR-1306).
	f.setEndpoint(t, `max_concurrency = 20, max_rps = 1`)
	ctx := context.Background()
	_, deliveryID := f.publish(t, "order.created", `{"id":"rate-held"}`)
	before := readDelivery(t, f, deliveryID)

	// Empty the bucket: FR-1306's bucket is a Redis hash the limiter refills
	// continuously, and an empty one is what an endpoint whose traffic has outrun
	// its rate looks like.
	if err := f.rdb.HSet(ctx, limit.RPSKey(f.endpointID), "tokens", float64(0), "ts", time.Now().UnixMilli()).Err(); err != nil {
		t.Fatalf("empty the rate bucket: %v", err)
	}

	m := metrics.New()
	f.runWorker(t, func(o *Options) {
		o.Concurrency = 4
		o.RetryConcurrency = 2
		o.Limits = f.limits
		o.MaxConnsPerHost = 10
		o.PollInterval = time.Millisecond
		o.Metrics = m
	})

	if !waitFor(5*time.Second, func() bool { return readDelivery(t, f, deliveryID).heldSince.Valid }) {
		t.Fatal("a delivery due at an endpoint whose rate bucket is empty was never held back (FR-1304, FR-1306)")
	}
	held := readDelivery(t, f, deliveryID)
	if held.heldReason.String != HoldReasonRate {
		t.Errorf("held_back_reason is %q, want %q (FR-1306's bucket is what refused it)", held.heldReason.String, HoldReasonRate)
	}
	if got := served.Load(); got != 0 {
		t.Errorf("%d requests reached the receiver while the delivery was held (FR-1304)", got)
	}
	if held.attemptRows != 0 || held.attempts != before.attempts {
		t.Errorf("a held delivery took an attempt: %d rows, attempts %d -> %d (FR-1304)",
			held.attemptRows, before.attempts, held.attempts)
	}
	if held.retryDeadline != before.retryDeadline {
		t.Errorf("retry_deadline moved from %v to %v on a hold (FR-1207, FR-1304)", before.retryDeadline, held.retryDeadline)
	}
	if got := m.HeldBackFor(f.endpointID); got.Total < 1 || got.Seconds() <= 0 {
		t.Errorf("endpoint_held_back_total is %d and the held seconds are %v, want both to advance (OBS-13)", got.Total, got.Seconds())
	}

	// And when the bucket refills, the delivery's owed attempt is the one that runs.
	if !waitFor(10*time.Second, func() bool { return readDelivery(t, f, deliveryID).firstAttemptAt.Valid }) {
		t.Fatal("the delivery was never attempted after its endpoint's rate bucket refilled (FR-1306)")
	}
	if after := readDelivery(t, f, deliveryID); after.attempts != 1 || after.status != "delivered" {
		t.Errorf("after the bucket refilled: status %s, attempts %d; want delivered on attempt 1 (FR-1304)", after.status, after.attempts)
	}
}

// TestAC52RegressionHealthyEndpointNotParkedBehindCappedBacklog is the regression
// pin for the failure this card fixes. At the base, one claim loop takes the
// process slot, claims the oldest due row (the capped endpoint's backlog), and
// SLEEPS on the limiter while holding both - so the healthy endpoint's delivery,
// which stands behind that backlog in id order, waits a full slot TTL or more.
// After the change the healthy endpoint's first attempt starts as soon as the
// worker does, and the capped backlog cannot be in its way.
//
// The measured number is publish -> first attempt at the healthy endpoint; the
// threshold is 2 s, against a base that parks for a whole slot TTL per refused
// admission (3 s here, the shipped 3 x attempt-timeout ratio).
func TestAC52RegressionHealthyEndpointNotParkedBehindCappedBacklog(t *testing.T) {
	const (
		processCap  = 4
		retryLane   = 2
		endpointCap = 2
		dueBacklog  = 5
		// probeBudget bounds the wait for the healthy endpoint's first attempt, and
		// threshold is the wall-clock guard under it: the property is the overlap
		// asserted below, and the clock is only what makes the base's parking
		// visible (it measures 15.9 s there, so 3 s separates the two cleanly).
		probeBudget = 5 * time.Second
		threshold   = 3 * time.Second
	)

	// The capped endpoint's receiver holds every request until the test ends, so its
	// slots stay leased for the whole run: that is the saturated endpoint whose
	// backlog parks a shared dispatch path. The release is closed by a cleanup
	// registered AFTER the server's, so it runs FIRST (cleanups are LIFO) and
	// httptest's Close never waits on a handler that is still parked.
	release := make(chan struct{})
	var backlogServed atomic.Int32
	srvA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		backlogServed.Add(1)
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srvA.Close)
	t.Cleanup(func() { close(release) })

	f := newS5Fixture(t, srvA.URL, "order.created")
	f.setEndpoint(t, `max_concurrency = `+strconv.Itoa(endpointCap))
	for i := 0; i < dueBacklog; i++ {
		f.publish(t, "order.created", `{"id":"backlog-`+strconv.Itoa(i)+`"}`)
	}
	seedRetryBacklog(t, f.admin, f.endpointID)

	// The healthy endpoint: its own receiver, one due first-attempt delivery
	// published AFTER the backlog, so at the base it is the last row in id order.
	firstAttempt := make(chan time.Time, 1)
	srvB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		select {
		case firstAttempt <- time.Now():
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srvB.Close()
	refB, idB := f.addEndpoint(t, "school_s11c", "eC", srvB.URL, "order.created")
	f.capEndpoint(t, idB, "max_concurrency = 20")
	// The healthy endpoint's one due first attempt is published AFTER the backlog,
	// so it is the youngest row: at the base it is the last one the single loop
	// would reach.
	f.publishTo(t, refB, idB, "order.created", `{"id":"healthy"}`)

	f.runWorker(t, func(o *Options) {
		o.Concurrency = processCap
		o.RetryConcurrency = retryLane
		o.Limits = f.limits
		o.MaxConnsPerHost = 10
		o.PollInterval = time.Millisecond
		o.Timeout = time.Second
		o.SlotTTL = 3 * time.Second
	})
	published := time.Now()
	var arrived time.Time
	served := waitFor(probeBudget, func() bool {
		select {
		case arrived = <-firstAttempt:
			return true
		default:
			return false
		}
	})
	elapsed := arrived.Sub(published)
	if !served {
		elapsed = time.Since(published)
	}
	t.Logf("healthy endpoint publish->first-attempt: %s (the capped endpoint had served %d requests and still had %d of its %d deliveries pending)",
		elapsed.Round(time.Millisecond), backlogServed.Load(), pendingOn(t, f.admin, f.endpointID), dueBacklog)
	if !served {
		t.Fatalf("the healthy endpoint's first attempt had not started %s after publish, want it while the capped endpoint's backlog is still there: the dispatch path is parked behind that backlog (FR-1303, FR-1304)",
			elapsed.Round(time.Millisecond))
	}
	if elapsed >= threshold {
		t.Errorf("the healthy endpoint's publish->first-attempt took %s, want < %s: the dispatch path is parked behind the capped endpoint's backlog (FR-1303, FR-1304)",
			elapsed.Round(time.Millisecond), threshold)
	}
	// And it started while the backlog was still there, which is the property rather
	// than a wall clock: at the base the single loop works through that backlog
	// first (a whole slot TTL parked on each refused admission), so the healthy
	// endpoint's row is reached only after every backlog delivery has been claimed.
	if still := pendingOn(t, f.admin, f.endpointID); still < dueBacklog {
		t.Errorf("the healthy endpoint's first attempt started only after %d of the capped endpoint's %d deliveries had left the pending set, so it waited behind the backlog (FR-1303)",
			dueBacklog-still, dueBacklog)
	}
}
