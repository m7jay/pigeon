package store

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// seedRoutingProducer registers a fresh producer for one routing test.
func seedRoutingProducer(t *testing.T, admin *pgxpool.Pool, slug string) string {
	t.Helper()
	pid, _ := seedProducer(t, admin, slug)
	return pid
}

// mustConsumer registers a consumer in the caller's scope.
func mustConsumer(t *testing.T, st *Store, sc Scope, ref, name string) Consumer {
	t.Helper()
	c, err := st.CreateConsumer(context.Background(), sc, ref, name)
	if err != nil {
		t.Fatalf("CreateConsumer(%q): %v", ref, err)
	}
	return c
}

// mustConsumerEvent creates a consumer-routed event type in the caller's scope.
func mustConsumerEvent(t *testing.T, st *Store, sc Scope, name string) EventType {
	t.Helper()
	et, err := st.CreateEventType(context.Background(), sc, NewEventType{
		Name:          name,
		PayloadSchema: json.RawMessage(`{}`),
		RoutingMode:   RoutingConsumer,
	})
	if err != nil {
		t.Fatalf("CreateEventType(%q, consumer): %v", name, err)
	}
	return et
}

// mustVerifiedEndpoint registers and verifies an endpoint for a consumer, so it
// is deliverable in live (FR-403 answered).
func mustVerifiedEndpoint(t *testing.T, st *Store, sc Scope, consumerID, name, url string) Endpoint {
	t.Helper()
	created, err := st.CreateEndpoint(context.Background(), sc, NewEndpoint{
		ConsumerID: consumerID, Name: name, URL: url,
	})
	if err != nil {
		t.Fatalf("CreateEndpoint(%s): %v", name, err)
	}
	if err := st.VerifyEndpoint(context.Background(), sc, created.Endpoint.ID, created.VerificationToken); err != nil {
		t.Fatalf("VerifyEndpoint(%s): %v", name, err)
	}
	return created.Endpoint
}

// mustSubscribe subscribes an endpoint to an event type in the caller's scope.
func mustSubscribe(t *testing.T, st *Store, sc Scope, endpointID, eventType string) {
	t.Helper()
	if _, err := st.CreateSubscription(context.Background(), sc, NewSubscription{
		EndpointID: endpointID, EventType: eventType,
	}); err != nil {
		t.Fatalf("CreateSubscription(%s->%s): %v", endpointID, eventType, err)
	}
}

func countDeliveries(t *testing.T, admin *pgxpool.Pool, eventID string) int {
	t.Helper()
	var n int
	if err := admin.QueryRow(context.Background(),
		`select count(*) from deliveries where event_id = $1`, eventID).Scan(&n); err != nil {
		t.Fatalf("count deliveries: %v", err)
	}
	return n
}

// scanEventRouting reads the event-level routing outcome the UI-facing view
// surfaces, so AC-23 and AC-24 are distinguishable without joining deliveries.
func scanEventRouting(t *testing.T, admin *pgxpool.Pool, eventID string) (routingState, reason string) {
	t.Helper()
	if err := admin.QueryRow(context.Background(),
		`select coalesce(routing_state,''), coalesce(non_delivery_reason,'') from events where id = $1`,
		eventID).Scan(&routingState, &reason); err != nil {
		t.Fatalf("scan event routing: %v", err)
	}
	return routingState, reason
}

// resetRoutingTables wipes the routing/fan-out state so a test starts clean. The
// disposable test database is shared across local runs, and event types,
// consumers and endpoints are not created idempotently, so a second run would
// collide on its own fixtures. Producers and api keys are kept (each test re-seeds
// its own, on-conflict-do-nothing), so truncating the child tables is enough.
func resetRoutingTables(t *testing.T, admin *pgxpool.Pool) {
	t.Helper()
	if _, err := admin.Exec(context.Background(),
		`truncate table events, outbox, event_types, consumers, endpoints, subscriptions, deliveries cascade`); err != nil {
		t.Fatalf("reset routing tables: %v", err)
	}
}

// AC-02: an event with three subscriptions produces three deliveries, each
// pending with its own attempt trail (the worker fills the trail later; fan-out
// creates the rows and sets the lease at creation, FR-703/FR-813).
func TestFanOutCreatesOneDeliveryPerSubscription(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	st := New(appPool(t, admin))
	ctx := context.Background()
	pid := seedRoutingProducer(t, admin, "ac02")
	sc := Scope{ProducerID: pid, Env: "live"}

	et := mustConsumerEvent(t, st, sc, "signup.completed")
	con := mustConsumer(t, st, sc, "school_1", "School 1")
	ep1 := mustVerifiedEndpoint(t, st, sc, con.ID, "e1", "https://8.8.8.8/hook")
	ep2 := mustVerifiedEndpoint(t, st, sc, con.ID, "e2", "https://1.1.1.1/hook")
	ep3 := mustVerifiedEndpoint(t, st, sc, con.ID, "e3", "https://9.9.9.9/hook")
	mustSubscribe(t, st, sc, ep1.ID, et.Name)
	mustSubscribe(t, st, sc, ep2.ID, et.Name)
	mustSubscribe(t, st, sc, ep3.ID, et.Name)

	pub, err := st.PublishEvent(ctx, sc, PublishInput{
		EventType:   et.Name,
		Payload:     json.RawMessage(`{}`),
		ConsumerRef: ptr(con.Ref),
	})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}

	res, err := st.FanOutEvent(ctx, sc, pub.ID)
	if err != nil {
		t.Fatalf("FanOutEvent: %v", err)
	}
	if res.Deliveries != 3 {
		t.Fatalf("fan-out created %d deliveries, want 3", res.Deliveries)
	}
	if res.EventReason != "" || len(res.Skipped) != 0 {
		t.Fatalf("fan-out result = %+v, want a clean 3-delivery routable event", res)
	}
	if n := countDeliveries(t, admin, pub.ID); n != 3 {
		t.Fatalf("deliveries table has %d rows, want 3", n)
	}
	// Each delivery is pending with the lease set at creation and no attempts yet.
	var pending, attempts int
	if err := admin.QueryRow(ctx,
		`select count(*) filter (where status='pending'), count(*) from deliveries where event_id=$1`,
		pub.ID).Scan(&pending, &attempts); err != nil {
		t.Fatalf("scan deliveries: %v", err)
	}
	if pending != 3 || attempts != 3 {
		t.Fatalf("deliveries: %d pending of %d, want 3 of 3 pending (lease set at fan-out)", pending, attempts)
	}
}

// AC-03: a consumer-routed event with a consumer but nothing subscribed records
// the no_matching_subscription reason and creates zero deliveries.
func TestFanOutNoMatchingSubscription(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	st := New(appPool(t, admin))
	ctx := context.Background()
	pid := seedRoutingProducer(t, admin, "ac03")
	sc := Scope{ProducerID: pid, Env: "live"}

	et := mustConsumerEvent(t, st, sc, "signup.completed")
	con := mustConsumer(t, st, sc, "school_1", "School 1")
	// An endpoint exists but subscribes to nothing.
	mustVerifiedEndpoint(t, st, sc, con.ID, "e1", "https://8.8.8.8/hook")

	pub, err := st.PublishEvent(ctx, sc, PublishInput{
		EventType: et.Name, Payload: json.RawMessage(`{}`), ConsumerRef: ptr(con.Ref),
	})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	res, err := st.FanOutEvent(ctx, sc, pub.ID)
	if err != nil {
		t.Fatalf("FanOutEvent: %v", err)
	}
	if res.Deliveries != 0 || res.EventReason != ReasonNoMatchingSubscription {
		t.Fatalf("fan-out = %+v, want 0 deliveries and %q", res, ReasonNoMatchingSubscription)
	}
	if n := countDeliveries(t, admin, pub.ID); n != 0 {
		t.Fatalf("deliveries = %d, want 0", n)
	}
	// The event carries the reason the UI shows (AC-24 distinguishability below).
	if routing, reason := scanEventRouting(t, admin, pub.ID); reason != ReasonNoMatchingSubscription || routing != "routable" {
		t.Fatalf("event routing = (%q,%q), want routable/%q", routing, reason, ReasonNoMatchingSubscription)
	}
}

// AC-23: an unknown consumer_ref is accepted in live (recorded unroutable, zero
// deliveries) but refused in test, and the two are distinguishable in the UI view
// and would be in the metric label.
func TestPublishUnknownConsumerRefLiveAcceptsTestRejects(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	st := New(appPool(t, admin))
	ctx := context.Background()
	pid := seedRoutingProducer(t, admin, "ac23")
	live := Scope{ProducerID: pid, Env: "live"}
	test := Scope{ProducerID: pid, Env: "test"}

	// The type must exist in both planes to reach the ref check.
	mustConsumerEvent(t, st, live, "signup.completed")
	mustConsumerEvent(t, st, test, "signup.completed")

	const ghost = "school_ghost"

	// Live: accepted, recorded unroutable, zero deliveries.
	pub, err := st.PublishEvent(ctx, live, PublishInput{
		EventType: "signup.completed", Payload: json.RawMessage(`{}`), ConsumerRef: ptr(ghost),
	})
	if err != nil {
		t.Fatalf("live PublishEvent with unknown ref should be accepted (202): %v", err)
	}
	res, err := st.FanOutEvent(ctx, live, pub.ID)
	if err != nil {
		t.Fatalf("FanOutEvent: %v", err)
	}
	if res.Deliveries != 0 || res.EventReason != ReasonUnroutableNotRegistered {
		t.Fatalf("live fan-out = %+v, want 0 deliveries and %q", res, ReasonUnroutableNotRegistered)
	}
	if n := countDeliveries(t, admin, pub.ID); n != 0 {
		t.Fatalf("live deliveries = %d, want 0", n)
	}
	if routing, reason := scanEventRouting(t, admin, pub.ID); routing != "unroutable" || reason != ReasonUnroutableNotRegistered {
		t.Fatalf("live event routing = (%q,%q), want unroutable/%q", routing, reason, ReasonUnroutableNotRegistered)
	}

	// Test: refused as a 422 (ErrInvalid).
	if _, err := st.PublishEvent(ctx, test, PublishInput{
		EventType: "signup.completed", Payload: json.RawMessage(`{}`), ConsumerRef: ptr(ghost),
	}); !isInvalid(err) {
		t.Fatalf("test PublishEvent with unknown ref: err = %v, want ErrInvalid (422)", err)
	}
}

// AC-24: a known consumer with nothing subscribed is distinguishable from AC-23.
// It is routable (the consumer exists) with a no_matching_subscription reason,
// not unroutable. This test asserts the two reasons differ so the metric label
// and the UI can tell them apart.
func TestFanOutKnownConsumerNoSubscriptionDistinguishableFromUnroutable(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	st := New(appPool(t, admin))
	ctx := context.Background()
	pid := seedRoutingProducer(t, admin, "ac24")
	sc := Scope{ProducerID: pid, Env: "live"}

	et := mustConsumerEvent(t, st, sc, "signup.completed")
	con := mustConsumer(t, st, sc, "school_1", "School 1")
	mustVerifiedEndpoint(t, st, sc, con.ID, "e1", "https://8.8.8.8/hook")

	pub, err := st.PublishEvent(ctx, sc, PublishInput{
		EventType: et.Name, Payload: json.RawMessage(`{}`), ConsumerRef: ptr(con.Ref),
	})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	res, err := st.FanOutEvent(ctx, sc, pub.ID)
	if err != nil {
		t.Fatalf("FanOutEvent: %v", err)
	}
	if res.EventReason != ReasonNoMatchingSubscription {
		t.Fatalf("reason = %q, want %q", res.EventReason, ReasonNoMatchingSubscription)
	}
	if res.EventReason == ReasonUnroutableNotRegistered {
		t.Fatalf("no-matching-subscription must never read as unroutable (AC-24)")
	}
}

// AC-25: a consumer event type takes exactly one of consumer_id / consumer_ref.
// Both or neither is a 422 naming the conflict.
func TestPublishConsumerKeyConflict(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	st := New(appPool(t, admin))
	ctx := context.Background()
	pid := seedRoutingProducer(t, admin, "ac25")
	sc := Scope{ProducerID: pid, Env: "live"}

	et := mustConsumerEvent(t, st, sc, "signup.completed")
	con := mustConsumer(t, st, sc, "school_1", "School 1")

	// Both.
	if _, err := st.PublishEvent(ctx, sc, PublishInput{
		EventType: et.Name, Payload: json.RawMessage(`{}`),
		ConsumerID: ptr(con.ID), ConsumerRef: ptr(con.Ref),
	}); !isInvalid(err) {
		t.Fatalf("both keys: err = %v, want ErrInvalid (422)", err)
	}
	// Neither.
	if _, err := st.PublishEvent(ctx, sc, PublishInput{
		EventType: et.Name, Payload: json.RawMessage(`{}`),
	}); !isInvalid(err) {
		t.Fatalf("neither key: err = %v, want ErrInvalid (422)", err)
	}
}

// AC-27: an event for consumer A reaches only A's endpoints; consumer B's
// endpoints see no delivery. Fan-out is scoped by the resolved consumer id.
func TestFanOutScopedToConsumer(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	st := New(appPool(t, admin))
	ctx := context.Background()
	pid := seedRoutingProducer(t, admin, "ac27")
	sc := Scope{ProducerID: pid, Env: "live"}

	et := mustConsumerEvent(t, st, sc, "signup.completed")
	conA := mustConsumer(t, st, sc, "school_A", "School A")
	conB := mustConsumer(t, st, sc, "school_B", "School B")
	epA := mustVerifiedEndpoint(t, st, sc, conA.ID, "a", "https://8.8.8.8/hook")
	epB := mustVerifiedEndpoint(t, st, sc, conB.ID, "b", "https://1.1.1.1/hook")
	mustSubscribe(t, st, sc, epA.ID, et.Name)
	mustSubscribe(t, st, sc, epB.ID, et.Name)

	pub, err := st.PublishEvent(ctx, sc, PublishInput{
		EventType: et.Name, Payload: json.RawMessage(`{}`), ConsumerRef: ptr(conA.Ref),
	})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	res, err := st.FanOutEvent(ctx, sc, pub.ID)
	if err != nil {
		t.Fatalf("FanOutEvent: %v", err)
	}
	if res.Deliveries != 1 {
		t.Fatalf("fan-out created %d deliveries, want 1 (only consumer A)", res.Deliveries)
	}
	// The one delivery belongs to A's endpoint; B's endpoint has none.
	var gotA, gotB int
	if err := admin.QueryRow(ctx,
		`select count(*) from deliveries d join endpoints e on e.id=d.endpoint_id
		 where d.event_id=$1 and e.consumer_id=$2`, pub.ID, conA.ID).Scan(&gotA); err != nil {
		t.Fatalf("count A: %v", err)
	}
	if err := admin.QueryRow(ctx,
		`select count(*) from deliveries d join endpoints e on e.id=d.endpoint_id
		 where d.event_id=$1 and e.consumer_id=$2`, pub.ID, conB.ID).Scan(&gotB); err != nil {
		t.Fatalf("count B: %v", err)
	}
	if gotA != 1 || gotB != 0 {
		t.Fatalf("deliveries: A=%d B=%d, want A=1 B=0", gotA, gotB)
	}
}

// FR-704: a re-fan-out of the same event is idempotent, not a duplicate.
func TestFanOutIsIdempotent(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	st := New(appPool(t, admin))
	ctx := context.Background()
	pid := seedRoutingProducer(t, admin, "idem")
	sc := Scope{ProducerID: pid, Env: "live"}

	et := mustConsumerEvent(t, st, sc, "signup.completed")
	con := mustConsumer(t, st, sc, "school_1", "School 1")
	ep := mustVerifiedEndpoint(t, st, sc, con.ID, "e1", "https://8.8.8.8/hook")
	mustSubscribe(t, st, sc, ep.ID, et.Name)

	pub, err := st.PublishEvent(ctx, sc, PublishInput{
		EventType: et.Name, Payload: json.RawMessage(`{}`), ConsumerRef: ptr(con.Ref),
	})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	if _, err := st.FanOutEvent(ctx, sc, pub.ID); err != nil {
		t.Fatalf("first fan-out: %v", err)
	}
	first := countDeliveries(t, admin, pub.ID)
	// The relay re-leases and re-fans-out after a crash; the second call must not
	// add rows.
	res, err := st.FanOutEvent(ctx, sc, pub.ID)
	if err != nil {
		t.Fatalf("second fan-out: %v", err)
	}
	if res.Deliveries != 0 {
		t.Fatalf("re-fan-out created %d deliveries, want 0 (idempotent)", res.Deliveries)
	}
	if n := countDeliveries(t, admin, pub.ID); n != first {
		t.Fatalf("deliveries = %d after re-fan-out, want %d (no duplicates)", n, first)
	}
}

// FR-406 / FR-509: a paused endpoint is skipped with its own reason, not silently
// dropped, and a re-fan-out after resume delivers.
func TestFanOutSkipsPausedEndpointWithReason(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	st := New(appPool(t, admin))
	ctx := context.Background()
	pid := seedRoutingProducer(t, admin, "paused")
	sc := Scope{ProducerID: pid, Env: "live"}

	et := mustConsumerEvent(t, st, sc, "signup.completed")
	con := mustConsumer(t, st, sc, "school_1", "School 1")
	ep := mustVerifiedEndpoint(t, st, sc, con.ID, "e1", "https://8.8.8.8/hook")
	mustSubscribe(t, st, sc, ep.ID, et.Name)
	if _, err := st.SetEndpointPaused(ctx, sc, ep.ID, true); err != nil {
		t.Fatalf("SetEndpointPaused: %v", err)
	}

	pub, err := st.PublishEvent(ctx, sc, PublishInput{
		EventType: et.Name, Payload: json.RawMessage(`{}`), ConsumerRef: ptr(con.Ref),
	})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	res, err := st.FanOutEvent(ctx, sc, pub.ID)
	if err != nil {
		t.Fatalf("FanOutEvent: %v", err)
	}
	if res.Deliveries != 0 || len(res.Skipped) != 1 || res.Skipped[0].Reason != ReasonEndpointPaused {
		t.Fatalf("paused fan-out = %+v, want 0 deliveries and one endpoint_paused skip", res)
	}
}

// FR-703/FR-704/FR-706 + FR-508/FR-509: one event can skip two of a consumer's
// subscribed endpoints for DIFFERENT reasons — one paused, one active-but-
// unverified in live. Each skipped delivery row must carry ITS OWN endpoint's
// reason, not an arbitrary one drawn from the batch. This is the regression
// guard for the skip-path batch insert: the endpoint id and its reason must be
// paired by position (a single unnest($5,$6) zip), not by cartesian product. A
// CROSS JOIN of two separate unnest() calls pairs each endpoint with every
// reason, and ON CONFLICT (event_id, endpoint_id) DO NOTHING then keeps an
// arbitrary reason per endpoint — corrupting both the stored row and the metric
// label the relay derives from it. The Go-side result.Skipped was always correct;
// only the written rows were wrong, so this test asserts the durable rows.
func TestFanOutSkipsTwoEndpointsEachWithItsOwnReason(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	st := New(appPool(t, admin))
	ctx := context.Background()
	pid := seedRoutingProducer(t, admin, "skip2")
	sc := Scope{ProducerID: pid, Env: "live"}

	et := mustConsumerEvent(t, st, sc, "signup.completed")
	con := mustConsumer(t, st, sc, "school_1", "School 1")

	// epPaused: verified (so it is otherwise deliverable) then paused -> paused.
	epPaused := mustVerifiedEndpoint(t, st, sc, con.ID, "paused", "https://8.8.8.8/hook")
	mustSubscribe(t, st, sc, epPaused.ID, et.Name)
	if _, err := st.SetEndpointPaused(ctx, sc, epPaused.ID, true); err != nil {
		t.Fatalf("SetEndpointPaused: %v", err)
	}

	// epUnverified: created but never verified, still active -> unverified in live.
	created, err := st.CreateEndpoint(ctx, sc, NewEndpoint{ConsumerID: con.ID, Name: "unverified", URL: "https://9.9.9.9/hook"})
	if err != nil {
		t.Fatalf("CreateEndpoint(unverified): %v", err)
	}
	epUnverified := created.Endpoint
	mustSubscribe(t, st, sc, epUnverified.ID, et.Name)

	pub, err := st.PublishEvent(ctx, sc, PublishInput{
		EventType: et.Name, Payload: json.RawMessage(`{}`), ConsumerRef: ptr(con.Ref),
	})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}

	res, err := st.FanOutEvent(ctx, sc, pub.ID)
	if err != nil {
		t.Fatalf("FanOutEvent: %v", err)
	}
	if res.Deliveries != 0 {
		t.Fatalf("deliveries = %d, want 0 (both endpoints skipped)", res.Deliveries)
	}
	// The in-memory result already lists each endpoint with its own reason.
	got := map[string]string{}
	for _, s := range res.Skipped {
		got[s.EndpointID] = s.Reason
	}
	if len(got) != 2 || got[epPaused.ID] != ReasonEndpointPaused || got[epUnverified.ID] != ReasonEndpointUnverified {
		t.Fatalf("res.Skipped = %v, want paused->%q and unverified->%q", res.Skipped, ReasonEndpointPaused, ReasonEndpointUnverified)
	}

	// The durable proof: each skipped row carries its own endpoint's reason.
	// A cross-join skip insert gives both rows the same (arbitrary) reason; this
	// fails exactly there.
	want := map[string]string{epPaused.ID: ReasonEndpointPaused, epUnverified.ID: ReasonEndpointUnverified}
	for epID, wantReason := range want {
		var reason, status string
		if err := admin.QueryRow(ctx,
			`select non_delivery_reason, status from deliveries where event_id=$1 and endpoint_id=$2`,
			pub.ID, epID).Scan(&reason, &status); err != nil {
			t.Fatalf("read skipped delivery for %s: %v", epID, err)
		}
		if reason != wantReason {
			t.Fatalf("skipped delivery for %s has reason %q, want %q (each skipped endpoint must keep its own reason)", epID, reason, wantReason)
		}
	}
	// Exactly two skipped rows, one per endpoint — no cartesian duplication.
	var n int
	if err := admin.QueryRow(ctx,
		`select count(*) from deliveries where event_id=$1 and status='skipped'`, pub.ID).Scan(&n); err != nil {
		t.Fatalf("count skipped: %v", err)
	}
	if n != 2 {
		t.Fatalf("skipped rows = %d, want 2 (one per endpoint)", n)
	}
}
