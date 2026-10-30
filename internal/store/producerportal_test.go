package store

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// sameJSON compares two JSON documents by value: payload_schema is a jsonb
// column, so PostgreSQL normalises the bytes it was given.
func sameJSON(t *testing.T, got, want []byte) bool {
	t.Helper()
	var a, b any
	if err := json.Unmarshal(got, &a); err != nil {
		t.Fatalf("decode the returned schema: %v", err)
	}
	if err := json.Unmarshal(want, &b); err != nil {
		t.Fatalf("decode the expected schema: %v", err)
	}
	return reflect.DeepEqual(a, b)
}

// S9a's producer-portal reads (internal/store/portal.go) and the two S9a
// invariants they expose: UI-2/UI-11's health and unroutable reads, UI-4's
// synthetic-test marker, and UI-9/UI-14's subscribable event types.

// insertDelivery writes one delivery row directly. EndpointHealth is derived from
// these rows at read time, so an exact streak assertion needs a fixture with
// chosen statuses and timestamps - and a fixture that writes rows itself has to
// create each day's partition, which fan-out would normally have done.
func insertDelivery(t *testing.T, admin *pgxpool.Pool, sc Scope, eventID, endpointID, consumerID, status string, reason *string, createdAt time.Time) {
	t.Helper()
	if _, err := admin.Exec(context.Background(),
		`select pigeon_ensure_partition('deliveries', $1::date)`, createdAt); err != nil {
		t.Fatalf("ensure deliveries partition for %s: %v", createdAt.UTC().Format("2006-01-02"), err)
	}
	if _, err := admin.Exec(context.Background(), `
		insert into deliveries (event_id, endpoint_id, producer_id, env, consumer_id, status, non_delivery_reason, created_at)
		values ($1,$2,$3,$4,$5,$6,$7,$8)`,
		eventID, endpointID, sc.ProducerID, sc.Env, consumerID, status, reason, createdAt); err != nil {
		t.Fatalf("insert %s delivery: %v", status, err)
	}
}

// TestEndpointHealthCountsTheFailureStreak is UI-2/UI-11's number: the count of
// an endpoint's non-deliveries since its most recent delivered row. Both
// directions are asserted - the streak it reports, and that an intervening
// delivered row resets it - plus the two shapes the doc comment names: an
// endpoint with no deliveries is absent, and a pending row neither breaks nor
// extends the streak.
func TestEndpointHealthCountsTheFailureStreak(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	st := newAppStore(t, admin)
	ctx := context.Background()

	pid, _ := seedProducer(t, admin, "s9ahealth")
	sc := Scope{ProducerID: pid, Env: EnvLive}

	et := mustConsumerEvent(t, st, sc, "order.placed")
	con := mustConsumer(t, st, sc, "school_health", "School Health")
	ep := mustVerifiedEndpoint(t, st, sc, con.ID, "health-1", "https://8.8.8.8/hook")
	quiet := mustVerifiedEndpoint(t, st, sc, con.ID, "health-2", "https://1.1.1.1/hook")

	// One real event to hang the fixture rows on (deliveries.event_id is a foreign
	// key). Fan-out is deliberately not run, so the only deliveries are ours.
	pub, err := st.PublishEvent(ctx, sc, PublishInput{
		EventType: et.Name, Payload: json.RawMessage(`{}`), ConsumerRef: ptr(con.Ref),
	})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}

	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	at := func(min int) time.Time { return base.Add(time.Duration(min) * time.Minute) }
	paused := ReasonEndpointPaused

	// readHealth returns the endpoint under test's health, and pins the shape of
	// the entry for an endpoint that has never had a delivery: a zero streak, no
	// status and no timestamp, which is what the caller renders as "no attempts
	// yet". (portal.go's comment calls that entry "absent from the map"; the
	// query joins off the input ids, so it is present and empty - the two read
	// the same to a caller, and the empty one is asserted here.)
	readHealth := func() EndpointHealth {
		t.Helper()
		m, err := st.EndpointHealth(ctx, sc, []string{ep.ID, quiet.ID})
		if err != nil {
			t.Fatalf("EndpointHealth: %v", err)
		}
		if h, ok := m[quiet.ID]; !ok {
			t.Fatalf("EndpointHealth dropped the endpoint that has no deliveries (map: %+v)", m)
		} else if h.FailureStreak != 0 || h.LastStatus != "" || h.LastDeliveryAt != nil {
			t.Fatalf("an endpoint with no deliveries reported %+v, want zero streak, no status and no timestamp", h)
		}
		h, ok := m[ep.ID]
		if !ok {
			t.Fatalf("EndpointHealth has no entry for %s (map: %+v)", ep.ID, m)
		}
		return h
	}

	// delivered, then a failed_terminal and a skipped after it: streak 2.
	insertDelivery(t, admin, sc, pub.ID, ep.ID, con.ID, statusDelivered, nil, at(0))
	insertDelivery(t, admin, sc, pub.ID, ep.ID, con.ID, "failed_terminal", nil, at(1))
	insertDelivery(t, admin, sc, pub.ID, ep.ID, con.ID, statusSkipped, &paused, at(2))

	h := readHealth()
	if h.FailureStreak != 2 {
		t.Fatalf("FailureStreak = %d, want 2 (failed_terminal + skipped since the last delivered)", h.FailureStreak)
	}
	if h.LastStatus != statusSkipped {
		t.Fatalf("LastStatus = %q, want %q", h.LastStatus, statusSkipped)
	}
	if h.LastDeliveryAt == nil || !h.LastDeliveryAt.Equal(at(2)) {
		t.Fatalf("LastDeliveryAt = %v, want %v", h.LastDeliveryAt, at(2))
	}

	// An intervening delivered row resets the streak to zero.
	insertDelivery(t, admin, sc, pub.ID, ep.ID, con.ID, statusDelivered, nil, at(3))
	h = readHealth()
	if h.FailureStreak != 0 {
		t.Fatalf("FailureStreak = %d after an intervening delivered row, want 0 (a delivery ends the streak)", h.FailureStreak)
	}
	if h.LastStatus != statusDelivered {
		t.Fatalf("LastStatus = %q, want %q", h.LastStatus, statusDelivered)
	}

	// ...and it accumulates again from the new baseline.
	insertDelivery(t, admin, sc, pub.ID, ep.ID, con.ID, "failed_terminal", nil, at(4))
	insertDelivery(t, admin, sc, pub.ID, ep.ID, con.ID, statusSkipped, &paused, at(5))
	h = readHealth()
	if h.FailureStreak != 2 {
		t.Fatalf("FailureStreak = %d after two more non-deliveries, want 2", h.FailureStreak)
	}
	if h.LastStatus != statusSkipped {
		t.Fatalf("LastStatus = %q, want %q", h.LastStatus, statusSkipped)
	}

	// A newer pending row is in flight: it neither breaks nor extends the streak,
	// and LastStatus is what tells "succeeded" from "not attempted yet".
	insertDelivery(t, admin, sc, pub.ID, ep.ID, con.ID, statusPending, nil, at(6))
	h = readHealth()
	if h.FailureStreak != 2 {
		t.Fatalf("FailureStreak = %d with a newer pending row, want 2 (an in-flight delivery is not a failure)", h.FailureStreak)
	}
	if h.LastStatus != statusPending {
		t.Fatalf("LastStatus = %q, want %q (the most recent delivery whatever its status)", h.LastStatus, statusPending)
	}

	// An empty input is the documented empty answer, with no query.
	if m, err := st.EndpointHealth(ctx, sc, nil); err != nil || len(m) != 0 {
		t.Fatalf("EndpointHealth with no ids = (%+v, %v), want an empty map and a nil error", m, err)
	}
}

// TestProducerPortalSeesOnlyItsOwnProducerEvents is UI-11's panel: the read
// reports this producer's events with the routing outcome fan-out recorded, and
// a sibling producer's event - even one addressed to a consumer registered under
// the *same* external ref - does not appear. A foreign consumer id is ErrNotFound
// (FR-108), so the panel cannot be aimed at another tenant to see whether it
// exists.
func TestProducerPortalSeesOnlyItsOwnProducerEvents(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	st := newAppStore(t, admin)
	ctx := context.Background()

	aID, _ := seedProducer(t, admin, "s9aportal-a")
	bID, _ := seedProducer(t, admin, "s9aportal-b")
	a := Scope{ProducerID: aID, Env: EnvLive}
	b := Scope{ProducerID: bID, Env: EnvLive}

	// Both producers register a consumer under the SAME external ref: if the read
	// were scoped by ref alone, the sibling's event would surface in this panel.
	const sharedRef = "school_shared"
	etA := mustConsumerEvent(t, st, a, "order.placed")
	etB := mustConsumerEvent(t, st, b, "order.placed")
	conA := mustConsumer(t, st, a, sharedRef, "School A")
	conB := mustConsumer(t, st, b, sharedRef, "School B")
	mustVerifiedEndpoint(t, st, a, conA.ID, "a-1", "https://8.8.8.8/hook")
	mustVerifiedEndpoint(t, st, b, conB.ID, "b-1", "https://1.1.1.1/hook")

	pubA, err := st.PublishEvent(ctx, a, PublishInput{
		EventType: etA.Name, Payload: json.RawMessage(`{}`), ConsumerRef: ptr(conA.Ref),
	})
	if err != nil {
		t.Fatalf("publish A: %v", err)
	}
	if _, err := st.FanOutEvent(ctx, a, pubA.ID); err != nil {
		t.Fatalf("fan-out A: %v", err)
	}
	pubB, err := st.PublishEvent(ctx, b, PublishInput{
		EventType: etB.Name, Payload: json.RawMessage(`{}`), ConsumerRef: ptr(conB.Ref),
	})
	if err != nil {
		t.Fatalf("publish B: %v", err)
	}
	if _, err := st.FanOutEvent(ctx, b, pubB.ID); err != nil {
		t.Fatalf("fan-out B: %v", err)
	}

	// The consumer exists but nothing is subscribed: the event routed (routable)
	// and carries §7.5a's no-matching-subscription reason.
	events, err := st.ConsumerEvents(ctx, a, conA.ID, 0)
	if err != nil {
		t.Fatalf("ConsumerEvents(A): %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("ConsumerEvents(A) returned %d events (%+v), want exactly the producer's own: a sibling's event under the same consumer_ref must not appear", len(events), events)
	}
	if events[0].EventID != pubA.ID {
		t.Fatalf("ConsumerEvents(A) returned event %s, want %s", events[0].EventID, pubA.ID)
	}
	if events[0].RoutingState != "routable" || events[0].Reason != ReasonNoMatchingSubscription {
		t.Fatalf("event reported (%q, %q), want routable/%q", events[0].RoutingState, events[0].Reason, ReasonNoMatchingSubscription)
	}

	// A foreign consumer id is ErrNotFound, never an empty panel.
	if _, err := st.ConsumerEvents(ctx, a, conB.ID, 0); err != ErrNotFound {
		t.Fatalf("ConsumerEvents with a sibling producer's consumer id = %v, want ErrNotFound", err)
	}

	// The unroutable leg: an event whose ref matched no consumer at fan-out. The
	// consumer that turns out to own that ref is registered afterwards, and the
	// panel reports the recorded routing_state and non_delivery_reason rather
	// than a bare "unroutable" label.
	const lateRef = "school_late"
	ghost, err := st.PublishEvent(ctx, a, PublishInput{
		EventType: etA.Name, Payload: json.RawMessage(`{}`), ConsumerRef: ptr(lateRef),
	})
	if err != nil {
		t.Fatalf("publish unroutable: %v", err)
	}
	res, err := st.FanOutEvent(ctx, a, ghost.ID)
	if err != nil {
		t.Fatalf("fan-out unroutable: %v", err)
	}
	if res.RoutingState != "unroutable" || res.EventReason != ReasonUnroutableNotRegistered {
		t.Fatalf("fan-out = (%q, %q), want unroutable/%q", res.RoutingState, res.EventReason, ReasonUnroutableNotRegistered)
	}
	conLate := mustConsumer(t, st, a, lateRef, "School Late")
	late, err := st.ConsumerEvents(ctx, a, conLate.ID, 0)
	if err != nil {
		t.Fatalf("ConsumerEvents(late consumer): %v", err)
	}
	if len(late) != 1 || late[0].EventID != ghost.ID {
		t.Fatalf("ConsumerEvents(late consumer) = %+v, want just the unroutable event %s", late, ghost.ID)
	}
	if late[0].RoutingState != "unroutable" || late[0].Reason != ReasonUnroutableNotRegistered {
		t.Fatalf("unroutable event reported (%q, %q), want unroutable/%q", late[0].RoutingState, late[0].Reason, ReasonUnroutableNotRegistered)
	}

	// The first consumer's panel is unchanged by the second consumer's events.
	again, err := st.ConsumerEvents(ctx, a, conA.ID, 0)
	if err != nil {
		t.Fatalf("ConsumerEvents(A) again: %v", err)
	}
	if len(again) != 1 || again[0].EventID != pubA.ID {
		t.Fatalf("ConsumerEvents(A) after the unroutable event = %+v, want still just %s", again, pubA.ID)
	}
}

// TestTestPublishMarkerRidesTheEventToTheDeliveryLog is UI-4: "Send test event"
// is clearly marked wherever the event is later seen. The marker is asserted in
// all three places a producer looks - the events row, the store's own event read,
// and the delivery log - for a test publish and for an ordinary one.
func TestTestPublishMarkerRidesTheEventToTheDeliveryLog(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	st := newAppStore(t, admin)
	ctx := context.Background()

	pid, _ := seedProducer(t, admin, "s9atestmarker")
	sc := Scope{ProducerID: pid, Env: EnvLive}
	et := mustConsumerEvent(t, st, sc, "order.placed")
	con := mustConsumer(t, st, sc, "school_marker", "School Marker")
	ep := mustVerifiedEndpoint(t, st, sc, con.ID, "marker-1", "https://8.8.8.8/hook")
	mustSubscribe(t, st, sc, ep.ID, et.Name)

	publish := func(test bool) PublishedEvent {
		t.Helper()
		pub, err := st.PublishEvent(ctx, sc, PublishInput{
			EventType: et.Name, Payload: json.RawMessage(`{}`), ConsumerRef: ptr(con.Ref), Test: test,
		})
		if err != nil {
			t.Fatalf("PublishEvent(test=%v): %v", test, err)
		}
		if _, err := st.FanOutEvent(ctx, sc, pub.ID); err != nil {
			t.Fatalf("FanOutEvent(%s): %v", pub.ID, err)
		}
		return pub
	}
	deliveryFor := func(eventID string) DeliverySummary {
		t.Helper()
		page, err := st.ListDeliveries(ctx, sc, DeliveryFilter{EndpointID: ep.ID}, 50, nil)
		if err != nil {
			t.Fatalf("ListDeliveries: %v", err)
		}
		for _, d := range page.Deliveries {
			if d.EventID == eventID {
				return d
			}
		}
		t.Fatalf("no delivery for event %s in the log (%+v)", eventID, page.Deliveries)
		return DeliverySummary{}
	}

	for _, tc := range []struct {
		test bool
		want bool
	}{{true, true}, {false, false}} {
		pub := publish(tc.test)
		if pub.IsTest != tc.want {
			t.Fatalf("PublishedEvent.IsTest = %v for a test=%v publish, want %v", pub.IsTest, tc.test, tc.want)
		}
		var stored bool
		if err := admin.QueryRow(ctx, `select is_test from events where id = $1`, pub.ID).Scan(&stored); err != nil {
			t.Fatalf("read events.is_test: %v", err)
		}
		if stored != tc.want {
			t.Fatalf("events.is_test = %v for a test=%v publish, want %v (the marker is a column, not a field of the 202)", stored, tc.test, tc.want)
		}
		view, err := st.EventView(ctx, sc, pub.ID)
		if err != nil {
			t.Fatalf("EventView: %v", err)
		}
		if view.IsTest != tc.want {
			t.Fatalf("EventView.IsTest = %v for a test=%v publish, want %v", view.IsTest, tc.test, tc.want)
		}
		if got := deliveryFor(pub.ID).IsTest; got != tc.want {
			t.Fatalf("DeliverySummary.IsTest = %v for a test=%v publish, want %v", got, tc.test, tc.want)
		}
	}
}

// TestSubscribableEventTypesCarryTheirSchema is UI-14's list and UI-9's contract
// detail in one read: name, payload_version and payload_schema for the types this
// producer has left publicly subscribable *and* enabled in the caller's
// environment. A closed type, a disabled type, a sibling producer's type and the
// other environment's type are all absent.
func TestSubscribableEventTypesCarryTheirSchema(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	st := newAppStore(t, admin)
	ctx := context.Background()

	pid, _ := seedProducer(t, admin, "s9asubscribable")
	siblingID, _ := seedProducer(t, admin, "s9asubsibling")
	sc := Scope{ProducerID: pid, Env: EnvLive}
	inTest := Scope{ProducerID: pid, Env: EnvTest}
	sibling := Scope{ProducerID: siblingID, Env: EnvLive}

	schema := json.RawMessage(`{"type":"object","properties":{"order_id":{"type":"string"}}}`)
	open, err := st.CreateEventType(ctx, sc, NewEventType{
		Name: "order.placed", PayloadSchema: schema, PayloadVersion: 3, RoutingMode: RoutingConsumer,
	})
	if err != nil {
		t.Fatalf("CreateEventType(open): %v", err)
	}
	if !open.PubliclySubscribable {
		t.Fatal("a freshly created type is not publicly subscribable; the default is the fixture's premise")
	}

	// A type the producer has closed to public subscription.
	if _, err := st.CreateEventType(ctx, sc, NewEventType{
		Name: "order.private", PayloadSchema: schema, RoutingMode: RoutingConsumer,
	}); err != nil {
		t.Fatalf("CreateEventType(private): %v", err)
	}
	if _, err := st.PatchEventType(ctx, sc, "order.private", EventTypePatch{PubliclySubscribable: ptr(false)}); err != nil {
		t.Fatalf("close order.private: %v", err)
	}

	// A publicly subscribable but disabled type: a disabled type can never be
	// published (FR-305), so subscribing to it would be a delivery that cannot
	// happen.
	if _, err := st.CreateEventType(ctx, sc, NewEventType{
		Name: "order.disabled", PayloadSchema: schema, RoutingMode: RoutingConsumer,
	}); err != nil {
		t.Fatalf("CreateEventType(disabled): %v", err)
	}
	if _, err := st.PatchEventType(ctx, sc, "order.disabled", EventTypePatch{Enabled: ptr(false)}); err != nil {
		t.Fatalf("disable order.disabled: %v", err)
	}

	// A sibling producer's type under the very same name, and this producer's
	// type of the same name in the other environment: neither belongs in this
	// list.
	if _, err := st.CreateEventType(ctx, sibling, NewEventType{
		Name: "order.placed", PayloadSchema: schema, RoutingMode: RoutingConsumer,
	}); err != nil {
		t.Fatalf("CreateEventType(sibling): %v", err)
	}
	if _, err := st.CreateEventType(ctx, inTest, NewEventType{
		Name: "order.placed", PayloadSchema: schema, RoutingMode: RoutingConsumer,
	}); err != nil {
		t.Fatalf("CreateEventType(test env): %v", err)
	}

	types, err := st.ListSubscribableEventTypes(ctx, sc)
	if err != nil {
		t.Fatalf("ListSubscribableEventTypes: %v", err)
	}
	if len(types) != 1 {
		var names []string
		for _, et := range types {
			names = append(names, et.Name)
		}
		t.Fatalf("ListSubscribableEventTypes returned %d types (%s), want only the one publicly subscribable, enabled type in this producer and environment", len(types), strings.Join(names, ","))
	}
	got := types[0]
	if got.Name != open.Name {
		t.Fatalf("type = %q, want %q", got.Name, open.Name)
	}
	if got.PayloadVersion != 3 {
		t.Fatalf("payload_version = %d, want 3 (the contract detail travels with the name)", got.PayloadVersion)
	}
	// payload_schema is a jsonb column, so the stored text is normalised; compare
	// the documents, not their byte spelling.
	if !sameJSON(t, got.PayloadSchema, schema) {
		t.Fatalf("payload_schema = %s, want %s", got.PayloadSchema, schema)
	}

	// The test plane answers for its own rows only.
	if types, err := st.ListSubscribableEventTypes(ctx, inTest); err != nil || len(types) != 1 || types[0].Name != "order.placed" {
		t.Fatalf("test-environment list = (%+v, %v), want the test plane's own order.placed", types, err)
	}
}
