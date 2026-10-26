package relay

import (
	"context"
	"testing"

	"github.com/m7jay/pigeon/internal/metrics"
	"github.com/m7jay/pigeon/internal/store"
)

func ptr[T any](v T) *T { return &v }

// TestRecordFanoutAdvancesEachReason proves the fan-out → metric mapping keeps
// the §7.5a reasons distinct (FR-508): the event-level reason and every skipped
// endpoint's reason each advance their own label, so "no matching subscription"
// is never folded into "endpoint paused" in the counter. That distinctness is the
// metric half of AC-23 vs AC-24.
func TestRecordFanoutAdvancesEachReason(t *testing.T) {
	m := metrics.New()
	recordFanout(m, store.FanOutResult{
		EventReason: store.ReasonNoMatchingSubscription,
		Skipped: []store.SkippedDelivery{
			{EndpointID: "end_1", Reason: store.ReasonEndpointPaused},
			{EndpointID: "end_2", Reason: store.ReasonEndpointUnverified},
		},
	})

	cases := map[string]int64{
		store.ReasonNoMatchingSubscription: 1,
		store.ReasonEndpointPaused:         1,
		store.ReasonEndpointUnverified:     1,
	}
	for reason, want := range cases {
		if got := m.NonDeliveryCount(reason); got != want {
			t.Fatalf("metric %q = %d, want %d", reason, got, want)
		}
	}
	// An unroutable event records only its own label, nothing else.
	m2 := metrics.New()
	recordFanout(m2, store.FanOutResult{EventReason: store.ReasonUnroutableNotRegistered})
	if m2.NonDeliveryCount(store.ReasonUnroutableNotRegistered) != 1 {
		t.Fatalf("unroutable metric missing")
	}
	if m2.NonDeliveryCount(store.ReasonNoMatchingSubscription) != 0 {
		t.Fatalf("unroutable event leaked into another label")
	}
}

// TestRecordFanoutFromRealFanOutCountsEachEndpointReasonOnce drives a real store
// fan-out that skips two subscribed endpoints of one consumer for DIFFERENT
// reasons — one paused, one active-but-unverified in live — then records the
// result through recordFanout. It is the end-to-end half of FR-508/FR-509: for
// that one event the endpoint_paused and endpoint_unverified metric counters are
// each advanced exactly once (never folded together), and the durable delivery
// rows each carry the same reason the metric advanced for them, so the stored
// row and the metric label can never disagree.
func TestRecordFanoutFromRealFanOutCountsEachEndpointReasonOnce(t *testing.T) {
	st, admin := harness(t)
	ctx := context.Background()

	// One producer, one consumer-routed type, one consumer, two subscribed
	// endpoints: epPaused verified-then-paused, epUnverified never verified.
	pid := seedProducer(t, admin, "relay-skip2")
	sc := store.Scope{ProducerID: pid, Env: "live"}
	et, err := st.CreateEventType(ctx, sc, store.NewEventType{
		Name: "signup.completed", PayloadSchema: jsonRaw(`{}`), RoutingMode: store.RoutingConsumer,
	})
	if err != nil {
		t.Fatalf("CreateEventType: %v", err)
	}
	con, err := st.CreateConsumer(ctx, sc, "school_1", "School 1")
	if err != nil {
		t.Fatalf("CreateConsumer: %v", err)
	}
	epPausedC, err := st.CreateEndpoint(ctx, sc, store.NewEndpoint{ConsumerID: con.ID, Name: "paused", URL: "https://8.8.8.8/hook"})
	if err != nil {
		t.Fatalf("CreateEndpoint(paused): %v", err)
	}
	if err := st.VerifyEndpoint(ctx, sc, epPausedC.Endpoint.ID, epPausedC.VerificationToken); err != nil {
		t.Fatalf("VerifyEndpoint: %v", err)
	}
	if _, err := st.SetEndpointPaused(ctx, sc, epPausedC.Endpoint.ID, true); err != nil {
		t.Fatalf("SetEndpointPaused: %v", err)
	}
	epUnverifiedC, err := st.CreateEndpoint(ctx, sc, store.NewEndpoint{ConsumerID: con.ID, Name: "unverified", URL: "https://9.9.9.9/hook"})
	if err != nil {
		t.Fatalf("CreateEndpoint(unverified): %v", err)
	}
	for _, ep := range []string{epPausedC.Endpoint.ID, epUnverifiedC.Endpoint.ID} {
		if _, err := st.CreateSubscription(ctx, sc, store.NewSubscription{EndpointID: ep, EventType: et.Name}); err != nil {
			t.Fatalf("CreateSubscription(%s): %v", ep, err)
		}
	}

	pub, err := st.PublishEvent(ctx, sc, store.PublishInput{
		EventType: et.Name, Payload: jsonRaw(`{}`), ConsumerRef: ptr(con.Ref),
	})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}

	res, err := st.FanOutEvent(ctx, sc, pub.ID)
	if err != nil {
		t.Fatalf("FanOutEvent: %v", err)
	}
	if res.Deliveries != 0 || len(res.Skipped) != 2 {
		t.Fatalf("fan-out = %+v, want 0 deliveries and 2 skips", res)
	}

	m := metrics.New()
	recordFanout(m, res)

	// Each endpoint reason is its own label, advanced exactly once; nothing
	// event-level advanced, because the event routed (to skipped rows).
	if got := m.NonDeliveryCount(store.ReasonEndpointPaused); got != 1 {
		t.Fatalf("endpoint_paused metric = %d, want 1", got)
	}
	if got := m.NonDeliveryCount(store.ReasonEndpointUnverified); got != 1 {
		t.Fatalf("endpoint_unverified metric = %d, want 1", got)
	}
	if got := m.NonDeliveryCount(store.ReasonNoMatchingSubscription) + m.NonDeliveryCount(store.ReasonUnroutableNotRegistered); got != 0 {
		t.Fatalf("event-level labels advanced (%d), want 0 for a routed-to-skipped event", got)
	}

	// Durable agreement: each skipped row's reason matches the metric advanced
	// for that endpoint.
	want := map[string]string{
		epPausedC.Endpoint.ID:     store.ReasonEndpointPaused,
		epUnverifiedC.Endpoint.ID: store.ReasonEndpointUnverified,
	}
	for epID, wantReason := range want {
		var reason string
		if err := admin.QueryRow(ctx,
			`select non_delivery_reason from deliveries where event_id=$1 and endpoint_id=$2`,
			pub.ID, epID).Scan(&reason); err != nil {
			t.Fatalf("read skipped row for %s: %v", epID, err)
		}
		if reason != wantReason {
			t.Fatalf("delivery row for %s reason = %q, want %q (row and metric must agree)", epID, reason, wantReason)
		}
	}
}
