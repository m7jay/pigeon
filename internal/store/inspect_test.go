package store

import (
	"testing"
	"time"
)

// These two are pure functions of the read surface, so they are tested without a
// database: the rollup (FR-1109) and the trail (FR-1110) are the parts a UI
// renders, and a regression in either is invisible to an integration test that
// only checks status codes.

// TestRollupIsTheDocumentedPrecedence is FR-1109: the five values, and the order
// the doc comment claims for them.
func TestRollupIsTheDocumentedPrecedence(t *testing.T) {
	str := func(s string) *string { return &s }
	const (
		routable   = "routable"
		unroutable = "unroutable"
	)

	cases := []struct {
		name    string
		state   *string
		reason  *string
		status  []string
		want    string
		because string
	}{
		{"unroutable beats everything", str(unroutable), str(ReasonUnroutableNotRegistered),
			[]string{statusDelivered}, RollupUnroutable,
			"the event never had a destination"},
		{"unroutable with no deliveries", str(unroutable), str(ReasonUnroutableNotRegistered),
			nil, RollupUnroutable, "§7.5a"},
		{"fan-out has not run", nil, nil, nil, RollupPending,
			"no rows and no reason: the relay has not resolved it yet"},
		{"consumer matched nothing", str(routable), str(ReasonNoMatchingSubscription),
			nil, RollupFailed, "the consumer exists but nothing is subscribed (AC-24)"},
		{"one delivery pending", str(routable), nil, []string{statusPending}, RollupPending, ""},
		{"a pending row outranks a delivered one", str(routable), nil,
			[]string{statusDelivered, statusPending}, RollupPending, "the outcome is not settled"},
		{"all delivered", str(routable), nil,
			[]string{statusDelivered, statusDelivered}, RollupAllDelivered, ""},
		{"a skipped row breaks all_delivered", str(routable), nil,
			[]string{statusDelivered, statusSkipped}, RollupPartiallyFailed,
			"a matched endpoint that could not take the event means the event did not fully deliver"},
		{"nothing delivered", str(routable), nil,
			[]string{"failed", "failed"}, RollupFailed, ""},
		{"only skipped", str(routable), nil, []string{statusSkipped}, RollupFailed,
			"skipped is not delivered"},
		{"one of each", str(routable), nil, []string{statusDelivered, "failed"}, RollupPartiallyFailed, ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Rollup(tc.state, tc.reason, tc.status)
			if got != tc.want {
				t.Fatalf("Rollup(%v, %v, %v) = %q, want %q (%s)", deref(tc.state), deref(tc.reason),
					tc.status, got, tc.want, tc.because)
			}
		})
	}
}

func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}

// TestTrailIsNeverEmptyForDeliveriesWithoutAttemptRows is FR-1110.
func TestTrailIsNeverEmptyForDeliveriesWithoutAttemptRows(t *testing.T) {
	started := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	delivered := started.Add(250 * time.Millisecond)

	// A successful first attempt: FR-811 writes the four columns and no attempt
	// row, so the trail is synthesized from the delivery itself.
	summary := DeliverySummary{
		ID: 7, Status: statusDelivered, Attempts: 1,
		FirstAttemptAt: &started, DeliveredAt: &delivered, CreatedAt: started,
	}
	detail := DeliveryDetail{Delivery: summary}
	detail.EffectiveAttempt = effectiveAttempt(summary)
	trail := detail.Trail()
	if len(trail) != 1 {
		t.Fatalf("a delivered first attempt rendered %d trail entries, want 1 (AC-32)", len(trail))
	}
	if trail[0].Attempt != 1 || trail[0].Outcome != statusDelivered {
		t.Fatalf("synthesized attempt = %+v, want attempt 1 delivered", trail[0])
	}
	if trail[0].DurationMs != 250 {
		t.Fatalf("synthesized attempt duration = %d ms, want 250 (FR-1110)", trail[0].DurationMs)
	}
	if !trail[0].OccurredAt.Equal(delivered) {
		t.Fatalf("synthesized attempt occurred at %v, want the delivery time %v", trail[0].OccurredAt, delivered)
	}

	// A skipped delivery was never attempted; its trail is the reason.
	skipped := DeliverySummary{
		ID: 8, Status: statusSkipped, CreatedAt: started,
		NonDeliveryReason: strPtr(ReasonEndpointUnverified),
	}
	skippedDetail := DeliveryDetail{Delivery: skipped, EffectiveAttempt: effectiveAttempt(skipped)}
	if trail := skippedDetail.Trail(); len(trail) != 1 || trail[0].Outcome != ReasonEndpointUnverified {
		t.Fatalf("a skipped delivery's trail = %+v, want the unverified reason", trail)
	}

	// A delivery with attempt rows reports them, unchanged and in order.
	failed := DeliverySummary{ID: 9, Status: "failed", Attempts: 2, CreatedAt: started}
	failedDetail := DeliveryDetail{
		Delivery: failed,
		Attempts: []Attempt{{Attempt: 1, OccurredAt: started, Outcome: "retryable"}, {Attempt: 2, OccurredAt: delivered}},
	}
	if trail := failedDetail.Trail(); len(trail) != 2 || trail[0].Attempt != 1 || trail[1].Attempt != 2 {
		t.Fatalf("a failed delivery's trail = %+v, want its two rows in order", trail)
	}

	// A delivery still in flight has nothing to render, and says so by being
	// empty: the rollup is `pending`, so there is no status to show yet.
	pending := DeliverySummary{ID: 10, Status: statusPending, CreatedAt: started}
	pendingDetail := DeliveryDetail{Delivery: pending, EffectiveAttempt: effectiveAttempt(pending)}
	if trail := pendingDetail.Trail(); len(trail) != 0 {
		t.Fatalf("a pending delivery rendered %+v, want nothing (it has not been attempted)", trail)
	}
}

func strPtr(s string) *string { return &s }
