package relay

import (
	"testing"

	"github.com/m7jay/pigeon/internal/metrics"
	"github.com/m7jay/pigeon/internal/store"
)

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
