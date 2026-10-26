package metrics

import "testing"

// TestRecordNonDelivery proves the counter is per-reason (FR-508): each §7.5a
// reason is its own series, so an un-onboarded consumer is separable from a
// paused endpoint in the metric, which is the whole point of the four-way split
// (AC-23 vs AC-24 in the metric label).
func TestRecordNonDelivery(t *testing.T) {
	m := New()
	m.RecordNonDelivery("unroutable:consumer_not_registered")
	m.RecordNonDelivery("unroutable:consumer_not_registered")
	m.RecordNonDelivery("not_delivered:no_matching_subscription")

	if got := m.NonDeliveryCount("unroutable:consumer_not_registered"); got != 2 {
		t.Fatalf("unroutable count = %d, want 2", got)
	}
	if got := m.NonDeliveryCount("not_delivered:no_matching_subscription"); got != 1 {
		t.Fatalf("no_matching count = %d, want 1", got)
	}
	// A reason never recorded reads as 0, not a panic or a wrong bucket.
	if got := m.NonDeliveryCount("not_delivered:endpoint_paused"); got != 0 {
		t.Fatalf("unrecorded reason count = %d, want 0", got)
	}

	snap := m.NonDeliverySnapshot()
	if snap["unroutable:consumer_not_registered"] != 2 || snap["not_delivered:no_matching_subscription"] != 1 {
		t.Fatalf("snapshot = %v", snap)
	}
	// The snapshot is a copy: mutating it must not touch the live counters.
	snap["unroutable:consumer_not_registered"] = 999
	if got := m.NonDeliveryCount("unroutable:consumer_not_registered"); got != 2 {
		t.Fatalf("snapshot leaked into the live counter: %d", got)
	}
}

// TestRecordNonDeliveryIgnoresEmptyReason guards against a non-delivery without a
// reason silently creating a "" bucket, which would be a bug to find.
func TestRecordNonDeliveryIgnoresEmptyReason(t *testing.T) {
	m := New()
	m.RecordNonDelivery("")
	if len(m.NonDeliverySnapshot()) != 0 {
		t.Fatalf("empty reason created a bucket: %v", m.NonDeliverySnapshot())
	}
}
