// Package metrics is the smallest counter set the core needs: no Prometheus
// dependency, just labelled counters the tests can read and the relay advances.
//
// It exists for one requirement in M0 (§7.5a, FR-508/FR-509): every non-delivery
// reason has its own owner and its own remediation, so each must be a distinct
// metric label. A single generic "not_delivered" counter could not tell an
// un-onboarded consumer from a paused endpoint, which is the whole point of the
// four-way split.
package metrics

import "sync"

// Metrics holds labelled counters. It is safe for concurrent use; the relay
// records from many goroutines as events fan out.
type Metrics struct {
	mu            sync.Mutex
	nonDeliveries map[string]int64
}

// New builds an empty Metrics.
func New() *Metrics {
	return &Metrics{nonDeliveries: map[string]int64{}}
}

// RecordNonDelivery advances the counter for one §7.5a reason. The reason IS the
// label (FR-508): "unroutable:consumer_not_registered",
// "not_delivered:no_matching_subscription", "not_delivered:endpoint_unverified",
// "not_delivered:endpoint_paused". An empty reason is ignored rather than
// creating a "" bucket, because a non-delivery without a reason is a bug to find.
func (m *Metrics) RecordNonDelivery(reason string) {
	if reason == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nonDeliveries[reason]++
}

// NonDeliveryCount is the read side, for tests and the /metrics scrape. A reason
// never recorded reads as 0.
func (m *Metrics) NonDeliveryCount(reason string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nonDeliveries[reason]
}

// NonDeliverySnapshot returns a copy of the current non-delivery counters, so a
// reader sees a consistent view without holding the lock.
func (m *Metrics) NonDeliverySnapshot() map[string]int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]int64, len(m.nonDeliveries))
	for k, v := range m.nonDeliveries {
		out[k] = v
	}
	return out
}
