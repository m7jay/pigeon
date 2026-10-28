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
	// counters is the S6 delivery/attempt/log-gate set, name -> value, exposed
	// verbatim at GET /metrics (LOG-2, AC-33).
	counters map[string]int64
	// Storage budget telemetry (SB-6): how many partitions retention has
	// dropped, and how many publishes were shed because the volume was over
	// budget. Both are operator-facing — /metrics is the scrape an alert rule
	// reads, and AC-18 turns on both being non-zero. They are typed fields
	// rather than map entries because their readers are typed (the sweeper and
	// the ingest gate read them without a string key) and because the series
	// must read zero before the first drop, not be absent.
	partitionDrops int64
	sheds          int64
}

// The counter names the delivery and flusher paths advance. They exist because
// LOG-2 and AC-33 require the volume a log line would have carried to live in a
// counter instead: "no per-success log lines; log volume stays under the
// documented bound; counters advance". Every name is also exposed at GET
// /metrics, so an operator reads the same numbers the log gate suppressed.
const (
	// DeliveriesDelivered counts successful attempts (OBS-4's success half).
	DeliveriesDelivered = "pigeon_deliveries_delivered_total"
	// DeliveriesRetried counts retryable failures that were re-queued (FR-805).
	DeliveriesRetried = "pigeon_deliveries_retried_total"
	// DeliveriesFailedTerminal counts deliveries that ended failed (FR-806, §9).
	DeliveriesFailedTerminal = "pigeon_deliveries_failed_terminal_total"
	// AttemptsBuffered counts evidence rows handed to the Redis buffer (D-1).
	AttemptsBuffered = "pigeon_attempts_buffered_total"
	// AttemptsFlushed counts evidence rows that reached Postgres (FR-815).
	AttemptsFlushed = "pigeon_attempts_flushed_total"
	// AttemptsConflicted counts buffered rows Postgres already had. A non-zero
	// value is an anomaly worth an alarm, never a lost row: the buffer's replay
	// after a crash re-inserts the same (delivery_id, attempt) on purpose.
	AttemptsConflicted = "pigeon_attempts_conflicted_total"
	// FlushBatches counts flush transactions (the denominator of FR-815's rows
	// per transaction, which is what the batching claim is about).
	FlushBatches = "pigeon_attempt_flush_batches_total"
	// FlushErrors counts flush attempts that failed. A failed insert left the
	// batch pending in Redis; a failed retirement left a batch whose rows are
	// already committed and whose entries stay pending until the stale reclaim
	// window. Either way nothing was lost, and the wrapped error says which half
	// it was.
	FlushErrors = "pigeon_attempt_flush_errors_total"
	// SuccessLogsEmitted and SuccessLogsSuppressed are LOG-2's own accounting:
	// the emitted/suppressed split is the evidence that the bound held.
	SuccessLogsEmitted    = "pigeon_success_logs_emitted_total"
	SuccessLogsSuppressed = "pigeon_success_logs_suppressed_total"
	// AttemptBufferPending is the buffer depth at the last flush pass. A gauge,
	// not a counter (OBS-9's queue depth in the same spirit).
	AttemptBufferPending = "pigeon_attempt_buffer_pending"
)

// New builds an empty Metrics.
func New() *Metrics {
	return &Metrics{nonDeliveries: map[string]int64{}, counters: map[string]int64{}}
}

// Inc advances a counter by one. A nil Metrics is a no-op, so a worker
// constructed without metrics (unit tests, the pre-S6 wiring) needs no guard at
// every call site.
func (m *Metrics) Inc(name string) { m.Add(name, 1) }

// Add advances a counter by n.
func (m *Metrics) Add(name string, n int64) {
	if m == nil || name == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.counters == nil {
		m.counters = map[string]int64{}
	}
	m.counters[name] += n
}

// Set records a gauge's current value.
func (m *Metrics) Set(name string, v int64) {
	if m == nil || name == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.counters == nil {
		m.counters = map[string]int64{}
	}
	m.counters[name] = v
}

// Counter reads one counter. A name never recorded reads as 0.
func (m *Metrics) Counter(name string) int64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counters[name]
}

// CounterSnapshot returns a copy of every counter, so a reader (the /metrics
// scrape, or a test asserting AC-33's "counters advance") sees a consistent view
// without holding the lock.
func (m *Metrics) CounterSnapshot() map[string]int64 {
	if m == nil {
		return map[string]int64{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]int64, len(m.counters))
	for k, v := range m.counters {
		out[k] = v
	}
	return out
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

// RecordPartitionDrop counts partitions retention removed (SB-3). It is the
// counter an operator alerts on: a non-zero rate means history is leaving
// because of disk pressure rather than by age.
func (m *Metrics) RecordPartitionDrop(n int) {
	if n <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.partitionDrops += int64(n)
}

// PartitionDropCount is the read side of RecordPartitionDrop.
func (m *Metrics) PartitionDropCount() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.partitionDrops
}

// RecordShed counts publishes refused with 429 because the volume was over the
// disk budget (SB-7). It is what tells "we shed deliberately" apart from "we
// were down".
func (m *Metrics) RecordShed(n int) {
	if n <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sheds += int64(n)
}

// ShedCount is the read side of RecordShed.
func (m *Metrics) ShedCount() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sheds
}
