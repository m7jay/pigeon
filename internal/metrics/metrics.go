// Package metrics is the smallest counter set the core needs: no Prometheus
// dependency, just labelled counters the tests can read and the relay advances.
//
// It exists for one requirement in M0 (§7.5a, FR-508/FR-509): every non-delivery
// reason has its own owner and its own remediation, so each must be a distinct
// metric label. A single generic "not_delivered" counter could not tell an
// un-onboarded consumer from a paused endpoint, which is the whole point of the
// four-way split.
package metrics

import (
	"sync"
	"time"
)

// Metrics holds labelled counters. It is safe for concurrent use; the relay
// records from many goroutines as events fan out.
type Metrics struct {
	mu            sync.Mutex
	nonDeliveries map[string]int64
	// counters is the S6 delivery/attempt/log-gate set, name -> value, exposed
	// verbatim at GET /metrics (LOG-2, AC-33).
	counters map[string]int64
	// heldBacks is OBS-13's per-endpoint hold set (FR-1304), keyed by endpoint
	// id. It is a label-keyed map for the same reason nonDeliveries is: the
	// label IS the question ("which endpoint is refusing work"), and the number
	// of endpoints is bounded by the deployment's own endpoint rows.
	heldBacks map[string]HeldBack
	// Storage budget telemetry (SB-6): how many partitions retention has
	// dropped, and how many publishes were shed because the volume was over
	// budget or because the write path was saturated (FR-818). Both are
	// operator-facing — /metrics is the scrape an alert rule reads, and AC-18
	// turns on both being non-zero. They are typed fields rather than map
	// entries because their readers are typed (the sweeper and the ingest gate
	// read them without a string key) and because the series must read zero
	// before the first drop, not be absent.
	partitionDrops int64
	sheds          int64
}

// HeldBack is one endpoint's hold state (OBS-13, FR-1304): how many holds were
// recorded and how long they were for in total. Millis is the accumulator; the
// exposition reports it as seconds.
type HeldBack struct {
	Total  int64
	Millis int64
}

// Seconds is the held time as Prometheus' own unit for
// `endpoint_held_back_seconds`.
func (h HeldBack) Seconds() float64 { return float64(h.Millis) / 1000 }

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
	// DedupeArchiveBytes is FR-905's archive gauge: the size of `delivery_dedupe`
	// as measured by pg_total_relation_size (heap + primary-key index + every day
	// partition). It is a gauge because the archive's size is a level an operator
	// reads, not an event count.
	DedupeArchiveBytes = "pigeon_dedupe_archive_bytes"
	// EndpointHeldBackTotal and EndpointHeldBackSeconds are OBS-13's two
	// per-endpoint series (FR-1304): how many times an endpoint's limits refused
	// to admit a delivery, and how long those holds lasted in total. They are
	// per endpoint because the question an operator asks is "which endpoint is
	// holding work back"; `endpoint_held_back_seconds` is exposed in seconds
	// with millisecond resolution, accumulated as milliseconds here.
	EndpointHeldBackTotal   = "pigeon_endpoint_held_back_total"
	EndpointHeldBackSeconds = "pigeon_endpoint_held_back_seconds"
	// WriteQueueDrains, WriteQueueRows and WriteQueuePending are FR-818's
	// bounded write budget (SPEC.md A-11). Drains is the denominator and Rows the
	// numerator of FR-815's rows-per-transaction on the batched control-state
	// path (the same ratio FlushBatches gives for buffered evidence), and Pending
	// is the accumulation buffer's depth as OBS-9's queue-depth gauge in the same
	// spirit as AttemptBufferPending. Exposing them matters because the budget is
	// a new admission point: an operator who sees publishes shed wants to know
	// whether the queue is what is full, before changing any limit.
	WriteQueueDrains    = "pigeon_write_queue_drains_total"
	WriteQueueRows      = "pigeon_write_queue_rows_total"
	WriteQueuePending   = "pigeon_write_queue_pending_rows"
	WriteQueueSaturated = "pigeon_write_queue_saturated_total"
)

// gauges are the series exposed with `# TYPE … gauge` rather than `counter`, so a
// Prometheus reader does not rate() a level (FR-905, OBS-9).
var gauges = map[string]bool{
	AttemptBufferPending: true,
	DedupeArchiveBytes:   true,
	WriteQueuePending:    true,
}

// IsGauge reports whether a counter name is exposed as a gauge.
func IsGauge(name string) bool { return gauges[name] }

// New builds an empty Metrics.
func New() *Metrics {
	return &Metrics{
		nonDeliveries: map[string]int64{},
		counters:      map[string]int64{},
		heldBacks:     map[string]HeldBack{},
	}
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

// RecordHeldBack advances OBS-13's counters for one endpoint by one hold of the
// given length (FR-1304). A non-positive wait is recorded as a hold with no
// duration rather than skipped: the hold happened, and OBS-13's count is what
// says the endpoint's limits are the reason work is not moving.
func (m *Metrics) RecordHeldBack(endpointID string, wait time.Duration) {
	if m == nil || endpointID == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.heldBacks == nil {
		m.heldBacks = map[string]HeldBack{}
	}
	h := m.heldBacks[endpointID]
	h.Total++
	if wait > 0 {
		h.Millis += wait.Milliseconds()
	}
	m.heldBacks[endpointID] = h
}

// HeldBackFor is the read side of RecordHeldBack: one endpoint's OBS-13 pair. An
// endpoint that has never held anything reads as zero, not as absent.
func (m *Metrics) HeldBackFor(endpointID string) HeldBack {
	if m == nil {
		return HeldBack{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.heldBacks[endpointID]
}

// HeldBackSnapshot returns a copy of every endpoint's OBS-13 pair, so the
// /metrics scrape and a test see one consistent view without holding the lock.
func (m *Metrics) HeldBackSnapshot() map[string]HeldBack {
	if m == nil {
		return map[string]HeldBack{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]HeldBack, len(m.heldBacks))
	for k, v := range m.heldBacks {
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
// disk budget (SB-7) or because the write path was saturated (FR-818, SPEC.md
// A-11: ingest admitted within its deadline or shed, never parked on the pool).
// It is what tells "we shed deliberately" apart from "we were down", and it is
// one counter rather than two on purpose: an operator's question is "is this
// instance refusing ingest", and the 429's own error code (`over_budget` vs
// `over_capacity`) is what separates the two causes for the producer that was
// refused.
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
