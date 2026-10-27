package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/m7jay/pigeon/internal/db"
)

// The delivery worker is a system worker like the relay: it has no producer
// scope and reaches the deliveries and attempts tables only through the SECURITY
// DEFINER functions in 0008, the same sanctioned bypass the relay uses for the
// outbox. Everything the worker needs to sign and POST a delivery, and to write
// the result, comes through these methods, so the durability class (control vs
// evidence, FR-812) lives here in one layer rather than in the worker.

// LeasedDelivery is one delivery row the worker has claimed. The claim returns
// the envelope fields, the endpoint's signing secret, and the attempt
// bookkeeping in one round trip, so the worker can sign and POST from a single
// read. The signing secret travels out only to the sign call: it is never logged,
// stored in an attempt row, or returned by any API (FR-1008).
type LeasedDelivery struct {
	DeliveryID     int64
	EventID        string
	ProducerID     string
	Env            string
	EndpointID     string
	EndpointURL    string
	SigningSecret  string
	EventType      string
	PayloadVersion int
	ConsumerRef    string
	OccurredAt     time.Time
	Payload        []byte
	// PriorAttempts is how many failed attempt rows already exist. It is only
	// for logging; the authoritative next number is NextAttemptNumber, derived
	// from this and the delivery row's own counter (D-2).
	PriorAttempts int
	// RowAttempts is deliveries.attempts: the control-state attempt counter,
	// advanced by pigeon_requeue_delivery when a retry is scheduled. It is
	// always current, including in buffered mode where the evidence rows are
	// still sitting in Redis.
	RowAttempts int
	// FirstAttemptAt is the time of the earliest failed attempt, or nil on a
	// first attempt. The completing worker records it as deliveries.first_attempt_at
	// so the timestamp is stable across the re-claims a crash produces.
	FirstAttemptAt *time.Time
}

// NextAttemptNumber is the 1-based number for this attempt (AC-07, FR-902).
//
// It is the maximum of the two counters, never just the evidence-row count:
//   - deliveries.attempts is advanced by the requeue that schedules the retry, so
//     it is current even when the previous attempt's row is still buffered in
//     Redis (D-2: the attempt counter MUST NOT depend on the attempt buffer);
//   - the evidence-row count is the authority for a database written before this
//     counter existed, and for sync mode, where both advance together.
//
// Taking the maximum means a buffered database and a synchronous one number the
// same attempts identically, which is what AC-07's "no duplicate attempt numbers"
// needs in both modes.
func (d LeasedDelivery) NextAttemptNumber() int {
	n := d.PriorAttempts
	if d.RowAttempts > n {
		n = d.RowAttempts
	}
	return n + 1
}

// ClaimDelivery leases the next due pending delivery with SKIP LOCKED (FR-813,
// FR-809), extends its lease so a concurrent claimant skips it, and returns the
// whole row. The boolean is false when nothing was claimable (empty queue, or
// every row still leased), which tells the worker to wait rather than spin. The
// lease is extended here, exactly as pigeon_lease_outbox does for the relay; the
// FR-813 lease that distinguishes this from a lifecycle write is the one set at
// fan-out, which the worker never rewrites except to schedule a retry.
func (s *Store) ClaimDelivery(ctx context.Context, lease time.Duration) (LeasedDelivery, bool, error) {
	var d LeasedDelivery
	err := s.pool.QueryRow(ctx, `select delivery_id, event_id, producer_id, env, endpoint_id,
		endpoint_url, signing_secret, event_type, payload_version, coalesce(consumer_ref, ''),
		occurred_at, payload, prior_attempts, row_attempts, first_attempt_at
		from pigeon_claim_delivery($1)`, lease,
	).Scan(&d.DeliveryID, &d.EventID, &d.ProducerID, &d.Env, &d.EndpointID,
		&d.EndpointURL, &d.SigningSecret, &d.EventType, &d.PayloadVersion,
		&d.ConsumerRef, &d.OccurredAt, &d.Payload, &d.PriorAttempts, &d.RowAttempts, &d.FirstAttemptAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return d, false, nil
	}
	if err != nil {
		return d, false, fmt.Errorf("claim delivery: %w", err)
	}
	return d, true, nil
}

// NextAttempt is the 1-based number for the next attempt on a delivery (AC-07):
// max(the delivery row's counter, existing attempts) + 1, so a worker that
// re-claims a delivery whose evidence is still buffered — or whose partial write
// it must account for — never reuses a number. It reads the same two counters
// LeasedDelivery.NextAttemptNumber combines, for callers that hold only an id.
func (s *Store) NextAttempt(ctx context.Context, deliveryID int64) (int, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `select pigeon_next_attempt($1)`, deliveryID).Scan(&n); err != nil {
		return 0, fmt.Errorf("next attempt: %w", err)
	}
	return n, nil
}

// AttemptRecord is one failed attempt's evidence (FR-804). A row is written only
// for a failed attempt; a successful first attempt leaves no row (FR-811, AC-28).
type AttemptRecord struct {
	DeliveryID     int64
	Attempt        int
	Outcome        string // "retryable" or "terminal"
	ErrorClass     string // timeout | dial_error | dns_error | http_retryable | http_terminal
	ResponseStatus *int   // the HTTP status, or nil for a timeout / connection / DNS error (FR-804)
	ResponseBody   string
	DurationMs     int
	// NextRetryAt is when the next attempt is scheduled (a retryable failure), or
	// nil on the last attempt of a delivery (terminal failure or success).
	NextRetryAt *time.Time
	// OccurredAt is when the attempt happened. The buffered path carries it
	// because the flush can be up to §6's 1 s later: the row records the attempt
	// time, not the flush time. A zero value means "now", which is what the
	// inline path (sync mode) uses.
	OccurredAt time.Time
}

// RecordAttempt writes one failed-attempt row. It is the sync-mode path (D-2)
// and is a one-row batch of RecordAttempts, so both modes apply SB-4's body cap
// and the attempt timestamp identically.
func (s *Store) RecordAttempt(ctx context.Context, r AttemptRecord) error {
	_, err := s.RecordAttempts(ctx, []AttemptRecord{r})
	return err
}

// RecordAttempts writes a whole batch of failed-attempt rows in ONE transaction
// with synchronous_commit=off (FR-812, FR-815): §6 already declares a ≤1 s tail
// of evidence loss acceptable, and paying an fsync per batch of rows whose loss
// is pre-declared is incoherent (L3).
//
// It returns how many rows were actually inserted. A row Postgres already has is
// not an error and is not counted — the buffer's own crash recovery replays
// entries on purpose, and the unique (delivery_id, attempt) key makes the replay
// a no-op, so a caller that sees a smaller count than its batch knows a genuine
// attempt-number collision happened rather than a duplicate delivery.
func (s *Store) RecordAttempts(ctx context.Context, batch []AttemptRecord) (int, error) {
	if len(batch) == 0 {
		return 0, nil
	}
	ids := make([]int64, len(batch))
	attempts := make([]int32, len(batch))
	outcomes := make([]string, len(batch))
	classes := make([]string, len(batch))
	statuses := make([]*int32, len(batch))
	bodies := make([]string, len(batch))
	durations := make([]int32, len(batch))
	nextRetries := make([]*time.Time, len(batch))
	occurred := make([]*time.Time, len(batch))
	for i, r := range batch {
		ids[i] = r.DeliveryID
		attempts[i] = int32(r.Attempt)
		outcomes[i] = r.Outcome
		classes[i] = r.ErrorClass
		if r.ResponseStatus != nil {
			st := int32(*r.ResponseStatus)
			statuses[i] = &st
		}
		bodies[i] = r.ResponseBody
		durations[i] = int32(r.DurationMs)
		nextRetries[i] = r.NextRetryAt
		if !r.OccurredAt.IsZero() {
			t := r.OccurredAt
			occurred[i] = &t
		}
	}
	// The body cap is applied again here, in SQL, from the configured value: the
	// rows are zipped positionally by one multi-argument unnest, so a body can
	// never be paired with another row's delivery (SB-4, FR-815).
	bodyCap := int32(s.attemptBodyCap)
	var inserted int
	err := db.EvidenceTx(ctx, s.pool, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `select pigeon_record_attempts($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
			ids, attempts, outcomes, classes, statuses, bodies, durations, nextRetries, occurred, bodyCap,
		).Scan(&inserted)
	})
	if err != nil {
		return 0, fmt.Errorf("record %d attempts: %w", len(batch), err)
	}
	return inserted, nil
}

// CompleteDelivery records the successful attempt: it sets the four columns
// FR-811 promises and writes NO attempt row (AC-28). Idempotent — it transitions
// a still-pending row only — so a worker that double-completes, or re-claims a
// delivery whose completion it lost, is a no-op rather than a duplicate. Returns
// true when this call performed the transition, false when the row had already
// left pending.
func (s *Store) CompleteDelivery(ctx context.Context, deliveryID int64, attempts int, firstAttempt, deliveredAt time.Time) (bool, error) {
	var done bool
	err := db.ControlTx(ctx, s.pool, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `select pigeon_complete_delivery($1,$2,$3,$4)`,
			deliveryID, attempts, firstAttempt, deliveredAt).Scan(&done)
	})
	if err != nil {
		return false, fmt.Errorf("complete delivery: %w", err)
	}
	return done, nil
}

// FinalizeDelivery ends a delivery as terminally failed (FR-806): the last
// attempt gave a terminal response, or the retry budget is spent. Idempotent like
// CompleteDelivery.
func (s *Store) FinalizeDelivery(ctx context.Context, deliveryID int64, attempts int, firstAttempt time.Time) (bool, error) {
	var done bool
	err := db.ControlTx(ctx, s.pool, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `select pigeon_finalize_delivery($1,$2,$3)`,
			deliveryID, attempts, firstAttempt).Scan(&done)
	})
	if err != nil {
		return false, fmt.Errorf("finalize delivery: %w", err)
	}
	return done, nil
}

// RequeueDelivery schedules the next attempt after a retryable failure by pushing
// the lease into the future (the backoff), and advances the delivery row's attempt
// counter to this attempt's number (D-2). The row is not claimable until the
// backoff elapses, so the retry cadence lives in the lease, not in a worker sleep.
//
// It runs with synchronous_commit=on: it is control state. The counter matters
// because it is what makes the next attempt number correct while this attempt's
// evidence row may still be sitting in the Redis buffer — the attempt number MUST
// NOT depend on the attempt buffer, and this is the write that keeps it
// independent of it. A lost requeue just leaves the lease at the claim value and
// the sweeper covers it (D-6), and a re-delivered requeue cannot walk the counter
// backwards (the function takes greatest()).
func (s *Store) RequeueDelivery(ctx context.Context, deliveryID int64, backoff time.Duration, attempt int) error {
	return db.ControlTx(ctx, s.pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `select pigeon_requeue_delivery($1,$2,$3)`, deliveryID, backoff, attempt)
		return err
	})
}

// SweeperRequeue re-queues ANY delivery whose lease has lapsed (D-6) and returns
// how many it touched, so the operator alerter can raise on a cluster that
// recovered a burst of crashed-worker deliveries. There is no lower bound on how
// long ago the lease lapsed: a lease that expired while no sweep was running (a
// leadership move, a stalled tick) is exactly the case D-6 exists for, and it must
// never be left for the worker's opportunistic claim loop to find by accident. A
// retry's backoff lease is always in the future, so it is never touched.
func (s *Store) SweeperRequeue(ctx context.Context) (int, error) {
	rows, err := s.pool.Query(ctx, `select pigeon_sweeper_requeue()`)
	if err != nil {
		return 0, fmt.Errorf("sweeper requeue: %w", err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
	}
	return n, rows.Err()
}
