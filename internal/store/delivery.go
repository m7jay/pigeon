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
	// for logging; the authoritative next number is NextAttempt, derived from the
	// rows so a crash that left a partial write cannot hand out a duplicate (AC-07).
	PriorAttempts int
	// FirstAttemptAt is the time of the earliest failed attempt, or nil on a
	// first attempt. The completing worker records it as deliveries.first_attempt_at
	// so the timestamp is stable across the re-claims a crash produces.
	FirstAttemptAt *time.Time
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
		occurred_at, payload, prior_attempts, first_attempt_at
		from pigeon_claim_delivery($1)`, lease,
	).Scan(&d.DeliveryID, &d.EventID, &d.ProducerID, &d.Env, &d.EndpointID,
		&d.EndpointURL, &d.SigningSecret, &d.EventType, &d.PayloadVersion,
		&d.ConsumerRef, &d.OccurredAt, &d.Payload, &d.PriorAttempts, &d.FirstAttemptAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return d, false, nil
	}
	if err != nil {
		return d, false, fmt.Errorf("claim delivery: %w", err)
	}
	return d, true, nil
}

// NextAttempt is the 1-based number for the next attempt on a delivery (AC-07):
// max(existing attempts) + 1, so a worker that re-claims a delivery whose
// partial write it must account for never reuses a number.
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
}

// RecordAttempt writes one failed-attempt row with synchronous_commit=off
// (FR-812): §6 already declares a ≤1 s tail of evidence loss acceptable, and the
// row is re-derivable from the retry anyway. It is idempotent on (delivery_id,
// attempt), so a worker that double-writes the same attempt is a no-op, not a
// duplicate (AC-07).
func (s *Store) RecordAttempt(ctx context.Context, r AttemptRecord) error {
	return db.EvidenceTx(ctx, s.pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `select pigeon_record_attempt($1,$2,$3,$4,$5,$6,$7,$8)`,
			r.DeliveryID, r.Attempt, r.Outcome, r.ErrorClass, r.ResponseStatus,
			r.ResponseBody, r.DurationMs, r.NextRetryAt)
		return err
	})
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
// the lease into the future (the backoff). The row is not claimable until the
// backoff elapses, so the retry cadence lives in the lease, not in a worker sleep.
// It runs with synchronous_commit=on: a lost requeue just leaves the lease at the
// claim value and the sweeper covers it (D-6) — a "just retried if lost" control
// write, not evidence.
func (s *Store) RequeueDelivery(ctx context.Context, deliveryID int64, backoff time.Duration) error {
	return db.ControlTx(ctx, s.pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `select pigeon_requeue_delivery($1,$2)`, deliveryID, backoff)
		return err
	})
}

// SweeperRequeue re-queues every delivery whose lease has lapsed within the last
// windowSeconds (D-6) and returns how many it touched, so the operator alerter
// can raise on a cluster that recovered a burst of crashed-worker deliveries.
func (s *Store) SweeperRequeue(ctx context.Context, windowSeconds int) (int, error) {
	rows, err := s.pool.Query(ctx, `select pigeon_sweeper_requeue($1)`, windowSeconds)
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
