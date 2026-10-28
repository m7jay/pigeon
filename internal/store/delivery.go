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
	DeliveryID    int64
	EventID       string
	ProducerID    string
	Env           string
	EndpointID    string
	EndpointURL   string
	SigningSecret string
	// PreviousSigningSecret is the secret this endpoint used before its most
	// recent rotation, and PreviousSecretExpiresAt is when the 24 h overlap ends
	// (FR-1004). Both are empty/nil for an endpoint that has never rotated, and
	// the store clears them once the window has passed. The signer applies the
	// expiry rather than the query, so the 24 h rule lives in one readable place.
	PreviousSigningSecret   string
	PreviousSecretExpiresAt *time.Time
	EventType               string
	PayloadVersion          int
	ConsumerRef             string
	OccurredAt              time.Time
	Payload                 []byte
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

	// --- S5: the retry window and the endpoint's policy ---------------------
	//
	// NextAttemptAt is when the platform intends to attempt this delivery next,
	// and RetryDeadline is the end of its retry window (FR-1208). Both are
	// stamped on the row, so a restart resumes the same window rather than
	// starting a new one, and a pause extends the deadline without touching the
	// rest (FR-1207).
	NextAttemptAt time.Time
	RetryDeadline time.Time
	// Endpoint carries the per-endpoint half of §9 and FR-1302/FR-1306: the
	// retry policy, the per-attempt timeout, and the two limits. They come back
	// with the claim so an attempt needs no second read to know its own limits.
	Endpoint EndpointPolicy

	// HeldBack is FR-1304's flag: this delivery was being held back by its
	// endpoint's limits when it was claimed. The worker clears the hold the
	// moment an attempt starts, and this is what tells it whether there is one
	// to clear - so ordinary traffic pays nothing for the hold existing.
	HeldBack bool

	// --- FR-405 custom headers ----------------------------------------------
	//
	// CustomHeaders are the endpoint's custom headers with their values
	// DECRYPTED, for the one request this attempt is about to send. This is the
	// only read path in the store that produces a plaintext header value
	// (FR-1006): everything that reads an endpoint row gets CustomHeaderView,
	// whose value is the mask. Nothing here may be logged or written to an
	// attempt row.
	CustomHeaders []CustomHeader
	// DroppedCustomHeaders names the headers whose ciphertext did NOT
	// authenticate - sealed for another endpoint, tampered with, or sealed under
	// a key this process does not have. They are dropped rather than sent, and
	// the delivery still goes out: a value nobody can authenticate must never
	// reach a receiver, and one bad entry must not wedge a delivery.
	DroppedCustomHeaders []string
}

// AttemptClass is FR-1303's attempt class: which lane a delivery belongs to. It
// is the class of the delivery's OWN counter, not of the event that produced it:
// `deliveries.attempts = 0` is a first attempt and `> 0` is a retry, and a replay
// creates a new row (FR-1106), so a replayed delivery's first attempt is a first
// attempt.
//
// It is a string type so an unknown value can be refused at this boundary: the
// alternative - letting it reach the claim function - would select nothing and
// look like an idle queue instead of a programming error.
type AttemptClass string

const (
	// AttemptClassFirst claims deliveries that have never been attempted.
	AttemptClassFirst AttemptClass = "first"
	// AttemptClassRetry claims deliveries that have been attempted at least once.
	AttemptClassRetry AttemptClass = "retry"
	// AttemptClassAny claims either class. It is what a single-lane worker uses
	// (FR-1303's `R = 0`, reached only at `PIGEON_DELIVERY_CONCURRENCY = 1`) and
	// what a caller that is not running lanes - the deterministic single-unit
	// path the tests drive - asks for.
	AttemptClassAny AttemptClass = "any"
)

// valid reports whether the class is one of the three the claim function
// understands.
func (c AttemptClass) valid() bool {
	switch c {
	case AttemptClassFirst, AttemptClassRetry, AttemptClassAny:
		return true
	}
	return false
}

// EndpointPolicy is one endpoint's retry policy and limits, as stored on its row
// (0013). Every field has a documented default and a documented bound, so an
// endpoint that has never been configured behaves exactly as §9, FR-1302 and
// FR-1306 describe.
type EndpointPolicy struct {
	MaxRetries     int
	BackoffBase    time.Duration
	BackoffMax     time.Duration
	RetryWindow    time.Duration
	Timeout        time.Duration
	MaxConcurrency int
	MaxRPS         int
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

// ClaimDelivery leases the next due pending delivery of one attempt class with
// SKIP LOCKED (FR-813, FR-809, FR-1303), extends its lease so a concurrent
// claimant skips it, and returns the whole row. The boolean is false when nothing
// was claimable (empty queue, every row still leased, or nothing due in this
// class), which tells the worker to wait rather than spin. The lease is extended
// here, exactly as pigeon_lease_outbox does for the relay; the FR-813 lease that
// distinguishes this from a lifecycle write is the one set at fan-out, which the
// worker never rewrites except to schedule a retry.
//
// The class is the lane's (FR-1303): a first-attempt lane must not claim a retry
// and a retry lane must not claim a first attempt, or the oldest row wins and the
// retry backlog is served ahead of every newer first attempt. A caller with no
// lanes passes AttemptClassAny, which is what the pre-S11 claim did.
func (s *Store) ClaimDelivery(ctx context.Context, lease, defaultRetryWindow time.Duration, class AttemptClass) (LeasedDelivery, bool, error) {
	if defaultRetryWindow <= 0 {
		defaultRetryWindow = 72 * time.Hour
	}
	if !class.valid() {
		return LeasedDelivery{}, false, fmt.Errorf("claim delivery: unknown attempt class %q", string(class))
	}
	var d LeasedDelivery
	var backoffBaseS, backoffMaxS, retryWindowS float64
	var timeoutMS int
	var sealedHeaders []byte
	// previous_signing_secret is NULL for an endpoint that has never rotated, and
	// pgx will not scan NULL into a *string: it needs a **string so it has
	// somewhere to put the NULL. The field on the struct stays a plain string
	// because "no previous secret" and "empty previous secret" are the same thing
	// to every caller (FR-1004's overlap is decided by the expiry).
	var previousSecret *string
	err := s.pool.QueryRow(ctx, `select delivery_id, event_id, producer_id, env, endpoint_id,
		endpoint_url, signing_secret, previous_signing_secret, previous_secret_expires_at,
		event_type, payload_version, coalesce(consumer_ref, ''),
		occurred_at, payload, prior_attempts, row_attempts, first_attempt_at,
		next_attempt_at, retry_deadline,
		max_retries, retry_backoff_base_s, retry_backoff_max_s, retry_window_s,
		delivery_timeout_ms, max_concurrency, max_rps, held_back, custom_headers
		from pigeon_claim_delivery($1, $2, $3)`, lease, defaultRetryWindow, string(class),
	).Scan(&d.DeliveryID, &d.EventID, &d.ProducerID, &d.Env, &d.EndpointID,
		&d.EndpointURL, &d.SigningSecret, &previousSecret, &d.PreviousSecretExpiresAt,
		&d.EventType, &d.PayloadVersion,
		&d.ConsumerRef, &d.OccurredAt, &d.Payload, &d.PriorAttempts, &d.RowAttempts, &d.FirstAttemptAt,
		&d.NextAttemptAt, &d.RetryDeadline,
		&d.Endpoint.MaxRetries, &backoffBaseS, &backoffMaxS, &retryWindowS,
		&timeoutMS, &d.Endpoint.MaxConcurrency, &d.Endpoint.MaxRPS, &d.HeldBack, &sealedHeaders)
	if errors.Is(err, pgx.ErrNoRows) {
		return d, false, nil
	}
	if err != nil {
		return d, false, fmt.Errorf("claim delivery: %w", err)
	}
	// FR-405/FR-1006: the header values are sealed in the column, so the claim
	// decrypts them here - under the endpoint id the row's AAD was built from -
	// and hands the worker the request-ready pair. An entry that does not
	// authenticate is reported by name, never sent.
	headers, dropped, err := s.openSealedHeaders(d.EndpointID, sealedHeaders)
	if err != nil {
		return d, false, fmt.Errorf("claim delivery: %w", err)
	}
	d.CustomHeaders, d.DroppedCustomHeaders = headers, dropped
	if previousSecret != nil {
		d.PreviousSigningSecret = *previousSecret
	}
	// The policy's durations arrive as epoch seconds (0013 casts them), so the
	// whole per-endpoint policy is a value the worker can use directly.
	d.Endpoint.BackoffBase = seconds(backoffBaseS)
	d.Endpoint.BackoffMax = seconds(backoffMaxS)
	d.Endpoint.RetryWindow = seconds(retryWindowS)
	d.Endpoint.Timeout = time.Duration(timeoutMS) * time.Millisecond
	return d, true, nil
}

// HoldDelivery records FR-1304's hold on a delivery the endpoint's limits could
// not admit: the reason, the start of the hold episode (preserved across repeated
// refusals), and the instant the row may be claimed again. It consumes no attempt
// and leaves retry_deadline alone - a hold is neither an attempt nor a pause - and
// it moves the claim gate, so the row is not re-claimed in the same instant.
//
// The boolean is false when the row was no longer pending (another worker
// finalized it while this one was being refused admission): there is nothing to
// hold, and that is not an error.
func (s *Store) HoldDelivery(ctx context.Context, deliveryID int64, reason string, until time.Time) (bool, error) {
	var held bool
	err := db.ControlTx(ctx, s.pool, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `select pigeon_hold_delivery($1,$2,$3)`,
			deliveryID, reason, until).Scan(&held)
	})
	if err != nil {
		return false, fmt.Errorf("hold delivery: %w", err)
	}
	return held, nil
}

// ClearDeliveryHold ends the hold episode: it is called when an attempt actually
// starts, so the producer's view says "held" only while the delivery is waiting.
// It is a no-op on a row that is not held, so a caller never has to read first.
func (s *Store) ClearDeliveryHold(ctx context.Context, deliveryID int64) error {
	if err := db.ControlTx(ctx, s.pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `select pigeon_clear_delivery_hold($1)`, deliveryID)
		return err
	}); err != nil {
		return fmt.Errorf("clear delivery hold: %w", err)
	}
	return nil
}

// seconds converts the epoch-seconds form the claim function returns into a
// duration, without the float→int truncation surprises of a direct cast.
func seconds(v float64) time.Duration {
	return time.Duration(v * float64(time.Second))
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
//
// The write goes through FR-818's budget as a one-row batch: it blocks while the
// budget is full rather than parking on the pool, it commits in the transaction
// the budget's writer pool opened (so it is batched with whatever else was in
// flight), and it still returns only once that transaction has committed - a
// caller that gets (true, nil) can trust the delivered row is durable.
func (s *Store) CompleteDelivery(ctx context.Context, deliveryID int64, attempts int, firstAttempt, deliveredAt time.Time) (bool, error) {
	batch := NewDeliveryBatch()
	i := batch.Add(DeliveryWrite{
		Op:           ControlComplete,
		DeliveryID:   deliveryID,
		Attempts:     attempts,
		FirstAttempt: firstAttempt,
		At:           deliveredAt,
	})
	if err := s.writes.Submit(ctx, batch); err != nil {
		return false, fmt.Errorf("complete delivery: %w", err)
	}
	return batch.Applied(i), nil
}

// FinalizeDelivery ends a delivery as terminally failed (FR-806): the last
// attempt gave a terminal response, or the retry budget is spent. Idempotent like
// CompleteDelivery, and batched the same way.
func (s *Store) FinalizeDelivery(ctx context.Context, deliveryID int64, attempts int, firstAttempt time.Time) (bool, error) {
	batch := NewDeliveryBatch()
	i := batch.Add(DeliveryWrite{
		Op:           ControlFinalize,
		DeliveryID:   deliveryID,
		Attempts:     attempts,
		FirstAttempt: firstAttempt,
	})
	if err := s.writes.Submit(ctx, batch); err != nil {
		return false, fmt.Errorf("finalize delivery: %w", err)
	}
	return batch.Applied(i), nil
}

// RequeueDelivery schedules the next attempt after a retryable failure: the next
// attempt's instant, the (possibly extended) retry deadline and the delivery's
// attempt counter all move together, so the row is not claimable until the
// backoff elapses, FR-1208's pair stays truthful, and FR-1201's window is the
// deadline's job (a caller that has run out of window does not requeue at all, it
// finalizes).
//
// The counter is advanced with greatest(), never assigned, so a re-delivered
// requeue (a retried control write) cannot walk it backwards. It matters because
// it is what makes the next attempt number correct while this attempt's evidence
// row may still be sitting in the Redis buffer (D-2): the attempt number MUST NOT
// depend on the attempt buffer, and this is the write that keeps it independent
// of it.
//
// It runs with synchronous_commit=on: it is control state. A lost requeue just
// leaves the lease at the claim value and the sweeper covers it (D-6).
//
// Like Complete and Finalize it submits through FR-818's budget, so a requeue
// waiting on a full budget blocks the attempt's own goroutine (the retry is at
// least a backoff away, so it blocks nothing that matters) instead of taking a
// pool connection out of the same budget ingest is admitted against.
func (s *Store) RequeueDelivery(ctx context.Context, deliveryID int64, next, retryDeadline time.Time, attempt int) error {
	batch := NewDeliveryBatch()
	batch.Add(DeliveryWrite{
		Op:            ControlRequeue,
		DeliveryID:    deliveryID,
		Attempts:      attempt,
		At:            next,
		RetryDeadline: retryDeadline,
	})
	if err := s.writes.Submit(ctx, batch); err != nil {
		return fmt.Errorf("requeue delivery: %w", err)
	}
	return nil
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
