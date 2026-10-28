// Package delivery is the worker that turns pending delivery rows into signed
// HTTP POSTs (FR-801). It leases the next due delivery the way the relay leases
// the outbox (SKIP LOCKED, extend the lease — FR-813, FR-809), signs the envelope
// with the endpoint's own secret (FR-1001), POSTs it under a per-attempt timeout
// with no redirect following (FR-802, FR-803, FR-819), classifies the outcome
// (FR-805 retry vs FR-806 terminal), and writes ONLY at the end of the attempt:
//
//   - a successful first attempt writes NO attempt row — the four columns on the
//     delivery row carry it (FR-811, AC-28);
//   - a failed attempt writes one evidence row (FR-804) and either re-leases the
//     delivery for its backoff or ends it terminally.
//
// It is a system worker like the relay: no producer scope, reaching the tenant
// tables only through the SECURITY DEFINER functions in 0008.
package delivery

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m7jay/pigeon/internal/attemptbuf"
	"github.com/m7jay/pigeon/internal/metrics"
	"github.com/m7jay/pigeon/internal/store"
)

// The two D-2 attempt-log modes, as the worker names them. They are string
// constants rather than a bool so a config value can be passed through
// unchanged, and so an unknown value is visible at the boundary.
const (
	AttemptLogSync     = "sync"
	AttemptLogBuffered = "buffered"
)

// defaultSuccessLogRPS is LOG-2's bound: above 100 successful deliveries a
// second the success path increments counters instead of writing a line each.
const defaultSuccessLogRPS = 100

// Options configures a Worker. Zero values take their documented defaults, with
// one exception: MaxBodyBytes's 0 is a meaningful setting ("capture nothing",
// SB-4), so a negative value is how a caller asks for its default.
type Options struct {
	// Workers is the size of the fixed claim/attempt pool (FR-816): a bounded
	// pool, never a goroutine per event. Defaults to 1.
	Workers int
	// Lease is how long a claimed delivery is held before it is claimable again.
	// A crashed worker lets it lapse and the row is retried (D-6, FR-809).
	// Defaults to 40 s.
	Lease time.Duration
	// Timeout is the per-attempt hard ceiling (FR-802). The caller caps it at 5 s;
	// this default is already 5 s. Defaults to 5 s.
	Timeout time.Duration
	// MaxBodyBytes caps the response body captured per attempt (FR-804, SB-4).
	// Defaults to 2048; 0 disables capture entirely, because SB-4 requires the cap
	// to be configurable down to zero. A negative value selects the 2048 default,
	// so 0 here is always the caller's explicit "capture nothing".
	MaxBodyBytes int
	// BackoffBase is the first retry's delay; the schedule doubles each attempt,
	// capped at BackoffMax (AC-04's 30 s, 1 m at the default base). Defaults to 30 s.
	BackoffBase time.Duration
	// BackoffMax caps the exponential backoff. Defaults to 1 h.
	BackoffMax time.Duration
	// MaxAttempts is the retry budget; a delivery that exhausts it ends terminally
	// rather than retrying forever. Defaults to 15 (the §9 budget).
	MaxAttempts int
	// MaxConnsPerHost is a per-endpoint transport's connection cap, equal to that
	// endpoint's concurrency cap (FR-817). Defaults to 1.
	MaxConnsPerHost int
	// PollInterval is how often a idle worker re-scans for due deliveries. Defaults
	// to 1 s.
	PollInterval time.Duration
	// FailBackoff is how long to wait after a claim-layer error before retrying,
	// so a transient DB fault does not spin. Defaults to 1 s.
	FailBackoff time.Duration
	// UserAgent identifies the platform (FR-810), e.g. "pigeon/1.0".
	UserAgent string
	Logger    *slog.Logger

	// AttemptLog is D-2's mode: AttemptLogSync writes every attempt inline,
	// AttemptLogBuffered pushes it to AttemptBuffer for the flusher to drain in
	// batches (D-1, FR-815). The zero value is sync, so a Worker built without
	// this option behaves exactly as S4 did; main passes the configured mode,
	// whose default is buffered.
	AttemptLog string
	// AttemptBuffer is the Redis-side buffer (D-1). It is required by
	// AttemptLogBuffered: a buffered worker with no buffer would be holding
	// evidence in process memory, which is the failure mode D-1 exists to
	// prevent, so such a worker writes inline and says so once instead.
	AttemptBuffer *attemptbuf.Buffer
	// Metrics carries the counters that replace per-success log lines (LOG-2,
	// AC-33) and the delivery outcome counts.
	Metrics *metrics.Metrics
	// SuccessLogRPS bounds per-success log lines (LOG-2): at most this rate
	// sustained, and at most 2 × it in any one-second window, because the bucket
	// holds one second's allowance (§13.1). 0 selects the default of 100. The
	// environment cannot turn the gate off: config.Load raises a value below 1 to
	// 1 before Validate runs, so nothing negative arrives from PIGEON_SUCCESS_LOG_RPS.
	// A negative value is the explicit "no gate" escape hatch for a caller that
	// builds a Worker (or calls New) directly - only sensible below a few hundred
	// deliveries a second - and the nil-gate branch in successLogGate.allow
	// honours it.
	SuccessLogRPS int
}

// Worker leases, signs, and delivers deliveries. Safe to construct once and Run
// for the life of the process; tests drive ProcessOne directly.
type Worker struct {
	store   *store.Store
	opts    Options
	log     *slog.Logger
	clients *transportCache
	metrics *metrics.Metrics
	gate    *successLogGate

	// noBufferLogged makes the "buffered mode without a buffer" warning a
	// one-time event instead of one line per attempt (which would itself violate
	// LOG-2, by hand).
	noBufferLogged atomic.Bool
}

// New builds a Worker. A nil logger is replaced rather than panicked on.
func New(st *store.Store, opts Options) *Worker {
	if opts.Workers <= 0 {
		opts.Workers = 1
	}
	if opts.Lease <= 0 {
		opts.Lease = 40 * time.Second
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 5 * time.Second
	}
	if opts.MaxBodyBytes < 0 {
		opts.MaxBodyBytes = 2048
	}
	if opts.BackoffBase <= 0 {
		opts.BackoffBase = 30 * time.Second
	}
	if opts.BackoffMax <= 0 {
		opts.BackoffMax = time.Hour
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 15
	}
	if opts.MaxConnsPerHost <= 0 {
		opts.MaxConnsPerHost = 1
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = time.Second
	}
	if opts.FailBackoff <= 0 {
		opts.FailBackoff = time.Second
	}
	if opts.UserAgent == "" {
		opts.UserAgent = "pigeon/dev"
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	w := &Worker{
		store:   st,
		opts:    opts,
		log:     opts.Logger,
		clients: newTransportCache(opts.MaxConnsPerHost),
		metrics: opts.Metrics,
	}
	// LOG-2's gate. A negative SuccessLogRPS is the explicit "log every success"
	// escape hatch (only sensible below a few hundred deliveries a second); 0
	// takes the requirement's default of 100. The environment cannot reach the
	// negative branch: config.Load floors PIGEON_SUCCESS_LOG_RPS at 1 before
	// Validate runs and main passes the floored value (A-8), so this is a
	// direct-caller path only.
	if opts.SuccessLogRPS >= 0 {
		rps := opts.SuccessLogRPS
		if rps == 0 {
			rps = defaultSuccessLogRPS
		}
		w.gate = newSuccessLogGate(rps, w.metrics)
	}
	return w
}

// Run starts the worker pool and blocks until ctx is cancelled, then drains: the
// loops stop claiming, in-flight attempts unwind (their contexts are cancelled
// with the parent, FR-819), and a cancelled attempt leaves its delivery pending
// under its lease so nothing is lost (AC-21). It returns ctx.Err().
func (w *Worker) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for i := 0; i < w.opts.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.loop(ctx)
		}()
	}
	<-ctx.Done()
	wg.Wait()
	return ctx.Err()
}

// loop is one worker's claim/attempt cycle. It stops the moment ctx is done so a
// shutdown never starts a new attempt it cannot finish.
func (w *Worker) loop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		did, err := w.ProcessOne(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.log.Warn("delivery: claim pass failed", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(w.opts.FailBackoff):
			}
			continue
		}
		if !did {
			select {
			case <-ctx.Done():
				return
			case <-time.After(w.opts.PollInterval):
			}
		}
	}
}

// ProcessOne leases and handles one delivery, returning whether it did any work.
// It is exported so tests can drive a single attempt deterministically.
func (w *Worker) ProcessOne(ctx context.Context) (bool, error) {
	d, got, err := w.store.ClaimDelivery(ctx, w.opts.Lease)
	if err != nil {
		return false, err
	}
	if !got {
		return false, nil
	}
	w.handle(ctx, d)
	return true, nil
}

// handle runs one delivery's attempt end to end. attemptNum is the 1-based number
// for this attempt (AC-07): NextAttemptNumber combines the delivery row's counter
// with the evidence-row count, so it stays correct while the previous attempt's
// row is still in the Redis buffer (D-2).
func (w *Worker) handle(ctx context.Context, d store.LeasedDelivery) {
	attemptNum := d.NextAttemptNumber()
	attemptStart := time.Now()
	first := firstAttemptOf(d, attemptStart)

	req, signErr := buildSignedRequest(d, attemptNum, w.opts.UserAgent, attemptStart)
	if signErr != nil {
		// Not fatal here: the request is signed with the stored value as it stands
		// and takes the normal path. The log names the real cause, because no
		// conforming consumer can verify a signature made with an undecodable
		// secret (FR-1001) — the peer's rejection is what ends the delivery.
		w.log.Error("delivery: signing secret is not a decodable whsec_ value (FR-1001)",
			"delivery_id", d.DeliveryID, "error", signErr)
	}
	// Per-attempt timeout (FR-819), derived from ctx so it is also cancelled on
	// shutdown, bounding the attempt in both time and goroutine lifetime.
	attemptCtx, cancel := context.WithTimeout(ctx, w.opts.Timeout)
	res := w.deliver(attemptCtx, req, w.opts.MaxBodyBytes)
	cancel()
	duration := time.Since(attemptStart)

	// Shutdown (AC-21): a cancelled parent ctx means the attempt was aborted
	// mid-flight. Write nothing — the delivery stays pending under its lease and
	// is retried after restart, so no attempt is lost and no partial row is left.
	if ctx.Err() != nil {
		w.log.Debug("delivery: attempt aborted on shutdown; delivery left pending",
			"delivery_id", d.DeliveryID, "attempt", attemptNum)
		return
	}

	if res.success {
		w.complete(ctx, d, attemptNum, first, duration)
		return
	}
	w.fail(ctx, d, attemptNum, first, attemptStart, duration, res)
}

// complete records the successful attempt: the four columns FR-811 promises, and
// no attempt row (AC-28). A failed write leaves the delivery pending so it is
// retried rather than lost.
//
// The success log line is gated (LOG-2): below SuccessLogRPS every success logs,
// above it the counter advances and the line is suppressed. The counter advances
// either way, so the volume the log line would have carried is never lost.
func (w *Worker) complete(ctx context.Context, d store.LeasedDelivery, attempts int, first time.Time, duration time.Duration) {
	done, err := w.store.CompleteDelivery(ctx, d.DeliveryID, attempts, first, time.Now())
	if err != nil {
		w.log.Error("delivery: complete failed; delivery left pending for retry",
			"delivery_id", d.DeliveryID, "error", err)
		return
	}
	if !done {
		// Another worker had already taken this row out of pending: not a
		// success to count, and not a failure either.
		w.log.Debug("delivery: completion was already recorded; nothing to do",
			"delivery_id", d.DeliveryID, "attempts", attempts)
		return
	}
	w.metrics.Inc(metrics.DeliveriesDelivered)
	if w.gate.allow() {
		w.log.Info("delivery delivered",
			"delivery_id", d.DeliveryID, "attempts", attempts, "duration_ms", duration.Milliseconds())
	}
}

// fail records the failed attempt, then either re-leases the delivery for its
// backoff (a retryable failure with budget left) or ends it terminally (FR-806,
// or the retry budget is spent).
//
// The order is control first, evidence second, and that is deliberate:
//
//   - The control write is the one §6 promises cannot be lost, and it is also
//     what advances the delivery's attempt counter (D-2). Writing it first means
//     a crash between the two writes leaves a delivery with a correct state and a
//     missing evidence row — the loss §6 already declares acceptable — instead of
//     an evidence row whose attempt number the next attempt could reuse.
//   - A failed control write writes no evidence at all: the delivery keeps its
//     lease, the sweeper re-queues it (D-6), and the attempt is redone under the
//     same number.
//
// Evidence then goes to the Redis buffer (D-1) in buffered mode, or inline in
// sync mode (D-2). Both paths are best-effort by contract, and both are counted.
func (w *Worker) fail(ctx context.Context, d store.LeasedDelivery, attemptNum int, first, attemptStart time.Time, duration time.Duration, res attemptResult) {
	retryable := res.outcome == outcomeRetry
	budgetLeft := attemptNum < w.opts.MaxAttempts
	retryNow := retryable && budgetLeft

	var nextRetry *time.Time
	var backoff time.Duration
	if retryNow {
		backoff = backoffFor(w.opts.BackoffBase, w.opts.BackoffMax, attemptNum, res.retryAfter)
		t := time.Now().Add(backoff)
		nextRetry = &t
	}
	// A transport error (timeout, dial, DNS) got no HTTP response, so response
	// status is null on the evidence row (FR-804); a real status is 100-599 and
	// 0 is never a valid code, so 0 means "no response" here.
	var statusPtr *int
	if res.status != 0 {
		s := res.status
		statusPtr = &s
	}

	rec := store.AttemptRecord{
		DeliveryID:     d.DeliveryID,
		Attempt:        attemptNum,
		Outcome:        outcomeString(res.outcome),
		ErrorClass:     res.errorClass,
		ResponseStatus: statusPtr,
		ResponseBody:   res.body,
		DurationMs:     int(duration.Milliseconds()),
		NextRetryAt:    nextRetry,
		OccurredAt:     attemptStart,
	}

	if retryNow {
		if err := w.store.RequeueDelivery(ctx, d.DeliveryID, backoff, attemptNum); err != nil {
			w.log.Error("delivery: requeue failed; delivery left for the sweeper (D-6)",
				"delivery_id", d.DeliveryID, "attempt", attemptNum, "error", err)
			return
		}
		w.metrics.Inc(metrics.DeliveriesRetried)
		w.recordAttempt(ctx, rec)
		// LOG-2: a failure always logs at warn, gated or not.
		w.log.Warn("delivery attempt failed; will retry",
			"delivery_id", d.DeliveryID, "attempt", attemptNum, "status", res.status,
			"class", res.errorClass, "backoff_ms", backoff.Milliseconds())
		return
	}

	// Terminal: a non-retryable status (FR-806), or a retryable status with the
	// budget spent. next_retry_at is null on this last attempt.
	done, err := w.store.FinalizeDelivery(ctx, d.DeliveryID, attemptNum, first)
	if err != nil {
		w.log.Error("delivery: finalize failed; delivery left pending for retry",
			"delivery_id", d.DeliveryID, "attempt", attemptNum, "error", err)
		return
	}
	if done {
		w.metrics.Inc(metrics.DeliveriesFailedTerminal)
	}
	w.recordAttempt(ctx, rec)
	if done {
		w.log.Warn("delivery terminally failed",
			"delivery_id", d.DeliveryID, "attempts", attemptNum, "status", res.status, "class", res.errorClass)
	}
}

// recordAttempt writes one attempt's evidence, through the Redis buffer in
// buffered mode and inline otherwise (D-1, D-2).
//
// A buffer push that fails is not dropped: the row is written inline instead, so
// a Redis outage costs throughput rather than evidence. That degradation is
// logged once per failure at warn, because it is a real state transition
// (LOG-2) and an operator needs to see it.
func (w *Worker) recordAttempt(ctx context.Context, rec store.AttemptRecord) {
	if w.opts.AttemptLog == AttemptLogBuffered {
		if w.opts.AttemptBuffer == nil {
			if w.noBufferLogged.CompareAndSwap(false, true) {
				w.log.Error("delivery: attempt_log.mode=buffered with no Redis buffer configured; writing attempt evidence inline (D-1)")
			}
		} else {
			switch err := w.opts.AttemptBuffer.Push(ctx, rec); {
			case err == nil:
				w.metrics.Inc(metrics.AttemptsBuffered)
				return
			default:
				if ctx.Err() != nil {
					// Shutdown: the attempt was aborted, so there is nothing to
					// record and no reason to write it inline either.
					return
				}
				w.log.Warn("delivery: buffering the attempt failed; writing it inline instead (evidence keeps flowing, throughput does not)",
					"delivery_id", rec.DeliveryID, "attempt", rec.Attempt, "error", err)
			}
		}
	}
	if err := w.store.RecordAttempt(ctx, rec); err != nil {
		// The delivery state is already correct, so this is the bounded evidence
		// loss §6 allows, not a lost attempt.
		w.log.Error("delivery: recording the attempt inline failed; the delivery state is already advanced",
			"delivery_id", rec.DeliveryID, "attempt", rec.Attempt, "error", err)
	}
}

func outcomeString(o outcome) string {
	if o == outcomeRetry {
		return "retryable"
	}
	return "terminal"
}

// firstAttemptOf is the timestamp recorded as deliveries.first_attempt_at: the
// earliest failed attempt if any exist (stable across crash re-claims), else this
// attempt's start (a first-attempt success).
func firstAttemptOf(d store.LeasedDelivery, now time.Time) time.Time {
	if d.FirstAttemptAt != nil {
		return *d.FirstAttemptAt
	}
	return now
}
