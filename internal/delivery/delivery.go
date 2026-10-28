// Package delivery is the worker that turns pending delivery rows into signed
// HTTP POSTs (FR-801). It leases the next due delivery the way the relay leases
// the outbox (SKIP LOCKED, extend the lease — FR-813, FR-809), signs the envelope
// with the endpoint's own secret (FR-1001), POSTs it under a per-attempt timeout
// with no redirect following (FR-802, FR-803, FR-819), classifies the outcome
// (FR-805 retry vs FR-806 terminal), and writes ONLY at the end of the attempt:
//
//   - a successful first attempt writes NO attempt row — the four columns on the
//     delivery row carry it (FR-811, AC-28);
//   - a failed attempt writes one evidence row (FR-804), counts toward the pause
//     thresholds (FR-1209) and either re-leases the delivery for its backoff or
//     ends it terminally: a terminal response (FR-806), the retry budget spent,
//     or the retry WINDOW spent — which takes precedence over the count
//     (FR-1201).
//
// It is a system worker like the relay: no producer scope, reaching the tenant
// tables only through the SECURITY DEFINER functions in 0008 and 0013.
//
// Concurrency (FR-816, FR-1302, FR-1303, FR-1307). "Workers" is the number of
// CLAIM loops; each claimed delivery runs its attempt in its own goroutine. Three
// bounds apply and they are not interchangeable:
//
//   - the per-endpoint concurrency cap and RPS are leased in Redis, so they hold
//     across replicas and adding a replica does not raise them (FR-1307, AC-41);
//   - the process's attempt budget is PARTITIONED into two attempt-class lanes
//     (FR-1303): a first-attempt lane (`deliveries.attempts = 0`) and a retry lane
//     (`> 0`), holding `C - R` and `R` slots, so retry work cannot consume the
//     allowance a first attempt draws on while attempts in flight for one process
//     still total at most `C = PIGEON_DELIVERY_CONCURRENCY`;
//   - the endpoint's transport cap bounds this process's connections. It is
//     backpressure, never the authority: the number it bounds is not the
//     endpoint's promise.
//
// An endpoint that cannot admit a request does not park this path (FR-1304): the
// delivery is rescheduled at the wait the limiter reports, the hold is recorded
// on the delivery row, and the loop moves on to the next claim.
package delivery

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m7jay/pigeon/internal/attemptbuf"
	"github.com/m7jay/pigeon/internal/limit"
	"github.com/m7jay/pigeon/internal/metrics"
	"github.com/m7jay/pigeon/internal/pause"
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

// Documented defaults for the S5 limits, used when neither the endpoint row nor
// the caller supplies one. FR-1302: 10 concurrent; FR-1306: 25 RPS.
const (
	defaultMaxConcurrency = 10
	defaultMaxRPS         = 25
	// defaultProcessConcurrency is the per-process goroutine budget (FR-816). It
	// is deliberately larger than the per-endpoint default: one process must be
	// able to saturate several endpoints' caps without a separate process per
	// endpoint, and it is not what bounds any single endpoint.
	defaultProcessConcurrency = 64
	// defaultRetryConcurrency is FR-1303's R: the retry lane's share of the
	// process's attempt budget. 16 of 64, so a retry storm can use a quarter of
	// this process's attempt slots while 48 stay with first attempts.
	defaultRetryConcurrency = 16
	// maxAttemptTimeout is FR-802's hard ceiling, absolute: no attempt outlives
	// five seconds, whatever the endpoint or the deployment asks for.
	maxAttemptTimeout = 5 * time.Second
	// defaultAttemptTimeout is what a caller that configured nothing gets.
	defaultAttemptTimeout = 5 * time.Second
)

// The two reasons FR-1304 records on a held delivery, named after the limiter
// that refused to admit it: the endpoint's cluster-wide concurrency cap (FR-1302)
// or its rate bucket (FR-1306). They travel to the producer through the delivery
// view, so they are a wire-facing vocabulary; `held_back_reason` also accepts
// nothing else today.
const (
	HoldReasonConcurrency = "endpoint_concurrency_cap"
	HoldReasonRate        = "endpoint_rps"
)

// Options configures a Worker. Zero values take their documented defaults, with
// one exception: MaxBodyBytes's 0 is a meaningful setting ("capture nothing",
// SB-4), so a negative value is how a caller asks for its default.
type Options struct {
	// Workers is the number of claim loops (FR-816). Each claimed delivery runs
	// its attempt in its own goroutine, bounded by Concurrency and by the
	// endpoint's own limits. Defaults to 1.
	Workers int
	// Concurrency is the per-process cap on concurrent attempts (FR-816) — the
	// backpressure bound, not the endpoint's promise. Defaults to 64.
	//
	// FR-1303 partitions it rather than adding to it: the retry lane takes
	// RetryConcurrency slots and the first-attempt lane keeps the rest.
	Concurrency int
	// RetryConcurrency is FR-1303's R, the retry lane's allowance, partitioned
	// out of Concurrency. Defaults to 16 (48 first-attempt slots at the default
	// C = 64).
	//
	// It is clamped into [0, Concurrency-1] so the first-attempt lane can never
	// be configured to zero; a zero here means "unset" and takes the default,
	// because config.Validate refuses a configured 0 (an allowance of zero reads
	// as "stop retrying", which FR-1201 forbids). R = 0 can therefore only be
	// reached by the clamp, at Concurrency = 1, where one lane serves both
	// classes.
	RetryConcurrency int
	// Lease is how long a claimed delivery is held before it is claimable again.
	// A crashed worker lets it lapse and the row is retried (D-6, FR-809).
	// Defaults to 40 s.
	Lease time.Duration
	// Timeout is the per-attempt hard ceiling (FR-802). The caller caps it at 5 s;
	// this default is already 5 s. An endpoint may set its own (2-5 s), which the
	// worker prefers because the row carries it. Defaults to 5 s.
	Timeout time.Duration
	// SlotTTL is how long a Redis concurrency slot is held before it expires.
	// It must outlast an attempt; when it does not, it is raised to three times
	// Timeout. It is what makes a crashed worker's slots expire (FR-1302).
	// Defaults to three times Timeout.
	SlotTTL time.Duration
	// MaxBodyBytes caps the response body captured per attempt (FR-804, SB-4).
	// Defaults to 2048; 0 disables capture entirely, because SB-4 requires the cap
	// to be configurable down to zero. A negative value selects the 2048 default,
	// so 0 here is always the caller's explicit "capture nothing".
	MaxBodyBytes int
	// BackoffBase is the first retry's delay; the schedule doubles each attempt,
	// capped at BackoffMax (AC-04's 30 s, 1 m at the default base). Defaults to 30 s.
	BackoffBase time.Duration
	// BackoffMax caps the exponential backoff. §9's default is 12 h.
	BackoffMax time.Duration
	// MaxRetries is §9's retry budget: 15 retries, so at most 16 attempts
	// including the first (AC-36 asserts the count stays within that).
	// Defaults to 15.
	MaxRetries int
	// RetryWindow is the per-delivery retry window (§9: 72 h). The window takes
	// precedence over the retry count (FR-1201). Defaults to 72 h.
	RetryWindow time.Duration
	// Jitter is the fractional spread on every computed delay (§9: 0.2, ±20%).
	// It is mandatory in production, because a burst of failures retrying in
	// lockstep is a second spike against an endpoint that is already down.
	// Defaults to 0.2.
	Jitter float64
	// Rand supplies the jitter source in [0,1). Nil means the shared source;
	// tests pin it so a schedule is reproducible.
	Rand func() float64
	// Now supplies the clock the backoff and the pause windows are measured on.
	// Nil means time.Now.
	Now func() time.Time
	// MaxConnsPerHost is the deployment's DEFAULT per-endpoint concurrency cap
	// (FR-1302's 10), used when the endpoint sets no override of its own. It is
	// one number for two jobs on purpose: the cluster-wide leased slot the worker
	// must respect and the per-endpoint transport's MaxConnsPerHost (FR-817), so
	// the local connection pool and the cluster-wide promise cannot disagree. A
	// pool smaller than the endpoint's promise would make the promise
	// unreachable; a larger one would let this process exceed the cap. Defaults
	// to 10.
	MaxConnsPerHost int
	// PollInterval is how often an idle worker re-scans for due deliveries. Defaults
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
	// Limits enforces the per-endpoint concurrency and rate caps (FR-1302,
	// FR-1306). Nil disables them, which is only correct for a single-process
	// test or a deployment with no Redis; the boot path warns when it selects
	// the in-process fallback.
	Limits limit.Registry
	// Pauses evaluates the failure thresholds (FR-1202, FR-1203). Nil disables
	// them, so a worker without a Redis does not pause anything.
	Pauses *pause.Tracker
	// Notifier receives the one notification per pause (FR-1204). Nil logs.
	Notifier pause.Notifier
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

	limits   limit.Registry
	pauses   *pause.Tracker
	notifier pause.Notifier

	// lanes are FR-1303's attempt-class lanes. Each holds its own allowance and
	// claims only its own class; together they are the per-process attempt budget
	// FR-816 caps, partitioned rather than raised: the sum of their allowances is
	// exactly opts.Concurrency. One lane (R = 0, reached only at C = 1) serves
	// both classes.
	lanes []*attemptLane
	// laneCursor rotates which lane a dispatch pass tries first, so a lane that
	// always has work cannot starve the other one. It is the reason a single
	// claim loop with a retry backlog still starts first attempts.
	laneCursor atomic.Uint64
	// inflight tracks attempt goroutines so Run does not return while one is
	// still unwinding (AC-21 uses the same guarantee through the parent ctx).
	inflight sync.WaitGroup
	// holder identifies this process in the concurrency set, so a release frees
	// only its own slot.
	holder string
}

// attemptLane is one attempt-class lane (FR-1303): the class it claims and the
// allowance it holds.
type attemptLane struct {
	class store.AttemptClass
	name  string
	sem   chan struct{}
}

// laneAllowance partitions a process's attempt budget into the two lanes
// (FR-1303). R is clamped into [0, C-1] so the first-attempt lane keeps at least
// one slot at every accepted C (config refuses C < 1), and a configured value
// above C-1 is clamped rather than refused, because the result still leaves the
// first-attempt lane one slot.
//
// R = 0 is a defined state rather than an error, and only this clamp reaches it,
// at C = 1: one lane serves both classes and FR-1303's isolation is not
// claimed for that process. Retries still run there, because a degenerate
// partition MUST NOT starve FR-1201's retry policy.
func laneAllowance(process, retry int) (first, retries int) {
	if process < 1 {
		process = 1
	}
	if retry < 0 {
		retry = 0
	}
	if retry > process-1 {
		retry = process - 1
	}
	return process - retry, retry
}

// newLanes builds a process's lanes: the shared lane when R = 0, the partition
// otherwise. The allowances sum to the process cap either way.
func newLanes(process, retry int) []*attemptLane {
	first, retries := laneAllowance(process, retry)
	if retries == 0 {
		return []*attemptLane{{
			class: store.AttemptClassAny,
			name:  "shared",
			sem:   make(chan struct{}, first+retries),
		}}
	}
	return []*attemptLane{
		{class: store.AttemptClassFirst, name: "first", sem: make(chan struct{}, first)},
		{class: store.AttemptClassRetry, name: "retry", sem: make(chan struct{}, retries)},
	}
}

// New builds a Worker. A nil logger is replaced rather than panicked on.
func New(st *store.Store, opts Options) *Worker {
	if opts.Workers <= 0 {
		opts.Workers = 1
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = defaultProcessConcurrency
	}
	// FR-1303: the retry lane's allowance. A zero or negative value here is
	// "unset" and takes the default - config.Validate refuses a configured 0, so
	// the only way to reach R = 0 is the clamp below, at C = 1.
	if opts.RetryConcurrency <= 0 {
		opts.RetryConcurrency = defaultRetryConcurrency
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
		opts.BackoffMax = 12 * time.Hour
	}
	if opts.MaxRetries <= 0 {
		opts.MaxRetries = 15
	}
	if opts.RetryWindow <= 0 {
		opts.RetryWindow = 72 * time.Hour
	}
	if opts.Jitter < 0 {
		opts.Jitter = 0
	}
	if opts.SlotTTL <= 0 || opts.SlotTTL <= opts.Timeout {
		// A slot that expires while its attempt is still running would let the
		// endpoint's cap be exceeded; three attempts' worth of headroom is the
		// smallest value that cannot do that under normal operation.
		opts.SlotTTL = 3 * opts.Timeout
	}
	if opts.MaxConnsPerHost <= 0 {
		// FR-1302's documented default. It is the deployment default for an
		// endpoint that sets no override, and the transport pool for that
		// endpoint, so the shipped configuration reaches the cap §9 promises.
		opts.MaxConnsPerHost = defaultMaxConcurrency
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
	if opts.Notifier == nil {
		opts.Notifier = logNotifier{log: opts.Logger}
	}
	w := &Worker{
		store:    st,
		opts:     opts,
		log:      opts.Logger,
		clients:  newTransportCache(opts.MaxConnsPerHost),
		metrics:  opts.Metrics,
		limits:   opts.Limits,
		pauses:   opts.Pauses,
		notifier: opts.Notifier,
		lanes:    newLanes(opts.Concurrency, opts.RetryConcurrency),
		holder:   holderName(),
	}
	// FR-1303: a configured retry allowance above C-1 is clamped rather than
	// refused (the result still leaves the first-attempt lane a slot), and an
	// operator reading "retry lane = 16" in the environment must not have to
	// guess why the process is running a different one.
	if opts.RetryConcurrency > 0 {
		first, retries := laneAllowance(opts.Concurrency, opts.RetryConcurrency)
		if retries != opts.RetryConcurrency {
			w.log.Warn("delivery: the retry lane's allowance was clamped so the first-attempt lane keeps a slot (FR-1303)",
				"configured", opts.RetryConcurrency, "process_cap", opts.Concurrency,
				"retry_lane", retries, "first_attempt_lane", first)
		}
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

// holderName identifies this process inside the concurrency set.
func holderName() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return host + ":" + strconv.Itoa(os.Getpid())
}

// now is the clock the backoff and the pause windows are measured on.
func (w *Worker) now() time.Time {
	if w.opts.Now != nil {
		return w.opts.Now()
	}
	return time.Now()
}

// policy is the deployment-wide §9 policy.
func (w *Worker) policy() RetryPolicy {
	return RetryPolicy{
		Base:       w.opts.BackoffBase,
		Factor:     2,
		Max:        w.opts.BackoffMax,
		MaxRetries: w.opts.MaxRetries,
		Window:     w.opts.RetryWindow,
		Jitter:     w.opts.Jitter,
		Rand:       w.opts.Rand,
	}.Normalize()
}

// policyFor is the policy a specific delivery runs under: §9's per-endpoint
// override when the endpoint sets one, the deployment default otherwise. The
// override is read from the row the claim already returned, so it costs no
// second round trip.
func (w *Worker) policyFor(d store.LeasedDelivery) RetryPolicy {
	p := w.policy()
	if d.Endpoint.MaxRetries > 0 {
		p.MaxRetries = d.Endpoint.MaxRetries
	}
	if d.Endpoint.BackoffBase > 0 {
		p.Base = d.Endpoint.BackoffBase
	}
	if d.Endpoint.BackoffMax > 0 {
		p.Max = d.Endpoint.BackoffMax
	}
	if d.Endpoint.RetryWindow > 0 {
		p.Window = d.Endpoint.RetryWindow
	}
	return p.Normalize()
}

// effectiveConcurrency is FR-1302's cap for one endpoint: the endpoint's own
// override when it sets one, else the deployment default
// (PIGEON_DELIVERY_MAX_CONNS_PER_HOST, §9's 10 concurrent). It is the ONE number
// the authoritative cluster-wide leased slot and the per-endpoint transport pool
// (FR-817) both read, which is what makes them agree on the NULL case - and NULL
// is the shipped default, so a second, smaller fallback here would make
// FR-1302's documented default unreachable in production.
func (w *Worker) effectiveConcurrency(d store.LeasedDelivery) int {
	if d.Endpoint.MaxConcurrency > 0 {
		return d.Endpoint.MaxConcurrency
	}
	return w.opts.MaxConnsPerHost
}

// timeoutFor is §9's per-attempt ceiling for one delivery. An endpoint may set
// its own, and the value is clamped to FR-802's 5 s hard ceiling whatever the row
// says: the rate the platform promises is "no attempt outlives 5 seconds", and a
// configuration mistake must not be able to break it.
func (w *Worker) timeoutFor(d store.LeasedDelivery) time.Duration {
	t := w.opts.Timeout
	if d.Endpoint.Timeout > 0 {
		t = d.Endpoint.Timeout
	}
	if t > maxAttemptTimeout {
		t = maxAttemptTimeout
	}
	if t <= 0 {
		t = defaultAttemptTimeout
	}
	return t
}

// Run starts the claim loops and blocks until ctx is cancelled, then drains: the
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
	w.inflight.Wait()
	return ctx.Err()
}

// loop is one claim loop. It stops the moment ctx is done so a shutdown never
// starts a new attempt it cannot finish.
func (w *Worker) loop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		did, err := w.dispatch(ctx)
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

// dispatch claims one delivery into whichever lane can serve it, takes that
// lane's allowance and the endpoint's limits, and runs the attempt in its own
// goroutine. It returns whether it did any work; the caller sleeps when it did
// not.
//
// A lane's allowance is taken BEFORE the claim, so a process never claims work it
// has no room to run - an unclaimed row is simply due later, while a
// claimed-but-unrunnable one holds a lease the sweeper then has to recover.
//
// The lanes are tried in a rotation and a lane that cannot serve this pass hands
// over to the next one. Both halves matter: the rotation is what stops a lane
// that always has work (a capped endpoint's retry backlog) from starving the
// other one, and handing over is what stops an idle lane from costing the busy
// one a whole poll interval.
func (w *Worker) dispatch(ctx context.Context) (bool, error) {
	start := int(w.laneCursor.Add(1)-1) % len(w.lanes)
	for k := range w.lanes {
		lane := w.lanes[(start+k)%len(w.lanes)]
		did, err := w.dispatchLane(ctx, lane)
		if err != nil {
			return false, err
		}
		if did {
			return true, nil
		}
	}
	return false, nil
}

// dispatchLane runs one pass for one lane: take its allowance, claim one
// delivery of its class, and either start the attempt or record FR-1304's hold.
// It returns false when this lane could not serve the pass - at its allowance, or
// nothing due in its class - which is the caller's cue to try the other lane.
func (w *Worker) dispatchLane(ctx context.Context, lane *attemptLane) (bool, error) {
	select {
	case lane.sem <- struct{}{}:
	case <-ctx.Done():
		return false, nil
	default:
		// At this lane's allowance: backpressure. Return rather than block, so
		// a shutdown is immediate and the other lane still gets its turn.
		return false, nil
	}
	releaseLane := func() { <-lane.sem }

	d, got, err := w.store.ClaimDelivery(ctx, w.opts.Lease, w.opts.RetryWindow, lane.class)
	if err != nil {
		releaseLane()
		return false, err
	}
	if !got {
		releaseLane()
		return false, nil
	}

	res, err := w.admit(ctx, d)
	if err != nil {
		if ctx.Err() != nil {
			// Shutting down: the delivery stays pending under its lease and is
			// retried after restart (AC-21).
			releaseLane()
			return false, nil
		}
		// The limiter is unreachable (a Redis outage). Attempt without it rather
		// than stalling every delivery behind a dependency that is also the
		// queue: the alternative loses deliveries, which no requirement asks
		// for. Logged at error because FR-1302 is P0 and this is unprotected.
		w.log.Error("delivery: per-endpoint limits unavailable; attempting unprotected (FR-1302)",
			"endpoint_id", d.EndpointID, "error", err)
		res = admitResult{release: func() {}}
	}
	if res.reason != "" {
		// FR-1304: the endpoint cannot admit this request. Nothing is attempted,
		// neither a claim nor a lane slot is held while it waits, and the
		// delivery is rescheduled at the wait the limiter reported.
		releaseLane()
		res.release()
		w.recordHold(ctx, d, res)
		return true, nil
	}

	w.clearHoldIfHeld(ctx, d)

	w.inflight.Add(1)
	go func() {
		defer w.inflight.Done()
		defer res.release()
		defer releaseLane()
		w.handle(ctx, d)
	}()
	return true, nil
}

// admitResult is FR-1304's answer to "may this endpoint take this request now?".
type admitResult struct {
	// release frees the concurrency slot this request took. It is a no-op when
	// no slot was taken, so a caller can always defer it.
	release func()
	// reason names the limiter that refused admission, and is empty when the
	// request was admitted. It is what held_back_reason stores and what the
	// producer reads.
	reason string
	// wait is how long the limiter says to wait before asking again. It is only
	// meaningful when reason is set.
	wait time.Duration
}

// admit asks the endpoint's limits for permission to start this delivery's
// request, and does NOT wait for it (FR-1304): the slot comes first (FR-1302) and
// then one rate token (FR-1306), each taken at most once, and a refusal is
// reported as a hold with the wait the limiter returned. The slot comes first
// because a token must be spent on a request that actually starts: taking the
// token first would charge the endpoint's rate for work that then blocked on
// concurrency, and FR-1306 is explicit that a slow request is still one request.
//
// The old shape waited here, inside the dispatch path and holding both the claim
// and the process's allowance, which is the thing that parked a whole process
// behind one capped endpoint (SPEC.md A-10).
func (w *Worker) admit(ctx context.Context, d store.LeasedDelivery) (admitResult, error) {
	if w.limits == nil {
		return admitResult{release: func() {}}, nil
	}
	concCap := w.effectiveConcurrency(d)
	lease, ok, wait, err := w.limits.Acquire(ctx, d.EndpointID, concCap, w.opts.SlotTTL, w.holder)
	if err != nil {
		return admitResult{}, err
	}
	if !ok {
		return admitResult{release: func() {}, reason: HoldReasonConcurrency, wait: wait}, nil
	}
	release := func() { lease.Release(context.Background()) }

	rps := float64(d.Endpoint.MaxRPS)
	if d.Endpoint.MaxRPS <= 0 {
		rps = defaultMaxRPS
	}
	ok, wait, err = w.limits.Allow(ctx, d.EndpointID, rps, int(rps))
	if err != nil {
		// A Redis fault between the slot and the token: give the slot back and
		// let the caller attempt unprotected, exactly as it does when the slot
		// itself cannot be leased.
		release()
		return admitResult{}, err
	}
	if !ok {
		// FR-1304: the request never starts, so the slot it would have held goes
		// back in the same breath.
		release()
		return admitResult{release: func() {}, reason: HoldReasonRate, wait: wait}, nil
	}
	return admitResult{release: release}, nil
}

// recordHold writes FR-1304's hold on the delivery row and counts it (OBS-13).
//
// The wait is the limiter's own, floored in the durable write
// (pigeon_hold_delivery) so a zero wait cannot make the row claimable in the same
// instant. Nothing else about the delivery moves: no attempt is consumed, and the
// retry deadline is untouched, because a hold is neither an attempt nor a pause
// (FR-1207's extension is for a pause).
//
// A failed write is logged at error rather than retried here: the delivery keeps
// its lease and the sweeper re-queues it (D-6), so the cost of the failure is a
// delayed attempt, never a lost one. The counter advances either way, because the
// hold - the endpoint refusing work - is what it measures, whatever the durable
// write managed to do.
func (w *Worker) recordHold(ctx context.Context, d store.LeasedDelivery, res admitResult) {
	held, err := w.store.HoldDelivery(ctx, d.DeliveryID, res.reason, w.now().Add(res.wait))
	switch {
	case err != nil:
		w.log.Error("delivery: recording the held-back state failed; the delivery stays leased for the sweeper (D-6, FR-1304)",
			"delivery_id", d.DeliveryID, "endpoint_id", d.EndpointID, "reason", res.reason, "error", err)
	case !held:
		// The row was no longer pending: another worker finished it while this
		// one was refused admission. Nothing was held and nothing is wrong.
		return
	}
	w.metrics.RecordHeldBack(d.EndpointID, res.wait)
	w.log.Debug("delivery held back by its endpoint's limits (FR-1304)",
		"delivery_id", d.DeliveryID, "endpoint_id", d.EndpointID,
		"reason", res.reason, "wait_ms", res.wait.Milliseconds())
}

// clearHoldIfHeld ends the hold episode when the attempt it was waiting for
// actually starts, so the producer's view says "held" only while nothing is being
// attempted. The claim reports whether there is one, so ordinary traffic pays no
// round trip for this existing: a delivery is held, is re-claimed when the hold
// lapses, and the flag it arrived with is the signal.
//
// A failure is logged and not fatal: the attempt runs either way, and a stale
// held_back_since is a wrong field on one delivery rather than a lost attempt.
func (w *Worker) clearHoldIfHeld(ctx context.Context, d store.LeasedDelivery) {
	if !d.HeldBack {
		return
	}
	if err := w.store.ClearDeliveryHold(ctx, d.DeliveryID); err != nil {
		w.log.Warn("delivery: clearing the held-back state failed; the attempt runs anyway (FR-1304)",
			"delivery_id", d.DeliveryID, "endpoint_id", d.EndpointID, "error", err)
	}
}

// ProcessOne leases and handles one delivery inline, returning whether it did any
// work. It is the single-unit entry point tests drive deterministically; Run uses
// dispatch, which runs the attempt in its own goroutine under the endpoint's
// limits and the process's lanes.
//
// It claims either attempt class (AttemptClassAny - there are no lanes here) but
// it does respect the endpoint's limits, and it holds rather than attempts a
// delivery the endpoint cannot admit (FR-1304): "MUST NOT be attempted" has no
// test-only exception, and a driver that skipped admission would be exercising a
// path production does not have.
func (w *Worker) ProcessOne(ctx context.Context) (bool, error) {
	d, got, err := w.store.ClaimDelivery(ctx, w.opts.Lease, w.opts.RetryWindow, store.AttemptClassAny)
	if err != nil {
		return false, err
	}
	if !got {
		return false, nil
	}
	res, err := w.admit(ctx, d)
	if err != nil {
		if ctx.Err() != nil {
			return false, nil
		}
		w.log.Error("delivery: per-endpoint limits unavailable; attempting unprotected (FR-1302)",
			"endpoint_id", d.EndpointID, "error", err)
		res = admitResult{release: func() {}}
	}
	if res.reason != "" {
		res.release()
		w.recordHold(ctx, d, res)
		return true, nil
	}
	defer res.release()
	w.clearHoldIfHeld(ctx, d)
	w.handle(ctx, d)
	return true, nil
}

// handle runs one delivery's attempt end to end. attemptNum is the 1-based number
// for this attempt (AC-07): NextAttemptNumber combines the delivery row's counter
// with the evidence-row count, so it stays correct while the previous attempt's
// row is still in the Redis buffer (D-2).
func (w *Worker) handle(ctx context.Context, d store.LeasedDelivery) {
	attemptNum := d.NextAttemptNumber()
	attemptStart := w.now()
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
	attemptCtx, cancel := context.WithTimeout(ctx, w.timeoutFor(d))
	res := w.deliver(attemptCtx, req, w.effectiveConcurrency(d), w.opts.MaxBodyBytes)
	cancel()
	duration := w.now().Sub(attemptStart)

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
	done, err := w.store.CompleteDelivery(ctx, d.DeliveryID, attempts, first, w.now())
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
// next attempt (a retryable failure with budget and window left, FR-1201) or
// ends it terminally (FR-806, the retry budget spent, or the retry window closed
// - the window takes precedence over the count).
//
// The schedule is §9's policy for THIS endpoint (its own override when it sets
// one), and FR-807's Retry-After under 300 s overrides the computed backoff
// inside it.
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
	policy := w.policyFor(d)
	now := w.now()
	next, retry, verdict := policy.Next(attemptNum, now, d.RetryDeadline, res.retryAfter)
	if res.outcome == outcomeTerminal {
		// FR-806: the response itself ended the delivery. The schedule is
		// irrelevant, and there is no next attempt to name.
		retry, next, verdict = false, time.Time{}, RetryNonRetryable
	}

	var nextRetry *time.Time
	if retry {
		t := next
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

	// FR-1209: every failed attempt counts, retriable or terminal, because a pause
	// exists to stop hammering a receiver - and the evidence row above is what the
	// notification quotes.
	w.observeFailure(ctx, d)

	if retry {
		if err := w.store.RequeueDelivery(ctx, d.DeliveryID, next, d.RetryDeadline, attemptNum); err != nil {
			w.log.Error("delivery: requeue failed; delivery left for the sweeper (D-6)",
				"delivery_id", d.DeliveryID, "attempt", attemptNum, "error", err)
			return
		}
		w.metrics.Inc(metrics.DeliveriesRetried)
		w.recordAttempt(ctx, rec)
		// LOG-2: a failure always logs at warn, gated or not.
		w.log.Warn("delivery attempt failed; will retry",
			"delivery_id", d.DeliveryID, "attempt", attemptNum, "status", res.status,
			"class", res.errorClass, "backoff_ms", next.Sub(now).Milliseconds(),
			"next_attempt_at", next.UTC().Format(time.RFC3339Nano),
			"retry_deadline", d.RetryDeadline.UTC().Format(time.RFC3339Nano))
		return
	}

	// Terminal: a non-retryable status (FR-806), the retry budget spent, or the
	// retry window closed (FR-1201, which takes precedence over the count).
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
			"delivery_id", d.DeliveryID, "attempts", attemptNum, "status", res.status,
			"class", res.errorClass, "verdict", string(verdict),
			"retry_deadline", d.RetryDeadline.UTC().Format(time.RFC3339Nano))
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

// observeFailure feeds one failed attempt to the pause thresholds and acts on what
// they say: an endpoint breadth trip (FR-1203) or a subscription depth trip
// (FR-1202), the first evaluated first (FR-1210), and at most one notification per
// pause episode (FR-1204).
//
// Failure to read or write here is logged, never fatal to the delivery: the pause
// is a durable flag written in Postgres and the counters are Redis, so the worst
// case of a Redis fault is a delayed pause (D-8).
func (w *Worker) observeFailure(ctx context.Context, d store.LeasedDelivery) {
	if w.pauses == nil || !w.pauses.Enabled() {
		return
	}
	verdict, err := w.pauses.Observe(ctx, pause.Observation{
		EndpointID: d.EndpointID,
		EventType:  d.EventType,
		At:         w.now(),
	})
	if err != nil {
		w.log.Warn("delivery: pause counters unavailable", "endpoint_id", d.EndpointID, "error", err)
		return
	}
	thr := w.pauses.Thresholds()
	active, _, total, err := w.store.EndpointSubscriptionStats(ctx, d.EndpointID)
	if err != nil {
		w.log.Warn("delivery: endpoint subscription stats failed", "endpoint_id", d.EndpointID, "error", err)
		return
	}

	// FR-1210: the endpoint threshold is evaluated first, and it is the only
	// threshold whose notification is aggregated.
	if target := thr.BreadthTarget(active); target > 0 && verdict.FailingSubscriptions >= target {
		p, err := w.store.PauseEndpointForFailures(ctx, d.EndpointID, verdict.FailingSubscriptions)
		if err != nil {
			w.log.Error("delivery: endpoint pause failed", "endpoint_id", d.EndpointID, "error", err)
			return
		}
		if !p.Transitioned {
			return
		}
		w.log.Warn("endpoint paused by the failure threshold (FR-1203)",
			"endpoint_id", d.EndpointID, "failing_subscriptions", p.FailingSubscriptions,
			"active_subscriptions", active, "total_subscriptions", total, "threshold", target)
		w.notifyPaused(ctx, pause.Notification{
			Kind: pause.KindEndpoint, ProducerID: p.ProducerID, Env: p.Env,
			EndpointID: p.EndpointID, FailingSubscriptions: p.FailingSubscriptions,
			FirstResponse: p.FirstResponse, LastResponse: p.LastResponse, PausedAt: p.PausedAt,
		})
		return
	}

	if verdict.Depth < thr.SubscriptionDepth {
		return
	}
	sp, err := w.store.PauseSubscriptionForFailures(ctx, d.EndpointID, d.EventType)
	if err != nil {
		w.log.Error("delivery: subscription pause failed",
			"endpoint_id", d.EndpointID, "event_type", d.EventType, "error", err)
		return
	}
	if !sp.Transitioned {
		return
	}
	w.log.Warn("subscription paused by the failure threshold (FR-1202)",
		"endpoint_id", d.EndpointID, "event_type", d.EventType, "depth", verdict.Depth)
	w.notifyPaused(ctx, pause.Notification{
		Kind: pause.KindSubscription, ProducerID: sp.ProducerID, Env: sp.Env,
		EndpointID: d.EndpointID, EventType: sp.EventType,
		FirstResponse: sp.FirstResponse, LastResponse: sp.LastResponse, PausedAt: sp.PausedAt,
	})
}

// notifyPaused hands the FR-1204 notification to the configured notifier. The
// durable record was written with the pause, so a notifier failure is a delivery
// problem, not a lost notification: it is logged and the pause stands.
func (w *Worker) notifyPaused(ctx context.Context, n pause.Notification) {
	if err := w.notifier.NotifyPaused(ctx, n); err != nil {
		w.log.Error("delivery: pause notification failed (the record is written; FR-1204)",
			"endpoint_id", n.EndpointID, "kind", n.Kind, "error", err)
	}
}

// logNotifier is the shipped FR-1204 notifier. v1.0 has no mailer until the
// portals land (S9); the durable pause_notifications row is the notification, and
// this seam is where an SMTP implementation plugs in without touching the worker.
type logNotifier struct{ log *slog.Logger }

func (n logNotifier) NotifyPaused(_ context.Context, p pause.Notification) error {
	n.log.Warn("webhook endpoint paused: notification (FR-1204)",
		"kind", p.Kind,
		"producer_id", p.ProducerID,
		"env", p.Env,
		"endpoint_id", p.EndpointID,
		"event_type", p.EventType,
		"failing_subscriptions", p.FailingSubscriptions,
		"first_response", p.FirstResponse,
		"last_response", p.LastResponse,
		"paused_at", p.PausedAt.UTC().Format(time.RFC3339Nano),
	)
	return nil
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
