// Package writeq is FR-818's bounded write budget (SPEC.md A-11): ONE write
// budget per process, shared by ingest and by every control-state writer.
//
// The measured defect it exists for: `handlePublish` used to take its pool
// connection with no deadline, so past the machine's write ceiling the instance
// *parked* on `pgxpool.Acquire` instead of shedding - 250 events/s offered for 30
// minutes produced zero 429s and a p99 that grew from 3,593.4 ms to 4,281.6 ms
// (S10's load test, card `t_b5272d2d`). A budget is what makes "full" a state the
// process can observe, and the shed is what makes saturation show up as a 429
// instead of as latency.
//
// # Two admission APIs, one budget
//
//   - Acquire/Admit take a budget SLOT for one database write the caller runs
//     itself. A slot is held from the moment it is taken until the caller
//     releases it, and released on completion - so the number of database writes
//     in flight never exceeds the budget, which is what keeps them off the pool's
//     unbounded wait rather than merely moving that wait further out.
//   - Submit hands a batch of control-state rows to the fixed batch-writer pool.
//     It BLOCKS when the budget is full, is ctx-cancellable, never errors on
//     fullness, never drops, and never grows a queue in memory: the accumulation
//     buffer is bounded in rows, and a caller whose batch is admitted waits for
//     the outcome of the transaction that carried it.
//
// # Why the budget has two bounded dimensions
//
// Both are configuration-bounded and both must exist:
//
//   - SLOTS (PIGEON_WRITE_QUEUE_DEPTH, default = follow PIGEON_DB_MAX_CONNS) are
//     what ingest is admitted against, and what a connection pool would otherwise
//     absorb silently. A slot holds a connection's worth of work.
//   - ROWS (Pending, default FR-815's 2,000-row floor) bound the accumulation
//     buffer and the size of one batch transaction. A budget that only bounded
//     slots would commit a handful of rows per transaction at the reference load
//     and miss FR-815; a budget that only bounded rows would let hundreds of
//     writers into a 40-connection pool and reintroduce exactly the parking A-11
//     removed.
//
// Nothing here is unbounded: the buffer is a fixed-size row count, the writer pool
// is fixed at boot (FR-816 - no goroutine is created per event or per delivery),
// and a batch larger than the row bound is refused rather than buffered.
//
// # The seam for the other writer
//
// Relay writes (the fan-out delivery insert and the outbox mark) reach this queue
// through the same `Batch` interface: the relay's drain unit is the batch, and
// its shape belongs to card `t_b9850e1b`. This package owns the queue, not the
// batch.
package writeq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/m7jay/pigeon/internal/metrics"
)

// Documented defaults, matching the knobs in internal/config (SPEC.md A-11).
// internal/config's own defaults are asserted against these in a test, so the
// two cannot drift.
const (
	// DefaultWorkers is PIGEON_WRITE_QUEUE_WORKERS: the fixed batch-writer pool
	// (FR-816). Fixed at boot, never one goroutine per event or per delivery.
	DefaultWorkers = 2
	// DefaultPending is the accumulation buffer's row capacity, and therefore the
	// ceiling on one batch transaction. It is FR-815's floor (2,000 rows per
	// transaction at the reference load): the buffer must be able to HOLD a
	// floor-sized batch, or the floor could never be reached.
	DefaultPending = 2000
	// DefaultAdmitTimeout is PIGEON_INGEST_ADMIT_TIMEOUT: NFR-1's rated publish
	// p99, because admission must never be the reason a publish exceeds it.
	DefaultAdmitTimeout = 100 * time.Millisecond
	// MinDepth is the floor an explicitly configured PIGEON_WRITE_QUEUE_DEPTH is
	// clamped to. A budget of one slot serialises every write in the process and
	// a budget of zero would admit nothing at all, so neither is a state an
	// operator can reach with a typo.
	MinDepth = 2
)

// The coalescing window. A drain commits at the first of:
//
//   - the row bound being reached (the normal case under load: callers are
//     arriving faster than this window, so the batch fills), or
//   - settleWindow elapsing with no new caller arriving (the light-traffic case:
//     a lone completion is written after ~2 ms rather than waiting for company
//     that is not coming), or
//   - flushWindow elapsing since the first row was queued (the bound on how long
//     a row may sit: a stream that keeps arriving slowly would otherwise reset
//     the settle window forever).
//
// settleWindow is deliberately tiny - it is the cost a caller pays for the
// batching, and the delivery worker's completion path runs inside an attempt's
// own goroutine (FR-816), so it must stay far below an attempt's latency budget.
// flushWindow is the worst case a caller can observe before its write commits.
const (
	settleWindow = 2 * time.Millisecond
	flushWindow  = 50 * time.Millisecond
)

// ErrSaturated is FR-818's ingest admission failure: the budget stayed full for
// the caller's admission deadline. The publish path maps it to `429` with code
// `over_capacity` and a `Retry-After` - deliberately NOT SB-7's `over_budget`,
// because an operator has to be able to tell disk pressure from write pressure.
var ErrSaturated = errors.New("write budget saturated")

// ErrClosed is returned once the queue has been closed: an item submitted to a
// stopped pool would sit in the buffer forever.
var ErrClosed = errors.New("write queue closed")

// ErrBatchTooLarge refuses a batch that cannot fit the accumulation buffer even
// on an empty queue. It is a caller bug, never a fullness condition: Submit never
// errors because the budget is full (it waits), and it never drops a row.
var ErrBatchTooLarge = errors.New("batch exceeds the accumulation buffer")

// Batch is one submission to the write budget: a set of control-state rows of one
// shape, plus the statement that writes them.
//
// Apply is called by a pool worker inside a transaction the worker owns. It MUST
// NOT commit, roll back, or begin a transaction of its own, and it MUST write
// every row of the batch in that one transaction (FR-815). It is called with the
// same transaction as every other batch of the same kind that was accumulated
// with it, so one drain is one commit.
type Batch interface {
	// Kind is the coalescing key. Batches of the same kind accumulated in one
	// drain share a transaction, and fold into one statement when they implement
	// Coalescer.
	Kind() string
	// Rows is how many rows this batch writes. It is what FR-815's floor is
	// measured in, and what the accumulation buffer is bounded by.
	Rows() int
	// Apply writes the batch in tx.
	Apply(ctx context.Context, tx pgx.Tx) error
}

// Coalescer is the optional half of Batch: a batch that can fold a same-kind
// submission into itself, so a drain writes ONE statement instead of one per
// submission. It returns whether it took the other batch; a false leaves the
// drainer to apply the other batch separately, in the same transaction (fewer
// rows per statement, never a lost row).
type Coalescer interface {
	Coalesce(other Batch) bool
}

// TxFunc runs fn inside ONE transaction. The queue uses it so the transaction
// policy (synchronous_commit=on, the AC-29 class marker) stays in internal/db
// where FR-812's durability split lives.
type TxFunc func(ctx context.Context, fn func(pgx.Tx) error) error

// Release returns a slot of the budget. It is what Admit and Acquire hand back.
type Release func()

// Options configures a Queue. Zero values take their documented defaults.
type Options struct {
	// Depth is PIGEON_WRITE_QUEUE_DEPTH: how many database writes may be in
	// flight at once. 0 means "follow the pool" (Pool), because the slot that
	// holds a connection and the budget that admits one are the same resource. An
	// explicit value is clamped into [MinDepth, Pool].
	Depth int
	// Pool is PIGEON_DB_MAX_CONNS: the ceiling a Depth cannot exceed, because a
	// budget larger than the pool it admits onto is parking, not capacity.
	Pool int
	// Workers is the fixed batch-writer pool.
	Workers int
	// Pending is the accumulation buffer's row capacity, and the ceiling on one
	// batch transaction (FR-815). Defaults to DefaultPending.
	Pending int
	// AdmitTimeout is how long Admit waits for a slot before refusing.
	AdmitTimeout time.Duration
	// Tx runs one accumulated drain in one transaction. Required.
	Tx TxFunc
	// Metrics, when set, receives the queue's own counters and its pending-rows
	// gauge, so an operator reads the new mechanism on the same scrape as
	// everything else.
	Metrics *metrics.Metrics
	// Logger receives drain failures. Nil uses slog.Default().
	Logger *slog.Logger
	// Parent is the lifetime of the worker pool. Nil means context.Background();
	// Close ends it either way.
	Parent context.Context
}

// Queue is one process's write budget. It is safe for concurrent use.
type Queue struct {
	depth     int
	workers   int
	pending   int
	admitWait time.Duration
	tx        TxFunc
	metrics   *metrics.Metrics
	log       *slog.Logger
	ctx       context.Context

	// slots is the budget: one token per database write in flight.
	slots chan struct{}
	// items carries admitted batches to the writer pool. Its capacity is the row
	// capacity, because every item carries at least one row.
	items chan *item
	// done is closed by Close; every waiter selects on it.
	done chan struct{}

	// roomFree is the accumulation buffer's free row capacity. It is guarded
	// rather than kept in a channel so that acquiring 2,000 rows is one lock, not
	// 2,000 channel operations.
	mu       sync.Mutex
	roomFree int
	roomWait chan struct{}
	closed   bool

	startOnce sync.Once
	closeOnce sync.Once
	wg        sync.WaitGroup

	statMu sync.Mutex
	drains int64
	rows   int64
}

// item is one admitted batch waiting for the transaction that will carry it.
type item struct {
	batch Batch
	rows  int
	// done receives the outcome of the drain that included this batch. It is
	// buffered, so a worker never blocks signalling a caller that has given up
	// waiting (a cancelled ctx).
	done chan error
}

// New builds a queue. It starts no goroutines: the fixed writer pool is started
// by the first Submit, so a process (or a test) that constructs a budget and
// never writes control state pays nothing for it.
func New(o Options) *Queue {
	depth := o.Depth
	if depth <= 0 {
		depth = o.Pool
	}
	if o.Pool > 0 && depth > o.Pool {
		// A budget wider than the pool admits onto is a longer queue in front of
		// the same connections - the parking A-11 rejected - so the pool wins.
		depth = o.Pool
	}
	if depth < 1 {
		depth = 1
	}
	workers := o.Workers
	if workers <= 0 {
		workers = DefaultWorkers
	}
	pending := o.Pending
	if pending <= 0 {
		pending = DefaultPending
	}
	admitWait := o.AdmitTimeout
	if admitWait <= 0 {
		admitWait = DefaultAdmitTimeout
	}
	parent := o.Parent
	if parent == nil {
		parent = context.Background()
	}
	log := o.Logger
	if log == nil {
		log = slog.Default()
	}
	q := &Queue{
		depth:     depth,
		workers:   workers,
		pending:   pending,
		admitWait: admitWait,
		tx:        o.Tx,
		metrics:   o.Metrics,
		log:       log,
		ctx:       parent,
		slots:     make(chan struct{}, depth),
		items:     make(chan *item, pending),
		done:      make(chan struct{}),
		roomFree:  pending,
		roomWait:  make(chan struct{}),
	}
	return q
}

// Depth is the resolved write budget: how many database writes may be in flight.
func (q *Queue) Depth() int { return q.depth }

// hold wraps a slot already taken from the budget in an idempotent release, so a
// caller that releases twice cannot widen the budget.
func (q *Queue) hold() Release {
	var once sync.Once
	return func() { once.Do(func() { <-q.slots }) }
}

// Acquire takes a slot of the budget for one database write the caller runs
// itself (the publish path's transaction is the one such caller). It blocks until
// a slot is free or ctx ends, and never fails on fullness.
func (q *Queue) Acquire(ctx context.Context) (Release, error) {
	select {
	case q.slots <- struct{}{}:
		return q.hold(), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-q.done:
		return nil, ErrClosed
	}
}

// Admit is FR-818's ingest admission: Acquire with the process's admission
// deadline. It returns ErrSaturated - not ctx.Err() - when the budget is full at
// the deadline, because that is the one outcome the publish path must turn into a
// 429 rather than an internal error.
//
// A slot that is free at that instant is taken anyway, even though the deadline
// has passed. The deadline expiring is not by itself evidence that the budget is
// full: a goroutine that was not scheduled while the deadline passed comes back to
// a select with two ready cases (its slot send, and the timer that already fired)
// and Go picks one at random, so a plain timer arm sheds for an EMPTY budget -
// measured at `InFlight=0 Depth=4` on a starved -race run. A shed therefore means
// exactly what ErrSaturated claims: the budget stayed full for the whole deadline.
// That is what keeps a loaded instance distinct from a full one (AC-45: the rated
// ingest load must see zero 429s, so a shed with the budget empty is the one
// outcome it may never suffer).
func (q *Queue) Admit(ctx context.Context) (Release, error) {
	timer := time.NewTimer(q.admitWait)
	defer timer.Stop()
	select {
	case q.slots <- struct{}{}:
		return q.hold(), nil
	case <-timer.C:
		select {
		case q.slots <- struct{}{}:
			return q.hold(), nil
		default:
		}
		if q.metrics != nil {
			q.metrics.Inc(metrics.WriteQueueSaturated)
		}
		return nil, fmt.Errorf("%w after %s", ErrSaturated, q.admitWait)
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-q.done:
		return nil, ErrClosed
	}
}

// Submit hands a batch of control-state rows to the writer pool and waits for the
// transaction that carried it.
//
// It blocks while the budget is full (ctx-cancellable), never errors on fullness,
// never drops a row, and never grows a queue in memory: the accumulation buffer is
// bounded in rows, and a batch that cannot fit it is refused as a caller bug
// rather than buffered.
//
// A ctx that ends after the batch was admitted does not withdraw it - "never
// drop" outranks "stop waiting" - so the rows are still written exactly once; the
// caller just stops waiting for the outcome. A ctx that ends before admission
// (the caller is shutting down, or the client is gone) leaves no row behind.
func (q *Queue) Submit(ctx context.Context, b Batch) error {
	if b == nil {
		return nil
	}
	rows := b.Rows()
	if rows <= 0 {
		return nil
	}
	if rows > q.pending {
		return fmt.Errorf("%w: batch carries %d rows, the accumulation buffer holds %d; split it",
			ErrBatchTooLarge, rows, q.pending)
	}
	if err := q.acquireRoom(ctx, rows); err != nil {
		return err
	}
	q.start()
	it := &item{batch: b, rows: rows, done: make(chan error, 1)}

	// The send happens under the same mutex Close uses to observe the buffer, so
	// a batch can never be enqueued into a pool that has already decided the
	// buffer was empty and stopped.
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		q.releaseRoom(rows)
		return ErrClosed
	}
	q.items <- it
	q.mu.Unlock()

	select {
	case err := <-it.done:
		return err
	case <-ctx.Done():
		// Admitted already: the pool applies it exactly once whatever the caller
		// does next. Stop waiting rather than holding the caller's goroutine.
		return ctx.Err()
	case <-q.done:
		// Closing: the pool drains what is buffered before it exits, so wait for
		// the outcome instead of abandoning a transaction that is about to run.
		return <-it.done
	}
}

// Close stops the writer pool after it has written what is already buffered. It
// waits for the pool, so a caller that has observed Close returning can trust
// that no further database write will be issued.
func (q *Queue) Close() {
	q.closeOnce.Do(func() {
		q.mu.Lock()
		q.closed = true
		q.mu.Unlock()
		close(q.done)
		q.wg.Wait()
	})
}

// Stats is the queue's observable state: how it is configured, how much of the
// accumulation buffer is in use, how many writes are in flight, and what it has
// committed. It is what the tests assert and what the metrics gauge publishes.
type Stats struct {
	Depth       int
	Workers     int
	Pending     int // row capacity
	PendingRows int // rows currently in the buffer or in flight
	InFlight    int // database writes currently holding a slot
	Drains      int64
	Rows        int64
}

// Stats reads the queue's current state.
func (q *Queue) Stats() Stats {
	q.mu.Lock()
	pendingRows := q.pending - q.roomFree
	q.mu.Unlock()
	q.statMu.Lock()
	drains, rows := q.drains, q.rows
	q.statMu.Unlock()
	return Stats{
		Depth:       q.depth,
		Workers:     q.workers,
		Pending:     q.pending,
		PendingRows: pendingRows,
		InFlight:    len(q.slots),
		Drains:      drains,
		Rows:        rows,
	}
}

// --- internals --------------------------------------------------------------

// start launches the fixed writer pool once.
func (q *Queue) start() {
	q.startOnce.Do(func() {
		for i := 0; i < q.workers; i++ {
			q.wg.Add(1)
			go q.worker()
		}
	})
}

// worker drains the buffer until the queue closes: it takes one admitted batch,
// collects whatever else is already queued, and writes the lot in one
// transaction.
func (q *Queue) worker() {
	defer q.wg.Done()
	for {
		first, ok := q.take()
		if !ok {
			return
		}
		q.apply(q.collect(first))
	}
}

// take blocks for the next admitted batch, or reports that the queue has closed
// and the buffer is empty.
func (q *Queue) take() (*item, bool) {
	select {
	case it := <-q.items:
		return it, true
	case <-q.done:
		// Closed: write what is already buffered (D-3 drains in-flight work)
		// and only then stop. The buffer is read under the mutex Submit sends
		// under, so a batch admitted just before the close is still taken.
		q.mu.Lock()
		defer q.mu.Unlock()
		select {
		case it := <-q.items:
			return it, true
		default:
			return nil, false
		}
	}
}

// collect gathers the batches this transaction will carry: the first one, then
// whatever else arrives until the row bound is reached or the queue falls quiet.
// See settleWindow/flushWindow for why the two timers exist.
func (q *Queue) collect(first *item) []*item {
	drain := []*item{first}
	rows := first.rows
	if rows >= q.pending {
		return drain
	}
	settle := time.NewTimer(settleWindow)
	defer settle.Stop()
	flush := time.NewTimer(flushWindow)
	defer flush.Stop()
	for rows < q.pending {
		select {
		case it := <-q.items:
			drain = append(drain, it)
			rows += it.rows
			if !settle.Stop() {
				select {
				case <-settle.C:
				default:
				}
			}
			settle.Reset(settleWindow)
		case <-settle.C:
			return drain
		case <-flush.C:
			return drain
		case <-q.done:
			return drain
		}
	}
	return drain
}

// apply writes one drain in one transaction and hands every caller in it its own
// outcome. A failed transaction is not a drop: each caller gets the error and
// decides (the delivery worker leaves the delivery pending for the sweeper, the
// relay leaves the outbox row pending), which is what FR-818's "a failed batch is
// not an accept and its callers get a retryable error" requires.
func (q *Queue) apply(drain []*item) {
	groups := group(drain)
	release, err := q.Acquire(q.ctx)
	if err == nil {
		err = q.tx(q.ctx, func(tx pgx.Tx) error {
			for _, g := range groups {
				if aerr := g.Apply(q.ctx, tx); aerr != nil {
					return aerr
				}
			}
			return nil
		})
		release()
	}

	// The drain is accounted for BEFORE any caller in it is released. Stats
	// documents itself as what the queue has committed, and a caller that has
	// been handed its outcome can read it in the same breath: recording after
	// the release (or interleaved with it) lets a caller that was just told its
	// batch committed see Drains:0 Rows:0 for the drain it completed, which is
	// exactly the read AC-3's and AC-5's tests make. The release of the room is
	// kept in the same pass as the row count so the pending gauge keeps its
	// post-drain meaning.
	rows := 0
	for _, it := range drain {
		rows += it.rows
		// The rows leave the buffer only now: while a transaction is in flight
		// they are still occupying memory, so the bound is on memory, not on
		// "rows not yet handed to a worker".
		q.releaseRoom(it.rows)
	}
	q.record(rows)
	for _, it := range drain {
		it.done <- err
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		q.log.Error("write queue: drain failed; every caller in it got a retryable error",
			"rows", rows, "batches", len(drain), "error", err)
	}
}

// group folds same-kind batches together so one drain writes one statement per
// kind instead of one per caller. A batch that cannot be folded is applied on its
// own inside the same transaction.
func group(drain []*item) []Batch {
	var groups []Batch
	for _, it := range drain {
		merged := false
		for i := range groups {
			if groups[i].Kind() != it.batch.Kind() {
				continue
			}
			c, ok := groups[i].(Coalescer)
			if ok && c.Coalesce(it.batch) {
				merged = true
				break
			}
		}
		if !merged {
			groups = append(groups, it.batch)
		}
	}
	return groups
}

// acquireRoom reserves n rows of the accumulation buffer, blocking while the
// buffer is full. It is the "blocks when the budget is full" half of Submit's
// contract, and it is ctx-cancellable so a shutting-down worker does not park
// here.
func (q *Queue) acquireRoom(ctx context.Context, n int) error {
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return ErrClosed
		}
		if q.roomFree >= n {
			q.roomFree -= n
			q.mu.Unlock()
			return nil
		}
		wait := q.roomWait
		q.mu.Unlock()
		select {
		case <-wait:
		case <-ctx.Done():
			return ctx.Err()
		case <-q.done:
			return ErrClosed
		}
	}
}

// releaseRoom returns n rows and wakes everyone waiting for room.
func (q *Queue) releaseRoom(n int) {
	q.mu.Lock()
	q.roomFree += n
	if q.roomFree > q.pending {
		q.roomFree = q.pending
	}
	close(q.roomWait)
	q.roomWait = make(chan struct{})
	q.mu.Unlock()
}

// record advances the queue's own counters and the metrics gauge. It is the
// "internal counter" FR-815's rows-per-transaction can be read from without
// sampling pg_stat_database.
func (q *Queue) record(rows int) {
	q.statMu.Lock()
	q.drains++
	q.rows += int64(rows)
	q.statMu.Unlock()
	if q.metrics != nil {
		q.metrics.Inc(metrics.WriteQueueDrains)
		q.metrics.Add(metrics.WriteQueueRows, int64(rows))
		q.metrics.Set(metrics.WriteQueuePending, int64(q.Stats().PendingRows))
	}
}
