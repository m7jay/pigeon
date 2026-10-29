package writeq

import (
	"context"
	"errors"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/m7jay/pigeon/internal/metrics"
)

// The tests in this file need no database: they drive the budget through a
// synthetic transaction function and synthetic batches, which is what makes them
// deterministic. What they assert is the queue's own contract - blocking, no
// drop, exactly-once, bounded memory, bounded goroutines - and the store-side
// assertions (rows per transaction against real rows) live in internal/store.

// recorder stands in for the database: it counts transactions, the rows each
// transaction carried, and how many times each batch was applied. A gate makes
// every transaction block, which is how a test holds every slot of the budget.
type recorder struct {
	mu      sync.Mutex
	gate    chan struct{}
	err     error
	txSeen  int
	txDone  int
	rows    int
	txRows  []int
	applied map[string]int
	order   []string
}

func newRecorder() *recorder { return &recorder{applied: map[string]int{}} }

// tx is the writeq.Options.Tx a queue is built with.
func (r *recorder) tx(ctx context.Context, fn func(pgx.Tx) error) error {
	r.mu.Lock()
	r.txSeen++
	r.txRows = append(r.txRows, 0)
	gate, fail := r.gate, r.err
	r.mu.Unlock()

	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if fail != nil {
		return fail
	}
	if err := fn(nil); err != nil {
		return err
	}
	r.mu.Lock()
	r.txDone++
	r.mu.Unlock()
	return nil
}

// record is what a batch's Apply does: one row per row the batch carried.
func (r *recorder) record(key string, rows int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.applied[key]++
	r.order = append(r.order, key)
	r.rows += rows
	r.txRows[len(r.txRows)-1] += rows
}

func (r *recorder) snapshot() (txSeen, txDone, rows int, txRows []int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.txSeen, r.txDone, r.rows, append([]int(nil), r.txRows...)
}

func (r *recorder) appliedCount(key string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.applied[key]
}

func (r *recorder) openGates() { // release every blocked transaction
	r.mu.Lock()
	defer r.mu.Unlock()
	select {
	case <-r.gate:
	default:
		close(r.gate)
	}
}

// hold makes every transaction block until the gates open, and registers the
// release with the test: a failing assertion must not leave a transaction blocked
// past t.Cleanup(q.Close), which would deadlock the whole run instead of failing
// it.
func (r *recorder) hold(t *testing.T) {
	t.Helper()
	r.mu.Lock()
	r.gate = make(chan struct{})
	r.mu.Unlock()
	t.Cleanup(r.openGates)
}

// testBatch is one synthetic submission.
type testBatch struct {
	key  string
	rows int
	rec  *recorder
	// folds makes same-kind batches fold into one group, the way DeliveryBatch
	// does, so a drain writes one statement per kind instead of one per caller.
	folds bool
}

func (b *testBatch) Kind() string { return b.key }
func (b *testBatch) Rows() int    { return b.rows }

func (b *testBatch) Apply(context.Context, pgx.Tx) error {
	b.rec.record(b.key, b.rows)
	return nil
}

// Coalesce is deliberately only implemented by a batch that asks for it: a batch
// without it must still be applied on its own inside the drain's transaction.
func (b *testBatch) Coalesce(other Batch) bool {
	if !b.folds {
		return false
	}
	o, ok := other.(*testBatch)
	if !ok {
		return false
	}
	b.rows += o.rows
	return true
}

func newQueue(t *testing.T, o Options) *Queue {
	t.Helper()
	if o.Parent == nil {
		o.Parent = context.Background()
	}
	q := New(o)
	t.Cleanup(q.Close)
	return q
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestWorkersBlockWhenTheWriteBudgetIsFull is AC-2. A worker's Submit on a full
// budget returns only when a slot frees; it never errors on fullness, never drops,
// and the batch is applied exactly once - and a cancelled ctx releases the budget
// cleanly instead of leaking a slot.
func TestWorkersBlockWhenTheWriteBudgetIsFull(t *testing.T) {
	ctx := context.Background()

	t.Run("slots", func(t *testing.T) {
		rec := newRecorder()
		q := newQueue(t, Options{Depth: 2, Workers: 2, Pending: 8, Tx: rec.tx})

		// Every slot held by a writer that has not committed yet: this is the
		// saturation itself, held through the budget's own admission API - the one
		// the publish path calls - rather than by a worker, because the point is
		// that a worker's Submit and ingest's Admit draw on the same slots.
		releases := make([]Release, 0, 2)
		for i := 0; i < 2; i++ {
			rel, err := q.Acquire(ctx)
			if err != nil {
				t.Fatalf("Acquire: %v", err)
			}
			releases = append(releases, rel)
		}
		if s := q.Stats(); s.InFlight != 2 {
			t.Fatalf("writes in flight = %d, want 2", s.InFlight)
		}

		batch := &testBatch{key: "k", rows: 1, rec: rec}
		done := make(chan error, 1)
		go func() { done <- q.Submit(ctx, batch) }()
		select {
		case err := <-done:
			t.Fatalf("Submit returned (%v) while every slot was held: the budget did not block a worker", err)
		case <-time.After(80 * time.Millisecond):
		}
		if n := rec.appliedCount("k"); n != 0 {
			t.Fatalf("a batch was applied while every slot was held (%d drains)", n)
		}

		// A slot frees, and only then does the batch commit.
		releases[0]()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Submit after a slot freed = %v, want nil", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Submit did not return after a slot freed")
		}
		if n := rec.appliedCount("k"); n != 1 {
			t.Fatalf("applied %d batches, want 1: a dropped or duplicated batch", n)
		}
		// Releasing twice must not free two slots: the budget is one resource, and
		// a double release is how a slot leaks the other way.
		releases[0]()
		releases[1]()
		if s := q.Stats(); s.InFlight != 0 {
			t.Fatalf("writes in flight = %d after every release, want 0", s.InFlight)
		}
	})

	t.Run("rows", func(t *testing.T) {
		// The row dimension is the other half of the budget: with room for two
		// rows and one worker, a third submitter must wait for the drain that
		// already holds them.
		rec := newRecorder()
		rec.hold(t)
		q := newQueue(t, Options{Depth: 8, Workers: 1, Pending: 2, Tx: rec.tx})

		for i := 0; i < 2; i++ {
			b := &testBatch{key: "k", rows: 1, rec: rec}
			go func() { _ = q.Submit(ctx, b) }()
		}
		waitFor(t, "the accumulation buffer to be full", func() bool {
			return q.Stats().PendingRows == 2
		})

		third := &testBatch{key: "k", rows: 1, rec: rec}
		done := make(chan error, 1)
		go func() { done <- q.Submit(ctx, third) }()
		select {
		case err := <-done:
			t.Fatalf("Submit returned (%v) with the buffer full", err)
		case <-time.After(80 * time.Millisecond):
		}
		// Whatever the drain has gathered is still inside its (blocked)
		// transaction, so no rows are committed yet.
		if _, _, rows, _ := rec.snapshot(); rows != 0 {
			t.Fatalf("%d rows committed while the transaction was still blocked", rows)
		}

		rec.openGates()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Submit after the drain = %v, want nil", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Submit did not return after the buffer drained")
		}
		// Every row is written exactly once - how the three submissions were
		// grouped into drains is the queue's business, not the caller's.
		_, txDone, rows, perTx := rec.snapshot()
		if rows != 3 {
			t.Fatalf("rows committed = %d, want 3: a row was dropped or written twice", rows)
		}
		if txDone != len(perTx) {
			t.Fatalf("%d transactions committed but %d were entered", txDone, len(perTx))
		}
		t.Logf("3 one-row submissions over a 2-row buffer -> %d transactions %v", txDone, perTx)
	})
}

// TestCancelledSubmitReleasesTheBudget is AC-2's last clause. A ctx cancelled
// before admission must leave no row behind and must not leak a slot or a row of
// the budget: after the queue drains, it still accepts work.
func TestCancelledSubmitReleasesTheBudget(t *testing.T) {
	rec := newRecorder()
	rec.hold(t)
	q := newQueue(t, Options{Depth: 4, Workers: 1, Pending: 2, Tx: rec.tx})

	// Fill the buffer and let the drain take it, so the budget is full.
	held := make(chan struct{})
	go func() {
		_ = q.Submit(context.Background(), &testBatch{key: "held", rows: 2, rec: rec})
		close(held)
	}()
	waitFor(t, "the drain to be in flight", func() bool { return q.Stats().PendingRows == 2 })

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	refused := &testBatch{key: "refused", rows: 1, rec: rec}
	if err := q.Submit(cancelled, refused); !errors.Is(err, context.Canceled) {
		t.Fatalf("Submit with a cancelled ctx = %v, want context.Canceled", err)
	}
	_, _, rows, _ := rec.snapshot()
	if rows != 0 {
		t.Fatalf("a cancelled submit reached the database: %d rows written, want 0", rows)
	}
	if n := rec.appliedCount("refused"); n != 0 {
		t.Fatalf("the refused batch was applied %d times", n)
	}

	rec.openGates()
	<-held

	// The budget is usable again and nothing leaked: the batch that was admitted
	// is committed exactly once, the refused one never was, and a fresh submit
	// goes through.
	if n := rec.appliedCount("held"); n != 1 {
		t.Fatalf("the admitted batch was applied %d times, want exactly once", n)
	}
	ok := &testBatch{key: "ok", rows: 1, rec: rec}
	if err := q.Submit(context.Background(), ok); err != nil {
		t.Fatalf("Submit after a cancelled one = %v, want nil: the budget leaked", err)
	}
	if err := q.Submit(context.Background(), refused); err != nil {
		t.Fatalf("Submit after cancelling a different caller = %v, want nil", err)
	}
	if s := q.Stats(); s.PendingRows != 0 || s.InFlight != 0 {
		t.Fatalf("budget state after the queue drained = %+v, want no rows and no writes in flight", s)
	}
}

// TestCancelledCallerStillGetsItsBatchWritten pins the one shape a cancelled
// caller must not change: a batch that was already admitted is still written
// exactly once, because "never drop" outranks "stop waiting".
func TestCancelledCallerStillGetsItsBatchWritten(t *testing.T) {
	rec := newRecorder()
	rec.hold(t)
	// One worker and a gate: the admitted batch sits in the drain, blocked.
	q := newQueue(t, Options{Depth: 2, Workers: 1, Pending: 4, Tx: rec.tx})

	ctx, cancel := context.WithCancel(context.Background())
	first := &testBatch{key: "k", rows: 1, rec: rec}
	go func() { _ = q.Submit(context.Background(), first) }()
	waitFor(t, "the first batch to be in flight", func() bool { return q.Stats().InFlight == 1 })

	admitted := make(chan error, 1)
	second := &testBatch{key: "k", rows: 1, rec: rec}
	go func() { admitted <- q.Submit(ctx, second) }()
	waitFor(t, "the second batch to be admitted", func() bool { return q.Stats().PendingRows == 2 })

	cancel()
	if err := <-admitted; !errors.Is(err, context.Canceled) {
		t.Fatalf("Submit after the caller gave up = %v, want context.Canceled", err)
	}

	rec.openGates()
	waitFor(t, "the admitted batch to commit", func() bool { return q.Stats().PendingRows == 0 })
	if n := rec.appliedCount("k"); n != 2 {
		t.Fatalf("applied %d batches, want 2: a batch admitted before its caller gave up was dropped", n)
	}
}

// TestDrainFailureReachesEveryCallerInIt is FR-818's "a failed batch is not an
// accept and its callers get a retryable error": one transaction carries several
// callers, and a failure is reported to every one of them rather than to the
// first or to none.
func TestDrainFailureReachesEveryCallerInIt(t *testing.T) {
	rec := newRecorder()
	rec.err = errors.New("control write refused")
	q := newQueue(t, Options{Depth: 4, Workers: 1, Pending: 8, Tx: rec.tx})

	const n = 4
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- q.Submit(context.Background(), &testBatch{key: "k", rows: 1, rec: rec})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err == nil {
			t.Fatal("a caller got no error from a failed drain")
		}
	}
	seen, done, rows, _ := rec.snapshot()
	if rows != 0 {
		t.Fatalf("a failed transaction recorded %d rows", rows)
	}
	if done != 0 {
		t.Fatalf("a failed transaction was counted as committed (%d)", done)
	}
	if seen == 0 {
		t.Fatal("no transaction was attempted")
	}
}

// TestDrainCoalescesSameKindBatches is FR-815's mechanism on the queue's side:
// the callers that are in flight when a drain runs share one transaction, and the
// batches that fold together are written as one statement.
func TestDrainCoalescesSameKindBatches(t *testing.T) {
	rec := newRecorder()
	q := newQueue(t, Options{Depth: 8, Workers: 1, Pending: 2000, Tx: rec.tx})

	const n = 16
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := q.Submit(context.Background(), &testBatch{key: "k", rows: 1, rec: rec, folds: true}); err != nil {
				t.Errorf("submit: %v", err)
			}
		}()
	}
	wg.Wait()

	_, _, rows, perTx := rec.snapshot()
	if rows != n {
		t.Fatalf("recorded %d rows, want %d", rows, n)
	}
	// The submitters arrive within the settle window, so most of them land in one
	// transaction. It cannot be exactly one - the first submitter starts the
	// drain before the others have arrived - so the assertion is on the ratio the
	// floor is actually about.
	commits := int64(len(perTx))
	if commits*4 > int64(n) {
		t.Fatalf("%d callers produced %d transactions (%v rows each); the queue is not batching", n, commits, perTx)
	}
	for i, r := range perTx {
		if r == 0 {
			t.Fatalf("transaction %d carried no rows: %v", i, perTx)
		}
	}
	t.Logf("%d one-row submissions -> %d transactions %v (%.1f rows each)",
		n, commits, perTx, float64(rows)/float64(commits))
}

// TestSubmitRefusesABatchLargerThanTheBuffer is the bound on memory: a batch that
// cannot fit the accumulation buffer is a caller bug and is refused, never
// buffered and never dropped silently.
func TestSubmitRefusesABatchLargerThanTheBuffer(t *testing.T) {
	rec := newRecorder()
	q := newQueue(t, Options{Depth: 4, Workers: 1, Pending: 16, Tx: rec.tx})

	big := &testBatch{key: "k", rows: 17, rec: rec}
	err := q.Submit(context.Background(), big)
	if !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("Submit of a 17-row batch into a 16-row buffer = %v, want ErrBatchTooLarge", err)
	}
	if s := q.Stats(); s.PendingRows != 0 {
		t.Fatalf("a refused batch left %d rows in the buffer", s.PendingRows)
	}
	if err := q.Submit(context.Background(), &testBatch{key: "k", rows: 16, rec: rec}); err != nil {
		t.Fatalf("a batch that exactly fills the buffer was refused: %v", err)
	}
}

// TestAdmitShedsWithinItsDeadline is the ingest half: Admit waits the configured
// deadline and then returns ErrSaturated, which is the state the publish path
// turns into a 429.
func TestAdmitShedsWithinItsDeadline(t *testing.T) {
	rec := newRecorder()
	rec.hold(t)
	m := metrics.New()
	q := newQueue(t, Options{
		Depth: 1, Workers: 1, Pending: 4, AdmitTimeout: 30 * time.Millisecond,
		Tx: rec.tx, Metrics: m,
	})

	rel, err := q.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire on an empty budget: %v", err)
	}

	start := time.Now()
	if _, err := q.Admit(context.Background()); !errors.Is(err, ErrSaturated) {
		t.Fatalf("Admit with the budget full = %v, want ErrSaturated", err)
	}
	if waited := time.Since(start); waited < 25*time.Millisecond {
		t.Fatalf("Admit gave up after %s, want the configured 30 ms deadline", waited)
	}
	if got := m.Counter(metrics.WriteQueueSaturated); got != 1 {
		t.Fatalf("saturation counter = %d, want 1", got)
	}

	// A slot that is free is admitted at once, and the counter does not move.
	rel()
	rel2, err := q.Admit(context.Background())
	if err != nil {
		t.Fatalf("Admit with a slot free = %v, want nil", err)
	}
	rel2()
	if got := m.Counter(metrics.WriteQueueSaturated); got != 1 {
		t.Fatalf("saturation counter = %d after a successful admit, want 1", got)
	}
}

// TestAShedAdmitMeansTheBudgetWasFull pins the other half of Admit's contract.
// ErrSaturated claims the budget stayed full for the whole deadline, so a shed
// with the budget free is the one outcome a loaded instance may never report -
// measured at `InFlight=0 Depth=4` on a starved -race run, where a plain timer arm
// shed because the goroutine came back to a select with both its slot send and the
// fired timer ready and Go picks between them at random. The deadline here has
// always already expired (1 ns), so every call faces exactly that choice: a shed
// can only come from the arm that returns without looking at the budget.
func TestAShedAdmitMeansTheBudgetWasFull(t *testing.T) {
	rec := newRecorder()
	m := metrics.New()
	q := newQueue(t, Options{
		Depth: 1, Workers: 1, Pending: 1, AdmitTimeout: time.Nanosecond,
		Tx: rec.tx, Metrics: m,
	})

	const tries = 1000
	for i := 0; i < tries; i++ {
		rel, err := q.Admit(context.Background())
		if err != nil {
			t.Fatalf("Admit on a free one-slot budget = %v on call %d: a shed is only ever a claim that the budget was full", err, i+1)
		}
		if got := q.Stats().InFlight; got != 1 {
			t.Fatalf("Admit on a free budget left %d writes in flight, want 1", got)
		}
		rel()
	}
	if got := q.Stats().InFlight; got != 0 {
		t.Fatalf("%d slots of the budget are still held after every admit was released", got)
	}
	if got := m.Counter(metrics.WriteQueueSaturated); got != 0 {
		t.Fatalf("saturation counter = %d over %d admits against a free budget, want 0", got, tries)
	}
}

// TestWritePathGrowsNoGoroutines is AC-3: offering far more batches and publishes
// than the budget holds must leave the goroutine count and the open descriptors a
// function of the budget and the fixed worker pool. Nothing in the steady state
// starts a goroutine per event or per delivery (FR-816), and nothing opens a
// descriptor per write.
func TestWritePathGrowsNoGoroutines(t *testing.T) {
	rec := newRecorder()
	q := newQueue(t, Options{Depth: 4, Workers: 2, Pending: 64, Tx: rec.tx})

	// Warm up: the writer pool is started by the first Submit, and the runtime's
	// own goroutines (GC, timers) settle after the first burst.
	for i := 0; i < 64; i++ {
		if err := q.Submit(context.Background(), &testBatch{key: "warm", rows: 1, rec: rec}); err != nil {
			t.Fatalf("warm-up submit: %v", err)
		}
	}
	for i := 0; i < 64; i++ {
		rel, err := q.Acquire(context.Background())
		if err != nil {
			t.Fatalf("warm-up acquire: %v", err)
		}
		rel()
	}
	waitFor(t, "the goroutine count to settle", func() bool {
		before := runtime.NumGoroutine()
		time.Sleep(20 * time.Millisecond)
		return runtime.NumGoroutine() == before
	})
	before := runtime.NumGoroutine()
	fdsBefore := openFDs(t)
	baseRows := q.Stats().Rows // the warm-up already committed its own rows

	// N >> budget: 2,000 batches from 32 concurrent submitters, plus the same
	// number of publishes through the admission half.
	const (
		submitter = 32
		perWorker = 64
	)
	var wg sync.WaitGroup
	errCh := make(chan error, submitter*perWorker)
	for w := 0; w < submitter; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				if err := q.Submit(context.Background(), &testBatch{key: "k", rows: 1, rec: rec, folds: true}); err != nil {
					errCh <- err
					return
				}
				rel, err := q.Admit(context.Background())
				if err != nil {
					errCh <- err
					return
				}
				rel()
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("submit: %v", err)
	}
	if got := q.Stats().Rows - baseRows; got != submitter*perWorker {
		t.Fatalf("committed %d rows, want %d", got, submitter*perWorker)
	}

	waitFor(t, "the goroutine count to settle again", func() bool {
		n := runtime.NumGoroutine()
		time.Sleep(20 * time.Millisecond)
		return runtime.NumGoroutine() == n
	})
	after := runtime.NumGoroutine()
	// The pool is 2 goroutines and it was already up before the measurement, so
	// the allowed growth is the runtime's own slack, not a per-write one.
	if after > before+2 {
		t.Fatalf("goroutines grew from %d to %d over %d batches through a %d-slot budget with %d writers",
			before, after, submitter*perWorker, q.Depth(), 2)
	}
	fdsAfter := openFDs(t)
	if fdsAfter > fdsBefore+8 {
		t.Fatalf("open descriptors grew from %d to %d over %d batches", fdsBefore, fdsAfter, submitter*perWorker)
	}
	t.Logf("goroutines %d -> %d, descriptors %d -> %d over %d batches (%d transactions, %d rows)",
		before, after, fdsBefore, fdsAfter, submitter*perWorker, q.Stats().Drains, q.Stats().Rows)
}

// TestCloseDrainsWhatIsBuffered is D-3's shutdown half: nothing admitted is lost
// to the close, and a submit after the close is refused rather than buffered.
func TestCloseDrainsWhatIsBuffered(t *testing.T) {
	rec := newRecorder()
	rec.hold(t)
	q := New(Options{Depth: 2, Workers: 1, Pending: 8, Tx: rec.tx, Parent: context.Background()})

	const n = 4
	submitted := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			submitted <- q.Submit(context.Background(), &testBatch{key: "k", rows: 1, rec: rec})
		}()
	}
	waitFor(t, "a drain to be inside its transaction", func() bool { return q.Stats().InFlight == 1 })

	closed := make(chan struct{})
	go func() { q.Close(); close(closed) }()
	time.Sleep(20 * time.Millisecond)
	rec.openGates()

	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not return")
	}
	written := 0
	for i := 0; i < n; i++ {
		// Every caller hears back - Close never leaves a caller waiting forever -
		// and the only failure it may report is ErrClosed for a batch that had not
		// been admitted yet: an admitted row is always written.
		err := <-submitted
		if err == nil {
			written++
			continue
		}
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("a caller got %v, want nil or ErrClosed", err)
		}
	}
	if written == 0 {
		t.Fatal("Close exited with every admitted row unwritten")
	}
	if _, _, rows, _ := rec.snapshot(); rows != written {
		t.Fatalf("%d callers were told their write happened, but %d rows reached the database", written, rows)
	}
	if err := q.Submit(context.Background(), &testBatch{key: "k", rows: 1, rec: rec}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Submit after Close = %v, want ErrClosed", err)
	}
}

// TestQueuePublishesItsOwnState is the observability half of AC-5's "or an
// internal counter": FR-815's ratio is readable from the queue and from the
// scrape, so an operator does not have to sample pg_stat_database.
func TestQueuePublishesItsOwnState(t *testing.T) {
	rec := newRecorder()
	m := metrics.New()
	q := newQueue(t, Options{Depth: 3, Workers: 1, Pending: 128, Tx: rec.tx, Metrics: m})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = q.Submit(context.Background(), &testBatch{key: "k", rows: 2, rec: rec, folds: true})
		}()
	}
	wg.Wait()

	s := q.Stats()
	if s.Rows != 16 || s.Drains == 0 {
		t.Fatalf("stats = %+v, want 16 rows over at least one drain", s)
	}
	if got := m.Counter(metrics.WriteQueueRows); got != 16 {
		t.Fatalf("rows counter = %d, want 16", got)
	}
	if got := m.Counter(metrics.WriteQueueDrains); got != s.Drains {
		t.Fatalf("drains counter = %d, Stats says %d", got, s.Drains)
	}
	if got := m.Counter(metrics.WriteQueuePending); got != 0 {
		t.Fatalf("pending gauge = %d after the queue settled, want 0", got)
	}
	if !metrics.IsGauge(metrics.WriteQueuePending) {
		t.Fatal("the pending rows series must be a gauge: it is a level, not an event count")
	}
}

// openFDs counts this process's open file descriptors. It is Linux-only, which is
// what the reference deployment and CI both are; a platform without /proc/self/fd
// reports 0 and the caller's tolerance simply never trips.
func openFDs(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0
	}
	return len(entries)
}

// TestNewClampsTheBudget documents the clamping New applies to a configuration: a
// budget wider than the pool it admits onto is parking rather than capacity.
func TestNewClampsTheBudget(t *testing.T) {
	for _, tc := range []struct {
		name  string
		opts  Options
		depth int
	}{
		{"named depth", Options{Depth: 12, Pool: 40}, 12},
		{"follows the pool", Options{Depth: 0, Pool: 40}, 40},
		{"wider than the pool", Options{Depth: 100, Pool: 40}, 40},
		{"a pool of one is still a budget", Options{Depth: 0, Pool: 1}, 1},
		{"no pool and no depth", Options{}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := newRecorder()
			tc.opts.Tx = rec.tx
			q := newQueue(t, tc.opts)
			if got := q.Depth(); got != tc.depth {
				t.Fatalf("depth = %d, want %d", got, tc.depth)
			}
		})
	}
}
