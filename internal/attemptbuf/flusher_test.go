package attemptbuf

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/m7jay/pigeon/internal/config"
	"github.com/m7jay/pigeon/internal/metrics"
	"github.com/m7jay/pigeon/internal/store"
)

// fakeSink records each batch it is asked to insert, so the tests can assert the
// transaction shape (FR-815) rather than only the row count. failNext makes one
// call fail, which is how the retry path is driven without a real database.
//
// entered and hold let a test cancel the flusher's context at a point where the
// flusher is provably inside the sink — and therefore is not acknowledging a batch
// on a context the cancellation is about to revoke. entered is closed on the first
// call, hold blocks every call until release. Both are set before Run starts and
// never written afterwards; see TestRunDrainsOnCancellation.
type fakeSink struct {
	mu       sync.Mutex
	batches  [][]store.AttemptRecord
	failNext error
	rows     int

	entered     chan struct{}
	hold        chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once
}

func (f *fakeSink) RecordAttempts(_ context.Context, batch []store.AttemptRecord) (int, error) {
	if f.entered != nil {
		f.enteredOnce.Do(func() { close(f.entered) })
	}
	// Held outside the lock: a test that polls the sink while a call is held would
	// otherwise block behind it.
	if f.hold != nil {
		<-f.hold
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batches = append(f.batches, append([]store.AttemptRecord(nil), batch...))
	if f.failNext != nil {
		err := f.failNext
		f.failNext = nil
		return 0, err
	}
	f.rows += len(batch)
	return len(batch), nil
}

// release unblocks a held call. It is safe to call more than once, so a test can
// both release deliberately and defer a release as an escape hatch.
func (f *fakeSink) release() {
	if f.hold == nil {
		return
	}
	f.releaseOnce.Do(func() { close(f.hold) })
}

func (f *fakeSink) batchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.batches)
}

// rowCount is the race-free read of rows. Tests poll it from the test goroutine
// while Run's goroutine increments rows inside RecordAttempts, so reading the
// field directly is a data race the -race detector (rightly) fails on.
func (f *fakeSink) rowCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rows
}

func newTestFlusher(t *testing.T, batch int) (*Flusher, *fakeSink, *Buffer) {
	t.Helper()
	buf, _ := newTestBuffer(t)
	sink := &fakeSink{}
	fl, err := NewFlusher(buf, sink, FlusherOptions{
		Batch:      batch,
		Interval:   10 * time.Millisecond,
		StaleAfter: time.Hour,
		DrainGrace: 2 * time.Second,
		Metrics:    metrics.New(),
	})
	// NewFlusher raises Batch to FR-815's floor, so a sub-floor request here is
	// deliberately a request for the floor (see TestNewFlusherRaisesTheBatchToTheFloor).
	_ = batch
	if err != nil {
		t.Fatalf("NewFlusher: %v", err)
	}
	return fl, sink, buf
}

// waitFor polls cond until it holds or d reaches, and reports which happened. The
// flusher runs on its own goroutine, so every state a test wants to see is waited
// for rather than sampled; d is what keeps a state that never arrives (a mutation
// that dropped the behaviour) a failure instead of a hang.
func waitFor(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
}

// sameRecords reports whether two batches carry the same attempt records, which is
// the retry property: the flusher re-inserts the batch it failed on, not a fresh
// read of something else.
func sameRecords(a, b []store.AttemptRecord) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].DeliveryID != b[i].DeliveryID || a[i].Attempt != b[i].Attempt {
			return false
		}
	}
	return true
}

// TestFlushOnceInsertsOneTransaction is FR-815's unit-level guarantee: every
// FlushOnce call is exactly one Sink.RecordAttempts call, however many entries it
// carried, and the batch is acknowledged only after the insert succeeds.
func TestFlushOnceInsertsOneTransaction(t *testing.T) {
	ctx := context.Background()
	fl, sink, buf := newTestFlusher(t, 10)
	for i := 1; i <= 4; i++ {
		if err := buf.Push(ctx, rec(int64(i), 1)); err != nil {
			t.Fatalf("Push: %v", err)
		}
	}

	n, err := fl.FlushOnce(ctx)
	if err != nil {
		t.Fatalf("FlushOnce: %v", err)
	}
	if n != 4 {
		t.Fatalf("FlushOnce inserted %d rows, want 4", n)
	}
	if got := sink.batchCount(); got != 1 {
		t.Fatalf("Sink was called %d times, want 1 transaction per flush", got)
	}
	if got := len(sink.batches[0]); got != 4 {
		t.Fatalf("Sink batch carried %d rows, want 4", got)
	}
	if depth, _ := buf.Depth(ctx); depth != 0 {
		t.Fatalf("buffer depth after a successful flush = %d, want 0 (entries must be acknowledged)", depth)
	}
}

// TestFlushFailureKeepsTheBatchPending is the durability argument: an insert that
// fails must NOT acknowledge the entries. They stay in Redis and in the flusher's
// retry slot, and the next flush inserts them exactly once.
func TestFlushFailureKeepsTheBatchPending(t *testing.T) {
	ctx := context.Background()
	fl, sink, buf := newTestFlusher(t, 10)
	for i := 1; i <= 3; i++ {
		if err := buf.Push(ctx, rec(int64(i), 1)); err != nil {
			t.Fatalf("Push: %v", err)
		}
	}

	sink.failNext = errors.New("database is down")
	if _, err := fl.FlushOnce(ctx); err == nil {
		t.Fatal("FlushOnce returned nil after a failed insert")
	}
	if depth, _ := buf.Depth(ctx); depth != 3 {
		t.Fatalf("buffer depth after a failed insert = %d, want 3 (nothing may be acknowledged)", depth)
	}

	// The retry re-reads the stuck batch and inserts it; the sink sees the same
	// records, and only then are they acknowledged.
	n, err := fl.FlushOnce(ctx)
	if err != nil {
		t.Fatalf("retry FlushOnce: %v", err)
	}
	if n != 3 {
		t.Fatalf("retry inserted %d rows, want 3", n)
	}
	if depth, _ := buf.Depth(ctx); depth != 0 {
		t.Fatalf("buffer depth after the retry = %d, want 0", depth)
	}
}

// TestPassDrainsMultipleFullBatches is FR-815 under load: when the buffer holds
// more than one full batch, one pass keeps flushing full batches instead of
// stopping after the first. The transaction size stays at the floor, not "whatever
// one poll happened to see". The floor is 2,000, so the load here is 2,500 rows.
func TestPassDrainsMultipleFullBatches(t *testing.T) {
	ctx := context.Background()
	fl, sink, buf := newTestFlusher(t, config.MinFlushBatch)
	for i := 1; i <= config.MinFlushBatch+500; i++ {
		if err := buf.Push(ctx, rec(int64(i), 1)); err != nil {
			t.Fatalf("Push: %v", err)
		}
	}

	n, err := fl.pass(ctx)
	if err != nil {
		t.Fatalf("pass: %v", err)
	}
	if n != config.MinFlushBatch+500 {
		t.Fatalf("pass inserted %d rows, want %d", n, config.MinFlushBatch+500)
	}
	// 2,000 then 500: two full-then-short reads, i.e. two transactions.
	if got := sink.batchCount(); got != 2 {
		t.Fatalf("pass made %d transactions, want 2 (batches of 2000 and 500)", got)
	}
	if got := len(sink.batches[0]); got != config.MinFlushBatch {
		t.Fatalf("first batch carried %d rows, want FR-815's floor %d", got, config.MinFlushBatch)
	}
	if depth, _ := buf.Depth(ctx); depth != 0 {
		t.Fatalf("buffer depth after the pass = %d, want 0", depth)
	}
}

// TestDrainEmptiesTheBuffer: D-3's shutdown operation flushes until the buffer is
// empty, then returns.
func TestDrainEmptiesTheBuffer(t *testing.T) {
	ctx := context.Background()
	fl, sink, buf := newTestFlusher(t, 10)
	for i := 1; i <= 7; i++ {
		if err := buf.Push(ctx, rec(int64(i), 1)); err != nil {
			t.Fatalf("Push: %v", err)
		}
	}
	n, err := fl.Drain(ctx)
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if n != 7 {
		t.Fatalf("Drain flushed %d rows, want 7", n)
	}
	if depth, _ := buf.Depth(ctx); depth != 0 {
		t.Fatalf("buffer depth after Drain = %d, want 0", depth)
	}
	if sink.rowCount() != 7 {
		t.Fatalf("sink inserted %d rows, want 7", sink.rowCount())
	}
}

// TestUnreadableEntriesAreRetiredWithoutInserting: a corrupt payload cannot ever
// become a row. It must be acknowledged (so it stops being re-read forever) while
// the sink is not called for it.
func TestUnreadableEntriesAreRetiredWithoutInserting(t *testing.T) {
	ctx := context.Background()
	fl, sink, buf := newTestFlusher(t, 10)
	if err := buf.Push(ctx, rec(1, 1)); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if err := buf.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: buf.Key(), Values: map[string]any{recordField: "garbage"},
	}).Err(); err != nil {
		t.Fatalf("XAdd garbage: %v", err)
	}

	n, err := fl.FlushOnce(ctx)
	if err != nil {
		t.Fatalf("FlushOnce: %v", err)
	}
	if n != 1 {
		t.Fatalf("FlushOnce inserted %d rows, want 1 decodable row", n)
	}
	if got := len(sink.batches[0]); got != 1 {
		t.Fatalf("sink batch carried %d rows, want only the decodable one", got)
	}
	if depth, _ := buf.Depth(ctx); depth != 0 {
		t.Fatalf("buffer depth after retiring the corrupt entry = %d, want 0", depth)
	}
}

// TestNewFlusherRaisesTheBatchToTheFloor is FR-815 applied in the constructor:
// even a programmatic caller cannot configure a sub-floor batch.
func TestNewFlusherRaisesTheBatchToTheFloor(t *testing.T) {
	buf, _ := newTestBuffer(t)
	fl, err := NewFlusher(buf, &fakeSink{}, FlusherOptions{Batch: 1})
	if err != nil {
		t.Fatalf("NewFlusher: %v", err)
	}
	if fl.opts.Batch != config.MinFlushBatch {
		t.Fatalf("Batch = %d, want it raised to FR-815's floor %d", fl.opts.Batch, config.MinFlushBatch)
	}
}

// TestNewFlusherRequiresASink: silently discarding evidence is the one thing the
// flush path must never do, so a nil sink is a construction error.
func TestNewFlusherRequiresASink(t *testing.T) {
	buf, _ := newTestBuffer(t)
	if _, err := NewFlusher(buf, nil, FlusherOptions{}); err == nil {
		t.Fatal("NewFlusher accepted a nil sink")
	}
	if _, err := NewFlusher(nil, &fakeSink{}, FlusherOptions{}); err == nil {
		t.Fatal("NewFlusher accepted a nil buffer")
	}
}

// TestRunDrainsOnCancellation is D-3 end to end: when the context is cancelled,
// Run flushes what is still buffered before returning ctx.Err().
//
// The cancellation is driven by the sink rather than raced against it: the first
// pass's insert is held open inside the sink (fakeSink.hold) and the test cancels
// while it is there. Two things follow, and both are deliberate.
//
// The flusher provably is not acknowledging a batch at that instant, so the
// cancellation cannot revoke an ack. That was this test's flake on a loaded box:
// `rowCount() >= 5` becomes visible inside the sink call, so the cancel landed
// between the insert and its ack, the ack (XACK/XDEL) was revoked, and the batch
// stayed in the stream with nothing able to reclaim it — StaleAfter is an hour
// here and the flusher's retry slot was already cleared by the successful insert.
// The test then read depth 5 and failed as if the drain had not run.
//
// Running the held insert into a failure instead leaves the batch in the flusher's
// retry slot when the shutdown starts, which is what gives the drain something to
// flush on its own uncancellable context — so the assertions below are about D-3
// rather than about one unlucky interleaving.
func TestRunDrainsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fl, sink, buf := newTestFlusher(t, 10)
	sink.failNext = errors.New("transient")
	// Hold the first pass inside the sink, and release it from whichever path
	// leaves this test, so a failure cannot strand Run's goroutine.
	sink.entered = make(chan struct{})
	sink.hold = make(chan struct{})
	defer sink.release()

	for i := 1; i <= 5; i++ {
		if err := buf.Push(context.Background(), rec(int64(i), 1)); err != nil {
			t.Fatalf("Push: %v", err)
		}
	}

	done := make(chan error, 1)
	go func() { done <- fl.Run(ctx) }()

	// The first pass has read the five entries and is inside the failing insert:
	// nothing is acknowledged, so the batch stays the drain's to flush.
	select {
	case <-sink.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not reach the sink within 5s")
	}
	cancel()
	sink.release()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}

	// The drain runs on its own context, so the batch the cancelled pass left
	// behind is inserted and acknowledged before Run returns.
	if got := sink.batchCount(); got != 2 {
		t.Fatalf("sink was called %d times, want 2 (the cancelled pass and the shutdown drain)", got)
	}
	if !sameRecords(sink.batches[0], sink.batches[1]) {
		t.Fatalf("the drain did not flush the batch the cancelled pass left: %+v then %+v",
			sink.batches[0], sink.batches[1])
	}
	if got := sink.rowCount(); got != 5 {
		t.Fatalf("sink inserted %d rows, want 5 (the shutdown drain must flush what is buffered)", got)
	}
	if depth, _ := buf.Depth(context.Background()); depth != 0 {
		t.Fatalf("buffer depth after the shutdown drain = %d, want 0", depth)
	}
}

// TestRunRetriesAStuckBatch: a failed insert leaves the batch in the retry slot,
// and the next pass inserts it without waiting for the stale-reclaim window. This
// is what keeps a transient database blip from delaying evidence by 30 s.
//
// StaleAfter is an hour here, so a second sink call cannot come from the
// stale-reclaim path: it can only be the retry of the batch the failed insert left
// in the slot. The context is cancelled only once that retry has been
// acknowledged, because a pass acknowledges on the context Run was given — a
// cancel landing between the retry's sink call and its ack revokes the ack and
// leaves the entry in the stream, and the assertions below would then be reading
// an interleaving rather than the retry.
func TestRunRetriesAStuckBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fl, sink, buf := newTestFlusher(t, 10)
	sink.failNext = errors.New("transient")

	if err := buf.Push(context.Background(), rec(1, 1)); err != nil {
		t.Fatalf("Push: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- fl.Run(ctx) }()

	// The retry: the sink's second call, which the flusher's own retry slot
	// produces on the pass after the failure — far inside the hour StaleAfter
	// would have made it wait for.
	if !waitFor(5*time.Second, func() bool { return sink.batchCount() >= 2 }) {
		t.Fatalf("Run did not retry the failed batch within 5s; sink calls=%d, rows=%d",
			sink.batchCount(), sink.rowCount())
	}
	// And it is acknowledged: that ack is what retires the entries from the buffer.
	if !waitFor(5*time.Second, func() bool {
		depth, err := buf.Depth(context.Background())
		return err == nil && depth == 0
	}) {
		depth, _ := buf.Depth(context.Background())
		t.Fatalf("the retry was not acknowledged within 5s; buffer depth=%d, sink calls=%d, rows=%d",
			depth, sink.batchCount(), sink.rowCount())
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}

	if got := sink.batchCount(); got != 2 {
		t.Fatalf("sink was called %d times, want 2 (the failed insert and its retry)", got)
	}
	if !sameRecords(sink.batches[0], sink.batches[1]) {
		t.Fatalf("the retry did not carry the failed batch: first %+v, retry %+v", sink.batches[0], sink.batches[1])
	}
	if got := sink.rowCount(); got != 1 {
		t.Fatalf("sink inserted %d rows, want 1", got)
	}
	// Run has returned, so reading the flusher's own state is race-free here: the
	// retry slot is empty again, and the entry is out of the buffer.
	if got := len(fl.stuck); got != 0 {
		t.Fatalf("the flusher still holds %d entries in its retry slot after a successful retry", got)
	}
	if depth, _ := buf.Depth(context.Background()); depth != 0 {
		t.Fatalf("buffer depth after the retry = %d, want 0", depth)
	}
}

// TestMetricsAdvanceOnFlush is AC-33's "counters advance": a successful flush
// increments the batch and row counters so an operator can see the evidence path
// working without reading the log stream.
func TestMetricsAdvanceOnFlush(t *testing.T) {
	ctx := context.Background()
	buf, _ := newTestBuffer(t)
	m := metrics.New()
	fl, err := NewFlusher(buf, &fakeSink{}, FlusherOptions{Batch: 10, Metrics: m})
	if err != nil {
		t.Fatalf("NewFlusher: %v", err)
	}
	for i := 1; i <= 3; i++ {
		if err := buf.Push(ctx, rec(int64(i), 1)); err != nil {
			t.Fatalf("Push: %v", err)
		}
	}
	if _, err := fl.FlushOnce(ctx); err != nil {
		t.Fatalf("FlushOnce: %v", err)
	}
	if got := m.Counter(metrics.FlushBatches); got != 1 {
		t.Fatalf("%s = %d, want 1", metrics.FlushBatches, got)
	}
	if got := m.Counter(metrics.AttemptsFlushed); got != 3 {
		t.Fatalf("%s = %d, want 3", metrics.AttemptsFlushed, got)
	}
}

// cancelAfterCommitSink fails the first insert, so the batch lands in the
// flusher's retry slot, and cancels the pass's context from inside the retry's
// own RecordAttempts. The cancellation therefore lands after the rows are
// committed and before the flusher retires the entries - the exact window W1
// measured - and it is driven by the sink rather than raced against the flusher,
// so the instant is the same on every run.
type cancelAfterCommitSink struct {
	mu     sync.Mutex
	cancel context.CancelFunc
	calls  int
	rows   int
}

func (s *cancelAfterCommitSink) RecordAttempts(_ context.Context, batch []store.AttemptRecord) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls == 1 {
		return 0, errors.New("transient")
	}
	s.cancel()
	s.rows += len(batch)
	return len(batch), nil
}

// count is the race-free read of the sink's own state: Run's goroutine writes it
// while the test goroutine reads it.
func (s *cancelAfterCommitSink) count() (calls, rows int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls, s.rows
}

// TestRunRetiresTheBatchEvenWhenThePassIsCancelled is D-9's property for W1: a
// cancellation that lands after the rows are committed must not leave a batch the
// process cannot hand back. The retry slot is already empty at that instant, the
// stale window is an hour, and the entries are still pending - so if the
// retirement is revocable, nothing short of the next start's reclaim(0) can
// recover them (depth 1, pending 1, and a duplicate insert plus an
// AttemptsConflicted bump next boot).
func TestRunRetiresTheBatchEvenWhenThePassIsCancelled(t *testing.T) {
	ctx := context.Background()
	buf, _ := newTestBuffer(t)
	if err := buf.Push(ctx, rec(1, 1)); err != nil {
		t.Fatalf("Push: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	sink := &cancelAfterCommitSink{cancel: cancel}
	fl, err := NewFlusher(buf, sink, FlusherOptions{
		Batch:       10,
		Interval:    10 * time.Millisecond,
		StaleAfter:  time.Hour,
		FailBackoff: 5 * time.Millisecond,
		DrainGrace:  2 * time.Second,
		Metrics:     metrics.New(),
	})
	if err != nil {
		t.Fatalf("NewFlusher: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- fl.Run(runCtx) }()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the cancelled pass")
	}

	if calls, rows := sink.count(); calls != 2 || rows != 1 {
		t.Fatalf("the sink saw %d calls and %d rows, want 2 calls (the failure and its retry) and 1 row", calls, rows)
	}
	// Run has returned, so reading the flusher's own state is race-free here.
	if got := len(fl.stuck); got != 0 {
		t.Fatalf("the flusher still holds %d entries in its retry slot", got)
	}
	if depth, err := buf.Depth(ctx); err != nil || depth != 0 {
		t.Fatalf("buffer depth after Run returned = %d, %v; want 0: a cancelled pass must not leave a batch nothing can hand back", depth, err)
	}
	if got := pendingCount(t, buf); got != 0 {
		t.Fatalf("pending after Run returned = %d, want 0: the retirement must not be revocable by the cancellation", got)
	}
}

// TestStrandedEntriesAreSweptAtStartup: a strand created before this process
// started - the tear a transaction cannot cover - is retired by Run's startup
// sweep, so the depth gauge OBS-9 watches stops counting entries that are not
// work (D-9, OBS-9).
func TestStrandedEntriesAreSweptAtStartup(t *testing.T) {
	ctx := context.Background()
	fl, sink, buf := newTestFlusher(t, 10)

	for i := 1; i <= 3; i++ {
		if err := buf.Push(ctx, rec(int64(i), 1)); err != nil {
			t.Fatalf("Push: %v", err)
		}
	}
	entries, err := buf.Read(ctx, 10)
	if err != nil || len(entries) != 3 {
		t.Fatalf("Read = %d entries, %v; want 3", len(entries), err)
	}
	// Strand the first two the way a torn retirement does, and leave the third
	// pending for the startup reclaim to pick up.
	if err := buf.rdb.XAck(ctx, buf.Key(), buf.group, entries[0].ID, entries[1].ID).Err(); err != nil {
		t.Fatalf("XAck: %v", err)
	}
	// And one entry nobody has read.
	if err := buf.Push(ctx, rec(4, 1)); err != nil {
		t.Fatalf("Push: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- fl.Run(runCtx) }()

	// The pending entry and the unread one are the run's to flush, so the only
	// reason the depth can reach 0 is the sweep retiring the stranded pair.
	if !waitFor(5*time.Second, func() bool {
		depth, err := buf.Depth(ctx)
		return err == nil && depth == 0
	}) {
		depth, _ := buf.Depth(ctx)
		t.Fatalf("the buffer never drained: depth=%d pending=%d rows=%d (the startup sweep must retire the stranded entries)",
			depth, pendingCount(t, buf), sink.rowCount())
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}

	// The flush path saw each real entry exactly once: the reclaimed pending one
	// and the unread one. The two strands carry rows a previous process already
	// committed, so they must not be inserted again.
	if got := sink.rowCount(); got != 2 {
		t.Fatalf("the sink inserted %d rows, want 2 (the pending entry and the unread one)", got)
	}
	if got := sink.batchCount(); got != 2 {
		t.Fatalf("the sink was called %d times, want 2 (the startup reclaim and the read)", got)
	}
	if got := pendingCount(t, buf); got != 0 {
		t.Fatalf("pending after the run = %d, want 0", got)
	}
	if depth, err := buf.Depth(ctx); err != nil || depth != 0 {
		t.Fatalf("buffer depth after the run = %d, %v; want 0", depth, err)
	}
}

// TestNewFlusherDefaultsTheAckTimeout: the retirement's bound defaults to 2 s and
// lives on FlusherOptions rather than on a PIGEON_ knob, which is FailBackoff's
// status too - an operator has no business tuning it.
func TestNewFlusherDefaultsTheAckTimeout(t *testing.T) {
	buf, _ := newTestBuffer(t)
	fl, err := NewFlusher(buf, &fakeSink{}, FlusherOptions{})
	if err != nil {
		t.Fatalf("NewFlusher: %v", err)
	}
	if fl.opts.AckTimeout != 2*time.Second {
		t.Fatalf("AckTimeout = %v, want the 2 s default", fl.opts.AckTimeout)
	}
	fl, err = NewFlusher(buf, &fakeSink{}, FlusherOptions{AckTimeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("NewFlusher: %v", err)
	}
	if fl.opts.AckTimeout != 50*time.Millisecond {
		t.Fatalf("AckTimeout = %v, want the caller's 50 ms", fl.opts.AckTimeout)
	}
}

// TestInsertRetiresWithinAckTimeoutOnAHungRedis: the retirement's non-revocable
// context is bounded by AckTimeout and not by the client's read timeout, so a
// Redis that has stopped answering cannot hold a pass open (D-9). The rows are
// committed before the retirement, which is why this may fail as an error without
// being a loss.
func TestInsertRetiresWithinAckTimeoutOnAHungRedis(t *testing.T) {
	rdb, err := clientFromURL("redis://" + hungListener(t).Addr().String() + "/0")
	if err != nil {
		t.Fatalf("clientFromURL: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	buf, err := New(rdb, Options{Consumer: "test-flusher"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	fl, err := NewFlusher(buf, &fakeSink{}, FlusherOptions{AckTimeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatalf("NewFlusher: %v", err)
	}

	start := time.Now()
	// The sink is a fake, so the insert commits and this is the retirement path:
	// one MULTI/EXEC against a socket that accepts and never answers.
	inserted, err := fl.insert(context.Background(), []Entry{{ID: "1-0", Record: rec(1, 1)}})
	elapsed := time.Since(start)
	if inserted != 1 {
		t.Fatalf("insert reported %d rows, want 1: the commit happens before the retirement", inserted)
	}
	if err == nil {
		t.Fatal("insert reported success against a Redis that never answers")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("insert returned %v, want a deadline error", err)
	}
	if elapsed >= time.Second {
		t.Fatalf("the retirement took %v, want about AckTimeout (200 ms): the bound was not the one that was set", elapsed)
	}
}

// refuseRetireHook fails every pipeline carrying a retirement's XACK, so
// Buffer.Ack reports an error with nothing applied: what a Redis that is down,
// or that has stopped answering within AckTimeout, looks like to the flusher.
// The refusal is raised on the client (the hook returns before the commands are
// sent) rather than by a server answering errors mid-transaction, because a
// MULTI answered with an error leaves go-redis holding an unread reply and
// dropping the connection - noise in a test about the flusher's bookkeeping.
type refuseRetireHook struct {
	mu       sync.Mutex
	refusals int
}

func (h *refuseRetireHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *refuseRetireHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }

func (h *refuseRetireHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, c := range cmds {
			if c.Name() == "xack" {
				h.mu.Lock()
				h.refusals++
				h.mu.Unlock()
				return errors.New("LOADING Redis is loading the dataset in memory")
			}
		}
		return next(ctx, cmds)
	}
}

// refused is the race-free read of how many retirements the hook failed: the
// test's proof that a retirement was attempted at all.
func (h *refuseRetireHook) refused() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.refusals
}

// TestAFailedRetirementLeavesTheRetrySlotEmpty pins the order inside insert():
// the retry slot is cleared once the rows are committed and BEFORE the
// retirement is attempted, so a retirement that fails after the commit returns
// its error without re-parking the batch (D-9).
//
// Why the slot must stay empty (the rejected alternative, the decision record's
// section 2 q3): readBatch returns the retry slot FIRST, so a batch retained
// there is handed back on every pass. Nothing new is read while Redis is
// unhappy, every retry re-inserts a batch whose rows are already in Postgres
// (which counts AttemptsConflicted), and the stream grows behind it.
//
// The retirement fails here the way a Redis that cannot apply the
// acknowledgement makes it fail (refuseRetireHook). The hung listener
// TestInsertRetiresWithinAckTimeoutOnAHungRedis uses fails it just as well and
// would do for the slot assertion, but it never answers again, and the last
// assertions below need a healthy buffer: the pass after the failure has to be
// able to read.
func TestAFailedRetirementLeavesTheRetrySlotEmpty(t *testing.T) {
	ctx := context.Background()
	buf, _ := newTestBuffer(t)
	hook := &refuseRetireHook{}
	buf.rdb.AddHook(hook)
	sink := &fakeSink{}
	fl, err := NewFlusher(buf, sink, FlusherOptions{
		Interval:   10 * time.Millisecond,
		StaleAfter: time.Hour,
		AckTimeout: 200 * time.Millisecond,
		DrainGrace: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewFlusher: %v", err)
	}

	// The state the invariant is about, driven rather than seeded: one entry is
	// pushed, the sink refuses the insert, and insert parks the batch in the
	// retry slot. StaleAfter is an hour, so nothing below can be a stale reclaim
	// handing the same entry back.
	if err := buf.Push(ctx, rec(1, 1)); err != nil {
		t.Fatalf("Push: %v", err)
	}
	sink.failNext = errors.New("transient")
	if _, err := fl.FlushOnce(ctx); err == nil {
		t.Fatal("the first FlushOnce returned nil after a failed insert")
	}
	if got := len(fl.stuck); got != 1 {
		t.Fatalf("the failed insert left %d entries in the retry slot, want 1", got)
	}
	failed := fl.stuck[0]

	// A second entry arrives while the first batch is still unretired: it is what
	// the pass after the failed retirement must read.
	if err := buf.Push(ctx, rec(2, 1)); err != nil {
		t.Fatalf("Push: %v", err)
	}

	// The retry's insert commits - the fake sink accepts it - and its retirement
	// cannot be applied.
	inserted, err := fl.FlushOnce(ctx)
	if got := hook.refused(); got != 1 {
		t.Fatalf("the retirement was attempted %d times, want 1: the batch is retired after its insert", got)
	}
	if err == nil {
		t.Fatal("FlushOnce reported success while the retirement could not be applied")
	}
	if inserted != 1 {
		t.Fatalf("FlushOnce inserted %d rows, want 1: the insert commits before the retirement, so the failure is the retirement's", inserted)
	}

	// The invariant: the retry slot is empty, so the next pass is free to read.
	if got := len(fl.stuck); got != 0 {
		t.Fatalf("after a failed retirement the retry slot holds %d entries, want 0: readBatch returns the retry slot first, so a retained batch is handed back on every pass and nothing new is read while Redis is unhappy", got)
	}
	// And the batch really was not retired: its entry stays pending for the stale
	// reclaim window, which is what makes retaining it a stall rather than a loss.
	if got := pendingCount(t, buf); got != 1 {
		t.Fatalf("pending entries after the failed retirement = %d, want 1 (the committed batch is still pending; nothing acknowledged it)", got)
	}

	// The next read is the entry pushed during the failure, not the batch whose
	// retirement failed. A flusher that retained that batch hands it back here
	// instead, and that is the stall this pins.
	entries, err := fl.readBatch(ctx)
	if err != nil {
		t.Fatalf("readBatch after the failed retirement: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("readBatch returned %d entries %v, want the one entry pushed after the failure", len(entries), IDs(entries))
	}
	if entries[0].ID == failed.ID {
		t.Fatalf("readBatch returned the batch whose retirement failed (%s, delivery %d) instead of the entry pushed after it (delivery 2): readBatch returns the retry slot first, so a retained batch is read again on every pass and nothing new is", failed.ID, failed.Record.DeliveryID)
	}
	if got := entries[0].Record.DeliveryID; got != 2 {
		t.Fatalf("readBatch returned the entry with delivery %d, want the one pushed after the failure (delivery 2)", got)
	}
}
