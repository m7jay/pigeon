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
type fakeSink struct {
	mu       sync.Mutex
	batches  [][]store.AttemptRecord
	failNext error
	rows     int
}

func (f *fakeSink) RecordAttempts(_ context.Context, batch []store.AttemptRecord) (int, error) {
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

// TestRunDrainsOnCancellation is D-3 end to end: Run keeps flushing while the
// context lives, and when the context is cancelled it flushes what is left before
// returning ctx.Err().
func TestRunDrainsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fl, sink, buf := newTestFlusher(t, 10)
	for i := 1; i <= 5; i++ {
		if err := buf.Push(context.Background(), rec(int64(i), 1)); err != nil {
			t.Fatalf("Push: %v", err)
		}
	}

	done := make(chan error, 1)
	go func() { done <- fl.Run(ctx) }()

	// Wait for the first pass to insert the five entries, then cancel.
	deadline := time.Now().Add(2 * time.Second)
	for sink.rowCount() < 5 {
		if time.Now().After(deadline) {
			t.Fatalf("Run did not flush within 2s; rows=%d", sink.rowCount())
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
	if depth, _ := buf.Depth(context.Background()); depth != 0 {
		t.Fatalf("buffer depth after the shutdown drain = %d, want 0", depth)
	}
}

// TestRunRetriesAStuckBatch: a failed insert leaves the batch in the retry slot,
// and the next pass inserts it without waiting for the stale-reclaim window. This
// is what keeps a transient database blip from delaying evidence by 30 s.
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

	deadline := time.Now().Add(2 * time.Second)
	for sink.rowCount() < 1 {
		if time.Now().After(deadline) {
			t.Fatalf("Run did not retry the failed batch within 2s; batches=%d", sink.batchCount())
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	if depth, _ := buf.Depth(context.Background()); depth != 0 {
		t.Fatalf("buffer depth after the retry = %d, want 0", depth)
	}
	if got := sink.batchCount(); got < 2 {
		t.Fatalf("sink was called %d times, want at least 2 (the failure and the retry)", got)
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
