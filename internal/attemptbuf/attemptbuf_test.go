package attemptbuf

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/m7jay/pigeon/internal/store"
)

// newTestBuffer starts a disposable miniredis and a Buffer over it. The stream
// semantics this package depends on — XADD, consumer groups, XAUTOCLAIM and
// XACK — are all exercised against miniredis here; CI runs the same assertions
// against a real Redis when PIGEON_TEST_REDIS_URL is set (broker_test.go's
// pattern).
func newTestBuffer(t *testing.T) (*Buffer, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	b, err := New(rdb, Options{Consumer: "test-flusher"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return b, mr
}

func rec(id int64, attempt int) store.AttemptRecord {
	return store.AttemptRecord{
		DeliveryID:   id,
		Attempt:      attempt,
		Outcome:      "retryable",
		ErrorClass:   "http_retryable",
		DurationMs:   12,
		OccurredAt:   time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC),
		ResponseBody: strings.Repeat("x", 32),
	}
}

// TestPushReadAckRoundTrip is D-1's core promise: a push that returns nil is in
// Redis, a read leaves it pending until the ack, and the ack retires it.
func TestPushReadAckRoundTrip(t *testing.T) {
	ctx := context.Background()
	b, _ := newTestBuffer(t)

	for i := 1; i <= 3; i++ {
		if err := b.Push(ctx, rec(int64(i), 1)); err != nil {
			t.Fatalf("Push(%d): %v", i, err)
		}
	}
	if n, err := b.Depth(ctx); err != nil || n != 3 {
		t.Fatalf("Depth = %d, %v; want 3", n, err)
	}

	entries, err := b.Read(ctx, 10)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("Read returned %d entries, want 3", len(entries))
	}
	// The round trip must preserve the record, including the attempt timestamp
	// the buffered path carries so the row records the attempt, not the flush.
	if got := entries[0].Record; got.DeliveryID != 1 || got.Attempt != 1 || !got.OccurredAt.Equal(rec(1, 1).OccurredAt) {
		t.Fatalf("round trip lost the record: %+v", got)
	}
	if entries[0].Unreadable {
		t.Fatal("a freshly pushed record was decoded as unreadable")
	}

	// Pending until acknowledged: a second read of new entries finds nothing.
	if again, err := b.Read(ctx, 10); err != nil || len(again) != 0 {
		t.Fatalf("second Read = %d entries, %v; want 0 (the first batch is pending)", len(again), err)
	}
	if err := b.Ack(ctx, IDs(entries)); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if n, err := b.Depth(ctx); err != nil || n != 0 {
		t.Fatalf("Depth after ack = %d, %v; want 0", n, err)
	}
}

// TestReclaimRecoversAPendingBatch is D-1's "0 lost on a worker crash": entries a
// dead flusher read but never acked are reclaimed on the next start (minIdle 0),
// while a healthy peer's fresh batch is left alone (StaleAfter).
func TestReclaimRecoversAPendingBatch(t *testing.T) {
	ctx := context.Background()
	b, _ := newTestBuffer(t)
	for i := 1; i <= 2; i++ {
		if err := b.Push(ctx, rec(int64(i), 1)); err != nil {
			t.Fatalf("Push: %v", err)
		}
	}
	// Simulate a flusher that died after reading: read and never ack.
	entries, err := b.Read(ctx, 10)
	if err != nil || len(entries) != 2 {
		t.Fatalf("Read = %d entries, %v", len(entries), err)
	}

	// A healthy peer must not steal a fresh pending batch.
	if n, err := b.Reclaim(ctx, 10, time.Hour); err != nil || len(n) != 0 {
		t.Fatalf("Reclaim(stale 1h) = %d entries, %v; want 0", len(n), err)
	}
	// A restart reclaims everything: at startup every pending entry is ours.
	reclaimed, err := b.Reclaim(ctx, 10, 0)
	if err != nil {
		t.Fatalf("Reclaim(0): %v", err)
	}
	if len(reclaimed) != 2 {
		t.Fatalf("Reclaim(0) returned %d entries, want 2", len(reclaimed))
	}
	if IDs(reclaimed)[0] == "" {
		t.Fatal("reclaimed entry lost its acknowledgement id")
	}
	if err := b.Ack(ctx, IDs(reclaimed)); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if n, _ := b.Depth(ctx); n != 0 {
		t.Fatalf("Depth after reclaim+ack = %d, want 0", n)
	}
}

// TestReadCreatesTheGroupIdempotently: a flusher that starts before the first
// push must not error, and a second read must not fail on BUSYGROUP.
func TestReadCreatesTheGroupIdempotently(t *testing.T) {
	ctx := context.Background()
	b, _ := newTestBuffer(t)
	for i := 0; i < 2; i++ {
		entries, err := b.Read(ctx, 10)
		if err != nil {
			t.Fatalf("Read #%d: %v", i+1, err)
		}
		if len(entries) != 0 {
			t.Fatalf("Read #%d = %d entries, want 0 on an empty buffer", i+1, len(entries))
		}
	}
}

// TestUnreadableEntriesAreRetiredNotDroppedSilently: an entry nobody can decode
// is marked Unreadable and still handed to the flusher, which retires it with a
// warning. Dropping it silently would lose the fact that evidence was lost.
func TestUnreadableEntriesAreRetiredNotDroppedSilently(t *testing.T) {
	ctx := context.Background()
	b, _ := newTestBuffer(t)
	if err := b.Push(ctx, rec(1, 1)); err != nil {
		t.Fatalf("Push: %v", err)
	}
	// Corrupt one entry directly in the stream, as a bad deploy or a truncated
	// write would.
	if err := b.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: b.Key(), Values: map[string]any{recordField: "{not json"},
	}).Err(); err != nil {
		t.Fatalf("XAdd corrupt: %v", err)
	}

	entries, err := b.Read(ctx, 10)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("Read returned %d entries, want 2", len(entries))
	}
	if got := len(Records(entries)); got != 1 {
		t.Fatalf("Records() = %d, want 1 decodable", got)
	}
	unreadable := Unreadable(entries)
	if len(unreadable) != 1 {
		t.Fatalf("Unreadable() = %d, want 1", len(unreadable))
	}
	if !unreadable[0].Unreadable || unreadable[0].Raw == "" {
		t.Fatalf("corrupt entry not marked or not preserved: %+v", unreadable[0])
	}
	if len(unreadable[0].Raw) > maxRawInLog {
		t.Fatalf("raw payload in the log line is %d bytes, want <= %d", len(unreadable[0].Raw), maxRawInLog)
	}
	if err := b.Ack(ctx, IDs(entries)); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestAckEmptyIsANoOp: the flusher acks every batch, including a batch that was
// all unreadable and therefore carried no records.
func TestAckEmptyIsANoOp(t *testing.T) {
	b, _ := newTestBuffer(t)
	if err := b.Ack(context.Background(), nil); err != nil {
		t.Fatalf("Ack(nil): %v", err)
	}
}

// TestPushConcurrentPreservesEveryRecord: the workers push from many goroutines
// while the flusher reads from one. No append may be lost or reordered into
// another entry's field.
func TestPushConcurrentPreservesEveryRecord(t *testing.T) {
	ctx := context.Background()
	b, _ := newTestBuffer(t)
	const workers, perWorker = 8, 25

	var wg sync.WaitGroup
	errs := make(chan error, workers*perWorker)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				if err := b.Push(ctx, rec(int64(w*perWorker+i+1), i+1)); err != nil {
					errs <- err
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent Push: %v", err)
	}
	if n, err := b.Depth(ctx); err != nil || n != workers*perWorker {
		t.Fatalf("Depth = %d, %v; want %d", n, err, workers*perWorker)
	}

	seen := map[int64]int{}
	for {
		entries, err := b.Read(ctx, 1000)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if len(entries) == 0 {
			break
		}
		for _, e := range entries {
			seen[e.Record.DeliveryID]++
		}
		if err := b.Ack(ctx, IDs(entries)); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
	if len(seen) != workers*perWorker {
		t.Fatalf("read %d distinct records, want %d", len(seen), workers*perWorker)
	}
}

// TestRecordSurvivesJSONRoundTrip pins the wire format the flusher and the worker
// share: a record's pointer fields must survive encode/decode without becoming a
// zero value that Postgres would read as "no response" or "no next retry".
func TestRecordSurvivesJSONRoundTrip(t *testing.T) {
	status := 429
	next := time.Date(2026, 2, 3, 4, 6, 0, 0, time.UTC)
	in := rec(7, 2)
	in.ResponseStatus = &status
	in.NextRetryAt = &next

	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var out store.AttemptRecord
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if out.ResponseStatus == nil || *out.ResponseStatus != 429 {
		t.Fatalf("ResponseStatus did not survive the round trip: %+v", out.ResponseStatus)
	}
	if out.NextRetryAt == nil || !out.NextRetryAt.Equal(next) {
		t.Fatalf("NextRetryAt did not survive the round trip: %+v", out.NextRetryAt)
	}
}

// TestDialRejectsAMalformedURL: a misconfigured buffer must fail the boot (OPS-7)
// rather than accept pushes it can never flush.
func TestDialRejectsAMalformedURL(t *testing.T) {
	if _, err := Dial(context.Background(), "://nonsense", Options{}); err == nil {
		t.Fatal("Dial accepted a malformed Redis URL")
	}
}

// TestDialPingsTheServer: a URL that parses but points at nothing must fail
// loudly, not defer the failure to the first attempt's push.
func TestDialPingsTheServer(t *testing.T) {
	// A port nothing listens on. The ping must fail within the dial timeout.
	_, err := Dial(context.Background(), "redis://127.0.0.1:1/0", Options{})
	if err == nil {
		t.Fatal("Dial succeeded against a port nothing listens on")
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("Dial returned the context error instead of a ping failure: %v", err)
	}
}

// ackPipelineHook records the command names of every pipeline go-redis sends.
// That is the only observable seam the transaction has: a tear between two
// commands cannot be injected without a special Redis, so the mechanism - one
// pipeline carrying both commands, in order - is what the test asserts.
type ackPipelineHook struct {
	mu        sync.Mutex
	pipelines [][]string
}

func (h *ackPipelineHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *ackPipelineHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }

func (h *ackPipelineHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		names := make([]string, 0, len(cmds))
		for _, c := range cmds {
			names = append(names, c.Name())
		}
		h.mu.Lock()
		h.pipelines = append(h.pipelines, names)
		h.mu.Unlock()
		return next(ctx, cmds)
	}
}

// seen copies the recorded pipelines, so a test reads them without holding the
// hook's lock.
func (h *ackPipelineHook) seen() [][]string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([][]string, len(h.pipelines))
	copy(out, h.pipelines)
	return out
}

// pendingCount is XPENDING's summary count: the entries the group holds as
// unacknowledged. Tests assert it separately from XLEN, because the two
// disagreeing is exactly the strand this package must not produce.
func pendingCount(t *testing.T, b *Buffer) int64 {
	t.Helper()
	sum, err := b.rdb.XPending(context.Background(), b.Key(), b.group).Result()
	if err != nil {
		t.Fatalf("XPENDING: %v", err)
	}
	return sum.Count
}

// TestAckIsOneTransaction: the retirement is ONE transaction - multi, xack, xdel,
// exec, in that order - and it leaves nothing behind (D-9). Two sequential
// commands do not appear as a pipeline at all, and the window between them is the
// strand no recovery path in this package can reach.
func TestAckIsOneTransaction(t *testing.T) {
	ctx := context.Background()
	b, _ := newTestBuffer(t)
	hook := &ackPipelineHook{}
	b.rdb.AddHook(hook)

	for i := 1; i <= 2; i++ {
		if err := b.Push(ctx, rec(int64(i), 1)); err != nil {
			t.Fatalf("Push: %v", err)
		}
	}
	entries, err := b.Read(ctx, 10)
	if err != nil || len(entries) != 2 {
		t.Fatalf("Read = %d entries, %v; want 2", len(entries), err)
	}

	if err := b.Ack(ctx, IDs(entries)); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	pipelines := hook.seen()
	if len(pipelines) != 1 {
		t.Fatalf("Ack sent %d pipelines, want exactly 1 carrying both commands: %v", len(pipelines), pipelines)
	}
	want := []string{"multi", "xack", "xdel", "exec"}
	if len(pipelines[0]) != len(want) {
		t.Fatalf("the retire pipeline carried %v, want %v", pipelines[0], want)
	}
	for i, name := range want {
		if pipelines[0][i] != name {
			t.Fatalf("the retire pipeline carried %v, want %v (the xack precedes the xdel)", pipelines[0], want)
		}
	}
	if got := pendingCount(t, b); got != 0 {
		t.Fatalf("pending after the ack = %d, want 0", got)
	}
	if depth, err := b.Depth(ctx); err != nil || depth != 0 {
		t.Fatalf("XLEN after the ack = %d, %v; want 0", depth, err)
	}
}

// TestAckCancelledBeforeTheTransactionAppliesNothing: a context already cancelled
// when the retire is attempted applies NEITHER command, so the entry stays pending
// AND in the stream. The failed retire leaves the batch recoverable, which a
// half-applied one would not: D-9 is both-or-neither in both directions.
func TestAckCancelledBeforeTheTransactionAppliesNothing(t *testing.T) {
	ctx := context.Background()
	b, _ := newTestBuffer(t)
	if err := b.Push(ctx, rec(1, 1)); err != nil {
		t.Fatalf("Push: %v", err)
	}
	entries, err := b.Read(ctx, 10)
	if err != nil || len(entries) != 1 {
		t.Fatalf("Read = %d entries, %v; want 1", len(entries), err)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	err = b.Ack(cancelled, IDs(entries))
	if err == nil {
		t.Fatal("Ack on a cancelled context returned nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Ack returned %v, want a context.Canceled error", err)
	}
	if got := pendingCount(t, b); got != 1 {
		t.Fatalf("pending after the cancelled ack = %d, want 1 (nothing may be acked)", got)
	}
	if depth, err := b.Depth(ctx); err != nil || depth != 1 {
		t.Fatalf("XLEN after the cancelled ack = %d, %v; want 1 (nothing may be trimmed)", depth, err)
	}
	// Still the flusher's to hand back: reclaim with no idle requirement finds
	// it, which is what makes a failed retire a delay rather than a loss.
	reclaimed, err := b.Reclaim(ctx, 10, 0)
	if err != nil || len(reclaimed) != 1 {
		t.Fatalf("Reclaim(0) = %d entries, %v; want the entry still pending", len(reclaimed), err)
	}
}
