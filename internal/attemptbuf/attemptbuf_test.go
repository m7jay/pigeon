package attemptbuf

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
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

// hungListener accepts connections and never answers a byte: what a Redis that is
// up but wedged looks like to a client (the dial succeeds, the command hangs).
func hungListener(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	// The accepted connections are held open and dropped when the test ends, so
	// a client that is still waiting cannot leak a goroutine past the run.
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
	})
	return ln
}

// TestAckDeadlineBoundsAHungRedis: the client the package builds honours the
// caller's context deadline, so the deadline is a bound and not a wish. Without
// ContextTimeoutEnabled go-redis ignores the context entirely and rides its own
// 3 s ReadTimeout (measured: a 200 ms deadline returned after 3.001 s), which is
// exactly the shape of the bound D-9 puts on a retirement.
func TestAckDeadlineBoundsAHungRedis(t *testing.T) {
	addr := hungListener(t).Addr().String()

	// Both branches of clientFromURL: a bare host:port is the documented
	// fallback, and a bound that only exists on one of them is not a bound.
	for _, raw := range []string{"redis://" + addr + "/0", addr} {
		c, err := clientFromURL(raw)
		if err != nil {
			t.Fatalf("clientFromURL(%q): %v", raw, err)
		}
		enabled := c.Options().ContextTimeoutEnabled
		_ = c.Close()
		if !enabled {
			t.Fatalf("clientFromURL(%q) left ContextTimeoutEnabled false; a context deadline is then not a bound", raw)
		}
	}

	rdb, err := clientFromURL("redis://" + addr + "/0")
	if err != nil {
		t.Fatalf("clientFromURL: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	b, err := New(rdb, Options{Consumer: "test-flusher"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = b.Ack(ctx, []string{"1-0"})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("Ack reported success against a Redis that never answers")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Ack returned %v, want a deadline error", err)
	}
	// The client's read timeout is what this pins: a bound that is not the bound
	// reads as "the flusher is slow", not as "the deadline was ignored".
	if elapsed >= time.Second {
		t.Fatalf("Ack with a 200 ms deadline returned after %v; the client rode its own read timeout instead", elapsed)
	}
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

// assertSweepKeepsPendingAndUnread is the sweep's safety property, and it is run
// against both servers the package can talk to: W2's shape - three entries read,
// the first two acknowledged without being removed - plus one entry nobody has
// read. The sweep removes exactly the stranded pair, because the bound is the
// smallest pending id: anything older than the oldest unacknowledged entry has
// been delivered and is not pending, so it is already acknowledged. XTRIM does
// not consult the pending list, so that bound is the whole safety argument (D-9).
func assertSweepKeepsPendingAndUnread(t *testing.T, b *Buffer) {
	t.Helper()
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		if err := b.Push(ctx, rec(int64(i), 1)); err != nil {
			t.Fatalf("Push: %v", err)
		}
	}
	entries, err := b.Read(ctx, 10)
	if err != nil || len(entries) != 3 {
		t.Fatalf("Read = %d entries, %v; want 3", len(entries), err)
	}
	// Strand the first two the way a torn retirement does: acknowledged, so out
	// of the pending list, but never removed from the stream.
	if err := b.rdb.XAck(ctx, b.Key(), b.group, entries[0].ID, entries[1].ID).Err(); err != nil {
		t.Fatalf("XAck: %v", err)
	}
	// And one entry nobody has read yet.
	if err := b.Push(ctx, rec(4, 1)); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if before, _ := b.Depth(ctx); before != 4 {
		t.Fatalf("XLEN before the sweep = %d, want 4", before)
	}

	n, err := b.SweepStranded(ctx)
	if err != nil {
		t.Fatalf("SweepStranded: %v", err)
	}
	if n != 2 {
		t.Fatalf("SweepStranded removed %d entries, want 2 (the stranded pair)", n)
	}
	if after, _ := b.Depth(ctx); after != 2 {
		t.Fatalf("XLEN after the sweep = %d, want 2 (the pending entry and the unread one)", after)
	}
	if got := pendingCount(t, b); got != 1 {
		t.Fatalf("pending after the sweep = %d, want 1: the sweep removed a pending entry", got)
	}
	// The pending entry is still the flusher's to hand back...
	reclaimed, err := b.Reclaim(ctx, 10, 0)
	if err != nil || len(reclaimed) != 1 || reclaimed[0].ID != entries[2].ID {
		t.Fatalf("Reclaim(0) = %v, %v; want the pending entry %s", IDs(reclaimed), err, entries[2].ID)
	}
	// ...and the unread entry is still unread, not silently dropped.
	unread, err := b.Read(ctx, 10)
	if err != nil || len(unread) != 1 || unread[0].Record.DeliveryID != 4 {
		t.Fatalf("Read = %+v, %v; want only the unread entry (delivery 4)", unread, err)
	}
}

// assertSweepTrimsTheLastAckedEntry pins the +1 at the end of the bound. With
// nothing pending the bound is the id AFTER the group's last delivery, because
// the last delivered entry can itself be a strand: an acked-but-not-removed entry
// whose id is exactly last-delivered-id, which a bound without the +1 leaves in
// the stream for the life of the stream. It is run against both servers.
func assertSweepTrimsTheLastAckedEntry(t *testing.T, b *Buffer) {
	t.Helper()
	ctx := context.Background()

	for i := 1; i <= 2; i++ {
		if err := b.Push(ctx, rec(int64(i), 1)); err != nil {
			t.Fatalf("Push: %v", err)
		}
	}
	entries, err := b.Read(ctx, 10)
	if err != nil || len(entries) != 2 {
		t.Fatalf("Read = %d entries, %v; want 2", len(entries), err)
	}
	// Both delivered entries are stranded and nothing is left pending, so the
	// group's last-delivered id is the second entry's id.
	if err := b.rdb.XAck(ctx, b.Key(), b.group, IDs(entries)...).Err(); err != nil {
		t.Fatalf("XAck: %v", err)
	}
	if err := b.Push(ctx, rec(3, 1)); err != nil {
		t.Fatalf("Push: %v", err)
	}

	n, err := b.SweepStranded(ctx)
	if err != nil {
		t.Fatalf("SweepStranded: %v", err)
	}
	if n != 2 {
		t.Fatalf("SweepStranded removed %d entries, want 2 (both strands, including the one whose id is last-delivered-id)", n)
	}
	remaining, err := b.rdb.XRange(ctx, b.Key(), "-", "+").Result()
	if err != nil {
		t.Fatalf("XRANGE: %v", err)
	}
	if len(remaining) != 1 {
		t.Fatalf("XLEN after the sweep = %d, want 1 (only the unread entry)", len(remaining))
	}
	unread, err := b.Read(ctx, 10)
	if err != nil || len(unread) != 1 || unread[0].Record.DeliveryID != 3 {
		t.Fatalf("Read = %+v, %v; want only the unread entry (delivery 3)", unread, err)
	}
}

// TestSweepStrandedKeepsEveryPendingAndUnreadEntry runs the sweep's safety
// property on the package's own test Redis.
func TestSweepStrandedKeepsEveryPendingAndUnreadEntry(t *testing.T) {
	b, _ := newTestBuffer(t)
	assertSweepKeepsPendingAndUnread(t, b)
}

// TestSweepStrandedTrimsTheLastAckedEntryWhenNothingIsPending runs the bound's +1
// case on the package's own test Redis.
func TestSweepStrandedTrimsTheLastAckedEntryWhenNothingIsPending(t *testing.T) {
	b, _ := newTestBuffer(t)
	assertSweepTrimsTheLastAckedEntry(t, b)
}

// TestSweepStrandedAgainstTheRealServer runs both bound cases against the real
// Redis CI provides (PIGEON_TEST_REDIS_URL), and against miniredis when it is
// unset - broker_test.go's pattern, so the case is never skipped (and CI fails a
// run that skips). The sweep is the only destructive command in this package and
// its safety argument rests on server semantics rather than on this package's
// code: XINFO GROUPS reporting Pending and last-delivered-id, XPENDING's summary
// reporting the smallest pending id, and XTRIM MINID not consulting the pending
// list.
func TestSweepStrandedAgainstTheRealServer(t *testing.T) {
	t.Run("pending and unread survive", func(t *testing.T) {
		assertSweepKeepsPendingAndUnread(t, newSweepBuffer(t))
	})
	t.Run("the last acked entry with nothing pending is trimmed", func(t *testing.T) {
		assertSweepTrimsTheLastAckedEntry(t, newSweepBuffer(t))
	})
}

// newSweepBuffer builds a Buffer the way newTestBuffer does - on the real Redis
// CI provides when PIGEON_TEST_REDIS_URL is set, on a fresh in-process miniredis
// otherwise. On the shared server the stream gets a key of this test's own, so no
// other test's entries are in range of the sweep and none are left behind.
func newSweepBuffer(t *testing.T) *Buffer {
	t.Helper()
	key := DefaultKey
	if addr := os.Getenv("PIGEON_TEST_REDIS_URL"); addr != "" {
		rdb, err := clientFromURL(addr)
		if err != nil {
			t.Fatalf("clientFromURL(%q): %v", addr, err)
		}
		key = fmt.Sprintf("%s:test:%s:%d", DefaultKey, strings.ReplaceAll(t.Name(), "/", "-"), time.Now().UnixNano())
		b, err := New(rdb, Options{Key: key, Consumer: "test-flusher"})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() {
			if err := rdb.Del(context.Background(), key).Err(); err != nil {
				t.Logf("cleanup: DEL %s: %v", key, err)
			}
			_ = rdb.Close()
		})
		return b
	}
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	b, err := New(rdb, Options{Key: key, Consumer: "test-flusher"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return b
}

// TestSweepStrandedIsANoOpOnAnEmptyAndOnASteadyStateBuffer: the sweep must not
// issue a destructive command when there is nothing stranded, which is the
// steady state it runs in on every start.
func TestSweepStrandedIsANoOpOnAnEmptyAndOnASteadyStateBuffer(t *testing.T) {
	ctx := context.Background()
	b, _ := newTestBuffer(t)

	n, err := b.SweepStranded(ctx)
	if err != nil || n != 0 {
		t.Fatalf("SweepStranded on an empty buffer = %d, %v; want 0, nil", n, err)
	}

	for i := 1; i <= 2; i++ {
		if err := b.Push(ctx, rec(int64(i), 1)); err != nil {
			t.Fatalf("Push: %v", err)
		}
	}
	entries, err := b.Read(ctx, 10)
	if err != nil || len(entries) != 2 {
		t.Fatalf("Read = %d entries, %v; want 2", len(entries), err)
	}
	// Every entry is pending, so XLEN == pending and nothing can be stranded.
	n, err = b.SweepStranded(ctx)
	if err != nil || n != 0 {
		t.Fatalf("SweepStranded with every entry pending = %d, %v; want 0, nil", n, err)
	}
	if got := pendingCount(t, b); got != 2 {
		t.Fatalf("pending after the sweep = %d, want 2", got)
	}
}

// TestNextStreamID pins the bound's arithmetic: the sequence steps first, a full
// sequence steps the millisecond, and an id that does not parse yields a bound
// that trims nothing rather than a guess.
func TestNextStreamID(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"0-0", "0-1"},
		{"1793179919951-2", "1793179919951-3"},
		{"1793179919951-18446744073709551615", "1793179919952-0"},
		{"", "0-1"},
		{"garbage", "0-1"},
		{"12-", "0-1"},
		{"-1", "0-1"},
	} {
		if got := nextStreamID(tc.in); got != tc.want {
			t.Errorf("nextStreamID(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
