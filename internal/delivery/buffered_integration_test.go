package delivery

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/m7jay/pigeon/internal/attemptbuf"
	"github.com/m7jay/pigeon/internal/metrics"
	"github.com/m7jay/pigeon/internal/store"
)

// These integration tests prove S6's buffered path end to end against a real
// Postgres and a real Redis protocol (miniredis, which speaks the stream commands
// the flusher uses). They gate on PIGEON_TEST_DATABASE_URL exactly as the rest of
// the delivery suite does.

// TestBufferedAttemptsFlowFromWorkerThroughRedisToPostgres is D-1/D-2 end to end:
// the worker appends the failed attempt to the Redis buffer instead of writing a
// row, the delivery is re-queued with its counter advanced, and only the flusher
// makes the row visible — in ONE transaction, with the attempt timestamp the
// buffer carried rather than the flush time.
func TestBufferedAttemptsFlowFromWorkerThroughRedisToPostgres(t *testing.T) {
	st, admin := harness(t)

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	buf, err := attemptbuf.NewFromClient(rdb, attemptbuf.Options{Consumer: "itest"})
	if err != nil {
		t.Fatalf("attemptbuf.NewFromClient: %v", err)
	}

	m := metrics.New()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, "boom")
	}))
	defer srv.Close()

	_, _, deliveryID := seedHarness(t, st, admin, srv.URL)

	opts := testOptions(time.Second, time.Second, time.Minute)
	opts.AttemptLog = AttemptLogBuffered
	opts.AttemptBuffer = buf
	opts.Metrics = m
	opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	w := New(st, opts)

	ctx := context.Background()
	did, err := w.ProcessOne(ctx)
	if err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	if !did {
		t.Fatal("ProcessOne did no work; the delivery was not claimed")
	}

	// D-1: the evidence is in Redis, and NOT yet in Postgres.
	if depth, err := buf.Depth(ctx); err != nil || depth != 1 {
		t.Fatalf("buffer depth = %d, %v; want exactly 1 buffered attempt", depth, err)
	}
	if _, _, rows, _, _ := deliveryState(t, admin, deliveryID); rows != 0 {
		t.Fatalf("attempts table has %d rows before the flush; buffered evidence must not be written inline", rows)
	}
	if got := m.Counter(metrics.AttemptsBuffered); got != 1 {
		t.Fatalf("%s = %d, want 1", metrics.AttemptsBuffered, got)
	}

	// D-2: the retry was scheduled by the control write, and the delivery row's
	// counter advanced — the attempt number does not depend on the buffer.
	status, dbAttempts, _, _, _ := deliveryState(t, admin, deliveryID)
	if status != "pending" {
		t.Fatalf("delivery status after a retryable failure = %q, want pending", status)
	}
	if dbAttempts != 1 {
		t.Fatalf("delivery.attempts = %d, want 1 (the requeue advances the control counter)", dbAttempts)
	}

	// The flusher makes the row visible.
	fl, err := attemptbuf.NewFlusher(buf, st, attemptbuf.FlusherOptions{
		Batch: 2000, Interval: time.Second, Metrics: m,
	})
	if err != nil {
		t.Fatalf("NewFlusher: %v", err)
	}
	n, err := fl.FlushOnce(ctx)
	if err != nil {
		t.Fatalf("FlushOnce: %v", err)
	}
	if n != 1 {
		t.Fatalf("FlushOnce inserted %d rows, want 1", n)
	}

	attempt, respStatus, outcome, class, body, nextRetry := attemptRow(t, admin, deliveryID)
	if attempt != "1" {
		t.Fatalf("attempt = %q, want 1", attempt)
	}
	if !respStatus.Valid || respStatus.Int32 != 500 {
		t.Fatalf("response_status = %+v, want 500", respStatus)
	}
	if outcome != "retryable" || class != "http_retryable" {
		t.Fatalf("outcome/class = %q/%q, want retryable/http_retryable", outcome, class)
	}
	if body != "boom" {
		t.Fatalf("response_body = %q, want the captured body", body)
	}
	if !nextRetry.Valid {
		t.Fatal("next_retry_at is null on a retryable failure")
	}
	if depth, _ := buf.Depth(ctx); depth != 0 {
		t.Fatalf("buffer depth after the flush = %d, want 0 (the entry must be acknowledged)", depth)
	}
	if got := m.Counter(metrics.AttemptsFlushed); got != 1 {
		t.Fatalf("%s = %d, want 1", metrics.AttemptsFlushed, got)
	}
}

// TestBatchedEvidenceInsertIsOneIdempotentStatement is FR-815 at the store level:
// 2,000 rows go in as one call, a replay of the same batch inserts nothing, and
// the configured body cap is what truncates — not a constant.
func TestBatchedEvidenceInsertIsOneIdempotentStatement(t *testing.T) {
	st, admin := harness(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	_, _, deliveryID := seedHarness(t, st, admin, srv.URL)

	ctx := context.Background()
	const rows = 2000
	next := time.Now().Add(time.Minute)
	status := 500
	built := make([]store.AttemptRecord, 0, rows)
	for i := 1; i <= rows; i++ {
		built = append(built, store.AttemptRecord{
			DeliveryID:     deliveryID,
			Attempt:        i,
			Outcome:        "retryable",
			ErrorClass:     "http_retryable",
			ResponseStatus: &status,
			ResponseBody:   strings.Repeat("x", 4096), // longer than the cap
			DurationMs:     5,
			NextRetryAt:    &next,
			OccurredAt:     time.Now(),
		})
	}
	n, err := st.RecordAttempts(ctx, built)
	if err != nil {
		t.Fatalf("RecordAttempts: %v", err)
	}
	if n != rows {
		t.Fatalf("RecordAttempts inserted %d rows, want %d", n, rows)
	}
	if _, _, count, _, _ := deliveryState(t, admin, deliveryID); count != rows {
		t.Fatalf("attempts table holds %d rows, want %d", count, rows)
	}

	// The replay of the same batch is a no-op: the buffer's crash recovery
	// re-inserts the same (delivery_id, attempt) on purpose.
	again, err := st.RecordAttempts(ctx, built)
	if err != nil {
		t.Fatalf("replayed RecordAttempts: %v", err)
	}
	if again != 0 {
		t.Fatalf("replaying the batch inserted %d rows, want 0 (idempotent on (delivery_id, attempt))", again)
	}

	// SB-4's cap: a body longer than the cap is truncated to it.
	var bodyLen int
	if err := admin.QueryRow(ctx,
		`select length(response_body) from attempts where delivery_id=$1 and attempt=1`, deliveryID,
	).Scan(&bodyLen); err != nil {
		t.Fatalf("read body length: %v", err)
	}
	if bodyLen != 2048 {
		t.Fatalf("stored body length = %d, want the default 2 KB cap", bodyLen)
	}
}
