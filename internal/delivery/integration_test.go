package delivery

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m7jay/pigeon/internal/db"
	"github.com/m7jay/pigeon/internal/store"
)

// The delivery worker integration tests exercise the whole attempt end to end:
// a real Postgres for the deliveries/attempts tables and the SECURITY DEFINER
// functions in 0008, and an httptest server as the consumer endpoint. They gate
// on PIGEON_TEST_DATABASE_URL exactly as the store harness does, so "pass" proves
// the SQL, not a fake. Run with -p 1 and a disposable DB (see the local-test-db
// skill); the delivery package runs these alone, so there is no cross-package
// table stomping.

const (
	appRole     = "pigeon_app"
	appPassword = "pigeon_app"
)

// testPool is the harness: a real Postgres, migrated to the current schema
// (including 0008). The delivery path is system-scoped and reaches the tenant
// tables only through the definer functions, so the worker and the seeding both
// run against this one admin pool - there is no RLS-isolation property under test
// here, unlike the store package.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("PIGEON_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PIGEON_TEST_DATABASE_URL not set; skipping the delivery worker integration tests")
	}
	ctx := context.Background()
	admin, err := db.Connect(ctx, dsn, 5)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(admin.Close)
	if err := db.Migrate(ctx, admin, 30*time.Second); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return admin
}

func harness(t *testing.T) (*store.Store, *pgxpool.Pool) {
	t.Helper()
	admin := testPool(t)
	return store.New(admin), admin
}

func ptr[T any](v T) *T { return &v }

// testOptions builds worker options for the integration tests. The timeout is
// deliberately small (the tests pass it per case): the timeout classification
// path is identical whether the ceiling is 5 s (FR-802) or 150 ms, and a small
// value keeps the suite fast. BackoffBase/Max are likewise shrunk so a retry
// becomes claimable in milliseconds rather than seconds.
func testOptions(timeout, backoffBase, backoffMax time.Duration) Options {
	return Options{
		Workers:         1,
		Lease:           40 * time.Second,
		Timeout:         timeout,
		MaxBodyBytes:    2048,
		BackoffBase:     backoffBase,
		BackoffMax:      backoffMax,
		MaxRetries:      15,
		MaxConnsPerHost: 1,
		PollInterval:    time.Millisecond,
		FailBackoff:     time.Millisecond,
		UserAgent:       "pigeon/test",
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// seedHarness registers a full consumer stack pointing one verified endpoint at
// url, publishes a consumer-routed event, and fans it out, producing exactly one
// pending delivery with its lease set at fan-out (FR-813). It returns the event
// id, the endpoint's plaintext signing secret (FR-1001), and the delivery id.
func seedHarness(t *testing.T, st *store.Store, admin *pgxpool.Pool, url string) (eventID, secret string, deliveryID int64) {
	t.Helper()
	ctx := context.Background()
	if _, err := admin.Exec(ctx,
		`truncate table events, outbox, event_types, consumers, endpoints, subscriptions, deliveries cascade`); err != nil {
		t.Fatalf("reset routing tables: %v", err)
	}
	const slug = "delac"
	pid := "prd_" + slug
	if _, err := admin.Exec(ctx,
		`insert into producers (id, slug, name) values ($1,$2,$3) on conflict do nothing`,
		pid, slug, "Producer "+slug); err != nil {
		t.Fatalf("insert producer: %v", err)
	}
	sc := store.Scope{ProducerID: pid, Env: "test"}

	et, err := st.CreateEventType(ctx, sc, store.NewEventType{
		Name: "order.created", RoutingMode: store.RoutingConsumer,
		PayloadSchema: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("CreateEventType: %v", err)
	}
	con, err := st.CreateConsumer(ctx, sc, "school_1", "School 1")
	if err != nil {
		t.Fatalf("CreateConsumer: %v", err)
	}
	created, err := st.CreateEndpoint(ctx, sc, store.NewEndpoint{
		ConsumerID: con.ID, Name: "e1", URL: url, Internal: true, // internal: allow the loopback httptest URL
	})
	if err != nil {
		t.Fatalf("CreateEndpoint: %v", err)
	}
	if err := st.VerifyEndpoint(ctx, sc, created.Endpoint.ID, created.VerificationToken); err != nil {
		t.Fatalf("VerifyEndpoint: %v", err)
	}
	if _, err := st.CreateSubscription(ctx, sc, store.NewSubscription{
		EndpointID: created.Endpoint.ID, EventType: et.Name,
	}); err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	pub, err := st.PublishEvent(ctx, sc, store.PublishInput{
		EventType: et.Name, Payload: json.RawMessage(`{"id":"o1"}`), ConsumerRef: ptr(con.Ref),
	})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	if _, err := st.FanOutEvent(ctx, sc, pub.ID); err != nil {
		t.Fatalf("FanOutEvent: %v", err)
	}
	if err := admin.QueryRow(ctx, `select id from deliveries where event_id=$1 limit 1`, pub.ID).Scan(&deliveryID); err != nil {
		t.Fatalf("resolve delivery id: %v", err)
	}
	return pub.ID, created.Secret, deliveryID
}

// deliveryState reads the four FR-811 columns plus the count of attempt rows.
func deliveryState(t *testing.T, admin *pgxpool.Pool, deliveryID int64) (status string, dbAttempts, attemptRows int, deliveredAt, firstAttemptAt sql.NullTime) {
	t.Helper()
	ctx := context.Background()
	if err := admin.QueryRow(ctx,
		`select status, attempts, delivered_at, first_attempt_at from deliveries where id=$1`,
		deliveryID).Scan(&status, &dbAttempts, &deliveredAt, &firstAttemptAt); err != nil {
		t.Fatalf("read delivery: %v", err)
	}
	if err := admin.QueryRow(ctx,
		`select count(*) from attempts where delivery_id=$1`, deliveryID).Scan(&attemptRows); err != nil {
		t.Fatalf("count attempts: %v", err)
	}
	return
}

// attemptRow reads the single attempt row for a delivery (asserting there is one).
func attemptRow(t *testing.T, admin *pgxpool.Pool, deliveryID int64) (attempt string, respStatus sql.NullInt32, outcome, errorClass, body string, nextRetry sql.NullTime) {
	t.Helper()
	ctx := context.Background()
	err := admin.QueryRow(ctx,
		`select attempt, response_status, outcome, error_class, response_body, next_retry_at
		 from attempts where delivery_id=$1 order by attempt`, deliveryID).
		Scan(&attempt, &respStatus, &outcome, &errorClass, &body, &nextRetry)
	if err == pgx.ErrNoRows {
		t.Fatalf("expected an attempt row for delivery %d, found none", deliveryID)
	}
	if err != nil {
		t.Fatalf("read attempt: %v", err)
	}
	return
}

// verifySignature recomputes the Standard Webhooks signature the way a consumer
// would (FR-1001/FR-1002) and compares. It does not call the signer, and it
// re-derives the key itself (strip "whsec_", pad, standard base64) rather than
// through envelope.SecretBytes: a test that shares the signer's decode passes even
// when both sides are wrong about the key, which is exactly how the wrapper-as-key
// defect survived round 1.
func verifySignature(secret, id, ts, body, sig string) bool {
	enc := strings.TrimPrefix(secret, "whsec_")
	if enc == secret {
		return false
	}
	if pad := len(enc) % 4; pad != 0 {
		enc += strings.Repeat("=", 4-pad)
	}
	key, err := base64.StdEncoding.DecodeString(enc)
	if err != nil || len(key) == 0 {
		return false
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(id + "." + ts + "."))
	mac.Write([]byte(body))
	want := "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(sig), []byte(want))
}

// TestSuccessfulDeliveryWritesNoAttemptRow is AC-28: a delivery that succeeds on
// the first attempt leaves ZERO rows in attempts, and the four columns on the
// delivery row (status, attempts, first_attempt_at, delivered_at) carry it
// (FR-811). It also proves the wire contract end to end: the right headers, the
// event id as webhook-id, and a signature the consumer can verify against the
// endpoint's own secret.
func TestSuccessfulDeliveryWritesNoAttemptRow(t *testing.T) {
	st, admin := harness(t)

	var (
		gotID, gotTS, gotSig, gotCT, gotUA string
		gotBody                            []byte
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotID = r.Header.Get("Webhook-Id")
		gotTS = r.Header.Get("Webhook-Timestamp")
		gotSig = r.Header.Get("Webhook-Signature")
		gotCT = r.Header.Get("Content-Type")
		gotUA = r.Header.Get("User-Agent")
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "accepted")
	}))
	defer srv.Close()

	eventID, secret, deliveryID := seedHarness(t, st, admin, srv.URL)

	w := New(st, testOptions(5*time.Second, 30*time.Second, time.Minute))
	did, err := w.ProcessOne(context.Background())
	if err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	if !did {
		t.Fatal("ProcessOne did no work; the delivery was not claimed")
	}

	status, dbAttempts, attemptRows, deliveredAt, firstAttemptAt := deliveryState(t, admin, deliveryID)
	if status != "delivered" {
		t.Fatalf("delivery status = %q, want delivered", status)
	}
	if dbAttempts != 1 {
		t.Fatalf("delivery.attempts = %d, want 1", dbAttempts)
	}
	if attemptRows != 0 {
		t.Fatalf("attempts table has %d rows, want 0 for a successful first attempt (AC-28, FR-811)", attemptRows)
	}
	if !deliveredAt.Valid {
		t.Fatal("deliveries.delivered_at is null; AC-28 requires it populated")
	}
	if !firstAttemptAt.Valid {
		t.Fatal("deliveries.first_attempt_at is null; AC-28 requires it populated")
	}

	// The request that was actually sent.
	if gotID != eventID {
		t.Fatalf("Webhook-Id = %q, want event id %q (FR-1001)", gotID, eventID)
	}
	if gotCT != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json (FR-801)", gotCT)
	}
	if gotUA != "pigeon/test" {
		t.Fatalf("User-Agent = %q, want pigeon/test (FR-810)", gotUA)
	}
	if len(gotSig) < 4 || gotSig[:3] != "v1," {
		t.Fatalf("Webhook-Signature = %q, want v1, prefix (FR-1001)", gotSig)
	}
	if !verifySignature(secret, gotID, gotTS, string(gotBody), gotSig) {
		t.Fatal("Webhook-Signature does not verify against the endpoint secret over <id>.<ts>.<body> (FR-1002)")
	}
	// The timestamp is integer Unix seconds of the attempt, not the event's
	// created_at (FR-1001/FR-1003): a conforming consumer compares this header to
	// its own clock, so an event-time stamp would already be outside the tolerance
	// window by attempt 4 (§9's schedule).
	ts, err := strconv.ParseInt(gotTS, 10, 64)
	if err != nil {
		t.Fatalf("Webhook-Timestamp = %q, want an integer Unix-seconds string (FR-1001)", gotTS)
	}
	if age := time.Since(time.Unix(ts, 0)); age < 0 || age > 30*time.Second {
		t.Fatalf("Webhook-Timestamp = %s is %s from now; want the attempt time (FR-1001, FR-1003)", gotTS, age)
	}
	// FR-1001: the minted secret is "whsec_" + STANDARD base64. A spec consumer
	// decodes it with the standard alphabet, which silently drops the URL-safe
	// "-"/"_" rather than failing, so a URL-safe secret would derive a short key
	// and verify for roughly one endpoint in four. The mint is unpadded; the
	// verifier (and this assertion) restores the padding before decoding.
	enc := strings.TrimPrefix(secret, "whsec_")
	if pad := len(enc) % 4; pad != 0 {
		enc += strings.Repeat("=", 4-pad)
	}
	raw, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		t.Fatalf("endpoint secret %q is not standard base64: %v", secret, err)
	}
	if len(raw) != 32 {
		t.Fatalf("endpoint secret decodes to %d bytes, want 32", len(raw))
	}
}

// TestTerminalFailureOn404 is AC-05: a 404 fails terminal on the first attempt.
// One attempt row is written (FR-804: status, body, class), the delivery ends
// failed, and there is no retry.
func TestTerminalFailureOn404(t *testing.T) {
	st, admin := harness(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, "no such hook")
	}))
	defer srv.Close()

	_, _, deliveryID := seedHarness(t, st, admin, srv.URL)

	w := New(st, testOptions(5*time.Second, 30*time.Second, time.Minute))
	if _, err := w.ProcessOne(context.Background()); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}

	status, dbAttempts, attemptRows, deliveredAt, _ := deliveryState(t, admin, deliveryID)
	if status != "failed_terminal" {
		// 0013 renames the terminal status to failed_terminal: I-4's vocabulary
		// ("terminal only in delivered, failed_terminal, cancelled") and AC-36
		// both name that value, and 0007's `failed` was the only other spelling.
		t.Fatalf("delivery status = %q, want failed_terminal (AC-05, I-4)", status)
	}
	if dbAttempts != 1 {
		t.Fatalf("delivery.attempts = %d, want 1", dbAttempts)
	}
	if attemptRows != 1 {
		t.Fatalf("attempts table has %d rows, want 1 for the failed attempt (FR-811)", attemptRows)
	}
	if deliveredAt.Valid {
		t.Fatal("deliveries.delivered_at is set on a failed delivery")
	}
	attempt, respStatus, outcome, errorClass, body, nextRetry := attemptRow(t, admin, deliveryID)
	if attempt != "1" {
		t.Fatalf("attempt number = %q, want 1", attempt)
	}
	if !respStatus.Valid || respStatus.Int32 != int32(http.StatusNotFound) {
		t.Fatalf("response_status = %v, want 404 (FR-804)", respStatus)
	}
	if outcome != "terminal" {
		t.Fatalf("outcome = %q, want terminal (FR-806)", outcome)
	}
	if errorClass != classHTTPTerminal {
		t.Fatalf("error_class = %q, want %q", errorClass, classHTTPTerminal)
	}
	if body != "no such hook" {
		t.Fatalf("response_body = %q, want the captured body (FR-804)", body)
	}
	if nextRetry.Valid {
		t.Fatal("next_retry_at is set on a terminal failure; it must be null")
	}
}

// TestRetryThenDelivered is AC-04: 500, 500, 200 -> three attempts, then
// delivered. The two failed attempts each leave a retryable attempt row; the
// successful third writes none (FR-811), so delivery.attempts is 3 while the
// attempts table holds 2 rows. Each retry schedules a backoff (next_retry_at).
func TestRetryThenDelivered(t *testing.T) {
	st, admin := harness(t)

	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch atomic.AddInt32(&calls, 1) {
		case 1, 2:
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, "boom")
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	_, _, deliveryID := seedHarness(t, st, admin, srv.URL)

	// A tiny backoff so the re-queued delivery becomes claimable in milliseconds;
	// the exponential shape itself is proven by the backoff unit tests.
	w := New(st, testOptions(5*time.Second, time.Millisecond, 4*time.Millisecond))

	ctx := context.Background()
	delivered := false
	for i := 0; i < 40; i++ {
		did, err := w.ProcessOne(ctx)
		if err != nil {
			t.Fatalf("ProcessOne: %v", err)
		}
		if did {
			if status, _, _, _, _ := deliveryState(t, admin, deliveryID); status == "delivered" {
				delivered = true
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !delivered {
		t.Fatal("delivery never reached delivered (AC-04); it stalled in pending")
	}

	status, dbAttempts, attemptRows, _, _ := deliveryState(t, admin, deliveryID)
	if status != "delivered" {
		t.Fatalf("delivery status = %q, want delivered", status)
	}
	if dbAttempts != 3 {
		t.Fatalf("delivery.attempts = %d, want 3 (AC-04)", dbAttempts)
	}
	if attemptRows != 2 {
		t.Fatalf("attempts table has %d rows, want 2 (the two 500s; the success writes none)", attemptRows)
	}
	// Both failed attempts are retryable, carry the 500, and scheduled a retry.
	for _, wantAttempt := range []string{"1", "2"} {
		attempt, respStatus, outcome, errorClass, _, nextRetry := attemptRow(t, admin, deliveryID)
		_ = wantAttempt
		if !respStatus.Valid || respStatus.Int32 != int32(http.StatusInternalServerError) {
			t.Fatalf("attempt %s response_status = %v, want 500", attempt, respStatus)
		}
		if outcome != "retryable" || errorClass != classHTTPRetryable {
			t.Fatalf("attempt %s = (%q,%q), want (retryable,%q)", attempt, outcome, errorClass, classHTTPRetryable)
		}
		if !nextRetry.Valid {
			t.Fatalf("attempt %s next_retry_at is null; a retry must schedule one", attempt)
		}
	}
}

// TestTimeoutRecordedAndRetried is AC-06: the endpoint hangs, the attempt times
// out at the configured ceiling (well under the peer's hang), it is recorded as a
// timeout and retried. The attempt row has a null status (no response) and the
// whole call returns in well under the peer's hang time, proving the timeout
// bound, not a stuck goroutine.
func TestTimeoutRecordedAndRetried(t *testing.T) {
	st, admin := harness(t)

	// The endpoint hangs indefinitely; the worker must give up at its per-attempt
	// timeout and abandon the request. The handler blocks on release so the test,
	// not a fixed sleep, controls when the in-flight request finishes - keeping
	// Close prompt.
	release := make(chan struct{})
	var once sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	defer func() { once.Do(func() { close(release) }); srv.Close() }()

	_, _, deliveryID := seedHarness(t, st, admin, srv.URL)

	w := New(st, testOptions(150*time.Millisecond, time.Millisecond, 4*time.Millisecond))

	ctx := context.Background()
	start := time.Now()
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 3*time.Second {
		t.Fatalf("ProcessOne took %v; the 150 ms timeout did not bound the hung peer (AC-06)", elapsed)
	}

	// Retryable: the delivery is still pending, one timeout attempt row recorded.
	status, _, attemptRows, _, _ := deliveryState(t, admin, deliveryID)
	if status != "pending" {
		t.Fatalf("delivery status = %q, want pending (timeout is retryable, AC-06)", status)
	}
	if attemptRows != 1 {
		t.Fatalf("attempts table has %d rows, want 1 (the timeout)", attemptRows)
	}
	_, respStatus, outcome, errorClass, _, nextRetry := attemptRow(t, admin, deliveryID)
	if respStatus.Valid {
		t.Fatalf("response_status = %v, want empty for a timeout (no response, FR-804)", respStatus)
	}
	if outcome != "retryable" || errorClass != classTimeout {
		t.Fatalf("timeout classified as (%q,%q), want (retryable,%q)", outcome, errorClass, classTimeout)
	}
	if !nextRetry.Valid {
		t.Fatal("timeout retry next_retry_at is null; a retry must schedule one")
	}
}

// TestLeaseExpiryReclaimsWithoutDuplicateAttempts is AC-07 at the data layer: a
// worker is killed mid-attempt after it wrote attempt 1 but before it advanced
// the delivery. The lease lapses, the row becomes claimable again (FR-809, D-6),
// and the next attempt number is derived from the existing rows so it never
// reuses 1 (no duplicate attempt numbers). The unique (delivery_id, attempt)
// constraint makes a double-write of the same number a no-op.
func TestLeaseExpiryReclaimsWithoutDuplicateAttempts(t *testing.T) {
	st, admin := harness(t)
	ctx := context.Background()

	_, _, deliveryID := seedHarness(t, st, admin, "http://127.0.0.1:1/") // URL never contacted in this test

	// Claim, then simulate a crash: write attempt 1 as a partial, durable record.
	if _, got, err := st.ClaimDelivery(ctx, 40*time.Second, 72*time.Hour, store.AttemptClassAny); err != nil {
		t.Fatalf("first claim: %v", err)
	} else if !got {
		t.Fatal("first claim found nothing to lease")
	}
	if err := st.RecordAttempt(ctx, store.AttemptRecord{
		DeliveryID: deliveryID, Attempt: 1, Outcome: "retryable",
		ErrorClass: classDialError, DurationMs: 5,
	}); err != nil {
		t.Fatalf("record attempt 1 (the crash's partial write): %v", err)
	}

	// The lease lapses (a crashed worker let it go); the row is claimable again.
	if _, err := admin.Exec(ctx,
		`update deliveries set lease_expires_at = now() - interval '1 second' where id=$1`, deliveryID); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	d2, got, err := st.ClaimDelivery(ctx, 40*time.Second, 72*time.Hour, store.AttemptClassAny)
	if err != nil {
		t.Fatalf("re-claim: %v", err)
	}
	if !got {
		t.Fatal("expired lease did not make the delivery claimable again (FR-809, D-6)")
	}
	if d2.DeliveryID != deliveryID {
		t.Fatalf("re-claim returned delivery %d, want the same row %d", d2.DeliveryID, deliveryID)
	}

	// The next attempt number continues from the rows, never reusing 1 (AC-07).
	n, err := st.NextAttempt(ctx, deliveryID)
	if err != nil {
		t.Fatalf("NextAttempt: %v", err)
	}
	if n != 2 {
		t.Fatalf("NextAttempt = %d, want 2 (max(existing attempts)+1, no reuse)", n)
	}
	if err := st.RecordAttempt(ctx, store.AttemptRecord{
		DeliveryID: deliveryID, Attempt: n, Outcome: "terminal",
		ErrorClass: classHTTPTerminal, ResponseStatus: ptr(410), DurationMs: 3,
	}); err != nil {
		t.Fatalf("record attempt %d: %v", n, err)
	}

	// No duplicate attempt numbers exist.
	var rows int
	if err := admin.QueryRow(ctx, `select count(*) from attempts where delivery_id=$1`, deliveryID).Scan(&rows); err != nil {
		t.Fatalf("count attempts: %v", err)
	}
	if rows != 2 {
		t.Fatalf("attempts rows = %d, want 2 (numbers 1 and 2, no duplicate)", rows)
	}
	var distinct int
	if err := admin.QueryRow(ctx,
		`select count(distinct attempt) from attempts where delivery_id=$1`, deliveryID).Scan(&distinct); err != nil {
		t.Fatalf("count distinct attempts: %v", err)
	}
	if distinct != 2 {
		t.Fatalf("distinct attempt numbers = %d, want 2; a duplicate was handed out (AC-07)", distinct)
	}

	// Idempotency: re-recording attempt 1 is a no-op, not a duplicate.
	if err := st.RecordAttempt(ctx, store.AttemptRecord{
		DeliveryID: deliveryID, Attempt: 1, Outcome: "retryable",
		ErrorClass: classDialError, DurationMs: 99,
	}); err != nil {
		t.Fatalf("idempotent re-record: %v", err)
	}
	if err := admin.QueryRow(ctx, `select count(*) from attempts where delivery_id=$1`, deliveryID).Scan(&rows); err != nil {
		t.Fatalf("count attempts after re-record: %v", err)
	}
	if rows != 2 {
		t.Fatalf("attempts rows = %d after re-recording attempt 1, want 2 (on-conflict no-op)", rows)
	}
}

// seedAnotherDelivery publishes and fans out one more event on the stack
// seedHarness already built, so a test can assert over more than one pending
// delivery without re-seeding. The scope, event type and consumer ref are the
// fixed ones seedHarness creates.
func seedAnotherDelivery(t *testing.T, st *store.Store, admin *pgxpool.Pool, payload string) int64 {
	t.Helper()
	ctx := context.Background()
	sc := store.Scope{ProducerID: "prd_delac", Env: "test"}
	pub, err := st.PublishEvent(ctx, sc, store.PublishInput{
		EventType: "order.created", Payload: json.RawMessage(payload), ConsumerRef: ptr("school_1"),
	})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	if _, err := st.FanOutEvent(ctx, sc, pub.ID); err != nil {
		t.Fatalf("FanOutEvent: %v", err)
	}
	var id int64
	if err := admin.QueryRow(ctx, `select id from deliveries where event_id=$1 limit 1`, pub.ID).Scan(&id); err != nil {
		t.Fatalf("resolve delivery id: %v", err)
	}
	return id
}

// ageLease moves a delivery's lease the given interval into the past.
func ageLease(t *testing.T, admin *pgxpool.Pool, deliveryID int64, age string) {
	t.Helper()
	if _, err := admin.Exec(context.Background(),
		`update deliveries set lease_expires_at = now() - $2::interval where id=$1`, deliveryID, age); err != nil {
		t.Fatalf("age lease: %v", err)
	}
}

// TestSweeperRequeuesExpiredLease is D-6: the sweeper re-queues ANY delivery whose
// lease has lapsed. "Any" is literal — there is no lookback bound — because a lease
// that lapsed while no sweep was running (a leadership move, a stalled tick) is
// exactly the case the requirement exists for: leaving it to the worker's
// opportunistic claim loop would make the sweeper's guarantee conditional. A
// retry's backoff lease is in the future, so it must never be clobbered.
func TestSweeperRequeuesExpiredLease(t *testing.T) {
	st, admin := harness(t)
	ctx := context.Background()

	// Just lapsed.
	_, _, d1 := seedHarness(t, st, admin, "http://127.0.0.1:1/")
	ageLease(t, admin, d1, "5 seconds")
	// Long lapsed: re-queued too. This is the row the old lookback bound skipped.
	d2 := seedAnotherDelivery(t, st, admin, `{"id":"o2"}`)
	ageLease(t, admin, d2, "2 hours")
	// Waiting out a backoff: its lease is in the future, so the sweeper leaves it.
	d3 := seedAnotherDelivery(t, st, admin, `{"id":"o3"}`)
	if _, err := admin.Exec(ctx,
		`update deliveries set lease_expires_at = now() + interval '1 hour' where id=$1`, d3); err != nil {
		t.Fatalf("set future lease: %v", err)
	}

	n, err := st.SweeperRequeue(ctx)
	if err != nil {
		t.Fatalf("SweeperRequeue: %v", err)
	}
	if n != 2 {
		t.Fatalf("SweeperRequeue reclaimed %d, want 2 (D-6 re-queues any lapsed lease)", n)
	}
	// A swept row is immediately claimable, with no waiting for the old lease.
	if _, got, err := st.ClaimDelivery(ctx, 40*time.Second, 72*time.Hour, store.AttemptClassAny); err != nil {
		t.Fatalf("claim after sweep: %v", err)
	} else if !got {
		t.Fatal("a swept delivery was not claimable")
	}
	// The backoff row is untouched: a sweep must not turn every pending retry into
	// an immediate attempt.
	var lease sql.NullTime
	if err := admin.QueryRow(ctx, `select lease_expires_at from deliveries where id=$1`, d3).Scan(&lease); err != nil {
		t.Fatalf("read backoff lease: %v", err)
	}
	if !lease.Valid || time.Until(lease.Time) < 30*time.Minute {
		t.Fatalf("lease = %v; the sweeper clobbered a future backoff lease", lease)
	}
}

// TestShutdownLeavesInFlightDeliveryPending is AC-21: a SIGTERM during an
// in-flight attempt drains without loss. The worker cancels the attempt, writes
// NOTHING, and leaves the delivery pending under its lease so it is retried after
// restart - zero lost attempts, zero half-written rows.
func TestShutdownLeavesInFlightDeliveryPending(t *testing.T) {
	st, admin := harness(t)

	received := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(received) // the request is now in flight
		<-release       // hold it until the test cancels the attempt
		_, _ = io.WriteString(w, "late")
	}))
	defer func() { once.Do(func() { close(release) }); srv.Close() }()

	_, _, deliveryID := seedHarness(t, st, admin, srv.URL)

	w := New(st, testOptions(30*time.Second, 30*time.Second, time.Minute))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_, _ = w.ProcessOne(ctx)
		close(done)
	}()

	select {
	case <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("the delivery request never reached the endpoint")
	}
	cancel() // SIGTERM: the parent context is cancelled mid-attempt
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the worker did not unwind after cancellation (AC-21)")
	}

	status, _, attemptRows, _, _ := deliveryState(t, admin, deliveryID)
	if status != "pending" {
		t.Fatalf("delivery status = %q, want pending (an aborted attempt writes nothing, AC-21)", status)
	}
	if attemptRows != 0 {
		t.Fatalf("attempts table has %d rows after a cancelled attempt, want 0 (no partial write)", attemptRows)
	}
}

// TestSweeperLeadershipLockIsExclusive is D-7: exactly one sweeper is active per
// deployment. The sweeper takes a fixed-key Postgres advisory lock on a dedicated
// connection; a second competitor for the same key is refused, so a second
// replica stays a standby. This proves the exclusivity of the lock key itself -
// the mechanism the sweeper's acquireLeadership relies on.
func TestSweeperLeadershipLockIsExclusive(t *testing.T) {
	_, admin := harness(t)
	ctx := context.Background()

	c1, err := admin.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire conn 1: %v", err)
	}
	c2, err := admin.Acquire(ctx)
	if err != nil {
		c1.Release()
		t.Fatalf("acquire conn 2: %v", err)
	}
	defer c1.Release()
	defer c2.Release()

	var first, second bool
	if err := c1.QueryRow(ctx, `select pg_try_advisory_lock($1)`, sweeperLockID).Scan(&first); err != nil {
		t.Fatalf("first lock: %v", err)
	}
	if err := c2.QueryRow(ctx, `select pg_try_advisory_lock($1)`, sweeperLockID).Scan(&second); err != nil {
		t.Fatalf("second lock: %v", err)
	}
	if !first {
		t.Fatal("the first sweeper did not acquire the leadership lock")
	}
	if second {
		t.Fatal("a second sweeper acquired the leadership lock while the first holds it; D-7 is violated")
	}

	// The holder releases the lock (session-level pg_advisory_unlock); a fresh
	// connection can then win it, so the lock is not leaked.
	if _, err := c1.Exec(ctx, `select pg_advisory_unlock($1)`, sweeperLockID); err != nil {
		t.Fatalf("release lock: %v", err)
	}
	c1.Release()

	c3, err := admin.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire conn 3: %v", err)
	}
	defer c3.Release()
	var after bool
	if err := c3.QueryRow(ctx, `select pg_try_advisory_lock($1)`, sweeperLockID).Scan(&after); err != nil {
		t.Fatalf("third lock: %v", err)
	}
	if !after {
		t.Fatal("the lock was not free after the holder released it")
	}
}
