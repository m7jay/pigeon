package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/m7jay/pigeon/internal/store"
)

// This file is S7's acceptance suite: the delivery list and its cursor
// (FR-1101), the ordered attempt trail (FR-1102, FR-1103), the event-level
// rollup and the trail that is never empty (FR-1109, FR-1110, FR-1111), replay
// (AC-12, AC-13, FR-1106..FR-1108), consumer scoping (FR-1105) and the dedupe
// answer (FR-903).
//
// The fixtures go through the real paths - the API for everything a producer
// does, the store's fan-out and completion methods for the two things a *worker*
// does (S3's fan-out and S4's completion) - so a test that passes here is a
// statement about the API and the SQL, not about a stub.

// --- fixtures -------------------------------------------------------------

// s7Scope is a tenant's live plane, for the store calls that stand in for a
// worker.
func s7Scope(tn *tenant) store.Scope {
	return store.Scope{ProducerID: tn.ID, Env: store.EnvLive}
}

// seedInspectTenant builds the smallest tenant that can actually receive an
// event. seedTenant's endpoint is unverified and fan-out skips an unverified
// live endpoint (FR-403), so this registers a consumer-routed type, verifies the
// endpoint and subscribes it.
func (e *env) seedInspectTenant(t *testing.T, slug string) *tenant {
	t.Helper()
	tn := e.seedTenant(t, slug)
	tn.EventTypeName = strings.ReplaceAll(slug, "-", "") + ".order"
	code, raw := e.do(http.MethodPost, "/v1/event-types", tn.KeyPlain, map[string]any{
		"name":           tn.EventTypeName,
		"routing_mode":   "consumer",
		"payload_schema": map[string]any{"type": "object"},
	})
	mustStatus(t, code, raw, http.StatusCreated)
	e.verifyEndpoint(t, tn, tn.EndpointID, tn.EndpointVerifyToken)
	return tn
}

// addEndpoint registers a second endpoint for the same consumer, verifies it and
// subscribes it to the tenant's type, so one event fans out to two deliveries
// (which is how the rollup's "partially failed" case is built).
func (e *env) addEndpoint(t *testing.T, tn *tenant, name string) string {
	t.Helper()
	code, raw := e.do(http.MethodPost, "/v1/producer/consumers/"+tn.ConsumerID+"/endpoints?env=live",
		tn.OwnerToken, map[string]string{"name": name, "url": "https://8.8.4.4/hook"})
	mustStatus(t, code, raw, http.StatusCreated)
	created := decode[endpointCreatedJSON](t, raw)
	e.verifyEndpoint(t, tn, created.ID, created.VerificationToken)
	return created.ID
}

// verifyEndpoint answers the ownership challenge (FR-403) and subscribes the
// endpoint to the tenant's event type (FR-501), over the API.
func (e *env) verifyEndpoint(t *testing.T, tn *tenant, endpointID, token string) {
	t.Helper()
	e.verifyEndpointFor(t, tn, endpointID, token, tn.EventTypeName)
}

// verifyEndpointFor is verifyEndpoint for a named type, so a fixture can hold
// two types at once (the cross-producer tenant keeps a broadcast type for the
// ingest cases and a consumer-routed one for the delivery cases).
func (e *env) verifyEndpointFor(t *testing.T, tn *tenant, endpointID, token, eventType string) {
	t.Helper()
	code, raw := e.do(http.MethodPost,
		"/v1/producer/consumers/"+tn.ConsumerID+"/endpoints/"+endpointID+"/verify?env=live",
		tn.OwnerToken, map[string]string{"token": token})
	mustStatus(t, code, raw, http.StatusNoContent)
	code, raw = e.do(http.MethodPost, "/v1/producer/endpoints/"+endpointID+"/subscriptions?env=live",
		tn.OwnerToken, map[string]string{"event_type": eventType})
	mustStatus(t, code, raw, http.StatusCreated)
}

// publishEvent publishes one consumer-routed event for the tenant (FR-601).
func (e *env) publishEvent(t *testing.T, tn *tenant, consumerRef string) string {
	t.Helper()
	return e.publishTypedEvent(t, tn, tn.EventTypeName, consumerRef)
}

// publishTypedEvent publishes against a named type.
func (e *env) publishTypedEvent(t *testing.T, tn *tenant, eventType, consumerRef string) string {
	t.Helper()
	code, raw := e.do(http.MethodPost, "/v1/events", tn.KeyPlain, map[string]any{
		"event_type":   eventType,
		"consumer_ref": consumerRef,
		"payload":      map[string]any{"note": "s7"},
	})
	mustStatus(t, code, raw, http.StatusAccepted)
	return decode[publishResponse](t, raw).ID
}

// fanOutRaw runs S3's fan-out and returns the worker's own result, for the
// fixtures whose point is that fan-out recorded a skip or an event-level reason
// (FR-509, §7.5a).
func (e *env) fanOutRaw(t *testing.T, tn *tenant, eventID string) store.FanOutResult {
	t.Helper()
	res, err := e.st.FanOutEvent(context.Background(), s7Scope(tn), eventID)
	if err != nil {
		t.Fatalf("fan out %s: %v", eventID, err)
	}
	return res
}

// fanOut runs S3's fan-out for one event and returns the delivery ids it
// created, in id order.
func (e *env) fanOut(t *testing.T, tn *tenant, eventID string) []int64 {
	t.Helper()
	res := e.fanOutRaw(t, tn, eventID)
	if res.Idle || len(res.Skipped) > 0 || res.EventReason != "" {
		t.Fatalf("fan-out recorded idle=%v skipped=%v reason=%q; the fixture expects a routable event",
			res.Idle, res.Skipped, res.EventReason)
	}
	rows, err := e.admin.Query(context.Background(),
		`select id from deliveries where event_id = $1 order by id`, eventID)
	if err != nil {
		t.Fatalf("read deliveries of %s: %v", eventID, err)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		t.Fatalf("fan-out created no delivery for %s", eventID)
	}
	return ids
}

// deliver is one event fanning out to one delivery, in whatever state the caller
// asks for next.
func (e *env) deliver(t *testing.T, tn *tenant) int64 {
	t.Helper()
	return e.fanOut(t, tn, e.publishEvent(t, tn, tn.ConsumerRef))[0]
}

// completeDelivery runs S4's successful path (FR-811): the four columns on the
// delivery row and NO attempt row (AC-28), which is exactly the shape FR-1110
// and AC-32 are about.
func (e *env) completeDelivery(t *testing.T, id, attempts int64, started, done time.Time) {
	t.Helper()
	ok, err := e.st.CompleteDelivery(context.Background(), id, int(attempts), started, done)
	if err != nil || !ok {
		t.Fatalf("complete delivery %d: ok=%v err=%v", id, ok, err)
	}
}

// failDelivery runs S4's terminal path: one failed attempt row (FR-804) and a
// delivery left `failed` (FR-806).
func (e *env) failDelivery(t *testing.T, id int64, status int, attempts int, started time.Time) {
	t.Helper()
	for n := 1; n <= attempts; n++ {
		outcome, class := "retryable", "http_retryable"
		var next *time.Time
		if n == attempts {
			outcome, class = "terminal", "http_terminal"
		} else {
			at := started.Add(time.Duration(n) * 30 * time.Second)
			next = &at
		}
		err := e.st.RecordAttempt(context.Background(), store.AttemptRecord{
			DeliveryID: id, Attempt: n, Outcome: outcome, ErrorClass: class,
			ResponseStatus: &status, ResponseBody: "boom", DurationMs: 7, NextRetryAt: next,
		})
		if err != nil {
			t.Fatalf("record attempt %d of delivery %d: %v", n, id, err)
		}
	}
	ok, err := e.st.FinalizeDelivery(context.Background(), id, attempts, started)
	if err != nil || !ok {
		t.Fatalf("finalize delivery %d: ok=%v err=%v", id, ok, err)
	}
}

// countRows is a fixture read as the superuser: the tests assert on what the
// database holds, not only on what the API said.
func (e *env) countRows(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count (%s): %v", query, err)
	}
	return n
}

// bulkDeliveries inserts n delivered deliveries for one tenant and endpoint,
// each on its own event (the (event_id, endpoint_id) pair is unique), created at
// the given time.
//
// It is SQL rather than fan-out on purpose: AC-13 is a statement about the read
// path's bound, and building 5,000 pairs through publish + fan-out would measure
// the ingest path instead. The rows are still real rows in the real tables, so
// the selection the API bounds is the selection a support engineer would see.
func (e *env) bulkDeliveries(t *testing.T, tn *tenant, endpointID, tag string, n int, createdAt time.Time) {
	t.Helper()
	ctx := context.Background()
	_, err := e.admin.Exec(ctx, `
		insert into events (id, producer_id, env, event_type, payload_version, routing_mode,
		                    event_class, consumer_ref, occurred_at, payload)
		select $5 || i::text, $1, 'live', $2, 1, 'consumer', 'normal', $3, $4, '{}'::jsonb
		from generate_series(1, $6::int) i`,
		tn.ID, tn.EventTypeName, tn.ConsumerRef, createdAt, tag, n)
	if err != nil {
		t.Fatalf("bulk events (%s): %v", tag, err)
	}
	_, err = e.admin.Exec(ctx, `
		insert into deliveries (event_id, endpoint_id, producer_id, env, consumer_id, status, created_at)
		select $5 || i::text, $1, $2, 'live', $3, 'delivered', $4
		from generate_series(1, $6::int) i`,
		endpointID, tn.ID, tn.ConsumerID, createdAt, tag, n)
	if err != nil {
		t.Fatalf("bulk deliveries (%s): %v", tag, err)
	}
}

// --- wire shapes the tests decode -----------------------------------------

type deliveryListJSON struct {
	Deliveries []deliveryJSON `json:"deliveries"`
	NextCursor *int64         `json:"next_cursor"`
}

type replayJSONBody struct {
	Matched          int     `json:"matched"`
	Created          int     `json:"created"`
	DeliveryIDs      []int64 `json:"delivery_ids"`
	Repeated         bool    `json:"repeated"`
	ReleasePerSecond int     `json:"release_per_second"`
	DryRun           bool    `json:"dry_run"`
}

type eventViewJSONBody struct {
	Event      eventJSON      `json:"event"`
	Rollup     string         `json:"rollup"`
	Deliveries []deliveryJSON `json:"deliveries"`
}

type dedupeJSONBody struct {
	EndpointID      string  `json:"endpoint_id"`
	EventID         string  `json:"event_id"`
	Duplicate       bool    `json:"duplicate"`
	FirstDeliveryID int64   `json:"first_delivery_id"`
	FirstSeenAt     *string `json:"first_seen_at"`
	Deliveries      int     `json:"deliveries"`
}

func (e *env) listDeliveries(t *testing.T, token, query string) deliveryListJSON {
	t.Helper()
	code, raw := e.do(http.MethodGet, "/v1/producer/deliveries?env=live"+query, token, nil)
	mustStatus(t, code, raw, http.StatusOK)
	return decode[deliveryListJSON](t, raw)
}

func (e *env) eventView(t *testing.T, token, eventID string) eventViewJSONBody {
	t.Helper()
	code, raw := e.do(http.MethodGet, "/v1/producer/events/"+eventID+"?env=live", token, nil)
	mustStatus(t, code, raw, http.StatusOK)
	return decode[eventViewJSONBody](t, raw)
}

// --- FR-1101: the list, its filters and its cursor ------------------------

// TestDeliveryListFiltersAndCursorPagination is FR-1101: filters by endpoint,
// event type, status and time range, paginated by cursor.
func TestDeliveryListFiltersAndCursorPagination(t *testing.T) {
	e := setup(t)
	tn := e.seedInspectTenant(t, "s7-list")

	delivered := e.deliver(t, tn)
	e.completeDelivery(t, delivered, 1, time.Now().Add(-2*time.Second), time.Now().Add(-time.Second))
	failed := e.deliver(t, tn)
	e.failDelivery(t, failed, 500, 1, time.Now().Add(-time.Second))
	pending := e.deliver(t, tn)

	all := e.listDeliveries(t, tn.OwnerToken, "")
	if len(all.Deliveries) != 3 {
		t.Fatalf("listed %d deliveries, want 3", len(all.Deliveries))
	}
	if all.NextCursor != nil {
		t.Fatalf("a page shorter than the limit reported next_cursor %v", *all.NextCursor)
	}
	if all.Deliveries[0].ID != pending || all.Deliveries[2].ID != delivered {
		t.Fatalf("list is not newest-first: %d, %d, %d (want %d first, %d last)",
			all.Deliveries[0].ID, all.Deliveries[1].ID, all.Deliveries[2].ID, pending, delivered)
	}

	// The cursor walks the whole set exactly once, and the last page says so.
	seen := map[int64]bool{}
	cursor := ""
	for page := 0; page < 5; page++ {
		got := e.listDeliveries(t, tn.OwnerToken, "&limit=1"+cursor)
		if len(got.Deliveries) != 1 {
			t.Fatalf("page %d returned %d rows, want 1", page, len(got.Deliveries))
		}
		if seen[got.Deliveries[0].ID] {
			t.Fatalf("cursor returned delivery %d twice", got.Deliveries[0].ID)
		}
		seen[got.Deliveries[0].ID] = true
		if got.NextCursor == nil {
			break
		}
		cursor = fmt.Sprintf("&cursor=%d", *got.NextCursor)
	}
	if len(seen) != 3 {
		t.Fatalf("the cursor walked %d deliveries, want 3", len(seen))
	}

	// Status filter.
	only := e.listDeliveries(t, tn.OwnerToken, "&status=delivered")
	if len(only.Deliveries) != 1 || only.Deliveries[0].ID != delivered {
		t.Fatalf("status=delivered returned %+v, want just delivery %d", only.Deliveries, delivered)
	}
	if got := only.Deliveries[0]; got.DurationMs == nil || *got.DurationMs < 1000 {
		t.Fatalf("a delivered delivery reported duration_ms=%v, want the first-attempt duration", got.DurationMs)
	}

	// Event type and endpoint filters.
	if got := e.listDeliveries(t, tn.OwnerToken, "&event_type="+tn.EventTypeName); len(got.Deliveries) != 3 {
		t.Fatalf("event_type filter returned %d rows, want 3", len(got.Deliveries))
	}
	if got := e.listDeliveries(t, tn.OwnerToken, "&event_type=nothing.here"); len(got.Deliveries) != 0 {
		t.Fatalf("an unrelated event type returned %d rows, want 0", len(got.Deliveries))
	}
	if got := e.listDeliveries(t, tn.OwnerToken, "&endpoint_id="+tn.EndpointID); len(got.Deliveries) != 3 {
		t.Fatalf("endpoint filter returned %d rows, want 3", len(got.Deliveries))
	}
	if got := e.listDeliveries(t, tn.OwnerToken, "&endpoint_id=ep_does_not_exist"); len(got.Deliveries) != 0 {
		t.Fatalf("an unknown endpoint returned %d rows, want 0", len(got.Deliveries))
	}

	// Time range. One delivery is aged out of the window by hand, so the filter
	// is proven to read the column rather than to accept a parameter.
	if _, err := e.admin.Exec(context.Background(),
		`update deliveries set created_at = now() - interval '30 days' where id = $1`, delivered); err != nil {
		t.Fatal(err)
	}
	since := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	if got := e.listDeliveries(t, tn.OwnerToken, "&from="+since); len(got.Deliveries) != 2 {
		t.Fatalf("from=1h returned %d rows, want the two recent deliveries", len(got.Deliveries))
	}
	until := time.Now().Add(-24 * time.Hour).UTC().Format(time.RFC3339)
	if got := e.listDeliveries(t, tn.OwnerToken, "&to="+until); len(got.Deliveries) != 1 || got.Deliveries[0].ID != delivered {
		t.Fatalf("to=24h ago returned %+v, want just the aged delivery %d", got.Deliveries, delivered)
	}

	// Malformed parameters are refused rather than guessed at.
	code, raw := e.do(http.MethodGet, "/v1/producer/deliveries?env=live&cursor=nope", tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusBadRequest)
	if got := errorCode(t, raw); got != "invalid_cursor" {
		t.Fatalf("error code = %q, want invalid_cursor", got)
	}
	code, raw = e.do(http.MethodGet, "/v1/producer/deliveries?env=live&limit=1000", tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusBadRequest)
	code, raw = e.do(http.MethodGet, "/v1/producer/deliveries?env=live&from=yesterday", tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusBadRequest)
	if got := errorCode(t, raw); got != "invalid_time" {
		t.Fatalf("error code = %q, want invalid_time", got)
	}

	// The consumer portal's log (UI-15) is the same list, scoped by the claim.
	mine := e.doListAsConsumer(t, tn)
	if len(mine) != 3 {
		t.Fatalf("the consumer's own log returned %d deliveries, want 3", len(mine))
	}
}

// doListAsConsumer reads the consumer-scoped list.
func (e *env) doListAsConsumer(t *testing.T, tn *tenant) []deliveryJSON {
	t.Helper()
	code, raw := e.do(http.MethodGet, "/v1/consumer/deliveries", tn.ConsumerToken, nil)
	mustStatus(t, code, raw, http.StatusOK)
	return decode[deliveryListJSON](t, raw).Deliveries
}

// --- FR-1102, FR-1103: the ordered attempt trail --------------------------

// TestDeliveryDetailExposesOrderedAttempts is FR-1102 and FR-1103: one delivery
// with its attempts in order, each carrying number, timestamp, duration, status,
// truncated body, error class and next retry time.
func TestDeliveryDetailExposesOrderedAttempts(t *testing.T) {
	e := setup(t)
	tn := e.seedInspectTenant(t, "s7-detail")

	id := e.deliver(t, tn)
	started := time.Now().Add(-90 * time.Second).UTC().Truncate(time.Millisecond)
	e.failDelivery(t, id, 503, 2, started)

	code, raw := e.do(http.MethodGet, fmt.Sprintf("/v1/producer/deliveries/%d?env=live", id), tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusOK)
	detail := decode[deliveryJSON](t, raw)

	if detail.Status != "failed" || detail.Attempts != 2 {
		t.Fatalf("detail = status %q attempts %d, want failed/2", detail.Status, detail.Attempts)
	}
	if len(detail.AttemptsRows) != 2 {
		t.Fatalf("detail returned %d attempt rows, want 2", len(detail.AttemptsRows))
	}
	first, second := detail.AttemptsRows[0], detail.AttemptsRows[1]
	if first.Attempt != 1 || second.Attempt != 2 {
		t.Fatalf("attempts are not in order: %d then %d", first.Attempt, second.Attempt)
	}
	if first.ResponseStatus == nil || *first.ResponseStatus != 503 {
		t.Fatalf("attempt 1 status = %v, want 503", first.ResponseStatus)
	}
	if first.ErrorClass != "http_retryable" || first.Outcome != "retryable" {
		t.Fatalf("attempt 1 = %s/%s, want http_retryable/retryable", first.ErrorClass, first.Outcome)
	}
	if first.NextRetryAt == nil {
		t.Fatal("a retryable attempt has no next_retry_at (FR-1103)")
	}
	if second.ErrorClass != "http_terminal" || second.Outcome != "terminal" {
		t.Fatalf("attempt 2 = %s/%s, want http_terminal/terminal", second.ErrorClass, second.Outcome)
	}
	if second.NextRetryAt != nil {
		t.Fatalf("the last attempt reported a next retry at %v", *second.NextRetryAt)
	}
	if first.ResponseBody == nil || *first.ResponseBody == "" {
		t.Fatal("attempt has no response body (FR-804/FR-1103)")
	}
	// A failed delivery's trail is its rows: nothing is synthesized.
	if detail.EffectiveAttempt != nil {
		t.Fatalf("a failed delivery reported a synthesized attempt: %+v", detail.EffectiveAttempt)
	}

	// FR-804/SB-4: the body is truncated, not stored whole.
	big := strings.Repeat("x", 5000)
	err := e.st.RecordAttempt(context.Background(), store.AttemptRecord{
		DeliveryID: id, Attempt: 3, Outcome: "terminal", ErrorClass: "http_terminal",
		ResponseStatus: ptr(404), ResponseBody: big, DurationMs: 3,
	})
	if err != nil {
		t.Fatalf("record a large attempt: %v", err)
	}
	var stored int
	if err := e.admin.QueryRow(context.Background(),
		`select length(response_body) from attempts where delivery_id = $1 and attempt = 3`, id).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != 2048 {
		t.Fatalf("a 5000-byte body was stored as %d bytes, want 2048 (SB-4)", stored)
	}

	// A foreign id is a miss, whatever its shape (FR-108).
	code, raw = e.do(http.MethodGet, "/v1/producer/deliveries/99999999?env=live", tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusNotFound)
	code, raw = e.do(http.MethodGet, "/v1/producer/deliveries/not-a-number?env=live", tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusNotFound)
}

func ptr[T any](v T) *T { return &v }

// --- AC-32, FR-1109, FR-1110, FR-1111: the event view and the rollup ------

// TestEventRollupStatuses is FR-1109: one event, its fan-out outcome and the
// rollup, for every one of the five values.
func TestEventRollupStatuses(t *testing.T) {
	e := setup(t)
	tn := e.seedInspectTenant(t, "s7-rollup")
	second := e.addEndpoint(t, tn, "second endpoint")

	// pending: accepted, fanned out, nothing attempted yet.
	pendingEvent := e.publishEvent(t, tn, tn.ConsumerRef)
	e.fanOut(t, tn, pendingEvent)
	if got := e.eventView(t, tn.OwnerToken, pendingEvent).Rollup; got != store.RollupPending {
		t.Fatalf("a fanned-out event rolled up as %q, want %q", got, store.RollupPending)
	}

	// all_delivered: the state AC-32 opens.
	okEvent := e.publishEvent(t, tn, tn.ConsumerRef)
	affected := e.fanOut(t, tn, okEvent)
	for _, id := range affected {
		e.completeDelivery(t, id, 1, time.Now().Add(-time.Second), time.Now())
	}
	okView := e.eventView(t, tn.OwnerToken, okEvent)
	if okView.Rollup != store.RollupAllDelivered {
		t.Fatalf("delivered event rolled up as %q, want %q", okView.Rollup, store.RollupAllDelivered)
	}
	if len(okView.Deliveries) != 2 {
		t.Fatalf("the event view listed %d deliveries, want 2", len(okView.Deliveries))
	}

	// failed: fanned out, attempted, terminally failed.
	failedEvent := e.publishEvent(t, tn, tn.ConsumerRef)
	for _, id := range e.fanOut(t, tn, failedEvent) {
		e.failDelivery(t, id, 500, 1, time.Now().Add(-time.Second))
	}
	if got := e.eventView(t, tn.OwnerToken, failedEvent).Rollup; got != store.RollupFailed {
		t.Fatalf("a wholly failed event rolled up as %q, want %q", got, store.RollupFailed)
	}

	// partially_failed: two endpoint deliveries, one delivered and one failed.
	mixedEvent := e.publishEvent(t, tn, tn.ConsumerRef)
	mixed := e.fanOut(t, tn, mixedEvent)
	if len(mixed) != 2 {
		t.Fatalf("the mixed fixture fanned out to %d deliveries, want 2 (endpoints %s, %s)", len(mixed), tn.EndpointID, second)
	}
	e.completeDelivery(t, mixed[0], 1, time.Now().Add(-time.Second), time.Now())
	e.failDelivery(t, mixed[1], 502, 1, time.Now().Add(-time.Second))
	if got := e.eventView(t, tn.OwnerToken, mixedEvent).Rollup; got != store.RollupPartiallyFailed {
		t.Fatalf("a half-delivered event rolled up as %q, want %q", got, store.RollupPartiallyFailed)
	}

	// unroutable: the consumer_ref matched no consumer (§7.5a, AC-23).
	unroutableEvent := e.publishEvent(t, tn, "ref-nobody-onboarded-this")
	if reason := e.fanOutRaw(t, tn, unroutableEvent).EventReason; reason != store.ReasonUnroutableNotRegistered {
		t.Fatalf("fan-out of an unregistered ref recorded %q, want %s", reason, store.ReasonUnroutableNotRegistered)
	}
	code, raw := e.do(http.MethodGet, "/v1/producer/events/"+unroutableEvent+"?env=live", tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusOK)
	badView := decode[eventViewJSONBody](t, raw)
	if badView.Rollup != store.RollupUnroutable {
		t.Fatalf("an unroutable event rolled up as %q, want %q", badView.Rollup, store.RollupUnroutable)
	}
	if badView.Event.NonDeliveryReason == nil || *badView.Event.NonDeliveryReason != store.ReasonUnroutableNotRegistered {
		t.Fatalf("the view reported reason %v, want %s", badView.Event.NonDeliveryReason, store.ReasonUnroutableNotRegistered)
	}
	if len(badView.Deliveries) != 0 {
		t.Fatalf("an unroutable event listed %d deliveries, want 0", len(badView.Deliveries))
	}

	// A foreign event id is a miss (FR-108).
	code, raw = e.do(http.MethodGet, "/v1/producer/events/evt_nope?env=live", tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusNotFound)
}

// TestDeliveredFirstAttemptHasNoEmptyTrail is AC-32: open a delivered event, get
// the rollup, and every delivery shows a status, a timestamp and a duration even
// though a successful first attempt leaves no attempt row (FR-811) - the API
// never hands the UI a delivery whose trail is empty and unaccounted for
// (FR-1110).
func TestDeliveredFirstAttemptHasNoEmptyTrail(t *testing.T) {
	e := setup(t)
	tn := e.seedInspectTenant(t, "s7-trail")

	eventID := e.publishEvent(t, tn, tn.ConsumerRef)
	ids := e.fanOut(t, tn, eventID)
	started := time.Now().Add(-1500 * time.Millisecond).UTC().Truncate(time.Microsecond)
	done := time.Now().UTC().Truncate(time.Microsecond)
	for _, id := range ids {
		e.completeDelivery(t, id, 1, started, done)
	}

	view := e.eventView(t, tn.OwnerToken, eventID)
	if view.Rollup != store.RollupAllDelivered {
		t.Fatalf("rollup = %q, want %q", view.Rollup, store.RollupAllDelivered)
	}
	if len(view.Deliveries) == 0 {
		t.Fatal("the delivered event listed no deliveries")
	}
	for _, d := range view.Deliveries {
		if d.Status != "delivered" {
			t.Fatalf("delivery %d is %q, want delivered", d.ID, d.Status)
		}
		// AC-28's shape, re-proved here because the trail depends on it.
		if rows := e.countRows(t, `select count(*) from attempts where delivery_id = $1`, d.ID); rows != 0 {
			t.Fatalf("a successful first attempt wrote %d attempt rows (FR-811/AC-28)", rows)
		}
		if len(d.AttemptsRows) != 0 {
			t.Fatalf("delivery %d reported %d attempt rows, want none", d.ID, len(d.AttemptsRows))
		}
		// FR-1110: status, timestamp and duration, all present.
		if d.FirstAttemptAt == nil || d.DeliveredAt == nil || d.DurationMs == nil {
			t.Fatalf("delivery %d has no status/timestamp/duration: first=%v delivered=%v duration=%v",
				d.ID, d.FirstAttemptAt, d.DeliveredAt, d.DurationMs)
		}
		if *d.DurationMs < 1000 {
			t.Fatalf("delivery %d reported duration_ms=%d for a 1.5 s first attempt", d.ID, *d.DurationMs)
		}
		// The trail the UI renders is never empty: either the attempt rows, or
		// the synthesized effective attempt that stands in for the success.
		if len(d.AttemptsRows) == 0 && d.EffectiveAttempt == nil {
			t.Fatalf("delivery %d would render an empty trail (FR-1110)", d.ID)
		}
		if d.EffectiveAttempt == nil {
			t.Fatalf("delivery %d has no effective attempt to render", d.ID)
		}
		if !d.EffectiveAttempt.Synthesized || d.EffectiveAttempt.Status != "delivered" {
			t.Fatalf("effective attempt = %+v, want a synthesized delivered one", d.EffectiveAttempt)
		}
		if d.EffectiveAttempt.DurationMs == nil || *d.EffectiveAttempt.DurationMs != *d.DurationMs {
			t.Fatalf("effective attempt duration = %v, want %v", d.EffectiveAttempt.DurationMs, d.DurationMs)
		}
		if d.EffectiveAttempt.Attempt != 1 {
			t.Fatalf("effective attempt number = %d, want 1", d.EffectiveAttempt.Attempt)
		}
	}

	// The detail route says the same thing as the event view (FR-1102 + FR-1110).
	code, raw := e.do(http.MethodGet, fmt.Sprintf("/v1/producer/deliveries/%d?env=live", ids[0]), tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusOK)
	detail := decode[deliveryJSON](t, raw)
	if len(detail.AttemptsRows) != 0 || detail.EffectiveAttempt == nil {
		t.Fatalf("detail trail = %d rows / effective %v, want 0 rows and an effective attempt",
			len(detail.AttemptsRows), detail.EffectiveAttempt)
	}

	// A skipped delivery is the other shape that has no attempt rows: a matched
	// endpoint that could not take the event. Its trail is the reason, not an
	// empty list.
	pausedEvent := e.publishEvent(t, tn, tn.ConsumerRef)
	code, raw = e.do(http.MethodPatch, "/v1/producer/consumers/"+tn.ConsumerID+"/endpoints/"+tn.EndpointID+"?env=live",
		tn.OwnerToken, map[string]bool{"paused": true})
	mustStatus(t, code, raw, http.StatusOK)
	if skipped := e.fanOutRaw(t, tn, pausedEvent).Skipped; len(skipped) != 1 {
		t.Fatalf("fan-out against a paused endpoint skipped %d matches, want 1", len(skipped))
	}
	skippedView := e.eventView(t, tn.OwnerToken, pausedEvent)
	if len(skippedView.Deliveries) != 1 || skippedView.Deliveries[0].Status != "skipped" {
		t.Fatalf("paused-endpoint event view = %+v, want one skipped delivery", skippedView.Deliveries)
	}
	skipped := skippedView.Deliveries[0]
	if skipped.EffectiveAttempt == nil || skipped.EffectiveAttempt.Outcome != store.ReasonEndpointPaused {
		t.Fatalf("skipped delivery trail = %+v, want the endoint_paused reason", skipped.EffectiveAttempt)
	}
	if skippedView.Rollup != store.RollupFailed {
		t.Fatalf("an event whose only endpoint was paused rolled up as %q, want %q", skippedView.Rollup, store.RollupFailed)
	}
}

// --- AC-12, FR-1106, FR-1107, FR-1108: replay ----------------------------

// TestReplayCreatesNewDeliveriesIdempotently is AC-12 with FR-1106 and FR-1108:
// replaying twice with one request key produces one set of new deliveries, the
// deliveries being repaired are never touched, and the trail says which
// deliveries came from the replay.
func TestReplayCreatesNewDeliveriesIdempotently(t *testing.T) {
	e := setup(t)
	tn := e.seedInspectTenant(t, "s7-replay")

	brokenA := e.deliver(t, tn)
	e.failDelivery(t, brokenA, 500, 1, time.Now().Add(-time.Minute))
	brokenB := e.deliver(t, tn)
	e.failDelivery(t, brokenB, 503, 1, time.Now().Add(-time.Minute))
	healthy := e.deliver(t, tn)
	e.completeDelivery(t, healthy, 1, time.Now().Add(-time.Minute), time.Now().Add(-59*time.Second))

	before := e.deliverySnapshot(t, brokenA, brokenB, healthy)
	attemptsBefore := e.countRows(t, `select count(*) from attempts`)

	body := map[string]any{"status": "failed", "idempotency_key": "ac12-request-key"}
	code, raw := e.do(http.MethodPost, "/v1/producer/deliveries/replay?env=live", tn.OwnerToken, body)
	mustStatus(t, code, raw, http.StatusOK)
	first := decode[replayJSONBody](t, raw)

	if first.Matched != 2 || first.Created != 2 || len(first.DeliveryIDs) != 2 {
		t.Fatalf("first replay = %+v, want 2 matched and 2 created", first)
	}
	if first.Repeated {
		t.Fatalf("the first replay of a key reported itself as a repeat: %+v", first)
	}
	if first.ReleasePerSecond != store.ReplayReleasePerSecond {
		t.Fatalf("release rate = %d/s, want %d/s (FR-1107)", first.ReleasePerSecond, store.ReplayReleasePerSecond)
	}

	// FR-1106: the repaired deliveries are unchanged - status, timestamps,
	// attempt count and attempt rows alike.
	after := e.deliverySnapshot(t, brokenA, brokenB, healthy)
	if !equalSnapshots(before, after) {
		t.Fatalf("replay mutated the history it replays:\nbefore %+v\nafter  %+v", before, after)
	}
	if attempts := e.countRows(t, `select count(*) from attempts`); attempts != attemptsBefore {
		t.Fatalf("replay wrote %d attempt rows", attempts-attemptsBefore)
	}

	// The new deliveries: pending, linked to the row they repair, same event and
	// endpoint, no attempts yet, and released at 1/50 s apart per endpoint.
	for _, id := range first.DeliveryIDs {
		var status, eventID, endpointID string
		var replayOf *int64
		var attempts int
		var lease time.Time
		err := e.admin.QueryRow(context.Background(), `
			select status, event_id, endpoint_id, replay_of_delivery_id, attempts, lease_expires_at
			from deliveries where id = $1`, id).Scan(&status, &eventID, &endpointID, &replayOf, &attempts, &lease)
		if err != nil {
			t.Fatal(err)
		}
		if status != "pending" || attempts != 0 {
			t.Fatalf("replayed delivery %d = %s/%d attempts, want pending/0", id, status, attempts)
		}
		if replayOf == nil || (*replayOf != brokenA && *replayOf != brokenB) {
			t.Fatalf("replayed delivery %d links to %v, want one of the two failed deliveries", id, replayOf)
		}
		if endpointID != tn.EndpointID {
			t.Fatalf("replayed delivery %d went to endpoint %s, want %s", id, endpointID, tn.EndpointID)
		}
		if _, ok := e.eventFor(t, eventID); !ok {
			t.Fatalf("replayed delivery %d lost its event %s", id, eventID)
		}
		if lease.Before(time.Now().Add(-time.Second)) {
			t.Fatalf("replayed delivery %d is not scheduled: lease %v", id, lease)
		}
	}
	leaseGap := e.leaseGap(t, first.DeliveryIDs)
	if leaseGap <= 0 || leaseGap > 500*time.Millisecond {
		t.Fatalf("two replays to one endpoint were released %v apart, want the %d/s spacing",
			leaseGap, store.ReplayReleasePerSecond)
	}

	// FR-1108: the trail distinguishes a replay in both directions.
	code, raw = e.do(http.MethodGet, fmt.Sprintf("/v1/producer/deliveries/%d?env=live", brokenA), tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusOK)
	original := decode[deliveryJSON](t, raw)
	if original.ReplayOfDeliveryID != nil {
		t.Fatalf("the repaired delivery %d claims to be a replay of %v", brokenA, original.ReplayOfDeliveryID)
	}
	if len(original.ReplayedBy) != 1 || !containsID(first.DeliveryIDs, original.ReplayedBy[0]) {
		t.Fatalf("the repaired delivery reports replayed_by=%v, want the replay it produced", original.ReplayedBy)
	}
	replayedID := original.ReplayedBy[0]
	code, raw = e.do(http.MethodGet, fmt.Sprintf("/v1/producer/deliveries/%d?env=live", replayedID), tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusOK)
	replayed := decode[deliveryJSON](t, raw)
	if replayed.ReplayOfDeliveryID == nil || *replayed.ReplayOfDeliveryID != brokenA {
		t.Fatalf("the replay reports replay_of_delivery_id=%v, want %d", replayed.ReplayOfDeliveryID, brokenA)
	}

	// AC-12: the same key again is the same set, and creates nothing.
	code, raw = e.do(http.MethodPost, "/v1/producer/deliveries/replay?env=live", tn.OwnerToken, body)
	mustStatus(t, code, raw, http.StatusOK)
	second := decode[replayJSONBody](t, raw)
	if !second.Repeated || second.Created != 0 {
		t.Fatalf("second replay = %+v, want a repeat that created nothing", second)
	}
	if !sameIDs(first.DeliveryIDs, second.DeliveryIDs) {
		t.Fatalf("the repeat returned %v, want the first call's %v", second.DeliveryIDs, first.DeliveryIDs)
	}
	total := e.countRows(t, `select count(*) from deliveries where producer_id = $1 and replay_of_delivery_id is not null`, tn.ID)
	if total != 2 {
		t.Fatalf("two replays with one request key created %d deliveries, want one set (AC-12)", total)
	}

	// The replays are now pending, so a *new* key replays the same two failures
	// again - and does not select the rows it created.
	body["idempotency_key"] = "ac12-second-key"
	code, raw = e.do(http.MethodPost, "/v1/producer/deliveries/replay?env=live", tn.OwnerToken, body)
	mustStatus(t, code, raw, http.StatusOK)
	third := decode[replayJSONBody](t, raw)
	if third.Matched != 2 || third.Created != 2 {
		t.Fatalf("a second replay key matched/created %d/%d, want 2/2 (the pending replays are not selections)",
			third.Matched, third.Created)
	}
	if sameIDs(first.DeliveryIDs, third.DeliveryIDs) {
		t.Fatalf("a new request key returned the previous key's deliveries: %v", third.DeliveryIDs)
	}

	// A replay needs a request key: without one a retry would double the batch.
	code, raw = e.do(http.MethodPost, "/v1/producer/deliveries/replay?env=live", tn.OwnerToken,
		map[string]any{"status": "failed"})
	mustStatus(t, code, raw, http.StatusBadRequest)
	if got := errorCode(t, raw); got != "idempotency_key_required" {
		t.Fatalf("error code = %q, want idempotency_key_required", got)
	}

	// A dry run answers the count and the bound without writing.
	code, raw = e.do(http.MethodPost, "/v1/producer/deliveries/replay?env=live", tn.OwnerToken,
		map[string]any{"status": "failed", "idempotency_key": "ac12-dry-run", "dry_run": true})
	mustStatus(t, code, raw, http.StatusOK)
	dry := decode[replayJSONBody](t, raw)
	if dry.Matched != 2 || dry.Created != 0 || !dry.DryRun {
		t.Fatalf("dry run = %+v, want matched 2 and nothing created", dry)
	}
	if n := e.countRows(t, `select count(*) from replay_requests where producer_id = $1`, tn.ID); n != 2 {
		t.Fatalf("recorded %d replay requests, want exactly the two real replays (a dry run must not burn a key)", n)
	}
}

// TestReplayBoundIsRejectedNamingTheBound is AC-13: a selection larger than
// FR-1107's 1,000 is refused, the refusal names the bound, and nothing is
// written - including the request record, so the key is not burned.
func TestReplayBoundIsRejectedNamingTheBound(t *testing.T) {
	e := setup(t)
	tn := e.seedInspectTenant(t, "s7-bound")

	recent := time.Now().Add(-3 * time.Hour)
	old := time.Now().Add(-10 * 24 * time.Hour)
	e.bulkDeliveries(t, tn, tn.EndpointID, "evtbulkrecent", 1000, recent)
	e.bulkDeliveries(t, tn, tn.EndpointID, "evtbulkold", 4000, old)
	before := e.countRows(t, `select count(*) from deliveries where producer_id = $1`, tn.ID)

	// AC-13: 5,000 selected, refused, and the refusal says why.
	code, raw := e.do(http.MethodPost, "/v1/producer/deliveries/replay?env=live", tn.OwnerToken,
		map[string]any{"idempotency_key": "ac13-over"})
	mustStatus(t, code, raw, http.StatusUnprocessableEntity)
	if got := errorCode(t, raw); got != "replay_bound_exceeded" {
		t.Fatalf("error code = %q, want replay_bound_exceeded", got)
	}
	if !strings.Contains(string(raw), "1000") {
		t.Fatalf("the refusal does not name the bound (AC-13): %s", raw)
	}
	if n := e.countRows(t, `select count(*) from deliveries where producer_id = $1`, tn.ID); n != before {
		t.Fatalf("a refused replay wrote %d deliveries", n-before)
	}
	if n := e.countRows(t, `select count(*) from replay_requests where producer_id = $1`, tn.ID); n != 0 {
		t.Fatalf("a refused replay recorded %d request keys, want 0", n)
	}

	// The bound is inclusive: exactly 1,000 is one request's worth.
	cutoff := time.Now().Add(-6 * time.Hour).UTC().Format(time.RFC3339)
	code, raw = e.do(http.MethodPost, "/v1/producer/deliveries/replay?env=live", tn.OwnerToken,
		map[string]any{"from": cutoff, "idempotency_key": "ac13-at-the-bound"})
	mustStatus(t, code, raw, http.StatusOK)
	atBound := decode[replayJSONBody](t, raw)
	if atBound.Matched != 1000 || atBound.Created != 1000 {
		t.Fatalf("a 1000-delivery replay = %+v, want 1000 matched and created", atBound)
	}
	if n := e.countRows(t, `select count(*) from deliveries where producer_id = $1 and replay_of_delivery_id is not null`, tn.ID); n != 1000 {
		t.Fatalf("the at-bound replay created %d replayed deliveries, want 1000", n)
	}

	// And 1,001 is not: one more row in the window and the same request is out.
	if _, err := e.admin.Exec(context.Background(), `
		update deliveries set created_at = $2
		where id = (select id from deliveries
		            where producer_id = $1 and created_at < $2 and status = 'delivered'
		              and replay_of_delivery_id is null
		            order by id limit 1)`, tn.ID, recent); err != nil {
		t.Fatal(err)
	}
	code, raw = e.do(http.MethodPost, "/v1/producer/deliveries/replay?env=live", tn.OwnerToken,
		map[string]any{"from": cutoff, "idempotency_key": "ac13-over-by-one"})
	mustStatus(t, code, raw, http.StatusUnprocessableEntity)
	if !strings.Contains(string(raw), "1000") {
		t.Fatalf("the refusal does not name the bound: %s", raw)
	}

	// A dry run applies the same bound, so a confirmation step cannot report a
	// count it would then refuse.
	code, raw = e.do(http.MethodPost, "/v1/producer/deliveries/replay?env=live", tn.OwnerToken,
		map[string]any{"idempotency_key": "ac13-dry-over", "dry_run": true})
	mustStatus(t, code, raw, http.StatusUnprocessableEntity)
}

type deliveryRow struct {
	Status      string
	Attempts    int
	DeliveredAt *time.Time
	Reason      *string
	ReplayOf    *int64
}

// deliverySnapshot reads the fields FR-1106 says a replay may not touch.
func (e *env) deliverySnapshot(t *testing.T, ids ...int64) map[int64]deliveryRow {
	t.Helper()
	out := map[int64]deliveryRow{}
	for _, id := range ids {
		var row deliveryRow
		err := e.admin.QueryRow(context.Background(), `
			select status, attempts, delivered_at, non_delivery_reason, replay_of_delivery_id
			from deliveries where id = $1`, id).Scan(&row.Status, &row.Attempts, &row.DeliveredAt, &row.Reason, &row.ReplayOf)
		if err != nil {
			t.Fatal(err)
		}
		out[id] = row
	}
	return out
}

func equalSnapshots(a, b map[int64]deliveryRow) bool {
	if len(a) != len(b) {
		return false
	}
	for id, row := range a {
		other, ok := b[id]
		if !ok {
			return false
		}
		if row.Status != other.Status || row.Attempts != other.Attempts {
			return false
		}
		if !sameOptionalInt64(row.ReplayOf, other.ReplayOf) {
			return false
		}
		if !sameOptionalString(row.Reason, other.Reason) {
			return false
		}
		// Timestamps compare by value: two pointers to the same microsecond are
		// different pointers.
		if (row.DeliveredAt == nil) != (other.DeliveredAt == nil) {
			return false
		}
		if row.DeliveredAt != nil && !row.DeliveredAt.Equal(*other.DeliveredAt) {
			return false
		}
	}
	return true
}

func sameOptionalInt64(a, b *int64) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	return a == nil || *a == *b
}

func sameOptionalString(a, b *string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	return a == nil || *a == *b
}

// expectDeliveryUntouched asserts a delivery is still the delivered row the
// fixture created: FR-1106 says a replay never mutates the row it repairs.
func (e *env) expectDeliveryUntouched(t *testing.T, id int64) {
	t.Helper()
	var status string
	var replayOf *int64
	var attempts int
	err := e.admin.QueryRow(context.Background(),
		`select status, replay_of_delivery_id, attempts from deliveries where id = $1`, id).
		Scan(&status, &replayOf, &attempts)
	if err != nil {
		t.Fatalf("read delivery %d: %v", id, err)
	}
	if status != "delivered" || replayOf != nil || attempts != 1 {
		t.Fatalf("delivery %d = %s / attempts %d / replay_of %v, want the untouched delivered row",
			id, status, attempts, replayOf)
	}
}

// eventFor reports whether an event id exists, so a replayed delivery is proven
// to keep the event it replays.
func (e *env) eventFor(t *testing.T, eventID string) (string, bool) {
	t.Helper()
	var typ string
	err := e.admin.QueryRow(context.Background(), `select event_type from events where id = $1`, eventID).Scan(&typ)
	if err != nil {
		return "", false
	}
	return typ, true
}

// leaseGap is how far apart two replayed deliveries to one endpoint are
// scheduled: FR-1107's 50/s, made observable.
func (e *env) leaseGap(t *testing.T, ids []int64) time.Duration {
	t.Helper()
	if len(ids) < 2 {
		return 0
	}
	leases := make([]time.Time, 0, len(ids))
	for _, id := range ids {
		var lease time.Time
		if err := e.admin.QueryRow(context.Background(),
			`select lease_expires_at from deliveries where id = $1`, id).Scan(&lease); err != nil {
			t.Fatal(err)
		}
		leases = append(leases, lease)
	}
	first, last := leases[0], leases[0]
	for _, l := range leases[1:] {
		if l.Before(first) {
			first = l
		}
		if l.After(last) {
			last = l
		}
	}
	return last.Sub(first)
}

func containsID(ids []int64, want int64) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func sameIDs(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// --- FR-1105: a consumer user sees only its own consumer -----------------

// seedCrossProducerDelivery gives a tenant one delivered delivery, on a
// consumer-routed event type of its own, so the cross-producer suite's S7 cases
// (FR-1101..FR-1111) have a real delivery id and a real event id in each tenant
// to aim at the other.
//
// It is separate from seedCrossProducerEventType because that type is
// broadcast-routed (the ingest cases publish to it without a consumer), and a
// broadcast event has no fan-out target, so it produces no deliveries at all.
func seedCrossProducerDelivery(t *testing.T, e *env, tn *tenant) {
	t.Helper()
	tn.DeliveryID, tn.DeliveryEventID = 0, ""
	// A type of its own: the tenant's EventTypeName stays the broadcast type the
	// ingest cases publish to (a broadcast event has no fan-out target at all).
	tn.DeliveryEventTypeName = strings.ReplaceAll(tn.Slug, "-", "") + ".delivery"
	code, raw := e.do(http.MethodPost, "/v1/event-types", tn.KeyPlain, map[string]any{
		"name":           tn.DeliveryEventTypeName,
		"routing_mode":   "consumer",
		"payload_schema": map[string]any{"type": "object"},
	})
	mustStatus(t, code, raw, http.StatusCreated)
	e.verifyEndpointFor(t, tn, tn.EndpointID, tn.EndpointVerifyToken, tn.DeliveryEventTypeName)
	eventID := e.publishTypedEvent(t, tn, tn.DeliveryEventTypeName, tn.ConsumerRef)
	ids := e.fanOut(t, tn, eventID)
	if len(ids) != 1 {
		t.Fatalf("the cross-producer fixture fanned out to %d deliveries, want 1", len(ids))
	}
	e.completeDelivery(t, ids[0], 1, time.Now().Add(-time.Second), time.Now())
	tn.DeliveryID, tn.DeliveryEventID = ids[0], eventID
	e.remember(t, tn)
}

// TestConsumerSeesOnlyItsOwnDeliveries is FR-1105 (and AC-39's shape): the
// consumer-scoped list, detail and replay are forced to the session's consumer,
// and another consumer's delivery is a 404, never a 403.
func TestConsumerSeesOnlyItsOwnDeliveries(t *testing.T) {
	e := setup(t)
	mine := e.seedInspectTenant(t, "s7-consumer-mine")

	mineEvent := e.publishEvent(t, mine, mine.ConsumerRef)
	mineDelivery := e.fanOut(t, mine, mineEvent)

	// Same producer, different consumers: build the sibling in the same tenant,
	// so the isolation under test is the consumer's, not the tenant's.
	code, raw := e.do(http.MethodPost, "/v1/producer/consumers?env=live", mine.OwnerToken,
		map[string]string{"ref": "ref-second-consumer", "name": "Second consumer"})
	mustStatus(t, code, raw, http.StatusCreated)
	siblingConsumer := decode[consumerJSON](t, raw).ID
	code, raw = e.do(http.MethodPost, "/v1/producer/consumers/"+siblingConsumer+"/endpoints?env=live",
		mine.OwnerToken, map[string]string{"name": "sibling endpoint", "url": "https://1.1.1.1/hook"})
	mustStatus(t, code, raw, http.StatusCreated)
	sibling := decode[endpointCreatedJSON](t, raw)
	e.verifyEndpoint(t, mine, sibling.ID, sibling.VerificationToken)
	siblingEvent := e.publishEvent(t, mine, "ref-second-consumer")
	code, raw = e.do(http.MethodGet, "/v1/producer/events/"+siblingEvent+"?env=live", mine.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusOK)
	siblingIDs := e.fanOut(t, mine, siblingEvent)
	if len(siblingIDs) != 1 {
		t.Fatalf("the sibling consumer's event fanned out to %d deliveries, want 1", len(siblingIDs))
	}

	log := e.doListAsConsumer(t, mine)
	if len(log) != 1 || log[0].ID != mineDelivery[0] {
		t.Fatalf("the consumer's log returned %+v, want just its own delivery %d", log, mineDelivery[0])
	}
	for _, d := range log {
		if d.ConsumerID != mine.ConsumerID {
			t.Fatalf("the consumer's log contains consumer %s's delivery %d", d.ConsumerID, d.ID)
		}
	}

	// A sibling consumer's delivery is a 404 through the detail route.
	code, raw = e.do(http.MethodGet, fmt.Sprintf("/v1/consumer/deliveries/%d", siblingIDs[0]), mine.ConsumerToken, nil)
	mustStatus(t, code, raw, http.StatusNotFound)
	if got := errorCode(t, raw); got != "not_found" {
		t.Fatalf("error code = %q, want not_found (never forbidden)", got)
	}

	// And replaying with a sibling consumer's id in the body is not honoured:
	// the claim decides, so the selection is the caller's own.
	e.failDelivery(t, siblingIDs[0], 500, 1, time.Now().Add(-time.Minute))
	code, raw = e.do(http.MethodPost, "/v1/consumer/deliveries/replay", mine.ConsumerToken,
		map[string]any{"consumer_id": siblingConsumer, "status": "failed", "idempotency_key": "s7-consumer-replay"})
	mustStatus(t, code, raw, http.StatusOK)
	result := decode[replayJSONBody](t, raw)
	if result.Matched != 0 || result.Created != 0 {
		t.Fatalf("a consumer replay with a sibling's consumer_id in the body = %+v, want an empty selection", result)
	}
	// Scoped to the two deliveries in question: the harness shares one database
	// across the package's tests, so a global count would see other tests' rows.
	if n := e.countRows(t,
		`select count(*) from deliveries where replay_of_delivery_id = any($1)`, []int64{siblingIDs[0], mineDelivery[0]}); n != 0 {
		t.Fatalf("the refused consumer replay created %d deliveries", n)
	}

	// Its own consumer's deliveries are replayable from the portal (UI-16),
	// which is the API's own bound and rate, not a portal-invented one.
	if _, err := e.admin.Exec(context.Background(),
		`update deliveries set status = 'failed' where consumer_id = $1 and status = 'pending'`, mine.ConsumerID); err != nil {
		t.Fatal(err)
	}
	code, raw = e.do(http.MethodPost, "/v1/consumer/deliveries/replay", mine.ConsumerToken,
		map[string]any{"status": "failed", "idempotency_key": "s7-consumer-replay-own"})
	mustStatus(t, code, raw, http.StatusOK)
	own := decode[replayJSONBody](t, raw)
	if own.Matched != 1 || own.Created != 1 {
		t.Fatalf("the consumer's own replay = %+v, want its own 1 failed delivery", own)
	}
	if own.DeliveryIDs[0] == siblingIDs[0] {
		t.Fatal("the consumer's replay reached the sibling consumer's delivery")
	}
	if own.ReleasePerSecond != store.ReplayReleasePerSecond {
		t.Fatalf("the consumer replay used %d/s, want the API's %d/s (UI-16)",
			own.ReleasePerSecond, store.ReplayReleasePerSecond)
	}
}

// --- FR-903: the dedupe answer -------------------------------------------

// TestDedupeAnswerWasThisADuplicate is FR-903: the support question is
// answerable for the last 30 days from Postgres, and it counts a replay as a
// duplicate delivery of the same pair.
func TestDedupeAnswerWasThisADuplicate(t *testing.T) {
	e := setup(t)
	tn := e.seedInspectTenant(t, "s7-dedupe")
	other := e.seedInspectTenant(t, "s7-dedupe-other")

	eventID := e.publishEvent(t, tn, tn.ConsumerRef)
	deliveryID := e.fanOut(t, tn, eventID)[0]
	e.completeDelivery(t, deliveryID, 1, time.Now().Add(-time.Second), time.Now())

	ask := func(token, endpointID, event string) (int, []byte) {
		return e.do(http.MethodGet,
			"/v1/producer/deliveries/dedupe?env=live&endpoint_id="+endpointID+"&event_id="+event, token, nil)
	}

	code, raw := ask(tn.OwnerToken, tn.EndpointID, eventID)
	mustStatus(t, code, raw, http.StatusOK)
	answer := decode[dedupeJSONBody](t, raw)
	if !answer.Duplicate || answer.FirstDeliveryID != deliveryID || answer.FirstSeenAt == nil || answer.Deliveries != 1 {
		t.Fatalf("the first answer = %+v, want a duplicate of delivery %d", answer, deliveryID)
	}

	// A replay of the same pair is a second delivery of the same pair, and the
	// answer says so rather than counting the pair once.
	code, raw = e.do(http.MethodPost, "/v1/producer/deliveries/replay?env=live", tn.OwnerToken,
		map[string]any{"endpoint_id": tn.EndpointID, "idempotency_key": "s7-dedupe-replay"})
	mustStatus(t, code, raw, http.StatusOK)
	code, raw = ask(tn.OwnerToken, tn.EndpointID, eventID)
	mustStatus(t, code, raw, http.StatusOK)
	after := decode[dedupeJSONBody](t, raw)
	if after.Deliveries != 2 || after.FirstDeliveryID != deliveryID {
		t.Fatalf("after a replay the answer = %+v, want 2 deliveries and the same first one", after)
	}

	// An event this endpoint never received is not a duplicate - and is not an
	// error, because "no" is a support answer.
	code, raw = ask(tn.OwnerToken, tn.EndpointID, "evt_never_delivered")
	mustStatus(t, code, raw, http.StatusOK)
	if never := decode[dedupeJSONBody](t, raw); never.Duplicate || never.Deliveries != 0 {
		t.Fatalf("an undelivered event answered %+v, want duplicate false", never)
	}

	// Another tenant's endpoint does not exist in this scope (FR-108).
	code, raw = ask(tn.OwnerToken, other.EndpointID, eventID)
	mustStatus(t, code, raw, http.StatusNotFound)

	// The key is a pair: without it the question is not answerable.
	code, raw = e.do(http.MethodGet, "/v1/producer/deliveries/dedupe?env=live&endpoint_id="+tn.EndpointID, tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusBadRequest)
	if got := errorCode(t, raw); got != "dedupe_key_required" {
		t.Fatalf("error code = %q, want dedupe_key_required", got)
	}
}

// jsonBody is a debugging aid for the assertions above: it renders a response
// body indented so a failure in CI is readable.
func jsonBody(raw []byte) string {
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return string(raw)
	}
	pretty, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return string(raw)
	}
	return string(pretty)
}
