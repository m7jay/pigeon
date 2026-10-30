package consumer

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// This file is UI-15 and UI-16's evidence: the delivery log, the attempt detail
// with response bodies, the 404 a foreign delivery gets, and replay bounded by
// the API rather than by the portal. Every test drives the portal's own handler
// against a core API stand-in, because the portal's whole data path is API calls:
// there is no store to fake, and a call the portal did not make is a requirement
// it did not meet.

// seedDeliveryLog registers the two reads the log page makes: the caller's
// deliveries (the log itself) and the caller's endpoints (the endpoint filter's
// option set, which comes from the API so the control cannot offer an id the
// caller does not have).
func seedDeliveryLog(core *fakeCore, deliveries string, status int) {
	core.on(http.MethodGet, "/v1/consumer/deliveries", deliveries, status)
	core.on(http.MethodGet, "/v1/consumer/endpoints", `{"endpoints":[]}`, http.StatusOK)
}

// TestConsumerDeliveryLogListsItsOwnConsumer is UI-15's log as the browser sees
// it: the API's rows rendered newest first with a link into each attempt detail,
// the API's cursor carried into a next-page link, and the API's own filters
// forwarded verbatim. The page the placeholder used to be is gone with it.
func TestConsumerDeliveryLogListsItsOwnConsumer(t *testing.T) {
	core := newFakeCore(t)
	seedDeliveryLog(core, `{"deliveries":[`+
		`{"id":41,"event_id":"evt_1","event_type":"order.shipped","endpoint_id":"end1","status":"failed_terminal",`+
		`"attempts":2,"created_at":"2026-01-01T00:00:00Z","first_attempt_at":"2026-01-01T00:00:01Z",`+
		`"delivered_at":null,"duration_ms":3210,"replay_of_delivery_id":null}],`+
		`"next_cursor":"40"}`, http.StatusOK)
	h := testHandlerAt(t, core.srv.URL)

	rec := signedIn(t, h, http.MethodGet, "/deliveries?status=failed_terminal", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /deliveries status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"order.shipped",         // the API's row
		`href="/deliveries/41"`, // and the way into its attempt detail (UI-15)
		"cursor=40",             // the API's next_cursor became the next-page link
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("the delivery log is missing %s:\n%s", want, body)
		}
	}
	// The placeholder copy is gone: this page is the log, not a notice that the
	// log arrives with another slice.
	for _, gone := range []string{"is not live on this instance yet", "arrives with"} {
		if strings.Contains(body, gone) {
			t.Fatalf("the delivery log still carries the placeholder copy %q:\n%s", gone, body)
		}
	}

	sent := core.lastRequest(http.MethodGet, "/v1/consumer/deliveries")
	if !strings.Contains(sent.URI, "status=failed_terminal") {
		t.Fatalf("the portal did not forward the API's own status filter: %s", sent.URI)
	}
	// The read is made with the caller's own scoped session, which is what makes
	// the result set the API's and not the portal's.
	if sent.Auth != "Bearer test-session-token" {
		t.Fatalf("the log read carried %q, want the caller's scoped session token", sent.Auth)
	}
}

// TestConsumerDeliveryLogShowsTheAPIsOwnRefusalOfAFilter is §14 on the read side:
// the portal neither clamps a filter nor invents one. A `limit` outside the API's
// range is sent exactly as the browser gave it, and what the user reads is the
// API's refusal - not an empty log, which would read as "you have no deliveries".
func TestConsumerDeliveryLogShowsTheAPIsOwnRefusalOfAFilter(t *testing.T) {
	core := newFakeCore(t)
	seedDeliveryLog(core, `{"error":{"code":"invalid_limit","message":"limit must be an integer between 1 and 200"}}`,
		http.StatusBadRequest)
	h := testHandlerAt(t, core.srv.URL)

	rec := signedIn(t, h, http.MethodGet, "/deliveries?limit=5000", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /deliveries status = %d, want 200 carrying the API's reason", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "limit must be an integer between 1 and 200") {
		t.Fatalf("the page does not show the API's refusal:\n%s", body)
	}
	if strings.Contains(body, "No deliveries match this filter yet.") {
		t.Fatalf("a refused read rendered as an empty log:\n%s", body)
	}

	sent := core.lastRequest(http.MethodGet, "/v1/consumer/deliveries")
	if !strings.Contains(sent.URI, "limit=5000") {
		t.Fatalf("the portal clamped the API's own limit instead of passing it through: %s", sent.URI)
	}
}

// TestConsumerDeliveryLogNeverNamesAConsumer is FR-1105 and AC-39's first half:
// the consumer id comes from the session claim and never from the browser, so a
// consumer id in a URL or a form is dropped rather than forwarded. A portal that
// passed one on would be pretending it had a say in its own scope, and the API
// would be the only thing standing between the two consumers.
func TestConsumerDeliveryLogNeverNamesAConsumer(t *testing.T) {
	core := newFakeCore(t)
	seedDeliveryLog(core, `{"deliveries":[],"next_cursor":""}`, http.StatusOK)
	h := testHandlerAt(t, core.srv.URL)

	signedIn(t, h, http.MethodGet, "/deliveries?consumer_id=con_someone_else&status=failed_terminal", nil)
	read := core.lastRequest(http.MethodGet, "/v1/consumer/deliveries")
	if strings.Contains(read.URI, "consumer_id") {
		t.Fatalf("the log read named a consumer: %s", read.URI)
	}
	if !strings.Contains(read.URI, "status=failed_terminal") {
		t.Fatalf("the API's own filter was dropped along with the consumer id: %s", read.URI)
	}

	// The same rule on the write side: a consumer id posted to the replay is not
	// forwarded either (it is the API that forces the claim, FR-1105).
	core.on(http.MethodPost, "/v1/consumer/deliveries/replay",
		`{"matched":0,"created":0,"delivery_ids":[],"repeated":false,"release_per_second":50,"dry_run":false}`,
		http.StatusOK)
	signedIn(t, h, http.MethodPost, "/deliveries/replay",
		url.Values{"consumer_id": {"con_someone_else"}, "status": {"failed_terminal"}, "idempotency_key": {"k1"}})
	write := core.lastRequest(http.MethodPost, "/v1/consumer/deliveries/replay")
	if strings.Contains(write.Body, "consumer_id") {
		t.Fatalf("the replay named a consumer: %s", write.Body)
	}
	if !strings.Contains(write.Body, `"status":"failed_terminal"`) {
		t.Fatalf("the replay dropped the API's own filter: %s", write.Body)
	}
}

// TestConsumerDeliveryDetailShowsResponseBodies is UI-15's "including response
// bodies": the detail page is the API's detail (FR-1102) rendered in full, with
// every attempt's body, response status, error class and next retry time
// (FR-1103). The body is the one field a delivery log exists for.
func TestConsumerDeliveryDetailShowsResponseBodies(t *testing.T) {
	core := newFakeCore(t)
	core.on(http.MethodGet, "/v1/consumer/deliveries/41",
		`{"id":41,"event_id":"evt_1","event_type":"order.shipped","endpoint_id":"end1","status":"failed_terminal",`+
			`"attempts":2,"created_at":"2026-01-01T00:00:00Z","first_attempt_at":"2026-01-01T00:00:01Z",`+
			`"delivered_at":null,"duration_ms":3210,"attempts_rows":[`+
			`{"attempt":1,"occurred_at":"2026-01-01T00:00:01Z","duration_ms":1000,"response_status":500,`+
			`"response_body":"upstream said no","error_class":"http_5xx","outcome":"failed","next_retry_at":"2026-01-01T00:00:31Z"},`+
			`{"attempt":2,"occurred_at":"2026-01-01T00:00:31Z","duration_ms":2210,"response_status":200,`+
			`"response_body":"HTTP 200 OK","error_class":"","outcome":"delivered","next_retry_at":null}],`+
			`"effective_attempt":null,"replayed_by":[]}`, http.StatusOK)
	h := testHandlerAt(t, core.srv.URL)

	rec := signedIn(t, h, http.MethodGet, "/deliveries/41", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /deliveries/41 status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		`<pre class="snippet">upstream said no</pre>`, // the first attempt's body
		`<pre class="snippet">HTTP 200 OK</pre>`,      // and the second's
		"http_5xx",                                    // FR-1103's error class
		"2026-01-01T00:00:31Z",                        // FR-1103's next retry time
		"1000 ms", "2210 ms",                          // and each attempt's duration
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("the attempt detail is missing %s:\n%s", want, body)
		}
	}
}

// TestDeliveryDetailRendersTheEffectiveAttemptNotAnEmptyTrail is FR-1110, and the
// UI observable of AC-32: a delivery whose first attempt succeeded has no attempt
// row (FR-811 keeps it on the delivery), so the API's effective attempt stands in
// for it and the page never renders an empty trail. The second half proves the
// assertion is not vacuous: a delivery that genuinely has not been attempted yet
// does say so.
func TestDeliveryDetailRendersTheEffectiveAttemptNotAnEmptyTrail(t *testing.T) {
	core := newFakeCore(t)
	core.on(http.MethodGet, "/v1/consumer/deliveries/42",
		`{"id":42,"event_id":"evt_2","event_type":"order.shipped","endpoint_id":"end1","status":"delivered",`+
			`"attempts":1,"created_at":"2026-01-01T00:00:00Z","first_attempt_at":"2026-01-01T00:00:05Z",`+
			`"delivered_at":"2026-01-01T00:00:05Z","duration_ms":250,"attempts_rows":[],`+
			`"effective_attempt":{"attempt":1,"occurred_at":"2026-01-01T00:00:05Z","duration_ms":250,`+
			`"status":"delivered","outcome":"delivered"},"replayed_by":[]}`, http.StatusOK)
	core.on(http.MethodGet, "/v1/consumer/deliveries/43",
		`{"id":43,"event_id":"evt_3","event_type":"order.shipped","endpoint_id":"end1","status":"pending",`+
			`"attempts":0,"created_at":"2026-01-01T00:00:00Z","first_attempt_at":null,`+
			`"delivered_at":null,"duration_ms":null,"attempts_rows":[],"effective_attempt":null,"replayed_by":[]}`,
		http.StatusOK)
	h := testHandlerAt(t, core.srv.URL)

	delivered := signedIn(t, h, http.MethodGet, "/deliveries/42", nil).Body.String()
	if !strings.Contains(delivered, "(no attempt row)") {
		t.Fatalf("a successful first attempt rendered no trail entry (FR-1110):\n%s", delivered)
	}
	if !strings.Contains(delivered, "2026-01-01T00:00:05Z") || !strings.Contains(delivered, "250 ms") {
		t.Fatalf("the effective attempt did not carry its own timestamp and duration:\n%s", delivered)
	}
	if strings.Contains(delivered, "has not been attempted yet") {
		t.Fatalf("the page says an attempted delivery was never attempted:\n%s", delivered)
	}

	pending := signedIn(t, h, http.MethodGet, "/deliveries/43", nil).Body.String()
	if !strings.Contains(pending, "has not been attempted yet") {
		t.Fatalf("a delivery with no attempt at all did not say so:\n%s", pending)
	}
}

// TestForeignDeliveryIsNotFoundFromThePortal is AC-39 at the portal: a delivery
// outside the caller's consumer is the API's 404, and the portal answers with
// that status rather than a friendly page. The read goes through the caller's own
// token, so the portal has nothing of its own to leak in the first place.
func TestForeignDeliveryIsNotFoundFromThePortal(t *testing.T) {
	core := newFakeCore(t)
	core.on(http.MethodGet, "/v1/consumer/deliveries/999",
		`{"error":{"code":"not_found","message":"no such resource"}}`, http.StatusNotFound)
	h := testHandlerAt(t, core.srv.URL)

	rec := signedIn(t, h, http.MethodGet, "/deliveries/999", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET /deliveries/999 status = %d, want the API's 404 passed through", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "No such delivery") {
		t.Fatalf("the 404 page does not say what is missing:\n%s", body)
	}
	// The portal holds no deliveries of its own: it asked the API, with the
	// caller's scoped session, and reported the answer.
	sent := core.lastRequest(http.MethodGet, "/v1/consumer/deliveries/999")
	if sent.Auth != "Bearer test-session-token" {
		t.Fatalf("the detail read carried %q, want the caller's scoped session token", sent.Auth)
	}
}

// TestConsumerReplayIsConfirmedByTheAPIsOwnCountAndRate is UI-16's first step:
// the panel states the API's count and rate, which it can only do by asking the
// API (UI-5's "explicit confirmation stating count and rate"). The fixture's rate
// is deliberately not FR-1107's 50/s: a portal that assumed or hardcoded the
// bound instead of quoting the API would fail here.
func TestConsumerReplayIsConfirmedByTheAPIsOwnCountAndRate(t *testing.T) {
	core := newFakeCore(t)
	seedDeliveryLog(core, `{"deliveries":[],"next_cursor":""}`, http.StatusOK)
	core.on(http.MethodPost, "/v1/consumer/deliveries/replay",
		`{"matched":7,"created":0,"delivery_ids":[],"repeated":false,"release_per_second":37,"dry_run":true}`,
		http.StatusOK)
	h := testHandlerAt(t, core.srv.URL)

	// A plain post gets the log page with the confirmation in place - not a
	// redirect, which would drop the confirmed selection and mint a new key.
	rec := signedIn(t, h, http.MethodPost, "/deliveries/replay/confirm",
		url.Values{"status": {"failed_terminal"}, "idempotency_key": {"key-abc"}})
	if rec.Code != http.StatusOK {
		t.Fatalf("confirm: status = %d, want the log page carrying the confirmation", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"matched <strong>7</strong>",             // the API's count
		"<strong>37/s</strong>",                  // the API's rate, not a portal constant
		`action="/deliveries/replay"`,            // and the step that commits it
		`name="idempotency_key" value="key-abc"`, // the same request key, so a double submit is one replay
		`name="status" value="failed_terminal"`,  // the same selection the log was showing
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("the replay confirmation is missing %s:\n%s", want, body)
		}
	}

	sent := core.lastRequest(http.MethodPost, "/v1/consumer/deliveries/replay")
	if sent.Idem != "key-abc" {
		t.Fatalf("the confirmation carried Idempotency-Key %q, want FR-1107's request key", sent.Idem)
	}
	if !strings.Contains(sent.Body, `"dry_run":true`) {
		t.Fatalf("the confirmation did not ask the API first (dry_run): %s", sent.Body)
	}
	if !strings.Contains(sent.Body, `"status":"failed_terminal"`) {
		t.Fatalf("the confirmation was not about the selection the log was showing: %s", sent.Body)
	}
}

// TestConsumerReplayShowsTheAPIsBoundWhereTheUserActed is UI-16's second step and
// FR-1107's bound: the portal enforces no bound of its own, so the refusal the
// user reads is the API's - here the API naming its own 1,000 - and it appears
// where they acted: in the panel for htmx, on the log page for a plain post. The
// commit carries the confirmation's request key, which is what makes a retried
// batch one replay rather than two (AC-12).
func TestConsumerReplayShowsTheAPIsBoundWhereTheUserActed(t *testing.T) {
	const boundRefusal = `{"error":{"code":"replay_bound_exceeded",` +
		`"message":"invalid: replay is bounded to 1000 deliveries per request"}}`
	core := newFakeCore(t)
	seedDeliveryLog(core, `{"deliveries":[],"next_cursor":""}`, http.StatusOK)
	core.on(http.MethodPost, "/v1/consumer/deliveries/replay", boundRefusal, http.StatusUnprocessableEntity)
	h := testHandlerAt(t, core.srv.URL)

	// htmx: the refusal lands in the panel the user pressed, and it is a fragment
	// (no layout) so it can replace that panel in place.
	frag := signedInHtmx(t, h, http.MethodPost, "/deliveries/replay",
		url.Values{"status": {"failed_terminal"}, "idempotency_key": {"key-1"}})
	if frag.Code != http.StatusOK {
		t.Fatalf("htmx replay: status = %d, want 200 carrying the panel", frag.Code)
	}
	panel := frag.Body.String()
	if strings.Contains(panel, "<html") {
		t.Fatalf("the htmx answer rendered a document instead of a fragment:\n%s", panel)
	}
	if !strings.HasPrefix(strings.TrimSpace(panel), `<div id="replay-panel">`) {
		t.Fatalf("the replay fragment does not carry the swap target on its root:\n%s", panel)
	}
	if !strings.Contains(panel, "replay is bounded to 1000 deliveries per request") {
		t.Fatalf("the panel does not show the API's refusal:\n%s", panel)
	}

	// A plain post of the same step gets a 303 back to the log with the reason,
	// which is the page the panel lives on: the user reads it where they acted.
	plain := signedIn(t, h, http.MethodPost, "/deliveries/replay",
		url.Values{"status": {"failed_terminal"}, "idempotency_key": {"key-1"}})
	if plain.Code != http.StatusSeeOther {
		t.Fatalf("plain replay: status = %d, want a 303 back to the log", plain.Code)
	}
	if loc := plain.Header().Get("Location"); loc != "/deliveries?status=failed_terminal" {
		t.Fatalf("plain replay redirected to %q, want the same filtered log", loc)
	}
	// The flash is read the way the page reads it: the message, not the cookie's
	// raw bytes (see flashOf).
	if flashed := flashOf(t, plain); !strings.Contains(flashed, "replay is bounded to 1000 deliveries per request") {
		t.Fatalf("the plain post lost the API's refusal: flash = %q", flashed)
	}

	// Both steps carried the request key the API requires, and the commit asked it
	// to write (dry_run off) - the bound is the API's to enforce, not the
	// portal's to guess.
	sent := core.lastRequest(http.MethodPost, "/v1/consumer/deliveries/replay")
	if sent.Idem != "key-1" {
		t.Fatalf("the commit carried Idempotency-Key %q, want FR-1107's request key", sent.Idem)
	}
	if strings.Contains(sent.Body, `"dry_run":true`) {
		t.Fatalf("the commit step still asked for a dry run: %s", sent.Body)
	}
}
