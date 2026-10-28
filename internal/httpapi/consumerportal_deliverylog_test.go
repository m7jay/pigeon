package httpapi

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/m7jay/pigeon/internal/portal"
	"github.com/m7jay/pigeon/internal/portal/consumer"
	"github.com/m7jay/pigeon/internal/portalui"
)

// This file is UI-15 and UI-16 end to end: the portal deployable is built against
// the real core API this package's env is serving, over a real Postgres, and
// driven through its own handler with a session cookie holding a real consumer
// session token. The unit tests in internal/portal/consumer prove the portal's
// behaviour against a stand-in; this proves the wire contract it is reading -
// field names, scoping and FR-1107's numbers - against the implementation that
// actually writes them.
//
// The portal is imported from a _test.go file only: AC-40's import-graph check
// reads non-test files, so a test may reach across the slice without re-coupling
// the core to a removable deployable.

// consumerPortal builds the consumer portal against the core API this env serves.
func consumerPortal(t *testing.T, e *env) *consumer.Handler {
	t.Helper()
	p, err := consumer.New(consumer.Config{
		Addr:         ":0",
		CoreURL:      e.srv.URL,
		Brand:        portalui.NeutralFallback,
		CookieName:   "consumer_session",
		CookieSecret: "test-cookie-secret",
	})
	if err != nil {
		t.Fatalf("consumer.New(): %v", err)
	}
	return p
}

// portalRequest drives the portal's own handler with the encrypted session cookie
// the portal issues, carrying a real core session token. That is the whole
// browser-to-portal hop: the token never reaches the browser, and every data call
// the portal then makes is a real API request with the caller's own scope.
func portalRequest(t *testing.T, p *consumer.Handler, token, method, target string, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	sc, err := portal.NewSecureCookie("consumer_session", "test-cookie-secret")
	if err != nil {
		t.Fatalf("NewSecureCookie(): %v", err)
	}
	enc, err := sc.Encode(token)
	if err != nil {
		t.Fatalf("encode session cookie: %v", err)
	}
	var body io.Reader
	if form != nil {
		body = strings.NewReader(form.Encode())
	}
	req := httptest.NewRequest(method, target, body)
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	req.AddCookie(&http.Cookie{Name: "consumer_session", Value: enc})
	rec := httptest.NewRecorder()
	p.Handler().ServeHTTP(rec, req)
	return rec
}

// TestConsumerPortalDeliveryLogEndToEnd is UI-15 and UI-16 against the real API
// and a real database: the log lists the session consumer's own delivery and not
// a sibling consumer's, the attempt detail carries the response body the delivery
// worker recorded, a sibling's delivery is the API's 404, and replay states and
// then performs the API's own count at the API's own rate.
//
// The session consumer is this test's own fixture consumer, not the shared
// tenant's built-in one: S7's fixtures count that consumer's log exactly
// (TestConsumerSeesOnlyItsOwnDeliveries wants one row), so a test that writes
// deliveries and replays for it breaks its neighbours. The tenant and this test's
// consumer/endpoint come from the shared S7 fixture; the second consumer below is
// created here, so nothing this test leaves behind is a row anyone else counts.
func TestConsumerPortalDeliveryLogEndToEnd(t *testing.T) {
	e, fx := setupS7(t)
	tn := fx.Tenant
	slug := refSlug(t.Name())

	// A consumer user bound to this test's own consumer, invited by the owner and
	// logged in exactly as a real consumer user is. That session is the portal's
	// whole credential: the portal holds the token server-side and every data call
	// it makes is a real API request with this consumer's scope.
	consumerEmail := "s9b-e2e-" + slug + "@example.test"
	code, raw := e.do(http.MethodPost, "/v1/producer/consumers/"+fx.ConsumerID+"/invites?env=live",
		tn.OwnerToken, map[string]string{"email": consumerEmail})
	mustStatus(t, code, raw, http.StatusCreated)
	e.acceptInvite(t, decode[inviteJSON](t, raw).TokenValue())
	session := e.login(consumerEmail, memberPassword, "")
	if session.Claims.ConsumerID != fx.ConsumerID {
		t.Fatalf("the test's consumer session claims %q, want %q (FR-110)", session.Claims.ConsumerID, fx.ConsumerID)
	}
	token := session.Token

	// The session consumer's own delivery, attempted and terminal: failDelivery
	// records a real attempt row (FR-804) whose response body is "boom".
	mine := e.deliverFor(t, fx)
	if len(mine) != 1 {
		t.Fatalf("the session consumer's event fanned out to %d deliveries, want 1", len(mine))
	}
	e.failDelivery(t, mine[0], 500, 1, time.Now().Add(-time.Minute))

	// A sibling consumer in the same tenant, with a delivery of its own: the one
	// row the log must never show. Same tenant, same event type - only the
	// consumer differs, so a log that leaked it would be a scoping bug and not a
	// tenant-isolation one.
	siblingRef := "ref-" + slug + "-sibling"
	code, raw = e.do(http.MethodPost, "/v1/producer/consumers?env=live", tn.OwnerToken,
		map[string]string{"ref": siblingRef, "name": "S9b sibling"})
	mustStatus(t, code, raw, http.StatusCreated)
	siblingConsumer := decode[consumerJSON](t, raw).ID
	e.newEndpoint(t, tn, siblingConsumer, slug+"sibling", tn.EventTypeName)
	sibling := e.fanOut(t, tn, e.publishTypedEvent(t, tn, tn.EventTypeName, siblingRef))

	p := consumerPortal(t, e)

	// --- UI-15: the log is the caller's own consumer's ---------------------
	log := portalRequest(t, p, token, http.MethodGet,
		"/deliveries?endpoint_id="+fx.EndpointID, nil)
	if log.Code != http.StatusOK {
		t.Fatalf("GET /deliveries status = %d, want 200 (%s)", log.Code, log.Body)
	}
	logBody := log.Body.String()
	wantLink := fmt.Sprintf(`href="/deliveries/%d"`, mine[0])
	if !strings.Contains(logBody, wantLink) {
		t.Fatalf("the log is missing the session consumer's delivery %d:\n%s", mine[0], logBody)
	}
	if !strings.Contains(logBody, "failed") {
		t.Fatalf("the log does not show the delivery's status:\n%s", logBody)
	}

	// The same log read without a filter: the sibling delivery is a newer row in
	// the same tenant, so an unscoped list would show it.
	unscoped := portalRequest(t, p, token, http.MethodGet, "/deliveries", nil).Body.String()
	if strings.Contains(unscoped, fmt.Sprintf(`href="/deliveries/%d"`, sibling[0])) {
		t.Fatalf("the portal's log shows consumer %s's delivery %d:\n%s", siblingConsumer, sibling[0], unscoped)
	}

	// --- UI-15: the attempt detail, response body included ------------------
	detail := portalRequest(t, p, token, http.MethodGet, fmt.Sprintf("/deliveries/%d", mine[0]), nil)
	if detail.Code != http.StatusOK {
		t.Fatalf("GET /deliveries/%d status = %d, want 200 (%s)", mine[0], detail.Code, detail.Body)
	}
	detailBody := detail.Body.String()
	for _, want := range []string{
		`<pre class="snippet">boom</pre>`, // FR-1103's truncated body, as the worker stored it
		"http_terminal",                   // FR-1103's error class
		"500",                             // the response status
	} {
		if !strings.Contains(detailBody, want) {
			t.Fatalf("the attempt detail is missing %s:\n%s", want, detailBody)
		}
	}

	// --- AC-39: a sibling consumer's delivery is the API's 404 --------------
	foreign := portalRequest(t, p, token, http.MethodGet, fmt.Sprintf("/deliveries/%d", sibling[0]), nil)
	if foreign.Code != http.StatusNotFound {
		t.Fatalf("the portal answered %d for a sibling consumer's delivery, want 404 (AC-39)", foreign.Code)
	}

	// --- UI-16: replay, bounded by the API's own numbers -------------------
	// What the API itself says about this selection, asked directly, so the
	// portal's answer can be compared against the API's rather than against a
	// number this test made up. The selection is this test's own endpoint and this
	// test's own failed delivery, so the API's answer is exactly one.
	code, raw = e.do(http.MethodPost, "/v1/consumer/deliveries/replay", token, map[string]any{
		"endpoint_id": fx.EndpointID, "status": "failed", "dry_run": true, "idempotency_key": "s9b-e2e-probe",
	})
	mustStatus(t, code, raw, http.StatusOK)
	api := decode[replayJSONBody](t, raw)
	if api.Matched != 1 {
		t.Fatalf("the API matched %d failed deliveries on this endpoint, want exactly the one this test failed", api.Matched)
	}

	selection := url.Values{
		"endpoint_id": {fx.EndpointID}, "status": {"failed"}, "idempotency_key": {"s9b-e2e-key"},
	}
	confirm := portalRequest(t, p, token, http.MethodPost, "/deliveries/replay/confirm", selection)
	if confirm.Code != http.StatusOK {
		t.Fatalf("the replay confirmation answered %d, want the log page (%s)", confirm.Code, confirm.Body)
	}
	confirmBody := confirm.Body.String()
	for _, want := range []string{
		fmt.Sprintf("matched <strong>%d</strong>", api.Matched),    // the API's count
		fmt.Sprintf("<strong>%d/s</strong>", api.ReleasePerSecond), // the API's rate (FR-1107)
		`action="/deliveries/replay"`,                              // and the step that commits it
		`name="idempotency_key" value="s9b-e2e-key"`,               // FR-1107's request key
	} {
		if !strings.Contains(confirmBody, want) {
			t.Fatalf("the confirmation is missing %s:\n%s", want, confirmBody)
		}
	}

	// Committing writes exactly what the API said it would, linked to the rows
	// they came from (FR-1106), and a second post of the same key writes nothing
	// more (FR-1107's idempotency, as the portal uses it).
	const replays = `select count(*) from deliveries where replay_of_delivery_id is not null and endpoint_id = $1`
	before := e.countRows(t, replays, fx.EndpointID)
	commit := portalRequest(t, p, token, http.MethodPost, "/deliveries/replay", selection)
	if commit.Code != http.StatusSeeOther {
		t.Fatalf("the replay commit answered %d, want a 303 back to the log (%s)", commit.Code, commit.Body)
	}
	if after := e.countRows(t, replays, fx.EndpointID); after-before != api.Matched {
		t.Fatalf("the portal's replay created %d deliveries, want the %d the API matched", after-before, api.Matched)
	}

	repeat := portalRequest(t, p, token, http.MethodPost, "/deliveries/replay", selection)
	if repeat.Code != http.StatusSeeOther {
		t.Fatalf("replaying with the same request key answered %d, want 303 (%s)", repeat.Code, repeat.Body)
	}
	if after := e.countRows(t, replays, fx.EndpointID); after-before != api.Matched {
		t.Fatalf("replaying with the same request key created %d more deliveries, want none (FR-1107)", after-before-api.Matched)
	}
}
