package consumer

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// The portal's part of FR-405 + FR-1006: it offers the consumer a way to set the
// headers their endpoints send (without which the consumer-facing half of FR-405
// is unreachable), passes them to the API without inventing any rules of its own,
// and shows what is configured as names plus the API's mask.

// TestEndpointRegisterSendsCustomHeaders: the "Name: value" lines become the
// API's custom_headers field, comments and blank lines are dropped, and the order
// the consumer wrote is the order sent.
func TestEndpointRegisterSendsCustomHeaders(t *testing.T) {
	core := newFakeCore(t)
	core.on(http.MethodPost, "/v1/consumer/endpoints",
		`{"id":"end9","signing_secret":"whsec_test","verification_token":"vrf_test"}`, http.StatusCreated)
	h := testHandlerAt(t, core.srv.URL)

	rec := signedIn(t, h, http.MethodPost, "/endpoints", url.Values{
		"name": {"orders"}, "url": {"https://8.8.8.8/hook"},
		"custom_headers": {"# per-endpoint credentials\nX-Api-Key: sk-live-canary\n\nX-Tenant: acme\n"},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("register: status = %d, want 200 with the one-time secret (%s)", rec.Code, rec.Body.String())
	}
	var sent struct {
		CustomHeaders []struct {
			Name  string `json:"name"`
			Value string `json:"value"`
		} `json:"custom_headers"`
	}
	if err := json.Unmarshal([]byte(core.lastBody(http.MethodPost, "/v1/consumer/endpoints")), &sent); err != nil {
		t.Fatalf("the portal did not post a JSON body: %v", err)
	}
	if len(sent.CustomHeaders) != 2 {
		t.Fatalf("sent %d custom headers, want 2 (a comment and a blank line are not headers): %+v",
			len(sent.CustomHeaders), sent.CustomHeaders)
	}
	if sent.CustomHeaders[0].Name != "X-Api-Key" || sent.CustomHeaders[0].Value != "sk-live-canary" {
		t.Fatalf("first header = %+v, want X-Api-Key: sk-live-canary", sent.CustomHeaders[0])
	}
	if sent.CustomHeaders[1].Name != "X-Tenant" || sent.CustomHeaders[1].Value != "acme" {
		t.Fatalf("second header = %+v, want X-Tenant: acme", sent.CustomHeaders[1])
	}
}

// TestEndpointRegisterReportsAMalformedHeaderLine: "X-NoColon" is the portal's own
// typo to report - the API would only say the name is not a valid header name -
// and nothing is sent, so a typo cannot half-register an endpoint.
func TestEndpointRegisterReportsAMalformedHeaderLine(t *testing.T) {
	core := newFakeCore(t)
	h := testHandlerAt(t, core.srv.URL)

	rec := signedIn(t, h, http.MethodPost, "/endpoints", url.Values{
		"name": {"orders"}, "url": {"https://8.8.8.8/hook"},
		"custom_headers": {"X-Api-Key: sk-live-canary\nX-NoColon\n"},
	})
	if rec.Code != http.StatusSeeOther || rec.Header().Get("Location") != "/endpoints" {
		t.Fatalf("status = %d location = %q, want a 303 back to the list", rec.Code, rec.Header().Get("Location"))
	}
	if flash := flashOf(t, rec); !strings.Contains(flash, "line 2") {
		t.Fatalf("flash = %q, want it to name the offending line", flash)
	}
	for _, call := range core.calls() {
		if call == "POST /v1/consumer/endpoints" {
			t.Fatal("a malformed header line still registered the endpoint")
		}
	}
}

// TestEndpointRegisterSurfacesTheAPIsHeaderRefusal: FR-405 is the API's rule, so
// the portal shows the API's reason verbatim - including a forbidden name - and
// the refusal never quotes a value (FR-1006).
func TestEndpointRegisterSurfacesTheAPIsHeaderRefusal(t *testing.T) {
	core := newFakeCore(t)
	const reason = "the Authorization header is reserved and cannot be set per endpoint (FR-405)"
	core.on(http.MethodPost, "/v1/consumer/endpoints",
		`{"error":{"code":"invalid","message":"`+reason+`"}}`, http.StatusUnprocessableEntity)
	h := testHandlerAt(t, core.srv.URL)

	rec := signedIn(t, h, http.MethodPost, "/endpoints", url.Values{
		"name": {"orders"}, "url": {"https://8.8.8.8/hook"},
		"custom_headers": {"Authorization: sk-live-canary"},
	})
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want a 303 with the reason in the flash", rec.Code)
	}
	flash := flashOf(t, rec)
	if !strings.Contains(flash, reason) {
		t.Fatalf("flash = %q, want the API's reason %q", flash, reason)
	}
	if strings.Contains(flash, "sk-live-canary") {
		t.Fatalf("the flash quotes the header value: %q", flash)
	}
}

// TestEndpointDetailShowsHeaderNamesAndMasks: the detail page lists what is
// configured, with the mask the API sent - the value cannot be rendered because
// the portal never receives one (FR-1006's "redactable in the UI").
func TestEndpointDetailShowsHeaderNamesAndMasks(t *testing.T) {
	core := newFakeCore(t)
	core.on(http.MethodGet, "/v1/consumer/endpoints/end1",
		`{"id":"end1","name":"orders-hook","url":"https://1.1.1.1/hook","status":"active","verified":true,
		  "created_at":"2026-01-01T00:00:00Z",
		  "custom_headers":[{"name":"X-Api-Key","value":"••••••••"},{"name":"X-Tenant","value":"••••••••"}]}`,
		http.StatusOK)
	core.on(http.MethodGet, "/v1/consumer/endpoints/end1/subscriptions", `{"subscriptions":[]}`, http.StatusOK)
	core.on(http.MethodGet, "/v1/consumer/event-types", `{"event_types":[]}`, http.StatusOK)
	h := testHandlerAt(t, core.srv.URL)

	rec := signedIn(t, h, http.MethodGet, "/endpoints/end1", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /endpoints/end1: status = %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{"Custom headers", "X-Api-Key", "X-Tenant", "••••••••", "never shown again"} {
		if !strings.Contains(body, want) {
			t.Errorf("the detail page is missing %q", want)
		}
	}
	// The empty-state branch must not be rendered for an endpoint that does have
	// headers (and the mask must appear twice: once per header).
	if strings.Contains(body, "none configured") {
		t.Errorf("the page claims no headers are configured while rendering two:\n%s", body)
	}
	if n := strings.Count(body, "••••••••"); n != 2 {
		t.Errorf("the mask appears %d times, want once per configured header (2)", n)
	}
}

// TestEndpointDetailShowsTheEmptyState: the other branch - an endpoint with no
// custom headers says so, rather than rendering an empty list.
func TestEndpointDetailShowsTheEmptyState(t *testing.T) {
	core := newFakeCore(t)
	core.on(http.MethodGet, "/v1/consumer/endpoints/end1",
		`{"id":"end1","name":"orders-hook","url":"https://1.1.1.1/hook","status":"active","verified":true,
		  "created_at":"2026-01-01T00:00:00Z","custom_headers":[]}`, http.StatusOK)
	core.on(http.MethodGet, "/v1/consumer/endpoints/end1/subscriptions", `{"subscriptions":[]}`, http.StatusOK)
	core.on(http.MethodGet, "/v1/consumer/event-types", `{"event_types":[]}`, http.StatusOK)
	h := testHandlerAt(t, core.srv.URL)

	body := signedIn(t, h, http.MethodGet, "/endpoints/end1", nil).Body.String()
	if !strings.Contains(body, "none configured") {
		t.Errorf("an endpoint with no custom headers does not say so:\n%s", body)
	}
}
