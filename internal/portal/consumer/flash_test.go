package consumer

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// TestFlashCarriesTheAPIsReasonByteForByte is the consumer portal's half of the
// flash cookie's byte-safety (internal/portal/flash.go). This portal shows the
// API's own reason on the redirect that follows the action, and that reason is
// text the API wrote: it can contain a ';', a '|', any spacing, or a non-ASCII
// character - exactly the bytes net/http silently DROPS when it serialises a
// cookie value, which is how a flash message used to arrive truncated with no
// error anywhere.
//
// It drives the whole path a browser does - POST the edit, the API refuses, the
// portal flashes and redirects, the next GET renders the banner - with a reason
// carrying the bytes that used to be dropped, and asserts the banner shows the
// reason intact.
func TestFlashCarriesTheAPIsReasonByteForByte(t *testing.T) {
	// The API's refusal, written the way an API writes one: a URL with a query
	// separator in it, the requirement id between pipes, a space, and non-ASCII
	// characters (é, ☃ - neither is a legal cookie byte).
	const reason = "the URL https://a.example/hook?q=a;b is not allowed (|FR-402|) - é ☃"

	refusal, err := json.Marshal(map[string]any{
		"error": map[string]string{"code": "endpoint_url_blocked", "message": reason},
	})
	if err != nil {
		t.Fatalf("marshal the core's refusal: %v", err)
	}

	core := newFakeCore(t)
	seedEndpointsPage(t, core, true)
	core.on(http.MethodPatch, "/v1/consumer/endpoints/end1", string(refusal), http.StatusUnprocessableEntity)
	h := testHandlerAt(t, core.srv.URL)

	rec := signedIn(t, h, http.MethodPost, "/endpoints/end1/edit", url.Values{
		"name": {"orders-hook"},
		"url":  {"https://a.example/hook?q=a;b"},
	})
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("POST /endpoints/end1/edit status = %d, want 303; body: %s", rec.Code, rec.Body.String())
	}

	// What the next request carries is the message the API wrote - not the same
	// message with the bytes net/http could not serialise cut out of it.
	if flash := flashOf(t, rec); flash != reason {
		t.Fatalf("the flash does not carry the API's reason:\n got %q\nwant %q", flash, reason)
	}

	// And the page the redirect lands on renders it inside the error banner: a
	// message is only worth carrying if the user can read it where they acted.
	page := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/endpoints/end1", nil)
	req.AddCookie(sessionCookie(t, h))
	req.AddCookie(cookieNamed(t, rec, flashCookie))
	h.Handler().ServeHTTP(page, req)

	body := page.Body.String()
	i := strings.Index(body, "flash flash-error")
	if i < 0 {
		t.Fatalf("the refusal rendered no error flash:\n%s", body)
	}
	if !strings.Contains(body[i:], reason) {
		t.Fatalf("the error banner does not show the API's reason %q:\n%s", reason, body[i:])
	}
}

// sessionCookie is the portal session cookie a signed-in browser would send, in
// the form the portal's own middleware accepts.
func sessionCookie(t *testing.T, h *Handler) *http.Cookie {
	t.Helper()
	enc, err := h.cookie.Encode("test-session-token")
	if err != nil {
		t.Fatalf("encode session cookie: %v", err)
	}
	return &http.Cookie{Name: h.cfg.CookieName, Value: enc}
}

// cookieNamed pulls the named cookie out of a response and fails when it is not
// there: a test that carried on without the cookie it means to send back would be
// asserting nothing.
func cookieNamed(t *testing.T, rec *httptest.ResponseRecorder, name string) *http.Cookie {
	t.Helper()
	for _, c := range rec.Result().Cookies() {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("the response set no %s cookie (Set-Cookie: %v)", name, rec.Header().Values("Set-Cookie"))
	return nil
}
