package consumer

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/m7jay/pigeon/internal/portalui"
)

// testHandler builds the portal with fixed, non-secret test config. New() parses
// the whole embedded template set, so a construction error is a template bug.
func testHandler(t *testing.T) *Handler {
	t.Helper()
	h, err := New(Config{
		Addr:         ":0",
		CoreURL:      "http://core:8080",
		Brand:        portalui.NeutralFallback,
		CookieName:   "consumer_session",
		CookieSecret: "test-cookie-secret",
	})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	return h
}

// TestNewParsesEveryTemplate proves the embedded template set parses: the shared
// base, the consumer nav, the htmx fragments and every page. A template syntax
// error only surfaces at render time, so construction is the earliest place to
// catch it; this runs it at test time rather than at deploy time.
func TestNewParsesEveryTemplate(t *testing.T) {
	h := testHandler(t)
	if h.Handler() == nil {
		t.Fatal("Handler() returned nil")
	}
}

// TestNewRefusesUnsafeConfig: an empty session secret or core URL is a broken
// deployment, refused at boot rather than at first request.
func TestNewRefusesUnsafeConfig(t *testing.T) {
	if _, err := New(Config{CoreURL: "http://core:8080", CookieSecret: "", Brand: portalui.NeutralFallback}); err == nil {
		t.Fatal("New() with no session secret should refuse to start")
	}
	if _, err := New(Config{CoreURL: "", CookieSecret: "s", Brand: portalui.NeutralFallback}); err == nil {
		t.Fatal("New() with no core URL should refuse to start")
	}
}

// TestHealthzLiveness exercises the container healthcheck endpoint the compose
// file runs the binary against (scratch images have no shell).
func TestHealthzLiveness(t *testing.T) {
	srv := httptest.NewServer(testHandler(t).Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /healthz status = %d, want 200", resp.StatusCode)
	}
}

// TestLoginPageRenders drives the full server-side render path (base layout +
// nav + a page). The login page is the anonymous entry point (UI-12).
func TestLoginPageRenders(t *testing.T) {
	srv := httptest.NewServer(testHandler(t).Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/login")
	if err != nil {
		t.Fatalf("GET /login: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /login status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "Log in") {
		t.Fatalf("login page did not render its heading: %s", body)
	}
}

// TestSnippetRendersVerifierLibrary is UI-18: the verification snippet names the
// Standard Webhooks Go verifier library and carries the endpoint secret, and is
// generated server-side from a value the portal never sends to the core.
func TestSnippetRendersVerifierLibrary(t *testing.T) {
	srv := httptest.NewServer(testHandler(t).Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/snippet?whsec=whsec_ABCDEF")
	if err != nil {
		t.Fatalf("GET /snippet: %v", err)
	}
	defer resp.Body.Close()
	s, _ := io.ReadAll(resp.Body)
	text := string(s)
	if !strings.Contains(text, "standardwebhooks") || !strings.Contains(text, "NewWebhook") {
		t.Fatalf("snippet did not include the Standard Webhooks verifier library:\n%s", text)
	}
	if !strings.Contains(text, "whsec_ABCDEF") {
		t.Fatalf("snippet did not carry the endpoint secret:\n%s", text)
	}
}
