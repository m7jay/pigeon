package consumer

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
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

// testHandlerAt builds the portal against a core API stand-in, so the data
// handlers can be exercised end to end (browser -> portal -> core API) with no
// database. The portal has no store to fake: every data path is a core API call.
func testHandlerAt(t *testing.T, coreURL string) *Handler {
	t.Helper()
	h, err := New(Config{
		Addr:         ":0",
		CoreURL:      coreURL,
		Brand:        portalui.NeutralFallback,
		CookieName:   "consumer_session",
		CookieSecret: "test-cookie-secret",
	})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	return h
}

// fakeCore is a minimal stand-in for the core API. It records what it was asked
// and answers with canned bodies, which is all the portal's data path needs: the
// portal decides nothing itself, so "what did it send the API" is the whole
// behaviour of an edit, a rotation or a subscribe.
type fakeCore struct {
	srv *httptest.Server

	mu       sync.Mutex
	seen     []string
	bodies   map[string]string
	handlers map[string]http.HandlerFunc
}

func newFakeCore(t *testing.T) *fakeCore {
	t.Helper()
	f := &fakeCore{bodies: map[string]string{}, handlers: map[string]http.HandlerFunc{}}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Method + " " + r.URL.Path
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.seen = append(f.seen, key)
		f.bodies[key] = string(body)
		h := f.handlers[key]
		f.mu.Unlock()
		if h == nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"code":"not_found","message":"no such resource"}}`))
			return
		}
		h(w, r)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

// on registers one canned answer: status and JSON body.
func (f *fakeCore) on(method, path, body string, status int) {
	f.handlers[method+" "+path] = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
}

func (f *fakeCore) lastBody(method, path string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.bodies[method+" "+path]
}

func (f *fakeCore) calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.seen...)
}

// signedIn drives the portal's own handler with a valid session cookie.
// requireLogin only decodes the cookie - a page render is answered by the core
// stand-in, not by the core session - so a synthetic token is enough to reach the
// handler under test while still proving the login gate is in front of it.
func signedIn(t *testing.T, h *Handler, method, target string, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	var body io.Reader
	if form != nil {
		body = strings.NewReader(form.Encode())
	}
	req := httptest.NewRequest(method, target, body)
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	enc, err := h.cookie.Encode("test-session-token")
	if err != nil {
		t.Fatalf("encode session cookie: %v", err)
	}
	req.AddCookie(&http.Cookie{Name: h.cfg.CookieName, Value: enc})
	rec := httptest.NewRecorder()
	h.Handler().ServeHTTP(rec, req)
	return rec
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

// TestSnippetRequiresLogin is UI-18's boundary: the snippet is generated behind
// login, and it is reached by POST. A GET was how the secret used to travel in a
// query string, where it lands in access logs, browser history and any pasted link.
func TestSnippetRequiresLogin(t *testing.T) {
	h := testHandler(t)
	cases := []struct {
		method string
		target string
		form   url.Values
	}{
		{http.MethodGet, "/snippet", nil},
		{http.MethodPost, "/snippet", url.Values{"whsec": {"whsec_ABCDEF"}}},
	}
	for _, tc := range cases {
		var body io.Reader
		if tc.form != nil {
			body = strings.NewReader(tc.form.Encode())
		}
		rec := httptest.NewRecorder()
		h.Handler().ServeHTTP(rec, httptest.NewRequest(tc.method, tc.target, body))
		if rec.Code != http.StatusSeeOther {
			t.Fatalf("%s %s without a session: status = %d, want 303 to login", tc.method, tc.target, rec.Code)
		}
		if loc := rec.Header().Get("Location"); !strings.HasPrefix(loc, "/login?next=") {
			t.Fatalf("%s %s redirected to %q, want the login page carrying next", tc.method, tc.target, loc)
		}
	}
}

// TestSnippetRendersVerifierLibrary is UI-18: the verification snippet names the
// Standard Webhooks Go verifier library and carries the endpoint secret, and is
// generated server-side from a value the portal never sends to the core. The
// secret arrives in the POST body, so it is not in any URL.
func TestSnippetRendersVerifierLibrary(t *testing.T) {
	core := newFakeCore(t)
	h := testHandlerAt(t, core.srv.URL)
	rec := signedIn(t, h, http.MethodPost, "/snippet", url.Values{
		"endpoint": {"end1"}, "whsec": {"whsec_ABCDEF"},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /snippet status = %d, want 200", rec.Code)
	}
	text := rec.Body.String()
	if !strings.Contains(text, "standardwebhooks") || !strings.Contains(text, "NewWebhook") {
		t.Fatalf("snippet did not include the Standard Webhooks verifier library:\n%s", text)
	}
	if !strings.Contains(text, "whsec_ABCDEF") {
		t.Fatalf("snippet did not carry the endpoint secret:\n%s", text)
	}
	// The form posts, and nothing about the secret is in a URL.
	if !strings.Contains(text, `method="post" action="/snippet"`) {
		t.Fatalf("the snippet form does not post:\n%s", text)
	}
	// UI-18: the snippet is the portal's own render, so the core API is never
	// asked to do anything with the secret.
	for _, call := range core.calls() {
		if strings.Contains(call, "snippet") {
			t.Fatalf("the portal sent the snippet request to the core API: %q", call)
		}
	}
}

// seedEndpointsPage registers the three core reads the endpoint detail page makes:
// the endpoint itself, its subscriptions and UI-14's subscribable types.
func seedEndpointsPage(t *testing.T, f *fakeCore, verified bool) {
	t.Helper()
	f.on(http.MethodGet, "/v1/consumer/endpoints/end1",
		`{"id":"end1","name":"orders-hook","url":"https://1.1.1.1/hook","status":"active","verified":`+
			boolJSON(verified)+`,"created_at":"2026-01-01T00:00:00Z"}`, http.StatusOK)
	f.on(http.MethodGet, "/v1/consumer/endpoints/end1/subscriptions",
		`{"subscriptions":[{"endpoint_id":"end1","event_type":"order.shipped","status":"active"}]}`, http.StatusOK)
	f.on(http.MethodGet, "/v1/consumer/event-types",
		`{"event_types":[{"name":"order.shipped"},{"name":"order.refunded"}]}`, http.StatusOK)
}

func boolJSON(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// TestEndpointPageOffersOnlySubscribableTypes is UI-14's read side as the browser
// sees it: the subscribe control offers exactly the types the API's gate accepts,
// and the panel shows the caller's existing subscription. A free-text field would
// offer a control the API refuses, which §14 forbids.
func TestEndpointPageOffersOnlySubscribableTypes(t *testing.T) {
	core := newFakeCore(t)
	seedEndpointsPage(t, core, true)
	h := testHandlerAt(t, core.srv.URL)

	rec := signedIn(t, h, http.MethodGet, "/endpoints/end1", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /endpoints/end1 status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		`<option value="order.shipped">`,
		`<option value="order.refunded">`,
		`name="event_type"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("endpoint page is missing %s:\n%s", want, body)
		}
	}
	// The free-text subscribe field is gone: the offered set is the API's.
	if strings.Contains(body, `name="event_type" type="text"`) {
		t.Fatalf("the subscribe control is still a free-text field:\n%s", body)
	}
	// The caller's own subscription is on the page.
	if !strings.Contains(body, "order.shipped") || !strings.Contains(body, "Opt out") {
		t.Fatalf("the subscriptions panel did not render the caller's subscription:\n%s", body)
	}
}

// TestEndpointVerifyAsksForTheChallenge is the fix for a control that could never
// succeed: the portal holds no copy of the ownership challenge (only its hash is
// stored server-side), so the verify button asks the user to paste it. A verified
// endpoint is not offered the control at all.
func TestEndpointVerifyAsksForTheChallenge(t *testing.T) {
	core := newFakeCore(t)
	seedEndpointsPage(t, core, false)
	h := testHandlerAt(t, core.srv.URL)

	body := signedIn(t, h, http.MethodGet, "/endpoints/end1", nil).Body.String()
	if !strings.Contains(body, "Verify ownership") || !strings.Contains(body, `name="token"`) {
		t.Fatalf("an unverified endpoint does not offer a challenge form:\n%s", body)
	}
	// The challenge is posted from the form, never pre-filled: an empty hidden
	// field is what made the old control fail with a 422 every time.
	if strings.Contains(body, `value="vrf_`) || strings.Contains(body, `type="hidden" name="token"`) {
		t.Fatalf("the challenge is pre-filled on the page:\n%s", body)
	}

	verified := newFakeCore(t)
	seedEndpointsPage(t, verified, true)
	h2 := testHandlerAt(t, verified.srv.URL)
	if b := signedIn(t, h2, http.MethodGet, "/endpoints/end1", nil).Body.String(); strings.Contains(b, "Verify ownership") {
		t.Fatalf("a verified endpoint still offers the ownership challenge:\n%s", b)
	}
}

// TestEndpointEditPostsThePatch is UI-13's edit as the browser drives it: the
// portal turns the form into the API's PATCH and sends every edited field, so the
// same edit from a script gets the same answer. The notification email is one of
// those fields, and an empty one is how it is cleared.
func TestEndpointEditPostsThePatch(t *testing.T) {
	core := newFakeCore(t)
	core.on(http.MethodPatch, "/v1/consumer/endpoints/end1",
		`{"id":"end1","name":"renamed","url":"https://1.1.1.1/hook2","status":"active","verified":true,"created_at":"2026-01-01T00:00:00Z"}`,
		http.StatusOK)
	h := testHandlerAt(t, core.srv.URL)

	rec := signedIn(t, h, http.MethodPost, "/endpoints/end1/edit", url.Values{
		"name": {"renamed"}, "url": {"https://1.1.1.1/hook2"}, "notification_email": {"ops@example.test"},
	})
	if rec.Code != http.StatusSeeOther || rec.Header().Get("Location") != "/endpoints/end1" {
		t.Fatalf("edit: status = %d location = %q, want 303 to the endpoint page", rec.Code, rec.Header().Get("Location"))
	}
	sent := core.lastBody(http.MethodPatch, "/v1/consumer/endpoints/end1")
	for _, want := range []string{`"name":"renamed"`, `"url":"https://1.1.1.1/hook2"`, `"notification_email":"ops@example.test"`} {
		if !strings.Contains(sent, want) {
			t.Fatalf("the edit PATCH did not carry %s: %s", want, sent)
		}
	}
}

// TestEndpointRotateShowsTheNewSecretOnce is UI-13's rotation from the browser:
// the portal posts the API's rotate route and renders the page from that same
// response, because the new secret exists in exactly one reply. It must not travel
// in a cookie - the flash cookie is unsigned and would then carry a credential.
func TestEndpointRotateShowsTheNewSecretOnce(t *testing.T) {
	core := newFakeCore(t)
	core.on(http.MethodPost, "/v1/consumer/endpoints/end1/rotate",
		`{"signing_secret":"whsec_ROTATED","signing_secret_hint":"whsec_ROTATED"}`+"\n", http.StatusCreated)
	seedEndpointsPage(t, core, true)
	h := testHandlerAt(t, core.srv.URL)

	rec := signedIn(t, h, http.MethodPost, "/endpoints/end1/rotate", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("rotate: status = %d, want the endpoint page rendered from the response", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "whsec_ROTATED") {
		t.Fatalf("the page after a rotation does not show the new secret:\n%s", body)
	}
	// The control the page posts is the API's rotate route: a button whose action
	// has no route is the "control that can never succeed" failure mode.
	if !strings.Contains(body, `action="/endpoints/end1/rotate"`) {
		t.Fatalf("the rotate control does not post to the rotate route:\n%s", body)
	}
	for _, c := range rec.Result().Cookies() {
		if strings.Contains(c.Value, "whsec_ROTATED") {
			t.Fatalf("the rotated secret was written into cookie %q", c.Name)
		}
	}
}
