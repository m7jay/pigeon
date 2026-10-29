package consumer

import (
	"html"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/m7jay/pigeon/internal/portal"
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
	reqs     []captured
}

// captured is one request the portal made to the core stand-in, in full: the URI
// with its query, the headers and the body. Two of UI-15/UI-16's claims are about
// what the portal *sends* rather than what it renders - the caller's scoped
// bearer, replay's Idempotency-Key header, and the absence of any consumer id -
// and none of those are visible in the portal's own response.
type captured struct {
	Method string
	Path   string
	URI    string // path plus query string
	Auth   string // the Authorization header the portal attached
	Idem   string // the Idempotency-Key header (FR-1107)
	Body   string
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
		f.reqs = append(f.reqs, captured{
			Method: r.Method, Path: r.URL.Path, URI: r.URL.RequestURI(),
			Auth: r.Header.Get("Authorization"), Idem: r.Header.Get("Idempotency-Key"),
			Body: string(body),
		})
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

// lastRequest is the most recent request the portal made to the core stand-in for
// one route, with its query, headers and body. Assertions about scoping and about
// FR-1107's request key read this rather than the rendered page: the portal's
// answer cannot show what it did *not* send.
func (f *fakeCore) lastRequest(method, path string) captured {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.reqs) - 1; i >= 0; i-- {
		if f.reqs[i].Method == method && f.reqs[i].Path == path {
			return f.reqs[i]
		}
	}
	return captured{}
}

// allRequests is every request the portal made to the core stand-in, most recent
// last. Tests that assert an absence - "the secret is in no request" - read this:
// the portal's own response cannot show what it did not send, and only the core's
// side of the wire can.
func (f *fakeCore) allRequests() []captured {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]captured(nil), f.reqs...)
}

// signedIn drives the portal's own handler with a valid session cookie.
// requireLogin only decodes the cookie - a page render is answered by the core
// stand-in, not by the core session - so a synthetic token is enough to reach the
// handler under test while still proving the login gate is in front of it.
func signedIn(t *testing.T, h *Handler, method, target string, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	return signedInWith(t, h, method, target, form, false)
}

// signedInHtmx is signedIn carrying htmx's request header. The portal answers a
// fragment to htmx and a 303 to every other caller, so a handler's two halves have
// to be driven separately - and a fragment handed to a plain post is the defect
// these tests exist for.
func signedInHtmx(t *testing.T, h *Handler, method, target string, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	return signedInWith(t, h, method, target, form, true)
}

func signedInWith(t *testing.T, h *Handler, method, target string, form url.Values, htmx bool) *httptest.ResponseRecorder {
	t.Helper()
	enc, err := h.cookie.Encode("test-session-token")
	if err != nil {
		t.Fatalf("encode session cookie: %v", err)
	}
	// FR-1009: every state-changing request needs the token the portal's
	// middleware accepts, so the helper carries the pair a rendered form would
	// (the cookie and the matching hidden field) exactly as a browser does. The
	// AC-50 test does the opposite on purpose: it strips or mismatches the token.
	var token string
	if method != http.MethodGet && method != http.MethodHead {
		token, err = portal.MintRandom(h.cfg.CookieSecret, enc)
		if err != nil {
			t.Fatalf("mint csrf token: %v", err)
		}
		if form == nil {
			form = url.Values{}
		}
		form.Set("csrf_token", token)
	}
	var body io.Reader
	if form != nil {
		body = strings.NewReader(form.Encode())
	}
	req := httptest.NewRequest(method, target, body)
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	if htmx {
		req.Header.Set("HX-Request", "true")
	}
	req.AddCookie(&http.Cookie{Name: h.cfg.CookieName, Value: enc})
	if token != "" {
		req.AddCookie(&http.Cookie{Name: portal.CSRFCookieName, Value: token})
	}
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
//
// The POST half carries a valid CSRF token and no session, which is the only
// combination that reaches the login gate: without a token the request is refused
// by FR-1009's middleware before any handler runs (AC-50), and that refusal is
// asserted separately. Order matters and is deliberate - the token check is
// cheaper, needs no session, and applies to anonymous routes too.
func TestSnippetRequiresLogin(t *testing.T) {
	h := testHandler(t)
	rec := httptest.NewRecorder()
	h.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/snippet", nil))
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("GET /snippet without a session: status = %d, want 303 to login", rec.Code)
	}
	if loc := rec.Header().Get("Location"); !strings.HasPrefix(loc, "/login?next=") {
		t.Fatalf("GET /snippet redirected to %q, want the login page carrying next", loc)
	}

	// A POST with a token but no session: the gate is what answers.
	token, err := portal.MintRandom(h.cfg.CookieSecret, "")
	if err != nil {
		t.Fatalf("mint token: %v", err)
	}
	form := url.Values{"whsec": {"whsec_ABCDEF"}, "csrf_token": {token}}
	req := httptest.NewRequest(http.MethodPost, "/snippet", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: portal.CSRFCookieName, Value: token})
	rec = httptest.NewRecorder()
	h.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("POST /snippet without a session: status = %d, want 303 to login", rec.Code)
	}
	if loc := rec.Header().Get("Location"); !strings.HasPrefix(loc, "/login?next=") {
		t.Fatalf("POST /snippet redirected to %q, want the login page carrying next", loc)
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

// TestFragmentSwapTargetsSitOnTheFragmentRoot is the round-2 finding that the swap
// ids sat on an inner <tbody> while the fragment renders a whole <table>, so every
// htmx swap put one table inside another: the browser ends up with two tables, a
// `table table` selector match and the header row rendered twice. The id has to
// belong to the element the fragment itself renders, and it has to be present on
// the page before htmx swaps anything.
func TestFragmentSwapTargetsSitOnTheFragmentRoot(t *testing.T) {
	core := newFakeCore(t)
	seedEndpointsPage(t, core, true)
	core.on(http.MethodGet, "/v1/consumer/endpoints",
		`{"endpoints":[{"id":"end1","name":"orders-hook","url":"https://1.1.1.1/hook","status":"active","verified":true,"created_at":"2026-01-01T00:00:00Z"}]}`,
		http.StatusOK)
	h := testHandlerAt(t, core.srv.URL)

	list := signedInHtmx(t, h, http.MethodGet, "/frag/endpoints", nil).Body.String()
	if !strings.HasPrefix(strings.TrimSpace(list), `<div id="endpoints-list">`) {
		t.Fatalf("the endpoints fragment does not carry the swap target on its root:\n%s", list)
	}
	if strings.Contains(list, "tbody id=") {
		t.Fatalf("the endpoints fragment still puts a swap id on a tbody, which nests a table on every swap:\n%s", list)
	}

	endpointsPage := signedIn(t, h, http.MethodGet, "/endpoints", nil).Body.String()
	if !strings.Contains(endpointsPage, `<div id="endpoints-list">`) {
		t.Fatalf("the endpoints page has no #endpoints-list for the first swap:\n%s", endpointsPage)
	}
	endpointPage := signedIn(t, h, http.MethodGet, "/endpoints/end1", nil).Body.String()
	if !strings.Contains(endpointPage, `<div id="subs-list">`) {
		t.Fatalf("the endpoint page has no #subs-list for the first swap:\n%s", endpointPage)
	}
	if strings.Contains(endpointPage, "tbody id=") {
		t.Fatalf("the endpoint page still puts a swap id on a tbody:\n%s", endpointPage)
	}
}

// TestFragmentControlsPostNativelyToo is the other half of the round-3 finding.
// A form carrying only hx-* attributes has no method and no action, so when the
// htmx script never loads the browser falls back to a GET of the current URL: the
// portal's GET catch-all answers 303 to /endpoints, and the control the user just
// pressed does nothing at all. Every mutating control must therefore carry a real
// method and action as well, so the no-JavaScript path posts to the same handler
// the htmx path does.
func TestFragmentControlsPostNativelyToo(t *testing.T) {
	core := newFakeCore(t)
	seedEndpointsPage(t, core, true)
	// One active and one paused endpoint, because the list renders exactly one of
	// pause/resume per row - the control that matches the row's current state.
	core.on(http.MethodGet, "/v1/consumer/endpoints",
		`{"endpoints":[`+
			`{"id":"end1","name":"orders-hook","url":"https://1.1.1.1/hook","status":"active","verified":true,"created_at":"2026-01-01T00:00:00Z"},`+
			`{"id":"end2","name":"refunds-hook","url":"https://1.1.1.1/refunds","status":"paused","verified":true,"created_at":"2026-01-01T00:00:00Z"}`+
			`]}`, http.StatusOK)
	h := testHandlerAt(t, core.srv.URL)

	// Each entry is the action a control must be able to reach without htmx, and
	// the page that renders that control. seedEndpointsPage gives end1 a single
	// "order.shipped" subscription, so that is the opt-out control on its page.
	for _, tc := range []struct{ action, page string }{
		{"/endpoints/end1/pause", "/endpoints"},
		{"/endpoints/end2/resume", "/endpoints"},
		{"/endpoints/end1/delete", "/endpoints"},
		{"/endpoints/end1/subscriptions/order.shipped/delete", "/endpoints/end1"},
	} {
		body := signedIn(t, h, http.MethodGet, tc.page, nil).Body.String()
		want := `<form method="post" action="` + tc.action + `"`
		if !strings.Contains(body, want) {
			t.Fatalf("%s does not render a native form for %s:\n%s", tc.page, tc.action, body)
		}
		// And the htmx attributes stay: progressive enhancement, not a replacement.
		if !strings.Contains(body, `hx-post="`+tc.action+`"`) {
			t.Fatalf("%s lost the hx-post attribute for %s:\n%s", tc.page, tc.action, body)
		}
	}
}

// TestSubscribeUpdatesThePanelOrRedirects is the round-2 finding that the detail
// page's subscribe control broke the page (UI-14's primary flow). The form posted
// plainly while the handler answered with the bare `frag:subs/list`, so the browser
// navigated to a fragment-only document - no layout, no htmx script - where the
// panel's own "Opt out" control was inert: with htmx absent that form submitted
// natively, which the portal's GET catch-all answers with a 303 to /endpoints, so
// the opt-out never happened. Both halves are asserted here: htmx gets the refreshed
// panel, a plain post gets the page.
func TestSubscribeUpdatesThePanelOrRedirects(t *testing.T) {
	core := newFakeCore(t)
	seedEndpointsPage(t, core, true)
	core.on(http.MethodPost, "/v1/consumer/endpoints/end1/subscriptions",
		`{"id":"sub2","endpoint_id":"end1","event_type":"order.refunded","status":"active"}`, http.StatusCreated)
	h := testHandlerAt(t, core.srv.URL)

	rec := signedInHtmx(t, h, http.MethodPost, "/endpoints/end1/subscriptions",
		url.Values{"event_type": {"order.refunded"}})
	if rec.Code != http.StatusOK {
		t.Fatalf("htmx subscribe: status = %d, want 200 carrying the fragment", rec.Code)
	}
	frag := rec.Body.String()
	if strings.Contains(frag, "<html") {
		t.Fatalf("the htmx answer rendered a document instead of a fragment:\n%s", frag)
	}
	if !strings.Contains(frag, `<div id="subs-list">`) {
		t.Fatalf("the subscribe fragment does not carry the swap target on its root:\n%s", frag)
	}
	if got := core.lastBody(http.MethodPost, "/v1/consumer/endpoints/end1/subscriptions"); !strings.Contains(got, `"event_type":"order.refunded"`) {
		t.Fatalf("the subscribe post did not carry the event type: %s", got)
	}

	plain := signedIn(t, h, http.MethodPost, "/endpoints/end1/subscriptions",
		url.Values{"event_type": {"order.refunded"}})
	if plain.Code != http.StatusSeeOther || plain.Header().Get("Location") != "/endpoints/end1" {
		t.Fatalf("plain subscribe: status = %d location = %q, want a 303 back to the endpoint page",
			plain.Code, plain.Header().Get("Location"))
	}
}

// TestDetailPageActionsAnswerWithPagesNotFragments covers the other two plain forms
// on the endpoint page - pause/resume and delete - which carried the same defect as
// subscribe: the endpoints list drives them with htmx, the detail page does not, so
// the handler has to answer each caller with something that caller can use.
func TestDetailPageActionsAnswerWithPagesNotFragments(t *testing.T) {
	core := newFakeCore(t)
	core.on(http.MethodPatch, "/v1/consumer/endpoints/end1",
		`{"id":"end1","status":"paused"}`, http.StatusOK)
	core.on(http.MethodDelete, "/v1/consumer/endpoints/end1", "", http.StatusNoContent)
	core.on(http.MethodGet, "/v1/consumer/endpoints",
		`{"endpoints":[{"id":"end1","name":"orders-hook","url":"https://1.1.1.1/hook","status":"paused","verified":true,"created_at":"2026-01-01T00:00:00Z"}]}`,
		http.StatusOK)
	h := testHandlerAt(t, core.srv.URL)

	paused := signedIn(t, h, http.MethodPost, "/endpoints/end1/pause", nil)
	if paused.Code != http.StatusSeeOther || paused.Header().Get("Location") != "/endpoints/end1" {
		t.Fatalf("plain pause: status = %d location = %q, want a 303 to the endpoint page",
			paused.Code, paused.Header().Get("Location"))
	}
	deleted := signedIn(t, h, http.MethodPost, "/endpoints/end1/delete", nil)
	if deleted.Code != http.StatusSeeOther || deleted.Header().Get("Location") != "/endpoints" {
		t.Fatalf("plain delete: status = %d location = %q, want a 303 to the endpoints list",
			deleted.Code, deleted.Header().Get("Location"))
	}

	// The list page's control is an htmx form and must still get the fragment.
	swapped := signedInHtmx(t, h, http.MethodPost, "/endpoints/end1/pause", nil)
	if swapped.Code != http.StatusOK {
		t.Fatalf("htmx pause: status = %d, want the refreshed list fragment", swapped.Code)
	}
	if body := swapped.Body.String(); !strings.Contains(body, `<div id="endpoints-list">`) {
		t.Fatalf("the htmx pause fragment does not carry its swap target on its root:\n%s", body)
	}
}

// TestConsumerSnippetRendersEveryLanguageAndNeverSendsTheSecret is UI-18's shared
// renderer from the consumer side. UI-7 (producer) and UI-18 (consumer) are the
// same page for two audiences, so the consumer portal calls the one renderer in
// internal/portal and shows the same four languages the producer portal does -
// curl, Go, Node and Python - rather than a second, drifting implementation of the
// signature rules.
//
// The other half is the property the page must not break: the secret the user
// submits is rendered by this process only and is never sent to the core API. The
// fake core records every request in full - the URI with its query, and the body -
// so "never sent" is asserted on what the portal actually put on the wire rather
// than on what the page shows.
func TestConsumerSnippetRendersEveryLanguageAndNeverSendsTheSecret(t *testing.T) {
	core := newFakeCore(t)
	h := testHandlerAt(t, core.srv.URL)
	const secret = "whsec_RENDER_ONCE_1234567890"

	rec := signedIn(t, h, http.MethodPost, "/snippet", url.Values{
		"endpoint": {"end1"}, "whsec": {secret},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /snippet status = %d, want 200", rec.Code)
	}
	text := rec.Body.String()

	// All four languages, each as its tab label and its stable language id, and
	// each backed by its published verifier (the shell sample uses the spec's own
	// algorithm, so it names openssl).
	for _, want := range []string{
		"(go)", "(node)", "(python)", "(curl)", "curl / shell",
		"standardwebhooks", "pip install standardwebhooks", "openssl",
		secret, // the secret really was rendered
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("the snippet page did not render %q:\n%s", want, text)
		}
	}

	// Rendering the snippets consults nothing: the whole page is built from the
	// value the user posted.
	if got := core.calls(); len(got) != 0 {
		t.Fatalf("the portal asked the core API for something while rendering the snippets: %v", got)
	}
	// And in no request the portal did make does the secret appear - not the path,
	// not the query string, not the body.
	for _, req := range core.allRequests() {
		if strings.Contains(req.Path, secret) || strings.Contains(req.URI, secret) || strings.Contains(req.Body, secret) {
			t.Fatalf("the secret was sent to the core API in %s %s: uri=%q body=%q",
				req.Method, req.Path, req.URI, req.Body)
		}
	}
}

// TestConsumerGuideRendersTheLiveEventTypeSchemas is UI-9 on the consumer side: the
// integration guide is rendered from the live event-type schemas the core exposes
// to this consumer, and from nothing else. The fake core answers one
// GET /v1/consumer/event-types carrying two subscribable types with their versions
// and schemas; the page must show both names, both versions and the schema text,
// and read exactly that one route - the same read UI-14's subscribe control uses,
// so the guide cannot disagree with what the API will accept.
func TestConsumerGuideRendersTheLiveEventTypeSchemas(t *testing.T) {
	core := newFakeCore(t)
	core.on(http.MethodGet, "/v1/consumer/event-types",
		`{"event_types":[`+
			`{"name":"order.shipped","payload_version":3,"payload_schema":{"type":"object","required":["data"]}},`+
			`{"name":"order.refunded","payload_version":2,"payload_schema":{"type":"object","properties":{"amount":{"type":"number"}}}}`+
			`]}`, http.StatusOK)
	h := testHandlerAt(t, core.srv.URL)

	rec := signedIn(t, h, http.MethodGet, "/guide", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /guide status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	// html/template escapes quotes in text nodes as &#34;; the browser shows the
	// schema's own characters, so the assertions read the page as a browser does.
	plain := html.UnescapeString(body)

	for _, want := range []string{
		"order.shipped", "order.refunded", // both names
		"Payload version", ">3<", ">2<", // both versions
		`"required"`, `"properties"`, "amount", // both schemas' text
		"live contract", // the page says this is the live contract, not a copy
	} {
		if !strings.Contains(plain, want) {
			t.Fatalf("the guide is missing %q:\n%s", want, body)
		}
	}
	// The schema is the producer's own document, pretty-printed rather than
	// re-serialised into a normalised shape.
	if !strings.Contains(plain, "\"type\": \"object\"") {
		t.Fatalf("the guide does not pretty-print the schema:\n%s", body)
	}

	// One read, and only the live list: the guide decides nothing the API did not
	// answer.
	if got := core.calls(); len(got) != 1 || got[0] != "GET /v1/consumer/event-types" {
		t.Fatalf("the guide made core calls %v, want only GET /v1/consumer/event-types", got)
	}
}
