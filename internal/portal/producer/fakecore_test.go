package producer

import (
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

// This file is the producer portal's test harness, shaped like the consumer
// portal's (internal/portal/consumer/consumer_test.go): the portal holds no state
// of its own, so "which core API calls did it make, with the session's bearer,
// and what did it render" is the whole of its behaviour. Every page test drives
// the portal's own http.Handler against a fakeCore - a stand-in for the core API
// that records what it was asked and answers with canned JSON. Nothing here
// touches a database or the real API.

const (
	portalCookieName   = "producer_session"
	portalCookieSecret = "test-cookie-secret"
	portalSessionToken = "core-session-token"
	portalProducerID   = "prd_1"
)

// newPortalAt builds a portal pointed at a core API stand-in.
func newPortalAt(t *testing.T, coreURL string) *Handler {
	t.Helper()
	h, err := New(Config{
		Addr:         ":0",
		CoreURL:      coreURL,
		Brand:        portalui.NeutralFallback,
		CookieName:   portalCookieName,
		CookieSecret: portalCookieSecret,
	})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	return h
}

// fakeCore is a minimal stand-in for the core API. It records the full request it
// received (method, path, query, Authorization, Idempotency-Key and body) and
// answers from a per-route table. "What did the portal send the API" is the whole
// behaviour of an edit, a rotation or a replay, and none of it is visible in the
// portal's own response.
type fakeCore struct {
	srv *httptest.Server

	mu       sync.Mutex
	seen     []string
	handlers map[string]http.HandlerFunc
	reqs     []coreCall
}

// coreCall is one request the portal made to the core stand-in, in full.
type coreCall struct {
	Method string
	Path   string
	URI    string // path plus query string
	Auth   string // the Authorization header the portal attached (FR-112/FR-208)
	Idem   string // the Idempotency-Key header (FR-1107)
	Body   string
}

func newFakeCore(t *testing.T) *fakeCore {
	t.Helper()
	f := &fakeCore{handlers: map[string]http.HandlerFunc{}}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Method + " " + r.URL.Path
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.seen = append(f.seen, key)
		f.reqs = append(f.reqs, coreCall{
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

// on registers one canned answer: a status and a JSON body.
func (f *fakeCore) on(method, path, body string, status int) {
	f.handlers[method+" "+path] = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
}

// calls is every request the portal made, in order, as "METHOD /path".
func (f *fakeCore) calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.seen...)
}

// all is a copy of every recorded request.
func (f *fakeCore) all() []coreCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]coreCall(nil), f.reqs...)
}

// lastRequest is the most recent recorded request for one route.
func (f *fakeCore) lastRequest(method, path string) coreCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.reqs) - 1; i >= 0; i-- {
		if f.reqs[i].Method == method && f.reqs[i].Path == path {
			return f.reqs[i]
		}
	}
	return coreCall{}
}

// count is how many times one route was called.
func (f *fakeCore) count(method, path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, r := range f.reqs {
		if r.Method == method && r.Path == path {
			n++
		}
	}
	return n
}

// seedMemberSession registers GET /v1/session with a producer membership of the
// given role. The portal reads the producer id and the role from this session
// (never from the request): a member route is scoped by the bearer the portal
// attaches, and the role only decides what is *rendered*.
func seedMemberSession(core *fakeCore, role string) {
	core.on(http.MethodGet, "/v1/session",
		`{"user":{"id":"usr_1","email":"owner@acme.test","must_change_password":false},`+
			`"claims":{"instance_admin":false,"producer_id":"`+portalProducerID+`","role":"`+role+`"},`+
			`"expires_at":"2026-12-31T00:00:00Z"}`, http.StatusOK)
}

// signedIn drives a member route with a valid session cookie. For a POST it mints
// the CSRF token the page would have rendered, bound to the same session cookie
// value, and submits the cookie/field pair a browser carries (FR-1009).
func signedIn(t *testing.T, h *Handler, method, target string, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	enc, err := h.cookie.Encode(portalSessionToken)
	if err != nil {
		t.Fatalf("encode session cookie: %v", err)
	}
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
	req.AddCookie(&http.Cookie{Name: h.cfg.CookieName, Value: enc})
	if token != "" {
		req.AddCookie(&http.Cookie{Name: portal.CSRFCookieName, Value: token})
	}
	rec := httptest.NewRecorder()
	h.Handler().ServeHTTP(rec, req)
	return rec
}

// getAnonymous issues a GET with no session, so the anonymous pages are driven
// the way a visitor without a cookie reaches them.
func getAnonymous(h *Handler, target string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
	return rec
}

// postAnonymous issues a POST with no session and the CSRF token an anonymous
// form would carry: the session binding is the empty cookie value, which is
// exactly what the middleware checks for a visitor with no session.
func postAnonymous(t *testing.T, h *Handler, target string, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	token, err := portal.MintRandom(h.cfg.CookieSecret, "")
	if err != nil {
		t.Fatalf("mint csrf token: %v", err)
	}
	if form == nil {
		form = url.Values{}
	}
	form.Set("csrf_token", token)
	req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: portal.CSRFCookieName, Value: token})
	rec := httptest.NewRecorder()
	h.Handler().ServeHTTP(rec, req)
	return rec
}

// postRaw issues a POST carrying exactly the cookies the caller names and no
// CSRF field, which is how a refused request is built.
func postRaw(h *Handler, target string, form url.Values, sessionCookie string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if sessionCookie != "" {
		req.AddCookie(&http.Cookie{Name: h.cfg.CookieName, Value: sessionCookie})
	}
	rec := httptest.NewRecorder()
	h.Handler().ServeHTTP(rec, req)
	return rec
}

// --- small assertion helpers ----------------------------------------------

func mustContain(t *testing.T, label, body string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(body, want) {
			t.Fatalf("%s: rendered page is missing %q:\n%s", label, want, body)
		}
	}
}

func mustNotContain(t *testing.T, label, body string, wants ...string) {
	t.Helper()
	for _, bad := range wants {
		if strings.Contains(body, bad) {
			t.Fatalf("%s: rendered page must not contain %q:\n%s", label, bad, body)
		}
	}
}

// attrValue pulls the value of the first occurrence of marker (which must end in a
// quote) out of a rendered page, so an assertion uses the value the page actually
// rendered rather than one the test made up.
func attrValue(t *testing.T, body, marker string) string {
	t.Helper()
	i := strings.Index(body, marker)
	if i < 0 {
		t.Fatalf("rendered page has no %q:\n%s", marker, body)
	}
	rest := body[i+len(marker):]
	j := strings.IndexByte(rest, '"')
	if j < 0 {
		t.Fatalf("rendered page has a malformed %q:\n%s", marker, body)
	}
	return rest[:j]
}

// sessionCookie returns the named cookie from a response, or nil.
func sessionCookie(rec *httptest.ResponseRecorder, name string) *http.Cookie {
	for _, c := range rec.Result().Cookies() {
		if c.Name == name {
			return c
		}
	}
	return nil
}

// checklistStates returns, in order, whether each step line of UI-1's checklist
// rendered as done. The checklist is derived from the API, so its shape is what
// the test asserts on.
func checklistStates(body string) []bool {
	var out []bool
	for _, part := range strings.Split(body, `<li class="step `) {
		switch {
		case strings.HasPrefix(part, "step-done"):
			out = append(out, true)
		case strings.HasPrefix(part, "step-todo"):
			out = append(out, false)
		}
	}
	return out
}

// assertFormsCarryCSRF scans rendered HTML for POST forms and requires each to
// carry a non-empty csrf_token before its closing tag. A GET form (the delivery
// log's filter) needs none: it changes nothing and its query string is the point.
func assertFormsCarryCSRF(t *testing.T, label, html string) {
	t.Helper()
	rest := html
	forms := 0
	for {
		i := strings.Index(rest, "<form")
		if i < 0 {
			break
		}
		rest = rest[i:]
		end := strings.Index(rest, "</form>")
		if end < 0 {
			t.Fatalf("%s: an unterminated <form> in the rendered page", label)
		}
		form := rest[:end]
		rest = rest[end:]
		head := form
		if j := strings.IndexByte(form, '>'); j >= 0 {
			head = form[:j]
		}
		if !strings.Contains(head, `method="post"`) {
			continue
		}
		forms++
		const marker = `name="csrf_token" value="`
		k := strings.Index(form, marker)
		if k < 0 {
			t.Fatalf("%s: a POST form rendered without a csrf_token field:\n%s", label, form)
		}
		tail := form[k+len(marker):]
		q := strings.IndexByte(tail, '"')
		if q <= 0 {
			t.Fatalf("%s: a POST form rendered an empty csrf_token:\n%s", label, form)
		}
	}
	if forms == 0 {
		t.Fatalf("%s: no POST form was rendered at all, so this assertion proved nothing", label)
	}
}
