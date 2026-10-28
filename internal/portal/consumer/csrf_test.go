package consumer

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/m7jay/pigeon/internal/portal"
)

// This file is FR-1009's evidence in the consumer portal: AC-49 (every cookie a
// portal emits carries HttpOnly, Secure and SameSite=Lax), AC-50 (a
// state-changing POST with the token absent or mismatched is 403 with no state
// change, the anonymous routes included) and AC-51 (the session cookie's lifetime
// is the core's expires_at, not a portal constant).
//
// Every assertion is made against the portal's own handler with a core API
// stand-in, because the portal's whole data path is API calls: "the core never
// saw the request" is the observable that proves no state changed, and a status
// code alone would not.

// csrfFromPage pulls the token the page rendered out of the HTML, so the assertions
// use the token a browser would actually submit rather than one the test minted for
// itself.
func csrfFromPage(t *testing.T, html string) string {
	t.Helper()
	const marker = `name="csrf_token" value="`
	i := strings.Index(html, marker)
	if i < 0 {
		t.Fatalf("no csrf_token field in the rendered page:\n%s", html)
	}
	rest := html[i+len(marker):]
	j := strings.IndexByte(rest, '"')
	if j < 0 {
		t.Fatalf("malformed csrf_token field in the rendered page:\n%s", html)
	}
	tok := rest[:j]
	if tok == "" {
		t.Fatalf("the csrf_token field rendered empty, so no form in this page can ever submit")
	}
	return tok
}

// seedSession registers the core's login response with a chosen expiry, which is
// the only thing about the session the portal consumes.
func seedSession(core *fakeCore, expiresAt time.Time) {
	core.on(http.MethodPost, "/v1/session",
		`{"token":"core-session-token","expires_at":"`+expiresAt.UTC().Format(time.RFC3339)+`"}`,
		http.StatusCreated)
}

// loginThroughPortal drives the anonymous login the way a browser does: GET the
// form, submit the token it rendered alongside the credentials.
func loginThroughPortal(t *testing.T, h *Handler, email, password string) *httptest.ResponseRecorder {
	t.Helper()
	get := httptest.NewRecorder()
	h.Handler().ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/login", nil))
	if get.Code != http.StatusOK {
		t.Fatalf("GET /login status = %d, want 200", get.Code)
	}
	token := csrfFromPage(t, get.Body.String())
	form := url.Values{"email": {email}, "password": {password}, "csrf_token": {token}}
	req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: portal.CSRFCookieName, Value: token})
	rec := httptest.NewRecorder()
	h.Handler().ServeHTTP(rec, req)
	return rec
}

// TestPortalCookiesCarryAllThreeFlags is AC-49: every Set-Cookie a portal emits -
// session, flash, CSRF - carries HttpOnly, Secure and SameSite=Lax, and no portal
// cookie lacks any of the three. It drives each flow that emits one and inspects
// the cookies the response actually carried, then asserts that each of the three
// cookie names was seen: a flow that silently stopped setting its cookie would make
// the per-cookie assertions vacuous, so the names are checked too.
func TestPortalCookiesCarryAllThreeFlags(t *testing.T) {
	core := newFakeCore(t)
	seedSession(core, time.Now().Add(24*time.Hour))
	core.on(http.MethodPatch, "/v1/consumer/endpoints/end1", `{}`, http.StatusOK)
	h := testHandlerAt(t, core.srv.URL)

	seen := map[string]bool{}
	check := func(label string, rec *httptest.ResponseRecorder) {
		t.Helper()
		cookies := rec.Result().Cookies()
		if len(cookies) == 0 {
			t.Fatalf("%s emitted no cookie, so AC-49 is unproven for it", label)
		}
		for _, c := range cookies {
			seen[c.Name] = true
			if !c.HttpOnly {
				t.Errorf("%s: cookie %q is not HttpOnly (AC-49)", label, c.Name)
			}
			if !c.Secure {
				t.Errorf("%s: cookie %q is not Secure (AC-49)", label, c.Name)
			}
			if c.SameSite != http.SameSiteLaxMode {
				t.Errorf("%s: cookie %q has SameSite %v, want Lax (AC-49)", label, c.Name, c.SameSite)
			}
		}
	}

	// 1. An anonymous page load: the CSRF cookie is minted here.
	get := httptest.NewRecorder()
	h.Handler().ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/login", nil))
	check("GET /login", get)

	// 2. A successful login: the session cookie.
	login := loginThroughPortal(t, h, "consumer@example.com", "correct-horse")
	if login.Code != http.StatusSeeOther {
		t.Fatalf("POST /login status = %d, want 303; body: %s", login.Code, login.Body.String())
	}
	check("POST /login", login)

	// 3. A state-changing action that the core refuses: the one-shot flash cookie
	// is the only cookie in that response, and it is the one this platform used to
	// emit without SameSite.
	core.on(http.MethodPatch, "/v1/consumer/endpoints/end1",
		`{"error":{"code":"endpoint_invalid","message":"no"}}`, http.StatusUnprocessableEntity)
	check("POST /endpoints/{id}/pause", signedIn(t, h, http.MethodPost, "/endpoints/end1/pause", nil))

	// 4. Logout: the expiring session cookie and the flash together.
	check("POST /logout", signedIn(t, h, http.MethodPost, "/logout", nil))

	for _, want := range []string{portal.CSRFCookieName, "consumer_session", flashCookie} {
		if !seen[want] {
			t.Errorf("no %s cookie was emitted by any flow, so AC-49 is unproven for it", want)
		}
	}
}

// TestCSRFMiddlewareRejectsMissingAndMismatchedTokens is AC-50. Every refusal must
// be a 403 *and* leave the core API untouched: the portal forwards every write, so
// "the core never saw it" is what proves no state changed, where the status code
// alone would not.
//
// The two anonymous routes are covered as well as the authenticated ones. A
// cross-site POST to /login is the login-CSRF attack - it logs a victim into the
// attacker's account - and it needs no session of the victim's to matter. The
// happy path at the end proves the fenced cases fail for the token's reason rather
// than because the route is broken.
func TestCSRFMiddlewareRejectsMissingAndMismatchedTokens(t *testing.T) {
	core := newFakeCore(t)
	seedSession(core, time.Now().Add(24*time.Hour))
	core.on(http.MethodPatch, "/v1/consumer/endpoints/end1", `{}`, http.StatusOK)
	core.on(http.MethodPost, "/v1/invites/accept", `{}`, http.StatusOK)
	core.on(http.MethodGet, "/v1/consumer/endpoints", `{"endpoints":[]}`, http.StatusOK)
	h := testHandlerAt(t, core.srv.URL)

	enc, err := h.cookie.Encode("test-session-token")
	if err != nil {
		t.Fatalf("encode session cookie: %v", err)
	}
	mint := func(session string) string {
		tok, err := portal.MintRandom(h.cfg.CookieSecret, session)
		if err != nil {
			t.Fatalf("mint token: %v", err)
		}
		return tok
	}
	other := mint("a-different-session-cookie")

	post := func(target string, form url.Values, sessionCookie, csrfCookie string) *httptest.ResponseRecorder {
		body := strings.NewReader(form.Encode())
		req := httptest.NewRequest(http.MethodPost, target, body)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if sessionCookie != "" {
			req.AddCookie(&http.Cookie{Name: h.cfg.CookieName, Value: sessionCookie})
		}
		if csrfCookie != "" {
			req.AddCookie(&http.Cookie{Name: portal.CSRFCookieName, Value: csrfCookie})
		}
		rec := httptest.NewRecorder()
		h.Handler().ServeHTTP(rec, req)
		return rec
	}

	good := mint(enc) // the token the page would have rendered for this session
	cases := []struct {
		name          string
		field         string
		csrfCookie    string
		sessionCookie string
	}{
		{"the hidden field is stripped (a same-origin request with the field removed)", "", good, enc},
		{"the cookie is absent", good, "", enc},
		{"the field does not match the cookie", other, good, enc},
		{"the token is bound to a different session", other, other, enc},
		{"there is no token at all", "", "", enc},
		{"there is no session to bind to and no token", "", "", ""},
	}
	for _, tc := range cases {
		rec := post("/endpoints/end1/pause", url.Values{"csrf_token": {tc.field}}, tc.sessionCookie, tc.csrfCookie)
		if rec.Code != http.StatusForbidden {
			t.Errorf("%s: status = %d, want 403", tc.name, rec.Code)
		}
	}
	// The anonymous routes are not an exception: neither a login nor an invitation
	// acceptance is reachable without the token.
	login := post("/login", url.Values{"email": {"victim@example.com"}, "password": {"attacker-chosen"}}, "", "")
	if login.Code != http.StatusForbidden {
		t.Errorf("anonymous POST /login with no token: status = %d, want 403", login.Code)
	}
	invite := post("/invite", url.Values{"token": {"pgni_x"}, "email": {"a@b.c"}, "password": {"12345678"}}, "", "")
	if invite.Code != http.StatusForbidden {
		t.Errorf("anonymous POST /invite with no token: status = %d, want 403", invite.Code)
	}
	for _, call := range core.calls() {
		if strings.Contains(call, "pause") || strings.Contains(call, "/v1/session") || strings.Contains(call, "/v1/invites/accept") {
			t.Fatalf("a refused request still reached the core API: %q (AC-50 requires no state change)", call)
		}
	}

	// Happy path 1: the token the page rendered, for a state-changing action. The
	// session cookie must be the very value the GET carried, because the token is
	// bound to that value and a second Encode of the same token is a different
	// string (a fresh GCM nonce each time) - which is exactly the binding this
	// asserts.
	list := getAs(h, enc, "/endpoints")
	if list.Code != http.StatusOK {
		t.Fatalf("GET /endpoints status = %d, want 200", list.Code)
	}
	rendered := csrfFromPage(t, list.Body.String())
	ok := post("/endpoints/end1/pause", url.Values{"csrf_token": {rendered}}, enc, rendered)
	if ok.Code != http.StatusSeeOther {
		t.Fatalf("POST /endpoints/end1/pause with the page's own token: status = %d, want 303; body: %s", ok.Code, ok.Body.String())
	}
	if got := core.lastBody(http.MethodPatch, "/v1/consumer/endpoints/end1"); !strings.Contains(got, `"paused":true`) {
		t.Fatalf("the accepted POST did not reach the core API, so the happy path proves nothing (body %q)", got)
	}

	// Happy path 2: the anonymous login page's own token.
	loginOK := loginThroughPortal(t, h, "consumer@example.com", "correct-horse")
	if loginOK.Code != http.StatusSeeOther {
		t.Fatalf("POST /login with the page's own token: status = %d, want 303; body: %s", loginOK.Code, loginOK.Body.String())
	}
}

// TestPortalSessionCookieLifetimeFollowsCoreExpiry is AC-51. The cookie's Max-Age
// is the core's issued lifetime, for both of the values the criterion names, and
// it is never the 12 h constant the portal used to hardcode. A core response the
// portal cannot read an expiry out of is a 502 with no cookie at all: guessing a
// lifetime is the defect, so there is no fallback to fall back to.
func TestPortalSessionCookieLifetimeFollowsCoreExpiry(t *testing.T) {
	for _, tc := range []struct {
		name string
		ttl  time.Duration
	}{
		{"24h", 24 * time.Hour},
		{"15m", 15 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			core := newFakeCore(t)
			seedSession(core, time.Now().Add(tc.ttl))
			h := testHandlerAt(t, core.srv.URL)

			rec := loginThroughPortal(t, h, "consumer@example.com", "correct-horse")
			if rec.Code != http.StatusSeeOther {
				t.Fatalf("POST /login status = %d, want 303; body: %s", rec.Code, rec.Body.String())
			}
			var session *http.Cookie
			for _, c := range rec.Result().Cookies() {
				if c.Name == h.cfg.CookieName {
					session = c
				}
			}
			if session == nil {
				t.Fatalf("the login set no %s cookie", h.cfg.CookieName)
			}
			want := int(tc.ttl.Seconds())
			// Max-Age is whole seconds and the expiry is computed before the round
			// trip, so the value is the issued lifetime truncated by the elapsed
			// time - never more than the core issued, and never a stale constant.
			if session.MaxAge > want || session.MaxAge < want-10 {
				t.Fatalf("session cookie Max-Age = %d, want the core's %d s (%s) - the portal must not carry a TTL of its own", session.MaxAge, want, tc.name)
			}
			if session.MaxAge == 12*3600 {
				t.Fatalf("session cookie Max-Age is the retired 12 h constant")
			}
		})
	}

	t.Run("a core response with no expiry starts no session", func(t *testing.T) {
		core := newFakeCore(t)
		core.on(http.MethodPost, "/v1/session", `{"token":"core-session-token"}`, http.StatusCreated)
		h := testHandlerAt(t, core.srv.URL)
		rec := loginThroughPortal(t, h, "consumer@example.com", "correct-horse")
		if rec.Code != http.StatusBadGateway {
			t.Fatalf("POST /login with no expires_at: status = %d, want 502", rec.Code)
		}
		for _, c := range rec.Result().Cookies() {
			if c.Name == h.cfg.CookieName {
				t.Fatalf("a session cookie was set from a response with no expiry: Max-Age %d", c.MaxAge)
			}
		}
	})
}

// TestEveryRenderedPostFormCarriesTheCSRFToken is AC-49/AC-50's structural half.
// The middleware refuses what has no token, so a form that rendered without one
// would be a control the user cannot use - and the AC-50 test would still pass,
// because that test supplies the token itself. This walks every page and every
// fragment the portal can render and requires each POST form to carry a non-empty
// token, so adding a form without the field fails here rather than at 3am.
func TestEveryRenderedPostFormCarriesTheCSRFToken(t *testing.T) {
	core := newFakeCore(t)
	seedDeliveryLog(core, `{"deliveries":[{"id":41,"event_id":"evt_1","event_type":"order.shipped",`+
		`"endpoint_id":"end1","status":"failed","attempts":2,"created_at":"2026-01-01T00:00:00Z",`+
		`"first_attempt_at":null,"delivered_at":null,"duration_ms":null,"replay_of_delivery_id":null}],"next_cursor":""}`,
		http.StatusOK)
	// After seedDeliveryLog, which registers an empty endpoint list for the log's
	// filter control: this test needs a row, so the fragment renders its forms.
	core.on(http.MethodGet, "/v1/consumer/endpoints", `{"endpoints":[`+
		`{"id":"end1","name":"prod","url":"https://example.com/hook","status":"active","verified":true,`+
		`"created_at":"2026-01-01T00:00:00Z"}]}`, http.StatusOK)
	core.on(http.MethodGet, "/v1/consumer/endpoints/end1",
		`{"id":"end1","name":"prod","url":"https://example.com/hook","status":"active","verified":false,`+
			`"notification_email":null,"created_at":"2026-01-01T00:00:00Z"}`, http.StatusOK)
	core.on(http.MethodGet, "/v1/consumer/endpoints/end1/subscriptions",
		`{"subscriptions":[{"endpoint_id":"end1","event_type":"order.shipped","status":"active"}]}`, http.StatusOK)
	core.on(http.MethodGet, "/v1/consumer/event-types", `{"event_types":[{"name":"order.shipped"}]}`, http.StatusOK)
	h := testHandlerAt(t, core.srv.URL)

	enc, err := h.cookie.Encode("test-session-token")
	if err != nil {
		t.Fatalf("encode session cookie: %v", err)
	}
	// The anonymous pages are loaded anonymously on purpose: with a session
	// /login and /invite redirect to the endpoints page, and the form under test
	// is the one an anonymous visitor gets.
	pages := []struct {
		label, target string
		sessionCookie string
	}{
		{"login", "/login", ""},
		{"invite", "/invite", ""},
		{"snippet", "/snippet", enc},
		{"endpoints", "/endpoints", enc},
		{"endpoint detail", "/endpoints/end1", enc},
		{"delivery log (replay panel)", "/deliveries", enc},
		{"endpoints fragment", "/frag/endpoints", enc},
	}
	for _, p := range pages {
		rec := getAs(h, p.sessionCookie, p.target)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status = %d, want 200; body: %s", p.label, rec.Code, rec.Body.String())
		}
		assertPostFormsCarryTokens(t, p.label, rec.Body.String())
	}
}

// getAs issues a GET carrying (or not carrying) one session cookie value, so a
// test can submit a token bound to the same cookie value it fetched the page with.
func getAs(h *Handler, sessionCookie, target string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, target, nil)
	if sessionCookie != "" {
		req.AddCookie(&http.Cookie{Name: h.cfg.CookieName, Value: sessionCookie})
	}
	rec := httptest.NewRecorder()
	h.Handler().ServeHTTP(rec, req)
	return rec
}

// assertPostFormsCarryTokens scans rendered HTML for POST forms and requires each
// to contain a non-empty csrf_token field before its closing tag. A GET form (the
// delivery log's filter) needs no token: it changes nothing and its query string is
// the point of it.
func assertPostFormsCarryTokens(t *testing.T, label, html string) {
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
