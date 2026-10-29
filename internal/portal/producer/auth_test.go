package producer

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/m7jay/pigeon/internal/oidc"
	"github.com/m7jay/pigeon/internal/portalui"
)

// The producer portal's own authentication surface: the password login (which is
// where the core's session becomes the portal's cookie), FR-111's reset pages and
// FR-106's OIDC hops. Every decision is the core API's; the portal's job is to
// carry a form to the API, a cookie back, and never to become an oracle of its
// own.

// --- login (FR-112, AC-49/AC-51) -----------------------------------------

func TestLoginStartsWithTheCoresSessionAndRefusesANonMemberSession(t *testing.T) {
	t.Run("a member session becomes the portal's own cookie", func(t *testing.T) {
		core := newFakeCore(t)
		expires := time.Now().Add(2 * time.Hour).UTC().Truncate(time.Second)
		core.on(http.MethodPost, "/v1/session",
			`{"token":"core-session-token","expires_at":"`+expires.Format(time.RFC3339)+`",`+
				`"user":{"email":"owner@acme.test","must_change_password":false},`+
				`"claims":{"producer_id":"prd_1","role":"owner"}}`, http.StatusCreated)
		h := newPortalAt(t, core.srv.URL)

		rec := postAnonymous(t, h, "/login", url.Values{"email": {"owner@acme.test"}, "password": {"correct-horse"}})
		if rec.Code != http.StatusSeeOther {
			t.Fatalf("POST /login status = %d, want 303; body: %s", rec.Code, rec.Body.String())
		}
		if loc := rec.Header().Get("Location"); loc != "/onboarding" {
			t.Fatalf("POST /login redirected to %q, want the checklist", loc)
		}
		got := core.lastRequest(http.MethodPost, "/v1/session")
		if got.Path != "/v1/session" {
			t.Fatalf("the login never POSTed /v1/session (calls: %v)", core.calls())
		}
		mustContain(t, "login call", got.Body, `"email":"owner@acme.test"`, `"password":"correct-horse"`)
		mustNotContain(t, "login URL", got.URI, "correct-horse")

		c := sessionCookie(rec, h.cfg.CookieName)
		if c == nil {
			t.Fatalf("POST /login set no session cookie")
		}
		if !c.HttpOnly || !c.Secure || c.SameSite != http.SameSiteLaxMode {
			t.Fatalf("the session cookie is missing HttpOnly/Secure/SameSite=Lax (AC-49): %+v", c)
		}
		if tok, err := h.cookie.Decode(c.Value); err != nil || tok != "core-session-token" {
			t.Fatalf("the session cookie does not carry the core's token (token=%q err=%v)", tok, err)
		}
		// The lifetime is the core's own expires_at, never a portal-side constant.
		want := int(time.Until(expires).Seconds())
		if c.MaxAge < want-20 || c.MaxAge > want+20 {
			t.Fatalf("session cookie Max-Age = %d, want the core's %d s derived from expires_at (AC-51)", c.MaxAge, want)
		}
	})

	t.Run("a session with no producer membership is refused and stored nowhere", func(t *testing.T) {
		core := newFakeCore(t)
		core.on(http.MethodPost, "/v1/session",
			`{"token":"core-session-token","expires_at":"2030-01-01T00:00:00Z",`+
				`"user":{"email":"nobody@example.test","must_change_password":false},`+
				`"claims":{"producer_id":"","role":""}}`, http.StatusCreated)
		core.on(http.MethodDelete, "/v1/session", "", http.StatusNoContent)
		h := newPortalAt(t, core.srv.URL)

		rec := postAnonymous(t, h, "/login", url.Values{"email": {"nobody@example.test"}, "password": {"correct-horse"}})
		if rec.Code != http.StatusForbidden {
			t.Fatalf("POST /login with no producer claim: status = %d, want 403; body: %s", rec.Code, rec.Body.String())
		}
		if c := sessionCookie(rec, h.cfg.CookieName); c != nil {
			t.Fatalf("a session with no producer membership was stored in a cookie: %+v", c)
		}
		mustContain(t, "non-member login", rec.Body.String(), "not a producer member")
		if core.lastRequest(http.MethodDelete, "/v1/session").Path != "/v1/session" {
			t.Fatalf("the refused non-member session was left un-revoked")
		}
	})
}

// --- FR-111: password reset ----------------------------------------------

// blankFieldValue replaces the value of every "name=field value=..." form field
// with an empty one, so two pages can be compared for everything but the
// per-request token they carry.
func blankFieldValue(body, field string) string {
	marker := `name="` + field + `" value="`
	out, from := body, 0
	for {
		i := strings.Index(out[from:], marker)
		if i < 0 {
			return out
		}
		i += from
		rest := out[i+len(marker):]
		j := strings.IndexByte(rest, '"')
		if j < 0 {
			return out
		}
		out = out[:i+len(marker)] + rest[j:]
		from = i + len(marker)
	}
}

func withoutCSRFToken(body string) string { return blankFieldValue(body, "csrf_token") }

func TestPasswordResetPagesCannotEnumerateAccounts(t *testing.T) {
	t.Run("the forgot form says the same thing for every core outcome", func(t *testing.T) {
		core := newFakeCore(t)
		// The API answers 202 with the same body whether or not the address has an
		// account (internal/httpapi/passwordreset.go:38); the portal must not add a
		// distinction of its own.
		core.on(http.MethodPost, "/v1/password-resets",
			`{"status":"accepted","detail":"if that address has an account, a single-use reset link has been sent to it"}`,
			http.StatusAccepted)
		h := newPortalAt(t, core.srv.URL)

		const sentence = "If that address has an account, a link has been sent."
		known := postAnonymous(t, h, "/forgot", url.Values{"email": {"owner@acme.test"}})
		unknown := postAnonymous(t, h, "/forgot", url.Values{"email": {"nobody@example.test"}})
		for _, rec := range []*httptest.ResponseRecorder{known, unknown} {
			if rec.Code != http.StatusOK {
				t.Fatalf("POST /forgot status = %d, want 200; body: %s", rec.Code, rec.Body.String())
			}
			mustContain(t, "forgot page", rec.Body.String(), sentence)
		}
		// The two answers are the same page: nothing distinguishes the address that
		// has an account from the one that does not, and neither address is echoed.
		if withoutCSRFToken(known.Body.String()) != withoutCSRFToken(unknown.Body.String()) {
			t.Fatalf("the forgot page answered differently for an address with and without an account:\n%s\n---\n%s",
				known.Body.String(), unknown.Body.String())
		}
		mustNotContain(t, "forgot page", known.Body.String(), "nobody@example.test")
		mustNotContain(t, "forgot page", unknown.Body.String(), "owner@acme.test")

		got := core.lastRequest(http.MethodPost, "/v1/password-resets")
		if got.Path != "/v1/password-resets" {
			t.Fatalf("the forgot form never asked the API (calls: %v)", core.calls())
		}
		mustContain(t, "forgot call", got.Body, `"email":`)
	})

	t.Run("a core error is not an oracle either", func(t *testing.T) {
		core := newFakeCore(t)
		core.on(http.MethodPost, "/v1/password-resets",
			`{"error":{"code":"invalid_request","message":"email: must be a valid address"}}`, http.StatusUnprocessableEntity)
		h := newPortalAt(t, core.srv.URL)

		rec := postAnonymous(t, h, "/forgot", url.Values{"email": {"not-an-address"}})
		mustContain(t, "forgot page (core error)", rec.Body.String(), "email: must be a valid address")
		mustNotContain(t, "forgot page (core error)", rec.Body.String(),
			"no such account", "does not exist", "not found", "unknown address", "not-an-address")
	})

	t.Run("the reset accept page renders one message for every unusable token", func(t *testing.T) {
		core := newFakeCore(t)
		// The API answers one 404 for every way a token can be unusable
		// (internal/httpapi/passwordreset.go:98).
		core.on(http.MethodPost, "/v1/password-resets/accept",
			`{"error":{"code":"not_found","message":"that reset link is not valid, has expired, or has already been used"}}`,
			http.StatusNotFound)
		h := newPortalAt(t, core.srv.URL)

		a := postAnonymous(t, h, "/reset", url.Values{"token": {"prt_bad_one"}, "new_password": {"a-new-secret"}})
		b := postAnonymous(t, h, "/reset", url.Values{"token": {"prt_bad_two"}, "new_password": {"another-secret"}})
		// Everything but the token the user opened is identical: one message, one
		// page, for every unusable token.
		norm := func(body string) string { return blankFieldValue(withoutCSRFToken(body), "token") }
		if norm(a.Body.String()) != norm(b.Body.String()) {
			t.Fatalf("two unusable tokens rendered different pages:\n%s\n---\n%s", a.Body.String(), b.Body.String())
		}
		mustContain(t, "reset page", a.Body.String(),
			"that reset link is not valid, has expired, or has already been used")
		// The new password is never echoed, and neither it nor the token lands in a URL.
		mustNotContain(t, "reset page", a.Body.String(), "a-new-secret", "another-secret")
		got := core.lastRequest(http.MethodPost, "/v1/password-resets/accept")
		if got.Path != "/v1/password-resets/accept" {
			t.Fatalf("the reset form never asked the API (calls: %v)", core.calls())
		}
		mustContain(t, "accept call", got.Body, `"token":"prt_bad_two"`, `"new_password":"another-secret"`)
		mustNotContain(t, "accept URL", got.URI, "prt_bad_two", "another-secret")
	})

	t.Run("the reset token is rendered only on the page the link opened", func(t *testing.T) {
		core := newFakeCore(t)
		h := newPortalAt(t, core.srv.URL)
		const tok = "prt_the_one_from_the_link"

		page := getAnonymous(h, "/reset?token="+tok)
		if page.Code != http.StatusOK {
			t.Fatalf("GET /reset status = %d, want 200", page.Code)
		}
		mustContain(t, "reset page", page.Body.String(), `name="token" value="`+tok+`"`)
		for _, other := range []string{"/login", "/forgot"} {
			mustNotContain(t, "reset token leak", getAnonymous(h, other).Body.String(), tok)
		}
		for _, c := range page.Result().Cookies() {
			if strings.Contains(c.Value, tok) {
				t.Fatalf("the reset token was written into cookie %q", c.Name)
			}
		}
	})
}

// --- FR-106: optional OIDC login -----------------------------------------

// newFakeIdP is a stand-in for an OIDC issuer: it serves the discovery document
// the portal's relying party reads. The issuer it advertises is its own URL, which
// is what Discover requires the document to echo.
func newFakeIdP(t *testing.T) *httptest.Server {
	t.Helper()
	var issuer string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/.well-known/openid-configuration" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"issuer":                 issuer,
			"authorization_endpoint": issuer + "/authorize",
			"token_endpoint":         issuer + "/token",
			"jwks_uri":               issuer + "/jwks",
		})
	}))
	issuer = srv.URL
	t.Cleanup(srv.Close)
	return srv
}

func newPortalWithOIDC(t *testing.T, coreURL, issuer string) *Handler {
	t.Helper()
	h, err := New(Config{
		Addr:         ":0",
		CoreURL:      coreURL,
		Brand:        portalui.NeutralFallback,
		CookieName:   portalCookieName,
		CookieSecret: portalCookieSecret,
		OIDC: oidc.Config{
			Issuer:       issuer,
			ClientID:     "client-1",
			ClientSecret: "secret-1",
			RedirectURL:  "https://portal.example/login/oidc/callback",
		},
	})
	if err != nil {
		t.Fatalf("New() with OIDC: %v", err)
	}
	return h
}

func TestOIDCControlIsAbsentWhenUnconfiguredAndCallbackChecksState(t *testing.T) {
	t.Run("unconfigured: no control, and the start route is a 404", func(t *testing.T) {
		core := newFakeCore(t)
		h := newPortalAt(t, core.srv.URL) // a zero OIDC config

		login := getAnonymous(h, "/login")
		if login.Code != http.StatusOK {
			t.Fatalf("GET /login status = %d, want 200", login.Code)
		}
		mustNotContain(t, "login page", login.Body.String(), "/login/oidc", "single sign-on")

		start := getAnonymous(h, "/login/oidc")
		if start.Code != http.StatusNotFound {
			t.Fatalf("GET /login/oidc with no provider: status = %d, want 404", start.Code)
		}
	})

	t.Run("configured: the start hop is a one-shot state/nonce/PKCE cookie", func(t *testing.T) {
		idp := newFakeIdP(t)
		core := newFakeCore(t)
		h := newPortalWithOIDC(t, core.srv.URL, idp.URL)

		start := getAnonymous(h, "/login/oidc")
		if start.Code != http.StatusSeeOther {
			t.Fatalf("GET /login/oidc status = %d, want 303; body: %s", start.Code, start.Body.String())
		}
		loc := start.Header().Get("Location")
		u, err := url.Parse(loc)
		if err != nil {
			t.Fatalf("the start hop redirected to an unparseable URL %q: %v", loc, err)
		}
		if u.Host != strings.TrimPrefix(idp.URL, "http://") {
			t.Fatalf("the start hop redirected to %q, not the configured issuer", loc)
		}
		q := u.Query()
		for _, p := range []string{"state", "nonce", "code_challenge"} {
			if q.Get(p) == "" {
				t.Fatalf("the authorization request carries no %s: %q", p, loc)
			}
		}
		if q.Get("code_challenge_method") != "S256" {
			t.Fatalf("the PKCE challenge is not S256: %q", loc)
		}
		if q.Get("response_type") != "code" || q.Get("client_id") == "" {
			t.Fatalf("the request is not an authorization-code request for this client: %q", loc)
		}

		c := sessionCookie(start, oidcCookieName)
		if c == nil {
			t.Fatalf("the start hop set no %s cookie", oidcCookieName)
		}
		if !c.HttpOnly || !c.Secure || c.SameSite != http.SameSiteLaxMode {
			t.Fatalf("the handshake cookie is missing HttpOnly/Secure/SameSite=Lax (AC-49): %+v", c)
		}
		if c.MaxAge <= 0 {
			t.Fatalf("the handshake cookie is not one-shot (Max-Age %d)", c.MaxAge)
		}
		raw, err := h.oidcCookie.Decode(c.Value)
		if err != nil {
			t.Fatalf("the handshake cookie could not be decoded: %v", err)
		}
		parts := strings.SplitN(raw, "|", 4)
		if len(parts) != 4 {
			t.Fatalf("the handshake cookie is not state|nonce|verifier|next: %q", raw)
		}
		if parts[0] != q.Get("state") || parts[1] != q.Get("nonce") {
			t.Fatalf("the cookie's state/nonce do not match the redirect: cookie=%q redirect state=%q nonce=%q",
				raw, q.Get("state"), q.Get("nonce"))
		}
		if parts[2] == "" {
			t.Fatalf("the handshake cookie carries no PKCE verifier")
		}
		if parts[3] != "/onboarding" {
			t.Fatalf("the handshake cookie's next = %q, want /onboarding", parts[3])
		}

		// A callback whose state does not match the cookie is refused, stores no
		// session, never reaches the core, and spends the one-shot cookie.
		req := httptest.NewRequest(http.MethodGet, "/login/oidc/callback?state=not-the-state&code=abc", nil)
		req.AddCookie(&http.Cookie{Name: oidcCookieName, Value: c.Value})
		rec := httptest.NewRecorder()
		h.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("a mismatched callback: status = %d, want 403; body: %s", rec.Code, rec.Body.String())
		}
		if sc := sessionCookie(rec, h.cfg.CookieName); sc != nil {
			t.Fatalf("a refused callback stored a session cookie: %+v", sc)
		}
		if spent := sessionCookie(rec, oidcCookieName); spent == nil || spent.MaxAge >= 0 {
			t.Fatalf("the one-shot handshake cookie was not spent: %+v", spent)
		}
		if core.lastRequest(http.MethodPost, "/v1/session/oidc").Path != "" {
			t.Fatalf("a refused callback still called the core: %v", core.calls())
		}
	})
}
