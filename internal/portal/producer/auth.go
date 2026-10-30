package producer

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/m7jay/pigeon/internal/oidc"
)

// The portal's own authentication surface: the password login, FR-111's reset
// pages, and FR-106's OIDC hops. It is deliberately the thinnest layer in the
// package - every decision (is this credential good, does this account have a
// claim, is this session alive) is the core API's, and the portal's job is to
// carry a form to the API and a cookie back.

// loginForm renders the login page.
func (h *Handler) loginForm(w http.ResponseWriter, r *http.Request) {
	next := r.URL.Query().Get("next")
	if !isLocalPath(next) {
		next = ""
	}
	h.renderPublic(w, r, http.StatusOK, "login", "Log in", loginView{
		Next: next, Env: h.env(r), OIDCEnable: h.cfg.OIDC.ClientEnabled(),
	})
}

// loginSubmit exchanges a password for a session at the core (POST /v1/session)
// and starts a portal session with it.
//
// The portal is stricter than the API here on one point only: a session that is
// not a producer member session cannot use this portal, so it is refused rather
// than stored. That is not a second authorization rule - it is the same rule the
// API enforces on every producer route (kindMember), applied at the door so the
// human gets a sentence instead of an empty page.
func (h *Handler) loginSubmit(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	email, password := r.FormValue("email"), r.FormValue("password")
	next := r.FormValue("next")
	if !isLocalPath(next) {
		next = "/onboarding"
	}
	if email == "" || password == "" {
		h.renderPublic(w, r, http.StatusBadRequest, "login", "Log in", loginView{
			Next: next, Error: "Email and password are both required.",
			Env: h.env(r), OIDCEnable: h.cfg.OIDC.ClientEnabled(),
		})
		return
	}
	resp, err := h.coreReq(r, http.MethodPost, "/v1/session", map[string]string{
		"email": email, "password": password,
	})
	if err != nil {
		h.renderPublic(w, r, http.StatusBadGateway, "login", "Log in", loginView{
			Next: next, Error: "The API could not be reached.",
			Env: h.env(r), OIDCEnable: h.cfg.OIDC.ClientEnabled(),
		})
		return
	}
	if resp.Status != http.StatusCreated {
		// The API's answer, which is one message for every credential failure so
		// the form is not a user-enumeration oracle.
		h.renderPublic(w, r, http.StatusOK, "login", "Log in", loginView{
			Next: next, Error: coreError(resp),
			Env: h.env(r), OIDCEnable: h.cfg.OIDC.ClientEnabled(),
		})
		return
	}
	var sess sessionResponseJSON
	if err := json.Unmarshal(resp.Body, &sess); err != nil || sess.Token == "" {
		h.renderPublic(w, r, http.StatusBadGateway, "login", "Log in", loginView{
			Next: next, Error: "The API did not return a usable session.",
			Env: h.env(r), OIDCEnable: h.cfg.OIDC.ClientEnabled(),
		})
		return
	}
	if sess.Claims.ProducerID == "" {
		// The session is real (the instance admin's, or a consumer user's) but this
		// portal acts inside one producer and it names none. Revoke it: an unused
		// session left behind is a credential with no owner watching it.
		h.coreReq(r, http.MethodDelete, "/v1/session", nil)
		h.renderPublic(w, r, http.StatusForbidden, "login", "Log in", loginView{
			Next: next,
			Error: "That account is not a producer member, and this portal shows one producer's data. " +
				"Ask an owner of your producer for an invitation.",
			Env: h.env(r), OIDCEnable: h.cfg.OIDC.ClientEnabled(),
		})
		return
	}
	expires, err := time.Parse(time.RFC3339, sess.ExpiresAt)
	if err != nil {
		expires = time.Now().Add(time.Hour)
	}
	if err := h.setTokenCookie(w, sess.Token, expires); err != nil {
		http.Error(w, "cannot start session", http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, next, http.StatusSeeOther)
}

// logout revokes the core session and drops the cookie. A failure to reach the
// core still clears the cookie: leaving a session cookie in a browser because the
// API was down would be the worse of the two failures, and the token expires on
// its own.
func (h *Handler) logout(w http.ResponseWriter, r *http.Request) {
	if h.tokenFromCookie(r) != "" {
		_, _ = h.coreReq(r, http.MethodDelete, "/v1/session", nil)
	}
	h.clearTokenCookie(w)
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

// --- FR-111: password reset -----------------------------------------------

// forgotForm renders the reset-request form.
func (h *Handler) forgotForm(w http.ResponseWriter, r *http.Request) {
	h.renderPublic(w, r, http.StatusOK, "forgot", "Reset your password", forgotView{})
}

// forgotSubmit asks the core to mint a reset (POST /v1/password-resets).
//
// The page says the same thing whatever happened, because the API does: "if that
// address has an account, a link has been sent". A portal that distinguished the
// two cases would rebuild the user-enumeration oracle the API refuses to be.
//
// The link itself is delivered by the deployment (there is no mailer in v1.0):
// see cmd/pigeon's resetNotifier for what an operator has to wire.
func (h *Handler) forgotSubmit(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	resp, err := h.coreReq(r, http.MethodPost, "/v1/password-resets", map[string]string{
		"email": strings.TrimSpace(r.FormValue("email")),
	})
	view := forgotView{Sent: true}
	if err != nil {
		view = forgotView{Error: "The request could not be sent: the API could not be reached."}
	} else if resp.Status != http.StatusAccepted {
		view = forgotView{Error: coreError(resp)}
	}
	status := http.StatusOK
	if view.Error != "" {
		status = http.StatusBadGateway
	}
	h.renderPublic(w, r, status, "forgot", "Reset your password", view)
}

// resetForm renders FR-111's accept form, carrying the token from the link.
func (h *Handler) resetForm(w http.ResponseWriter, r *http.Request) {
	h.renderPublic(w, r, http.StatusOK, "reset", "Choose a new password", resetView{
		Token: r.URL.Query().Get("token"),
	})
}

// resetSubmit spends the reset token (POST /v1/password-resets/accept). The API
// answers one 404 for every unusable token, so the page says one thing for all of
// them: the link does not work, ask for another.
//
// Nothing is signed in afterwards, deliberately. The user proves the new password
// at the login form, which is also where FR-101's forced-change state would
// otherwise be cleared by a reset nobody typed an old password for.
func (h *Handler) resetSubmit(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	view := resetView{Token: strings.TrimSpace(r.FormValue("token"))}
	resp, err := h.coreReq(r, http.MethodPost, "/v1/password-resets/accept", map[string]string{
		"token":        view.Token,
		"new_password": r.FormValue("new_password"),
	})
	switch {
	case err != nil:
		view.Error = "The password was not changed: the API could not be reached."
	case resp.Status != http.StatusNoContent:
		view.Error = coreError(resp)
	default:
		view.Done = true
	}
	status := http.StatusOK
	if view.Error != "" && resp.Status != http.StatusUnprocessableEntity {
		status = http.StatusBadGateway
	}
	h.renderPublic(w, r, status, "reset", "Choose a new password", view)
}

// --- FR-106: optional OIDC login ------------------------------------------

// oidcHandshake is the triple the portal holds in an encrypted cookie between the
// redirect to the issuer and the callback: the state it will compare, the nonce
// the core will compare against the token, and the PKCE verifier whose challenge
// it sent.
type oidcHandshake struct {
	State    string
	Nonce    string
	Verifier string
	Next     string
}

const oidcCookieName = "producer_oidc"

// oidcStart sends the browser to the IdP with a state, a nonce and a PKCE
// challenge, all fresh per attempt, and remembers the triple in an encrypted
// cookie (AC-49's three flags apply to it like any other cookie this portal sets).
func (h *Handler) oidcStart(w http.ResponseWriter, r *http.Request) {
	if !h.cfg.OIDC.ClientEnabled() {
		h.renderPublic(w, r, http.StatusNotFound, "problem", "No OIDC provider", problemView{
			Title:   "OIDC login is not configured",
			Message: "This instance has no OpenID Connect provider, so there is nothing to sign in against. Use the password form, or an invitation.",
		})
		return
	}
	p, err := h.oidc.Provider(r.Context())
	if err != nil {
		// The IdP could not be reached or its discovery document is unusable. The
		// portal says so rather than sending the browser to nowhere.
		h.renderPublic(w, r, http.StatusBadGateway, "problem", "The identity provider is unreachable", problemView{
			Title:   "The identity provider could not be reached",
			Message: err.Error(),
			Detail:  "Password login is unaffected: OIDC is an optional way in (FR-106).",
		})
		return
	}
	hs := oidcHandshake{
		State:    randomURLSafe(24),
		Nonce:    randomURLSafe(24),
		Verifier: randomURLSafe(32),
		Next:     r.URL.Query().Get("next"),
	}
	if !isLocalPath(hs.Next) {
		hs.Next = "/onboarding"
	}
	enc, err := h.oidcCookie.Encode(strings.Join([]string{hs.State, hs.Nonce, hs.Verifier, hs.Next}, "|"))
	if err != nil {
		http.Error(w, "cannot start login", http.StatusInternalServerError)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name: oidcCookieName, Value: enc, Path: "/",
		HttpOnly: true, Secure: true, SameSite: http.SameSiteLaxMode, MaxAge: 600,
	})
	http.Redirect(w, r, p.AuthCodeURL(hs.State, hs.Nonce, hs.Verifier), http.StatusSeeOther)
}

// oidcCallback completes the flow: check the state against the cookie, exchange
// the code at the IdP, and hand the id_token to the CORE, which is the trust
// anchor (it verifies the signature, the issuer, the audience, the expiry and the
// nonce, and refuses an account with no claim).
//
// The portal never decides who the user is. It forwards a token to the API and
// stores the session the API returns, which is what §14 requires of a thin client.
func (h *Handler) oidcCallback(w http.ResponseWriter, r *http.Request) {
	if !h.cfg.OIDC.ClientEnabled() {
		http.NotFound(w, r)
		return
	}
	fail := func(status int, title, msg string) {
		h.renderPublic(w, r, status, "problem", title, problemView{Title: title, Message: msg})
	}
	c, err := r.Cookie(oidcCookieName)
	if err != nil {
		fail(http.StatusBadRequest, "That sign-in attempt has expired", "Start again from the login page.")
		return
	}
	raw, err := h.oidcCookie.Decode(c.Value)
	if err != nil {
		fail(http.StatusBadRequest, "That sign-in attempt could not be read", "Start again from the login page.")
		return
	}
	parts := strings.SplitN(raw, "|", 4)
	if len(parts) != 4 {
		fail(http.StatusBadRequest, "That sign-in attempt could not be read", "Start again from the login page.")
		return
	}
	hs := oidcHandshake{State: parts[0], Nonce: parts[1], Verifier: parts[2], Next: parts[3]}
	// The one-shot cookie is spent either way: a replayed callback must not be able
	// to reuse the state it was issued for.
	http.SetCookie(w, &http.Cookie{
		Name: oidcCookieName, Value: "", Path: "/",
		HttpOnly: true, Secure: true, SameSite: http.SameSiteLaxMode, MaxAge: -1,
	})
	if r.URL.Query().Get("error") != "" {
		fail(http.StatusForbidden, "The identity provider refused the sign-in",
			r.URL.Query().Get("error_description"))
		return
	}
	gotState := r.URL.Query().Get("state")
	if subtle.ConstantTimeCompare([]byte(gotState), []byte(hs.State)) != 1 {
		fail(http.StatusForbidden, "That sign-in attempt did not match",
			"The state parameter did not match the one this browser started with, so the response was refused.")
		return
	}
	code := r.URL.Query().Get("code")
	if code == "" {
		fail(http.StatusBadRequest, "The identity provider sent no authorization code", "Start again from the login page.")
		return
	}
	p, err := h.oidc.Provider(r.Context())
	if err != nil {
		fail(http.StatusBadGateway, "The identity provider is unreachable", err.Error())
		return
	}
	tokens, err := p.Exchange(r.Context(), code, hs.Verifier)
	if err != nil {
		fail(http.StatusForbidden, "The sign-in could not be completed", err.Error())
		return
	}
	// The core verifies the token and mints the session. The portal's nonce
	// travels with it: the API has no browser cookie, so the value the browser
	// holds is what binds the token to this attempt.
	resp, err := h.coreReq(r, http.MethodPost, "/v1/session/oidc", map[string]string{
		"id_token": tokens.IDToken, "nonce": hs.Nonce,
	})
	if err != nil {
		fail(http.StatusBadGateway, "The sign-in could not be completed", "The API could not be reached.")
		return
	}
	if resp.Status != http.StatusCreated {
		fail(http.StatusForbidden, "The sign-in was refused", coreError(resp))
		return
	}
	var sess sessionResponseJSON
	if err := json.Unmarshal(resp.Body, &sess); err != nil || sess.Token == "" {
		fail(http.StatusBadGateway, "The sign-in could not be completed", "The API did not return a usable session.")
		return
	}
	expires, err := time.Parse(time.RFC3339, sess.ExpiresAt)
	if err != nil {
		expires = time.Now().Add(time.Hour)
	}
	if err := h.setTokenCookie(w, sess.Token, expires); err != nil {
		http.Error(w, "cannot start session", http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, hs.Next, http.StatusSeeOther)
}

// randomURLSafe returns n bytes of entropy as base64url, for a state, a nonce or
// a PKCE verifier. crypto/rand is the only source here: a predictable state is a
// login-CSRF, and a predictable verifier is no PKCE at all.
func randomURLSafe(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// A failing CSPRNG is not a condition this portal can recover from, and
		// continuing would mint a guessable state. Panicking is the honest answer;
		// the process supervisor restarts with a working one.
		panic(fmt.Sprintf("producer portal: crypto/rand failed: %v", err))
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

// newRequestKey mints FR-1107's request key for one replay confirmation. It is the
// portal's own identifier for a batch write, not a credential, but it is still
// unguessable so two confirmations cannot collide.
func newRequestKey() string { return "portal-replay-" + randomURLSafe(16) }

// oidcConfigFromEnv is the portal's half of FR-106, read from the same knobs the
// core reads for its half (PIGEON_OIDC_ISSUER/PIGEON_OIDC_CLIENT_ID) plus the two
// the relying party owns. It lives here so cmd/producer-portal stays a thin main.
func OIDCConfig(issuer, clientID, clientSecret, redirectURL, scopes string) oidc.Config {
	cfg := oidc.Config{
		Issuer: issuer, ClientID: clientID, ClientSecret: clientSecret, RedirectURL: redirectURL,
	}
	for _, s := range strings.Split(scopes, ",") {
		if s = strings.TrimSpace(s); s != "" {
			cfg.Scopes = append(cfg.Scopes, s)
		}
	}
	return cfg
}
