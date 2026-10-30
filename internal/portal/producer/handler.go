// Package producer is the producer portal deployable (M2a, §14.1). It is the
// sibling of internal/portal/consumer and holds to the same three invariants:
//
//   - **It is a thin client of the core API.** No database handle, no worker, no
//     state of its own: every panel is one or more documented core API calls with
//     the signed-in producer member's session token (FR-112), which is exactly
//     what makes §14's "nothing the portal can do may be impossible from the API"
//     checkable rather than aspirational. Ripping the container out leaves the API
//     serving the same data (AC-40).
//   - **Authorization stays server-side.** The producer id and the role come from
//     the session the core issues and re-checks on every call; the portal reads
//     them only to decide what to *render*. Hiding a control is not the control -
//     the API refuses the request either way - and an owner-only control is not
//     rendered to a member, but that is a courtesy, not the enforcement.
//   - **FR-1009 by inheritance.** Every form carries the shared csrf_field, no
//     route here changes state on a GET, every cookie this portal sets carries
//     HttpOnly, Secure and SameSite=Lax, and no session lifetime is hardcoded:
//     the cookie's Max-Age is the core's own expires_at.
package producer

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/m7jay/pigeon/internal/oidc"
	"github.com/m7jay/pigeon/internal/portal"
	"github.com/m7jay/pigeon/internal/portalui"
)

// Config is the producer portal's environment, all of it env-driven (OPS-3).
type Config struct {
	Addr         string // listen address, e.g. :8083
	CoreURL      string // base URL of the core API, e.g. http://core:8080
	Brand        portalui.Brand
	CookieName   string
	CookieSecret string // must be non-empty; the portal refuses to start otherwise
	// Env is the plane the portal starts in (test or live). It is a default, not a
	// decision: the signed-in human switches planes with the env form, whose value
	// travels in a cookie, and every API call carries the plane explicitly because
	// a producer member session names no environment of its own (FR-103).
	Env string
	// OIDC is FR-106's optional login, relying-party half: the portal holds the
	// client secret and the redirect URI, and the core - which never sees either -
	// verifies the id_token it is handed. A zero Config means the login page
	// renders no OIDC control at all.
	OIDC   oidc.Config
	Logger *slog.Logger
}

// Handler is the producer portal's http.Handler.
type Handler struct {
	cfg        Config
	core       *portal.Client
	cookie     *portal.SecureCookie
	oidcCookie *portal.SecureCookie
	oidc       *oidc.LazyProvider
	r          *portalui.Renderer
}

// New builds the portal: the encrypted session cookie, the core client, the
// optional OIDC relying party, and the parsed template set.
func New(cfg Config) (*Handler, error) {
	if cfg.CookieSecret == "" {
		return nil, fmt.Errorf("producer portal: PIGEON_PRODUCER_PORTAL_SESSION_SECRET must be set")
	}
	if cfg.CoreURL == "" {
		return nil, fmt.Errorf("producer portal: PIGEON_CORE_API_URL must be set")
	}
	if cfg.CookieName == "" {
		cfg.CookieName = "producer_session"
	}
	if cfg.Env == "" {
		cfg.Env = "test"
	}
	if cfg.Env != "test" && cfg.Env != "live" {
		return nil, fmt.Errorf("producer portal: PIGEON_PRODUCER_PORTAL_ENV must be test or live, got %q", cfg.Env)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	cookie, err := portal.NewSecureCookie(cfg.CookieName, cfg.CookieSecret)
	if err != nil {
		return nil, fmt.Errorf("session cookie: %w", err)
	}
	if cfg.OIDC.ClientEnabled() && cfg.OIDC.ClientSecret == "" {
		return nil, fmt.Errorf("producer portal: PIGEON_OIDC_CLIENT_SECRET is required with an issuer")
	}
	h := &Handler{
		cfg:    cfg,
		core:   portal.New(cfg.CoreURL),
		cookie: cookie,
	}
	if cfg.OIDC.ClientEnabled() {
		// The handshake cookie is encrypted with the same secret as the session
		// cookie but is its own value: a state/nonce/verifier triple must not be
		// interchangeable with a session token.
		h.oidcCookie, err = portal.NewSecureCookie("producer_oidc", cfg.CookieSecret)
		if err != nil {
			return nil, fmt.Errorf("oidc cookie: %w", err)
		}
		h.oidc = oidc.NewLazyProvider(cfg.OIDC)
	}
	h.r, err = buildRenderer()
	if err != nil {
		return nil, fmt.Errorf("templates: %w", err)
	}
	return h, nil
}

// buildRenderer parses the shared base, layers this portal's nav and fragments on
// it, and adds each page (a file under templates/pages with a single
// {{define "content"}} block).
func buildRenderer() (*portalui.Renderer, error) {
	base, err := portalui.Base()
	if err != nil {
		return nil, err
	}
	if _, err := base.ParseFS(templatesFS, "templates/nav.html", "templates/frags.html"); err != nil {
		return nil, fmt.Errorf("parse nav/frags: %w", err)
	}
	pages := map[string]string{}
	for _, name := range pageNames {
		b, err := templatesFS.ReadFile("templates/pages/" + name + ".html")
		if err != nil {
			return nil, err
		}
		pages[name] = string(b)
	}
	return portalui.NewRenderer(base, pages)
}

// pageNames is every page file, each a {{define "content"}} template. The set is
// UI-1..UI-11 plus the auth pages: one page per row, so "reachable in the portal"
// is a thing a test can point at.
var pageNames = []string{
	"login", "forgot", "reset", "onboarding", "consumers", "consumer", "endpoint",
	"deliveries", "delivery", "event", "testevent", "snippet", "guide", "usage",
	"account", "problem",
}

// Handler returns the portal's HTTP surface.
func (h *Handler) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /assets/", portalui.AssetHandler("/assets"))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	// Anonymous surface. FR-111's reset pages and FR-106's OIDC hops are here for
	// the same reason the login form is: the visitor has no session yet.
	mux.HandleFunc("GET /login", h.loginForm)
	mux.HandleFunc("POST /login", h.loginSubmit)
	mux.HandleFunc("GET /login/oidc", h.oidcStart)
	mux.HandleFunc("GET /login/oidc/callback", h.oidcCallback)
	mux.HandleFunc("GET /forgot", h.forgotForm)
	mux.HandleFunc("POST /forgot", h.forgotSubmit)
	mux.HandleFunc("GET /reset", h.resetForm)
	mux.HandleFunc("POST /reset", h.resetSubmit)
	mux.HandleFunc("POST /logout", h.logout)

	mux.HandleFunc("GET /", h.root)
	// The environment switch is a POST: it changes which plane the next requests
	// act in, and a state-changing GET is exactly what FR-1009 forbids.
	mux.HandleFunc("POST /env", h.setEnv)

	// UI-1: the onboarding checklist. Each step POSTs the real API call.
	mux.HandleFunc("GET /onboarding", h.member(h.onboarding))
	mux.HandleFunc("POST /onboarding/event-type", h.member(h.onboardingEventType))
	mux.HandleFunc("POST /onboarding/endpoint", h.member(h.onboardingEndpoint))

	// UI-2 and UI-10: every consumer, every endpoint, and consumer management.
	mux.HandleFunc("GET /consumers", h.member(h.consumersList))
	mux.HandleFunc("POST /consumers", h.member(h.consumerCreate))
	mux.HandleFunc("GET /consumers/{id}", h.member(h.consumerDetail))
	mux.HandleFunc("POST /consumers/{id}/invite", h.member(h.consumerInvite))
	mux.HandleFunc("POST /consumers/{id}/users/{userID}/revoke", h.member(h.consumerRevokeUser))

	// UI-2/UI-6: one endpoint's health, its challenge, and its secret rotation.
	mux.HandleFunc("GET /endpoints/{id}", h.member(h.endpointDetail))
	mux.HandleFunc("POST /endpoints/{id}/verify", h.member(h.endpointVerify))
	mux.HandleFunc("POST /endpoints/{id}/rotate", h.member(h.endpointRotate))
	mux.HandleFunc("POST /endpoints/{id}/pause", h.member(h.endpointSetPaused(true)))
	mux.HandleFunc("POST /endpoints/{id}/resume", h.member(h.endpointSetPaused(false)))

	// UI-3/UI-5: the delivery log, one delivery's trail, and replay.
	mux.HandleFunc("GET /deliveries", h.member(h.deliveryLog))
	mux.HandleFunc("GET /deliveries/{id}", h.member(h.deliveryDetail))
	mux.HandleFunc("POST /deliveries/replay/confirm", h.member(h.replayConfirm))
	mux.HandleFunc("POST /deliveries/replay", h.member(h.replay))

	// UI-1's last step, UI-4's landing page and UI-11's unroutable rows.
	mux.HandleFunc("GET /events/{id}", h.member(h.eventView))
	mux.HandleFunc("GET /test-event", h.member(h.testEventForm))
	mux.HandleFunc("POST /test-event", h.member(h.testEventSubmit))

	// UI-7, UI-9 and UI-8.
	mux.HandleFunc("GET /snippet", h.member(h.snippetPage))
	mux.HandleFunc("POST /snippet", h.member(h.snippetPage))
	mux.HandleFunc("GET /guide", h.member(h.guide))
	mux.HandleFunc("GET /usage", h.member(h.usage))

	// The account page: FR-101's forced change, and the way into FR-111's reset.
	mux.HandleFunc("GET /account", h.member(h.account))
	mux.HandleFunc("POST /account/password", h.member(h.accountPassword))

	// FR-1009/AC-50: the CSRF middleware is the outermost layer, so a refused
	// request never reaches a handler and the anonymous forms (login, forgot,
	// reset) are covered too. A cross-site login is as useful to an attacker as a
	// cross-site write.
	return portal.CSRFMiddleware(h.cfg.CookieSecret, h.cfg.CookieName, mux)
}

// root sends a signed-in visitor to the checklist and everyone else to login.
func (h *Handler) root(w http.ResponseWriter, r *http.Request) {
	if h.tokenFromCookie(r) == "" {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/onboarding", http.StatusSeeOther)
}

// --- session plumbing -----------------------------------------------------

// memberHandler is a page handler that has already been resolved to a producer
// member session, so no handler re-implements the login check and none of them
// can forget it.
type memberHandler func(w http.ResponseWriter, r *http.Request, sess memberSession)

// member resolves the core session and hands the handler its claims. A missing or
// rejected session is a redirect to login with the target preserved.
func (h *Handler) member(next memberHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.tokenFromCookie(r) == "" {
			http.Redirect(w, r, "/login?next="+url.QueryEscape(r.URL.Path), http.StatusSeeOther)
			return
		}
		sess, ok := h.fetchSession(r)
		if !ok {
			// The core refused the token (revoked, expired, or never a member
			// session). The portal cannot fix that and must not paper over it: drop
			// the cookie and send the human back to the form.
			h.clearTokenCookie(w)
			http.Redirect(w, r, "/login?next="+url.QueryEscape(r.URL.Path), http.StatusSeeOther)
			return
		}
		if sess.Claims.ProducerID == "" {
			// An authenticated account with no producer membership. FR-111 is why
			// this portal cannot offer a way in: an invitation from an owner is the
			// only path, so the page says so instead of rendering an empty shell.
			h.renderPageStatus(w, r, http.StatusForbidden, "problem", "Not a producer member", "", problemView{
				Title:   "This account is not a producer member",
				Message: "The producer portal shows one producer's data, and this account has no membership in one. Ask an owner of that producer for an invitation, then open the link from the invitation.",
			})
			return
		}
		next(w, r, sess)
	}
}

// memberSession is the subset of GET /v1/session the portal needs. It is read on
// every request rather than cached in a cookie: the core is the authority on what
// a session may do, and a portal copy of the role would be a second answer to the
// same question (and a stale one the moment an owner changes it).
type memberSession struct {
	User struct {
		ID                 string `json:"id"`
		Email              string `json:"email"`
		MustChangePassword bool   `json:"must_change_password"`
	} `json:"user"`
	Claims struct {
		InstanceAdmin bool   `json:"instance_admin"`
		ProducerID    string `json:"producer_id"`
		Role          string `json:"role"`
	} `json:"claims"`
	ExpiresAt time.Time `json:"expires_at"`
}

// isOwner reports whether the session may render owner-only controls. The API
// still refuses the call for a member (kindOwner); this only decides what the
// page offers, which the package doc says is a courtesy rather than the control.
func (s memberSession) isOwner() bool { return s.Claims.Role == "owner" }

func (h *Handler) fetchSession(r *http.Request) (memberSession, bool) {
	resp, err := h.coreReq(r, http.MethodGet, "/v1/session", nil)
	if err != nil || resp.Status != http.StatusOK {
		return memberSession{}, false
	}
	var sess memberSession
	if err := json.Unmarshal(resp.Body, &sess); err != nil {
		return memberSession{}, false
	}
	return sess, true
}

// tokenFromCookie resolves the encrypted core session token, or "" when there is
// no valid session cookie.
func (h *Handler) tokenFromCookie(r *http.Request) string {
	c, err := r.Cookie(h.cfg.CookieName)
	if err != nil {
		return ""
	}
	tok, err := h.cookie.Decode(c.Value)
	if err != nil {
		return ""
	}
	return tok
}

// csrfToken is the token CSRFMiddleware minted or validated for this request.
func (h *Handler) csrfToken(r *http.Request) string { return portal.TokenFromRequest(r) }

// setTokenCookie encrypts the token into the session cookie with the lifetime the
// CORE issued (AC-51): expires_at is the login response's, so the cookie dies
// when the session does. There is deliberately no portal-side TTL.
func (h *Handler) setTokenCookie(w http.ResponseWriter, token string, expiresAt time.Time) error {
	enc, err := h.cookie.Encode(token)
	if err != nil {
		return err
	}
	maxAge := int(time.Until(expiresAt).Seconds())
	if maxAge < 1 {
		maxAge = 1
	}
	http.SetCookie(w, &http.Cookie{
		Name: h.cfg.CookieName, Value: enc, Path: "/",
		HttpOnly: true, Secure: true, SameSite: http.SameSiteLaxMode, MaxAge: maxAge,
	})
	return nil
}

// clearTokenCookie expires the session cookie.
func (h *Handler) clearTokenCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name: h.cfg.CookieName, Value: "", Path: "/",
		HttpOnly: true, Secure: true, SameSite: http.SameSiteLaxMode, MaxAge: -1,
	})
}

// --- environment (test | live) --------------------------------------------

// envCookie remembers which plane the human is working in. It is a preference,
// not a credential, but it is still a cookie this portal emits, so AC-49 applies
// to it unchanged.
const envCookie = "producer_env"

// env is the plane this request acts in: the remembered one, else the configured
// default. Both are validated, so a hand-edited cookie cannot ask the API for an
// environment that does not exist (and every call still carries it explicitly, so
// a wrong value is a 400 from the API rather than a silent default).
func (h *Handler) env(r *http.Request) string {
	if c, err := r.Cookie(envCookie); err == nil && (c.Value == "test" || c.Value == "live") {
		return c.Value
	}
	return h.cfg.Env
}

func (h *Handler) setEnv(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	env := r.FormValue("env")
	if env != "test" && env != "live" {
		http.Error(w, "env must be test or live", http.StatusBadRequest)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name: envCookie, Value: env, Path: "/",
		HttpOnly: true, Secure: true, SameSite: http.SameSiteLaxMode,
	})
	next := r.FormValue("next")
	if !isLocalPath(next) {
		next = "/onboarding"
	}
	http.Redirect(w, r, next, http.StatusSeeOther)
}

// --- core calls -----------------------------------------------------------

// coreReq issues a core API request with the caller's session token.
func (h *Handler) coreReq(r *http.Request, method, path string, body any) (portal.Response, error) {
	return h.core.Request(r.Context(), h.tokenFromCookie(r), method, path, body)
}

// coreReqHeaders is coreReq with extra headers - replay's Idempotency-Key
// (FR-1107) travels in a header, and the portal passes it through rather than
// reimplementing the bound.
func (h *Handler) coreReqHeaders(r *http.Request, method, path string, body any,
	headers map[string]string) (portal.Response, error) {
	return h.core.RequestHeaders(r.Context(), h.tokenFromCookie(r), method, path, body, headers)
}

// envPath appends the environment a member route requires.
func (h *Handler) envPath(r *http.Request, path string) string {
	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}
	return path + sep + "env=" + url.QueryEscape(h.env(r))
}

// --- rendering ------------------------------------------------------------

func (h *Handler) page(r *http.Request, sess memberSession, title, active string, data any) portalui.Page {
	p := portalui.Page{
		Brand: h.cfg.Brand, Title: title, ActiveNav: active, Data: data,
		Email:     sess.User.Email,
		CSRFToken: h.csrfToken(r),
	}
	if r != nil {
		if k, m := pullFlash(r); m != "" {
			p.Flash, p.FlashKind = m, k
		}
	}
	return p
}

func (h *Handler) renderPage(w http.ResponseWriter, r *http.Request, sess memberSession, name, title, active string, data any) {
	h.renderPageStatus(w, r, http.StatusOK, name, title, active, data)
}

func (h *Handler) renderPageStatus(w http.ResponseWriter, r *http.Request, status int, name, title, active string, data any) {
	var sess memberSession
	if r != nil {
		sess, _ = h.fetchSession(r)
	}
	h.r.Page(w, status, name, h.page(r, sess, title, active, data))
}

// renderPublic renders a page for a visitor with no session (login, forgot,
// reset): the same layout, no user box, no nav.
func (h *Handler) renderPublic(w http.ResponseWriter, r *http.Request, status int, name, title string, data any) {
	h.r.Page(w, status, name, portalui.Page{
		Brand: h.cfg.Brand, Title: title, Data: data, CSRFToken: h.csrfToken(r),
	})
}

// --- flash and small helpers ----------------------------------------------

const flashCookie = "producer_flash"

// setFlash writes a one-shot message. The value's encoding and the cookie's three
// flags live in internal/portal, shared with the consumer portal: both flashes are
// the same shape, and keeping one copy is what stops the truncation fix (a cookie
// value is a restricted byte string, and net/http drops the bytes it cannot carry)
// from landing in one portal only.
func (h *Handler) setFlash(w http.ResponseWriter, kind, msg string) {
	portal.SetFlashCookie(w, flashCookie, kind, msg)
}

// pullFlash reads the message back: "" when there is none.
func pullFlash(r *http.Request) (kind, msg string) {
	return portal.PullFlashCookie(r, flashCookie)
}

// isLocalPath reports whether p is a same-site absolute path. It backs the
// post-login "next" redirect, so a crafted link cannot turn the login form into an
// open redirect.
func isLocalPath(p string) bool {
	return strings.HasPrefix(p, "/") && !strings.HasPrefix(p, "//")
}

// coreError turns a failed core call into the message the page shows. The API's
// error envelope carries the actionable sentence, so the portal shows it rather
// than inventing its own wording for the same refusal.
func coreError(resp portal.Response) string {
	var e struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(resp.Body, &e); err == nil && e.Error.Message != "" {
		return e.Error.Message
	}
	return fmt.Sprintf("the API answered %d", resp.Status)
}

// errCoreUnreachable is what a page shows when the core API cannot be reached at
// all. It is deliberately not a 500 with a stack: an operator reads one line.
var errCoreUnreachable = errors.New("cannot reach the core API")
