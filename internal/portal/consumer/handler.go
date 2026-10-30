// Package consumer is the consumer portal deployable (M2b, §14.2). It is a thin
// server-rendered client of the core API: the browser talks only to the portal
// (same origin), the portal holds the consumer user's scoped core session token
// in an encrypted httpOnly cookie, and every data call is a server-side core API
// request with that token. The consumer id is never taken from the browser — it
// is on the core session claim — so the portal can only ever act within the one
// consumer the session is bound to, and a direct API call with the same token
// gets exactly the same filtered set (FR-110, AC-39).
package consumer

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/m7jay/pigeon/internal/portal"
	"github.com/m7jay/pigeon/internal/portalui"
)

// Config is the consumer portal's environment. Everything is env-driven (OPS-3);
// the cmd wrapper reads the env and fills this.
type Config struct {
	Addr         string // listen address, e.g. :8082
	CoreURL      string // base URL of the core API, e.g. http://core:8080
	Brand        portalui.Brand
	CookieName   string
	CookieSecret string // must be non-empty; refuses to start otherwise
	Logger       *slog.Logger
}

// Handler is the consumer portal's http.Handler.
type Handler struct {
	cfg    Config
	core   *portal.Client
	cookie *portal.SecureCookie
	r      *portalui.Renderer
}

// New builds the portal: the encrypted session cookie, the core client, and the
// parsed template set (shared base + nav + fragments + this portal's pages). It
// refuses to start without a cookie secret, because an unsigned session cookie is
// a forgery waiting to happen.
func New(cfg Config) (*Handler, error) {
	if cfg.CookieSecret == "" {
		return nil, fmt.Errorf("consumer portal: PORTAL_SESSION_SECRET must be set")
	}
	if cfg.CoreURL == "" {
		return nil, fmt.Errorf("consumer portal: PIGEON_CORE_API_URL must be set")
	}
	if cfg.CookieName == "" {
		cfg.CookieName = "consumer_session"
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	cookie, err := portal.NewSecureCookie(cfg.CookieName, cfg.CookieSecret)
	if err != nil {
		return nil, fmt.Errorf("session cookie: %w", err)
	}
	h := &Handler{
		cfg:    cfg,
		core:   portal.New(cfg.CoreURL),
		cookie: cookie,
	}
	h.r, err = buildRenderer()
	if err != nil {
		return nil, fmt.Errorf("templates: %w", err)
	}
	return h, nil
}

// buildRenderer parses the shared base, layers the consumer nav and fragments on
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

// pageNames is the set of page files, each a {{define "content"}} template.
var pageNames = []string{"login", "invite", "me", "endpoints", "endpoint", "deliveries", "delivery", "problem", "snippet", "guide"}

// Handler returns the portal's HTTP surface: assets, then the portal routes.
func (h *Handler) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /assets/", portalui.AssetHandler("/assets"))

	// Liveness for the container healthcheck: the scratch image has no shell,
	// curl or wget, so the binary answers its own /healthz (cmd/consumer-portal).
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	mux.HandleFunc("GET /", h.root)
	mux.HandleFunc("GET /login", h.loginForm)
	mux.HandleFunc("POST /login", h.loginSubmit)
	mux.HandleFunc("POST /logout", h.logout)
	mux.HandleFunc("GET /invite", h.inviteForm)
	mux.HandleFunc("POST /invite", h.inviteSubmit)

	mux.HandleFunc("GET /me", h.requireLogin(h.me))
	mux.HandleFunc("GET /endpoints", h.requireLogin(h.endpointsList))
	mux.HandleFunc("POST /endpoints", h.requireLogin(h.endpointCreate))
	mux.HandleFunc("GET /endpoints/{id}", h.requireLogin(h.endpointDetail))
	mux.HandleFunc("POST /endpoints/{id}/edit", h.requireLogin(h.endpointEdit))
	mux.HandleFunc("POST /endpoints/{id}/rotate", h.requireLogin(h.endpointRotate))
	mux.HandleFunc("POST /endpoints/{id}/challenge", h.requireLogin(h.endpointReissue))
	mux.HandleFunc("POST /endpoints/{id}/verify", h.requireLogin(h.endpointVerify))
	mux.HandleFunc("POST /endpoints/{id}/pause", h.requireLogin(h.endpointSetPaused(true)))
	mux.HandleFunc("POST /endpoints/{id}/resume", h.requireLogin(h.endpointSetPaused(false)))
	mux.HandleFunc("POST /endpoints/{id}/delete", h.requireLogin(h.endpointDelete))
	mux.HandleFunc("POST /endpoints/{id}/subscriptions", h.requireLogin(h.subscribe))
	mux.HandleFunc("POST /endpoints/{id}/subscriptions/{event}/delete", h.requireLogin(h.unsubscribe))
	mux.HandleFunc("GET /frag/endpoints", h.requireLogin(h.fragEndpoints))

	mux.HandleFunc("GET /deliveries", h.requireLogin(h.deliveryLog))
	// UI-15's attempt detail. The route is the delivery id alone: the consumer id
	// is on the session claim, so there is no consumer segment to put in a path.
	mux.HandleFunc("GET /deliveries/{id}", h.requireLogin(h.deliveryDetail))
	// UI-16's two steps: the API's dry run (the count and rate the confirmation
	// states) and the commit, both bounded by the API and neither by the portal.
	mux.HandleFunc("POST /deliveries/replay/confirm", h.requireLogin(h.deliveryReplayConfirm))
	mux.HandleFunc("POST /deliveries/replay", h.requireLogin(h.deliveryReplay))
	// UI-18: the snippet form and its generation are both behind login, and the
	// secret is posted (never a query parameter), so it stays out of access logs,
	// browser history and shared links.
	mux.HandleFunc("GET /snippet", h.requireLogin(h.snippetForm))
	mux.HandleFunc("POST /snippet", h.requireLogin(h.snippetSubmit))
	// UI-9: the consumer-side integration guide, rendered from the live event-type
	// schemas the core exposes to this consumer. Same read UI-14 uses, one route.
	mux.HandleFunc("GET /guide", h.requireLogin(h.guide))
	// FR-1009/AC-50: every state-changing request in this portal carries a CSRF
	// token bound to the session cookie and verified in constant time, and the
	// anonymous /login and /invite are not exceptions - a cross-site login logs a
	// victim into the attacker's account. The middleware is the outermost layer,
	// so a refused request never reaches a handler at all: the "no state change"
	// half of AC-50 is structural rather than a promise each handler keeps.
	return portal.CSRFMiddleware(h.cfg.CookieSecret, h.cfg.CookieName, mux)
}

// root sends a logged-in visitor to their endpoints and everyone else to login.
func (h *Handler) root(w http.ResponseWriter, r *http.Request) {
	if h.tokenFromCookie(r) == "" {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/endpoints", http.StatusSeeOther)
}

// requireLogin wraps a handler that needs a core session: an unauthenticated
// visitor is sent to the login page with the target path preserved so they land
// back where they were.
func (h *Handler) requireLogin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.tokenFromCookie(r) == "" {
			nextPath := r.URL.Path
			if q := r.URL.RawQuery; q != "" {
				nextPath += "?" + q
			}
			http.Redirect(w, r, "/login?next="+url.QueryEscape(nextPath), http.StatusSeeOther)
			return
		}
		next(w, r)
	}
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

// csrfToken is the token CSRFMiddleware minted or validated for this request. The
// middleware owns the lifecycle, so a handler only ever renders it.
func (h *Handler) csrfToken(r *http.Request) string { return portal.TokenFromRequest(r) }

// setTokenCookie encrypts the token into the session cookie and gives the cookie
// the lifetime the CORE issued (AC-51): expiresAt is the login response's
// expires_at, so the cookie expires exactly when the session token does. There is
// deliberately no portal-side TTL constant and no fallback: a hardcoded 12 h
// outlived the core session and was the defect FR-1009 closes.
//
// Max-Age is integral seconds, so the value is truncated rather than rounded -
// rounding up would let the cookie outlive the token it carries by up to a
// second. A core expiry already in the past (a clock-skewed core, or a very slow
// round trip) collapses to the 1 s floor, which is still a cookie that a browser
// will not persist past the first request.
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
		Name:     h.cfg.CookieName,
		Value:    enc,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   true,
		MaxAge:   maxAge,
	})
	return nil
}

// clearTokenCookie expires the session cookie.
func (h *Handler) clearTokenCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     h.cfg.CookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   true,
		MaxAge:   -1,
	})
}

// flash is a one-shot message carried across a redirect in a short-lived cookie.
const flashCookie = "pigeon_flash"

// setFlash writes the one-shot message cookie. The value's encoding, the three
// AC-49 flags and the read side live in internal/portal, shared with the producer
// portal: its copy of this cookie was fixed for the truncation an unencoded value
// suffers (net/http silently drops the bytes a cookie value cannot carry, so a
// message containing ';' arrived mangled) and the consumer's copy kept the bug.
// SameSite is Lax rather than Strict because the flash legitimately arrives on the
// redirect the user just made - Strict would drop it on the first cross-site
// navigation and the message would silently disappear.
func (h *Handler) setFlash(w http.ResponseWriter, kind, msg string) {
	portal.SetFlashCookie(w, flashCookie, kind, msg)
}

// pullFlash reads the message back: "" when there is none.
func pullFlash(r *http.Request) (kind, msg string) {
	return portal.PullFlashCookie(r, flashCookie)
}

// coreReq issues a core API request with the caller's session token.
func (h *Handler) coreReq(r *http.Request, method, path string, body any) (portal.Response, error) {
	return h.core.Request(r.Context(), h.tokenFromCookie(r), method, path, body)
}

// coreReqHeaders is coreReq with extra headers: replay's `Idempotency-Key`
// (FR-1107) is the one core parameter that travels in a header rather than a
// body field, and the portal passes it through rather than reimplementing it.
func (h *Handler) coreReqHeaders(r *http.Request, method, path string, body any,
	headers map[string]string) (portal.Response, error) {
	return h.core.RequestHeaders(r.Context(), h.tokenFromCookie(r), method, path, body, headers)
}

// page builds a portalui.Page for a logged-in view.
func (h *Handler) page(r *http.Request, title, active string, data any) portalui.Page {
	p := portalui.Page{
		Brand:     h.cfg.Brand,
		Title:     title,
		ActiveNav: active,
		Data:      data,
		// FR-1009: every form the page renders carries the token the middleware
		// minted for this request, so no handler has to remember to mint one.
		CSRFToken: h.csrfToken(r),
	}
	if r != nil {
		if k, m := pullFlash(r); m != "" {
			p.Flash, p.FlashKind = m, k
		}
	}
	return p
}

// renderPage renders a full page.
func (h *Handler) renderPage(w http.ResponseWriter, r *http.Request, name, title, active string, data any) {
	h.renderPageStatus(w, r, http.StatusOK, name, title, active, data)
}

func (h *Handler) renderPageStatus(w http.ResponseWriter, r *http.Request, status int, name, title, active string, data any) {
	h.r.Page(w, status, name, h.page(r, title, active, data))
}
