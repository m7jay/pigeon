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
	"strings"

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
var pageNames = []string{"login", "invite", "me", "endpoints", "endpoint", "deliveries", "snippet"}

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
	mux.HandleFunc("POST /endpoints/{id}/verify", h.requireLogin(h.endpointVerify))
	mux.HandleFunc("POST /endpoints/{id}/pause", h.requireLogin(h.endpointSetPaused(true)))
	mux.HandleFunc("POST /endpoints/{id}/resume", h.requireLogin(h.endpointSetPaused(false)))
	mux.HandleFunc("POST /endpoints/{id}/delete", h.requireLogin(h.endpointDelete))
	mux.HandleFunc("POST /endpoints/{id}/subscriptions", h.requireLogin(h.subscribe))
	mux.HandleFunc("POST /endpoints/{id}/subscriptions/{event}/delete", h.requireLogin(h.unsubscribe))
	mux.HandleFunc("GET /frag/endpoints", h.requireLogin(h.fragEndpoints))

	mux.HandleFunc("GET /deliveries", h.requireLogin(h.deliveries))
	// UI-18: the snippet form and its generation are both behind login, and the
	// secret is posted (never a query parameter), so it stays out of access logs,
	// browser history and shared links.
	mux.HandleFunc("GET /snippet", h.requireLogin(h.snippetForm))
	mux.HandleFunc("POST /snippet", h.requireLogin(h.snippetSubmit))
	return mux
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

// setTokenCookie encrypts the token into the session cookie.
func (h *Handler) setTokenCookie(w http.ResponseWriter, token string) error {
	enc, err := h.cookie.Encode(token)
	if err != nil {
		return err
	}
	http.SetCookie(w, &http.Cookie{
		Name:     h.cfg.CookieName,
		Value:    enc,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   true,
		MaxAge:   12 * 3600, // 12h
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

func (h *Handler) setFlash(w http.ResponseWriter, kind, msg string) {
	val := kind + "|" + msg
	http.SetCookie(w, &http.Cookie{Name: flashCookie, Value: val, Path: "/", MaxAge: 60, HttpOnly: true, Secure: true})
}

func pullFlash(r *http.Request) (kind, msg string) {
	c, err := r.Cookie(flashCookie)
	if err != nil {
		return "", ""
	}
	i := strings.IndexByte(c.Value, '|')
	if i < 0 {
		return "", ""
	}
	return c.Value[:i], c.Value[i+1:]
}

// coreReq issues a core API request with the caller's session token.
func (h *Handler) coreReq(r *http.Request, method, path string, body any) (portal.Response, error) {
	return h.core.Request(r.Context(), h.tokenFromCookie(r), method, path, body)
}

// page builds a portalui.Page for a logged-in view.
func (h *Handler) page(r *http.Request, title, active string, data any) portalui.Page {
	p := portalui.Page{
		Brand:     h.cfg.Brand,
		Title:     title,
		ActiveNav: active,
		Data:      data,
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
