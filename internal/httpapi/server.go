// Package httpapi is the ingest and read API.
//
// M0 ships only the surface that proves the skeleton is honest: liveness,
// readiness, and an authenticated route whose response is filtered by the key
// that made the request (FR-108). Event and delivery endpoints arrive in M1/M2.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/pigeon-hq/pigeon/internal/store"
)

// Lookup authenticates a plaintext bearer key. Declared as a function type so the
// tests can exercise the middleware without a database.
type Lookup func(ctx context.Context, plaintext string) (store.Principal, error)

// Deps are the collaborators the handlers need. Nil Ready means "nothing to
// check", which is what the unit tests want.
type Deps struct {
	Lookup  Lookup
	Ready   func(ctx context.Context) error
	Version string
}

// Server holds the router and its dependencies.
type Server struct {
	deps Deps
	log  *slog.Logger
}

// New builds a Server. A nil logger is replaced rather than panicked on: a
// missing logger must not be an outage.
func New(deps Deps, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{deps: deps, log: log}
}

// Handler returns the fully wrapped router.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", s.handleLiveness)
	mux.HandleFunc("GET /health/ready", s.handleReadiness)
	mux.Handle("GET /v1/whoami", s.authenticate(http.HandlerFunc(s.handleWhoami)))

	return s.withRequestID(s.recoverer(mux))
}

// handleLiveness answers without touching a dependency: it says the process can
// answer HTTP, which is the only question a liveness probe may ask. A liveness
// check that pings Postgres restarts a healthy app when the database blinks.
func (s *Server) handleLiveness(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "version": s.deps.Version})
}

// handleReadiness is the opposite: it fails when a dependency is unusable, so a
// load balancer stops sending ingest traffic to this replica.
func (s *Server) handleReadiness(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if s.deps.Ready == nil {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
		return
	}
	if err := s.deps.Ready(ctx); err != nil {
		s.log.Warn("readiness failed", "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready", "detail": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// handleWhoami returns the principal the key proved. It exists because it is the
// smallest route that exercises auth, the RLS scope variable, and the 404 policy
// end to end, which is what M0's exit criteria test.
func (s *Server) handleWhoami(w http.ResponseWriter, r *http.Request) {
	p, ok := principalFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "no principal on request")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"key_id":      p.KeyID,
		"producer_id": p.ProducerID,
		"env":         p.Env,
		"replay_only": p.ReplayOnly,
	})
}

// authenticate resolves the bearer key and attaches the principal. Every failure
// is 401 with the same body: whether a key is unknown, revoked, or malformed is
// information an attacker would like and an operator does not need.
func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := bearerToken(r.Header.Get("Authorization"))
		if !ok {
			writeError(w, http.StatusUnauthorized, "unauthenticated", "Authorization: Bearer <key> is required")
			return
		}
		p, err := s.deps.Lookup(r.Context(), token)
		if err != nil {
			if !errors.Is(err, store.ErrNotFound) {
				s.log.Error("api key lookup failed", "error", err, "request_id", requestIDFrom(r.Context()))
			}
			writeError(w, http.StatusUnauthorized, "unauthenticated", "invalid api key")
			return
		}
		next.ServeHTTP(w, r.WithContext(withPrincipal(r.Context(), p)))
	})
}

func bearerToken(header string) (string, bool) {
	scheme, token, ok := strings.Cut(strings.TrimSpace(header), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return "", false
	}
	return token, true
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// writeError emits the structured error shape from §11 so a client can branch on
// a code instead of parsing prose.
func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{"code": code, "message": message},
	})
}
