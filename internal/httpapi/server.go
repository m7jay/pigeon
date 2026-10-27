// Package httpapi is the management and ingest API.
//
// Every route is declared once, in routes(), together with the kind of
// credential it needs. The mux is built from that list, and the tests assert
// against the same list, so "a route exists that nothing proves" is a failing
// test rather than a review question (FR-108's done-condition for S1).
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/m7jay/pigeon/internal/metrics"
	"github.com/m7jay/pigeon/internal/ratelimit"
	"github.com/m7jay/pigeon/internal/store"
)

// Lookup authenticates a plaintext bearer key. Declared as a function type so the
// tests can exercise the middleware without a database.
type Lookup func(ctx context.Context, plaintext string) (store.Principal, error)

// SessionLookup resolves a session token. Same reasoning as Lookup.
type SessionLookup func(ctx context.Context, token string) (store.Session, error)

// Data is the slice of the store the handlers use. It is an interface so the
// routing and authentication tests can run with no database, and so the handler
// layer cannot accidentally grow a query of its own: anything not in here is not
// reachable from HTTP.
//
// The concrete implementation is *store.Store, which enforces the tenant scope
// in SQL as well as in RLS (MT-1).
type Data interface {
	CreateProducer(ctx context.Context, actor store.Actor, in store.NewProducer) (store.Producer, store.Invite, error)
	ListProducers(ctx context.Context) ([]store.Producer, error)
	GetProducer(ctx context.Context, producerID string) (store.Producer, error)
	SetProducerSuspended(ctx context.Context, actor store.Actor, producerID string, suspended bool, reason string) (store.Producer, error)
	ListAudit(ctx context.Context, producerID string) ([]store.AuditEntry, error)
	SelfRegisterProducer(ctx context.Context, slug, name, email, password string) (store.Producer, error)

	CreateSession(ctx context.Context, in store.LoginInput, ttl time.Duration) (store.Session, string, error)
	RevokeSession(ctx context.Context, token string) error
	ChangePassword(ctx context.Context, userID, oldPassword, newPassword string) error
	AcceptInvite(ctx context.Context, token, password, callerUserID string) (store.AcceptedInvite, error)

	ListMembers(ctx context.Context, sc store.Scope) ([]store.Member, error)
	InviteMember(ctx context.Context, sc store.Scope, actor store.Actor, email, role string) (store.Invite, error)
	ListInvites(ctx context.Context, sc store.Scope) ([]store.InviteMeta, error)
	RevokeInvite(ctx context.Context, sc store.Scope, inviteID string) error

	ListConsumers(ctx context.Context, sc store.Scope) ([]store.Consumer, error)
	CreateConsumer(ctx context.Context, sc store.Scope, ref, name string) (store.Consumer, error)
	GetConsumer(ctx context.Context, sc store.Scope, consumerID string) (store.Consumer, error)
	RekeyConsumer(ctx context.Context, sc store.Scope, consumerID, newRef string) (store.Consumer, error)
	ListEndpointsForConsumer(ctx context.Context, sc store.Scope, consumerID string) ([]store.Endpoint, error)
	CreateEndpoint(ctx context.Context, sc store.Scope, in store.NewEndpoint) (store.EndpointCreated, error)
	GetEndpoint(ctx context.Context, sc store.Scope, endpointID string) (store.Endpoint, error)
	SetEndpointPaused(ctx context.Context, sc store.Scope, endpointID string, paused bool) (store.Endpoint, error)
	DeleteEndpoint(ctx context.Context, sc store.Scope, endpointID string) error
	VerifyEndpoint(ctx context.Context, sc store.Scope, endpointID, token string) error
	ListSubscriptionsForEndpoint(ctx context.Context, sc store.Scope, endpointID string) ([]store.Subscription, error)
	CreateSubscription(ctx context.Context, sc store.Scope, in store.NewSubscription) (store.Subscription, error)
	DeleteSubscription(ctx context.Context, sc store.Scope, endpointID, eventType string) error

	// M2b consumer self-service (UI-13, UI-14, AC-39): the same operations as the
	// producer-facing methods above, but every read and write is additionally
	// filtered by consumerID, the caller's own consumer from the session claim. A
	// foreign or deleted id is ErrNotFound, so guessing another consumer's id is a
	// 404 whether it is an endpoint, a pause, a delete or a subscription.
	CreateConsumerEndpoint(ctx context.Context, sc store.Scope, consumerID, name, url string) (store.EndpointCreated, error)
	GetConsumerEndpoint(ctx context.Context, sc store.Scope, consumerID, endpointID string) (store.Endpoint, error)
	SetConsumerEndpointPaused(ctx context.Context, sc store.Scope, consumerID, endpointID string, paused bool) (store.Endpoint, error)
	DeleteConsumerEndpoint(ctx context.Context, sc store.Scope, consumerID, endpointID string) error
	VerifyConsumerEndpoint(ctx context.Context, sc store.Scope, consumerID, endpointID, token string) error
	ListConsumerSubscriptions(ctx context.Context, sc store.Scope, consumerID, endpointID string) ([]store.Subscription, error)
	CreateConsumerSubscription(ctx context.Context, sc store.Scope, consumerID, endpointID, eventType string) (store.Subscription, error)
	DeleteConsumerSubscription(ctx context.Context, sc store.Scope, consumerID, endpointID, eventType string) error

	InviteConsumerUser(ctx context.Context, sc store.Scope, actor store.Actor, consumerID, email string) (store.Invite, error)
	ListConsumerUsers(ctx context.Context, sc store.Scope, consumerID string) ([]store.ConsumerUser, error)
	RevokeConsumerUser(ctx context.Context, sc store.Scope, consumerID, userID string) error

	IssueKey(ctx context.Context, sc store.Scope, actor store.Actor, replayScope bool) (store.IssuedKey, error)
	ListKeys(ctx context.Context, sc store.Scope) ([]store.KeyMeta, error)
	GetKey(ctx context.Context, sc store.Scope, keyID string) (store.KeyMeta, error)
	RevokeKey(ctx context.Context, sc store.Scope, keyID string) error

	// Ingest (§11, FR-301..FR-306, FR-601..FR-607). Event types are the producer's
	// published contracts; publishing writes the event and its outbox row in one
	// transaction so an accept can never outrun a publish intent.
	CreateEventType(ctx context.Context, sc store.Scope, in store.NewEventType) (store.EventType, error)
	ListEventTypes(ctx context.Context, sc store.Scope) ([]store.EventType, error)
	PatchEventType(ctx context.Context, sc store.Scope, name string, p store.EventTypePatch) (store.EventType, error)
	PublishEvent(ctx context.Context, sc store.Scope, in store.PublishInput) (store.PublishedEvent, error)
}

// Deps are the collaborators the handlers need. Nil Ready means "nothing to
// check", which is what the unit tests want.
type Deps struct {
	Lookup   Lookup
	Sessions SessionLookup
	Data     Data
	Ready    func(ctx context.Context) error
	Version  string

	// SessionTTL is how long a login lasts (PIGEON_SESSION_TTL).
	SessionTTL time.Duration

	// KeyLimiter and ProducerLimiter implement FR-207's two ceilings. A nil
	// limiter means "no limit", which is what the tests use.
	KeyLimiter      *ratelimit.Limiter
	ProducerLimiter *ratelimit.Limiter

	// TouchKey records FR-206's last_used_at. The middleware calls it on a
	// goroutine after a successful authentication, so a slow or failing evidence
	// write can never delay or fail the request that carried the key. Nil disables
	// it (unit tests only).
	TouchKey func(ctx context.Context, keyID string) error

	// SelfRegistration enables POST /v1/producers (FR-102's "enableable by
	// config"). Off by default: an internet-facing instance must not let
	// strangers create tenants.
	SelfRegistration bool

	// Metrics, when set, exposes the fan-out non-delivery counters (FR-508) at
	// GET /metrics. Nil disables the route, which the unit tests want.
	Metrics *metrics.Metrics
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
	if deps.SessionTTL <= 0 {
		deps.SessionTTL = 12 * time.Hour
	}
	return &Server{deps: deps, log: log}
}

// Handler returns the fully wrapped router.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	for _, rt := range s.routes() {
		mux.Handle(rt.pattern(), s.authorize(rt.Kind, rt.Handler))
	}
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

// handleWhoami returns the principal the key proved. It is the smallest route
// that exercises auth, the RLS scope variables and the 404 policy end to end,
// which is what M0's exit criteria test; S1 adds the suspension flag, because a
// suspended producer's key still authenticates and must be told why it is being
// refused rather than seeing a mystery 403 elsewhere.
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
		"suspended":   p.Suspended,
	})
}

// handleMetrics renders the fan-out non-delivery counters (FR-508) in a minimal
// Prometheus text format. Each §7.5a reason is its own series label, so an
// operator can see an un-onboarded consumer separate from a paused endpoint. The
// route is anonymous but unauthenticated: it leaks only counter values, never
// producer data, and the cross-producer suite proves it says the same thing to
// everyone.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if s.deps.Metrics == nil {
		writeError(w, http.StatusNotFound, "not_found", "metrics disabled")
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	snap := s.deps.Metrics.NonDeliverySnapshot()
	_, _ = fmt.Fprintf(w, "# HELP pigeon_non_deliveries_total Fan-out events that produced no delivery, by reason.\n")
	_, _ = fmt.Fprintf(w, "# TYPE pigeon_non_deliveries_total counter\n")
	for _, reason := range sortedReasons(snap) {
		_, _ = fmt.Fprintf(w, "pigeon_non_deliveries_total{reason=%q} %d\n", reason, snap[reason])
	}
}

// sortedReasons gives the series a stable, deterministic order so the scrape is
// byte-stable across calls (a test can assert on it).
func sortedReasons(snap map[string]int64) []string {
	reasons := make([]string, 0, len(snap))
	for r := range snap {
		reasons = append(reasons, r)
	}
	sort.Strings(reasons)
	return reasons
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

// maxBody bounds every request body. S1 has no payload larger than a few hundred
// bytes; the ingest limit (64 KB, FR-606) lands with the ingest path.
const maxBody = 64 << 10

// decodeJSON reads one JSON body into dst, answering 400 itself when the body is
// not JSON. It returns false when the caller should stop.
//
// Unknown fields are rejected rather than ignored: a typo in `role` or `env` must
// not silently become a default, because those two fields decide permissions.
func (s *Server) decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "request body must be a JSON object: "+err.Error())
		return false
	}
	return true
}

// storeError maps the store's sentinel errors onto the API's status codes. The
// important line is ErrNotFound: every tenant-scoped miss is a 404, never a 403
// (FR-108), so a wrong producer id is indistinguishable from a nonexistent one.
func (s *Server) storeError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		// AC-15 says "404, logged". Logging the miss is what makes enumeration
		// visible instead of indistinguishable from a typo (MT-2); the line
		// deliberately carries the path and the caller's tenant so an operator can
		// tell a probe from a bug without another query.
		s.logProbe(r, err)
		writeError(w, http.StatusNotFound, "not_found", "no such resource")
	case errors.Is(err, store.ErrConflict):
		writeError(w, http.StatusConflict, "conflict", "that resource already exists")
	case errors.Is(err, store.ErrInvalid), errors.Is(err, store.ErrInvalidEnv):
		msg := strings.TrimPrefix(err.Error(), store.ErrInvalid.Error()+": ")
		writeError(w, http.StatusUnprocessableEntity, "invalid", msg)
	case errors.Is(err, store.ErrAmbiguousProducer):
		writeError(w, http.StatusBadRequest, "producer_required",
			"this user belongs to several producers; send producer_slug to choose one")
	default:
		s.log.Error("request failed", "error", err, "path", r.URL.Path, "request_id", requestIDFrom(r.Context()))
		writeError(w, http.StatusInternalServerError, "internal", "internal error")
	}
}
