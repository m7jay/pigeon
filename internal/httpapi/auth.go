package httpapi

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/m7jay/pigeon/internal/store"
)

// authorize is the only place that decides whether a caller may reach a route.
// Every branch that fails a *scope* test answers 404 and not 403 (FR-108): an
// admin route does not exist for a producer owner, and a producer route does not
// exist for a consumer user. Only two failures are honest 403s, and both concern
// a caller who is already known to be who they say: a pending forced password
// change (FR-101) and a suspended producer's ingest key (FR-107).
func (s *Server) authorize(kind routeKind, next http.Handler) http.Handler {
	switch kind {
	case kindAnonymous:
		return next
	case kindProducerKey:
		return s.withAPIKey(next)
	default:
		return s.withSession(kind, next)
	}
}

// withAPIKey authenticates a producer key and applies FR-207's two rate limits
// and FR-107's suspension check, in that order: the limits are about load, so
// they are answered before anything touches the database again.
func (s *Server) withAPIKey(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := bearerToken(r.Header.Get("Authorization"))
		if !ok {
			writeError(w, http.StatusUnauthorized, "unauthenticated", "Authorization: Bearer *** is required")
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

		// FR-207: per key, then per producer. Both are checked so that one noisy
		// key cannot spend the whole producer's budget invisibly, and so one
		// producer's burst cannot exhaust another's.
		if wait, ok := s.deps.KeyLimiter.Allow(p.KeyID); !ok {
			writeRetryAfter(w, wait)
			return
		}
		if wait, ok := s.deps.ProducerLimiter.Allow(p.ProducerID); !ok {
			writeRetryAfter(w, wait)
			return
		}

		// FR-107: suspension stops ingestion. The key still authenticates, which
		// is deliberate - the caller is told why, instead of being handed a 401
		// that sends them to re-issue a perfectly valid key.
		if p.Suspended {
			writeError(w, http.StatusForbidden, "producer_suspended",
				"this producer is suspended; ingestion is stopped and non-terminal deliveries are held")
			return
		}

		// FR-206: last_used_at is evidence, not control state. It runs off the
		// request path under its own short deadline, so a database hiccup writing
		// a timestamp cannot turn a successful authentication into a 500.
		if s.deps.TouchKey != nil {
			touch := s.deps.TouchKey
			keyID := p.KeyID
			go func() {
				ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 2*time.Second)
				defer cancel()
				if err := touch(ctx, keyID); err != nil {
					s.log.Debug("touch api key", "error", err, "key_id", keyID)
				}
			}()
		}

		next.ServeHTTP(w, r.WithContext(withPrincipal(r.Context(), p)))
	})
}

// withSession authenticates a human session token and checks the route's kind.
func (s *Server) withSession(kind routeKind, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := bearerToken(r.Header.Get("Authorization"))
		if !ok {
			writeError(w, http.StatusUnauthorized, "unauthenticated", "Authorization: Bearer *** is required")
			return
		}
		// A producer key presented where a session belongs is not a session.
		// Rejecting it by prefix keeps the two credential types disjoint, so an
		// ingest key can never act as a human (FR-208's separation, one layer up).
		if !strings.HasPrefix(token, store.SessionPrefix) {
			writeError(w, http.StatusUnauthorized, "unauthenticated", "invalid session")
			return
		}
		if s.deps.Sessions == nil {
			writeError(w, http.StatusUnauthorized, "unauthenticated", "invalid session")
			return
		}
		sess, err := s.deps.Sessions(r.Context(), token)
		if err != nil {
			if !errors.Is(err, store.ErrNotFound) {
				s.log.Error("session lookup failed", "error", err, "request_id", requestIDFrom(r.Context()))
			}
			writeError(w, http.StatusUnauthorized, "unauthenticated", "invalid session")
			return
		}
		// Attach the claims before the kind check so that a refusal is logged with
		// the caller's tenant: an anonymous 404 and a cross-tenant 404 must be
		// tellable apart in the log, or the probe is invisible.
		r = r.WithContext(withSessionClaim(r.Context(), sess))

		// FR-101: the bootstrap credential is good for exactly one thing until it
		// is changed - plus the two routes that let the client see why it is
		// blocked and fix it.
		if sess.MustChangePassword && kind != kindPasswordChange && kind != kindSession {
			writeError(w, http.StatusForbidden, "password_change_required",
				"this credential must be changed before anything else: POST /v1/session/password")
			return
		}

		switch kind {
		case kindSession, kindPasswordChange:
			// Any valid session may ask who it is or change its password.
		case kindAdmin:
			if !sess.InstanceAdmin {
				s.writeNotFound(w, r)
				return
			}
		case kindMember:
			if !sess.IsProducerMember() {
				s.writeNotFound(w, r)
				return
			}
		case kindOwner:
			if !sess.IsProducerMember() || sess.Role != "owner" {
				s.writeNotFound(w, r)
				return
			}
		case kindConsumer:
			if !sess.IsConsumer() {
				s.writeNotFound(w, r)
				return
			}
		}

		next.ServeHTTP(w, r)
	})
}

// optionalSession resolves a session when the caller presented one, and is a
// no-op when they did not.
//
// Only POST /v1/invites/accept uses it, and it is the one route where both shapes
// are legitimate: a new invitee has no session yet (the invitation plus a chosen
// password creates the account), while an invitee who already has an account has
// to prove it, because the token is handed to the inviter and therefore proves
// nothing about the account it names.
func (s *Server) optionalSession(w http.ResponseWriter, r *http.Request) (store.Session, bool) {
	if strings.TrimSpace(r.Header.Get("Authorization")) == "" {
		return store.Session{}, true
	}
	token, ok := bearerToken(r.Header.Get("Authorization"))
	if !ok || !strings.HasPrefix(token, store.SessionPrefix) || s.deps.Sessions == nil {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "invalid session")
		return store.Session{}, false
	}
	sess, err := s.deps.Sessions(r.Context(), token)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "invalid session")
		return store.Session{}, false
	}
	return sess, true
}

// writeNotFound logs and answers a scope violation: the resource exists
// somewhere the caller may not see, or does not exist at all, and both answers
// must be identical (FR-108).
func (s *Server) writeNotFound(w http.ResponseWriter, r *http.Request) {
	s.logProbe(r, nil)
	writeError(w, http.StatusNotFound, "not_found", "no such resource")
}

// logProbe is AC-15's "logged" half and MT-2's intent: a request that asked for
// something outside its scope is recorded as an attempt, with the caller's tenant
// and the path that was asked for.
func (s *Server) logProbe(r *http.Request, err error) {
	attrs := []any{
		"path", r.URL.Path,
		"method", r.Method,
		"request_id", requestIDFrom(r.Context()),
		"caller_producer", callerProducer(r),
	}
	if err != nil {
		attrs = append(attrs, "reason", err.Error())
	}
	s.log.Warn("cross-producer attempt: no such resource in this scope", attrs...)
}

// callerProducer names the tenant the request came from, whichever credential
// carried it.
func callerProducer(r *http.Request) string {
	if p, ok := principalFrom(r.Context()); ok {
		return p.ProducerID
	}
	if sess, ok := sessionFrom(r.Context()); ok {
		return sess.ProducerID
	}
	return ""
}

// writeRetryAfter answers FR-207's over-limit case. Retry-After is whole seconds
// (RFC 9110) and rounded up, because rounding down would invite an early retry.
func writeRetryAfter(w http.ResponseWriter, wait time.Duration) {
	secs := int(math.Ceil(wait.Seconds()))
	if secs < 1 {
		secs = 1
	}
	w.Header().Set("Retry-After", fmt.Sprintf("%d", secs))
	writeError(w, http.StatusTooManyRequests, "rate_limited",
		"too many requests for this key or producer; retry after the Retry-After interval")
}

// errEnvRequired is the member-session counterpart of a key's fixed environment.
// A human works in both planes, so the request has to say which one; defaulting
// would silently address live from a test-shaped request (FR-103).
var errEnvRequired = errors.New("env query parameter is required (test or live)")

// memberScope resolves the (producer, environment) a member-session request acts
// on. The producer comes from the session claims, the environment from `?env=`;
// nothing in a body can move the scope.
func (s *Server) memberScope(r *http.Request) (store.Scope, error) {
	sess, ok := sessionFrom(r.Context())
	if !ok || !sess.IsProducerMember() {
		return store.Scope{}, store.ErrNotFound
	}
	env := r.URL.Query().Get("env")
	if env == "" {
		return store.Scope{}, errEnvRequired
	}
	if !store.ValidEnv(env) {
		return store.Scope{}, fmt.Errorf("%w: %q", store.ErrInvalidEnv, env)
	}
	return store.Scope{ProducerID: sess.ProducerID, Env: env}, nil
}

// producerScope is memberScope without an environment: producer-wide rows only
// (members, member invitations). Environment-bearing tables are invisible to it,
// so the accidental promotion of a producer-wide query into a data query fails
// closed instead of spanning both planes.
func (s *Server) producerScope(r *http.Request) (store.Scope, error) {
	sess, ok := sessionFrom(r.Context())
	if !ok || !sess.IsProducerMember() {
		return store.Scope{}, store.ErrNotFound
	}
	return store.Scope{ProducerID: sess.ProducerID}, nil
}

// consumerScope is FR-110's promise: the consumer owner's session names the
// producer and the environment, and the request cannot change either.
func consumerScope(r *http.Request) (store.Scope, error) {
	sess, ok := sessionFrom(r.Context())
	if !ok || !sess.IsConsumer() {
		return store.Scope{}, store.ErrNotFound
	}
	return store.Scope{ProducerID: sess.ProducerID, Env: sess.Env}, nil
}

// scopeError answers the scope-resolution failures: a missing or invalid
// environment is a bad request, a missing claim is a 404.
func (s *Server) scopeError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, errEnvRequired):
		writeError(w, http.StatusBadRequest, "env_required",
			"the env query parameter is required: test or live")
	case errors.Is(err, store.ErrInvalidEnv):
		writeError(w, http.StatusBadRequest, "invalid_env", "env must be test or live")
	default:
		s.writeNotFound(w, r)
	}
}

// actorFrom builds the audit identity for a write: the user behind a session, or
// the key behind a machine request, plus the request id that ties it to the logs
// (MT-5).
func actorFrom(r *http.Request) store.Actor {
	ctx := r.Context()
	a := store.Actor{RequestID: requestIDFrom(ctx)}
	if sess, ok := sessionFrom(ctx); ok {
		a.UserID = sess.UserID
	}
	if p, ok := principalFrom(ctx); ok {
		a.KeyID = p.KeyID
	}
	return a
}

// ctxSessionKey carries the resolved session claims.
type ctxSessionKey struct{}

func withSessionClaim(ctx context.Context, s store.Session) context.Context {
	return context.WithValue(ctx, ctxSessionKey{}, s)
}

func sessionFrom(ctx context.Context) (store.Session, bool) {
	s, ok := ctx.Value(ctxSessionKey{}).(store.Session)
	return s, ok
}
