package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/m7jay/pigeon/internal/ratelimit"
	"github.com/m7jay/pigeon/internal/store"
)

// newAuthServer builds a server whose only working credential types are the two
// fake lookups. Handlers are never reached in these tests: every assertion is
// about whether authorize() lets the request through.
func newAuthServer(t *testing.T, lookup Lookup, sessions SessionLookup, selfRegistration bool) *Server {
	t.Helper()
	return New(Deps{
		Lookup:           lookup,
		Sessions:         sessions,
		Version:          "test",
		SelfRegistration: selfRegistration,
		KeyLimiter:       ratelimit.New(1000, 1000),
		ProducerLimiter:  ratelimit.New(1000, 1000),
	}, slog.New(slog.NewTextHandler(nopWriter{}, nil)))
}

func TestRouteInventoryHasNoDuplicates(t *testing.T) {
	s := newAuthServer(t, nil, nil, false)
	seen := map[string]bool{}
	for _, rt := range s.routes() {
		key := rt.pattern()
		if seen[key] {
			t.Fatalf("route %s is registered twice; the second registration silently wins", key)
		}
		seen[key] = true
		if rt.Handler == nil {
			t.Fatalf("route %s has no handler", key)
		}
		if rt.Method == "" || !strings.HasPrefix(rt.Path, "/") {
			t.Fatalf("route %s is not a valid method+path pair", key)
		}
	}
	if len(seen) < 20 {
		t.Fatalf("only %d routes found; the S1 surface is larger than that, so something is not being registered", len(seen))
	}
}

// TestAnonymousRoutesAreOnlyTheOnesThatMustBe is FR-111 and FR-102's default read
// at the routing layer: with self-registration off, the only anonymous routes are
// health, the two ways to obtain a credential (the password login and S9a's
// optional OIDC login), invitation acceptance, and S9a's two password-reset
// routes. Every other route must have a credential, which is the S1
// done-condition "no route is reachable without a producer scope".
func TestAnonymousRoutesAreOnlyTheOnesThatMustBe(t *testing.T) {
	s := newAuthServer(t, nil, nil, false)
	allowed := map[string]bool{
		"GET /health":             true,
		"GET /health/ready":       true,
		"POST /v1/session":        true,
		"POST /v1/invites/accept": true,
		// FR-106's optional OIDC login (S9a): the second half of the flow, where
		// the core verifies the portal's id_token. It obtains the credential, so
		// it cannot require one; with no provider configured it answers 404.
		"POST /v1/session/oidc": true,
		// FR-111's password reset (S9a): the account asking has no session, which
		// is the problem the flow solves. The request answers the same 202 either
		// way, so it is not an account-enumeration oracle.
		"POST /v1/password-resets":        true,
		"POST /v1/password-resets/accept": true,
	}
	for _, rt := range s.routes() {
		if rt.Kind != kindAnonymous {
			continue
		}
		if !allowed[rt.pattern()] {
			t.Fatalf("anonymous route %s is not in the allow-list; anonymous access must be justified, not added", rt.pattern())
		}
		delete(allowed, rt.pattern())
	}
	for missing := range allowed {
		t.Fatalf("expected anonymous route %s is not registered", missing)
	}
}

// TestEveryNonAnonymousRouteRejectsAnUnauthenticatedCall walks the whole route
// table with no Authorization header. A 404 here would mean the route exists but
// is silently unreachable in a way a client cannot distinguish from a typo; a 200
// would mean it is open. Only 401 is acceptable.
func TestEveryNonAnonymousRouteRejectsAnUnauthenticatedCall(t *testing.T) {
	s := newAuthServer(t,
		func(context.Context, string) (store.Principal, error) { return store.Principal{}, store.ErrNotFound },
		func(context.Context, string) (store.Session, error) { return store.Session{}, store.ErrNotFound },
		false)
	h := s.Handler()

	for _, rt := range s.routes() {
		if rt.Kind == kindAnonymous {
			continue
		}
		path := strings.NewReplacer("{producerID}", "prd_x", "{keyID}", "key_x",
			"{consumerID}", "con_x", "{userID}", "usr_x", "{inviteID}", "inv_x").Replace(rt.Path)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(rt.Method, path, nil))
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s %s without a credential answered %d, want 401", rt.Method, path, rec.Code)
		}
	}
}

// sessionsFor is a fake session lookup keyed by token.
func sessionsFor(tokens map[string]store.Session) SessionLookup {
	return func(_ context.Context, token string) (store.Session, error) {
		if sess, ok := tokens[token]; ok {
			return sess, nil
		}
		return store.Session{}, store.ErrNotFound
	}
}

func call(t *testing.T, h http.Handler, method, path, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// TestScopeKindsAreNotInterchangeable is FR-108 at the credential layer: an admin
// session does not reach producer routes, a producer owner does not reach admin
// routes, and a consumer user reaches neither. All of it is 404, because the
// route does not exist for that credential.
func TestScopeKindsAreNotInterchangeable(t *testing.T) {
	s := newAuthServer(t, nil, sessionsFor(map[string]store.Session{
		"pgns_admin":    {UserID: "usr_admin", InstanceAdmin: true},
		"pgns_owner":    {UserID: "usr_owner", ProducerID: "prd_1", Role: "owner"},
		"pgns_dev":      {UserID: "usr_dev", ProducerID: "prd_1", Role: "developer"},
		"pgns_consumer": {UserID: "usr_consumer", ProducerID: "prd_1", ConsumerID: "con_1", Env: "live"},
	}), false)
	h := s.Handler()

	cases := []struct {
		token, method, path string
		// allowed reports whether authorize() should let the request through.
		// The handlers themselves are unreachable here (Deps.Data is nil), so an
		// allowed request surfaces as a recovered 500; what matters is that it is
		// neither 401 nor 404.
		allowed bool
	}{
		// Admin routes: only the admin session gets in.
		{"pgns_admin", http.MethodGet, "/v1/admin/producers", true},
		{"pgns_owner", http.MethodGet, "/v1/admin/producers", false},
		{"pgns_consumer", http.MethodGet, "/v1/admin/producers", false},

		// Producer routes: the admin session has no producer claim at all.
		{"pgns_admin", http.MethodGet, "/v1/producer/members", false},
		{"pgns_consumer", http.MethodGet, "/v1/producer/members", false},
		{"pgns_dev", http.MethodGet, "/v1/producer/members", true},
		// Owner-only routes: a developer is a member, not an owner.
		{"pgns_dev", http.MethodPost, "/v1/producer/invites", false},
		{"pgns_owner", http.MethodPost, "/v1/producer/invites", true},

		// Consumer route: only the consumer claim.
		{"pgns_consumer", http.MethodGet, "/v1/consumer/me", true},
		{"pgns_owner", http.MethodGet, "/v1/consumer/me", false},

		// Any session may ask who it is.
		{"pgns_owner", http.MethodGet, "/v1/session", true},
	}
	for _, tc := range cases {
		rec := call(t, h, tc.method, tc.path, tc.token)
		if tc.allowed {
			if rec.Code == http.StatusUnauthorized || rec.Code == http.StatusNotFound {
				t.Fatalf("%s %s as %s -> %d; the credential should be allowed through",
					tc.method, tc.path, tc.token, rec.Code)
			}
			continue
		}
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s %s as %s -> %d, want 404 (never 403, FR-108)",
				tc.method, tc.path, tc.token, rec.Code)
		}
	}
}

// TestForcedPasswordChangeBlocksEverythingElse is FR-101: the bootstrap
// credential works for exactly one thing until it is changed.
func TestForcedPasswordChangeBlocksEverythingElse(t *testing.T) {
	s := newAuthServer(t, nil, sessionsFor(map[string]store.Session{
		"pgns_fresh": {UserID: "usr_admin", InstanceAdmin: true, MustChangePassword: true},
	}), false)
	h := s.Handler()

	rec := call(t, h, http.MethodGet, "/v1/admin/producers", "pgns_fresh")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("admin route answered %d before the password change, want 403", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "password_change_required") {
		t.Fatalf("403 body does not name the reason: %s", body)
	}
	if rec := call(t, h, http.MethodGet, "/v1/session", "pgns_fresh"); rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/session answered %d for a session pending a password change, want 200", rec.Code)
	}
}

// TestSuspendedProducerKeyIsForbiddenNotUnauthorized is FR-107's HTTP contract: a
// suspended producer's key is still a valid key, so 401 would be a lie.
func TestSuspendedProducerKeyIsForbiddenNotUnauthorized(t *testing.T) {
	s := newAuthServer(t, func(context.Context, string) (store.Principal, error) {
		return store.Principal{KeyID: "key_1", ProducerID: "prd_1", Env: "live", Suspended: true}, nil
	}, nil, false)

	rec := call(t, s.Handler(), http.MethodGet, "/v1/whoami", "pgn_live_x")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("suspended producer's key answered %d, want 403", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "producer_suspended") {
		t.Fatalf("403 body does not name the reason: %s", rec.Body.String())
	}
}

// TestACredentialIsNotTheOtherKind keeps the two bearer namespaces disjoint: an
// API key cannot be replayed as a session, and a session token is not a publish
// credential (FR-208's separation, one layer up).
func TestACredentialIsNotTheOtherKind(t *testing.T) {
	s := newAuthServer(t, func(_ context.Context, token string) (store.Principal, error) {
		// Mirrors store.LookupAPIKey: a token that is not shaped like a producer
		// key is not a lookup, it is a miss.
		if !strings.HasPrefix(token, "pgn_") {
			return store.Principal{}, store.ErrNotFound
		}
		return store.Principal{KeyID: "key_1", ProducerID: "prd_1", Env: "live"}, nil
	}, sessionsFor(nil), false)
	h := s.Handler()

	if rec := call(t, h, http.MethodGet, "/v1/session", "pgn_live_keylookalike"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("an API key used as a session answered %d, want 401", rec.Code)
	}
	// And the reverse: a session token presented as an API key finds no key.
	if rec := call(t, h, http.MethodGet, "/v1/whoami", "pgns_sessiontoken"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("a session token used as an API key answered %d, want 401", rec.Code)
	}
}

// TestSelfRegistrationRouteOnlyExistsWhenEnabled is FR-102: the route is absent
// when the feature is off, which is a stronger statement than "forbidden".
func TestSelfRegistrationRouteOnlyExistsWhenEnabled(t *testing.T) {
	off := newAuthServer(t, nil, nil, false)
	if rec := call(t, off.Handler(), http.MethodPost, "/v1/producers", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("self-registration route answered %d with the feature off, want 404", rec.Code)
	}

	on := newAuthServer(t, nil, nil, true)
	found := false
	for _, rt := range on.routes() {
		if rt.pattern() == "POST /v1/producers" {
			found = true
		}
	}
	if !found {
		t.Fatal("self-registration enabled but the route is not registered")
	}
}

// TestLoginRequiresEmailAndPassword guards the one anonymous write that takes a
// credential: a body without both fields is a 400, not a 500 downstream.
func TestLoginRequiresEmailAndPassword(t *testing.T) {
	s := newAuthServer(t, nil, nil, false)
	req := httptest.NewRequest(http.MethodPost, "/v1/session", strings.NewReader(`{"email":"a@b.test"}`))
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("login without a password answered %d, want 400", rec.Code)
	}
}

// TestUnknownFieldsAreRejected keeps a typo (`role` vs `roles`) from silently
// becoming a default, which for these two fields decides permissions.
func TestUnknownFieldsAreRejected(t *testing.T) {
	s := newAuthServer(t, nil, nil, false)
	req := httptest.NewRequest(http.MethodPost, "/v1/session",
		strings.NewReader(`{"email":"a@b.test","password":"x","producer":"typo"}`))
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown field answered %d, want 400", rec.Code)
	}
}
