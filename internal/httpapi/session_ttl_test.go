package httpapi

import (
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/m7jay/pigeon/internal/ratelimit"
)

// withSessionTTL builds a second API over the same database with a chosen session
// lifetime, the way withLimits builds one with different rate limits: AC-51 names
// two concrete values, and the response that carries them has to be the real login
// response rather than a fixture.
func (e *env) withSessionTTL(t *testing.T, ttl time.Duration) *env {
	t.Helper()
	api := New(Deps{
		Lookup:          e.st.LookupAPIKey,
		Sessions:        e.st.LookupSession,
		Data:            e.st,
		Version:         "test-ttl",
		SessionTTL:      ttl,
		KeyLimiter:      ratelimit.New(1000, 1000),
		ProducerLimiter: ratelimit.New(1000, 1000),
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	out := &env{admin: e.admin, app: e.app, st: e.st, api: api, adminToken: e.adminToken}
	out.srv = httptest.NewServer(api.Handler())
	t.Cleanup(out.srv.Close)
	return out
}

// TestSessionExpiryFollowsTheConfiguredTTL is the core half of AC-51: the login
// response's `expires_at` is PIGEON_SESSION_TTL from *now*, for both of the values
// the criterion names. The portal half - that the cookie's Max-Age is that expiry
// rather than a portal constant - is asserted in internal/portal/consumer
// (TestPortalSessionCookieLifetimeFollowsCoreExpiry); together they pin the chain
// from the knob to the Set-Cookie header.
//
// The tolerance is a few seconds: expires_at is computed before the response
// round trip, so asserting equality to the microsecond would assert the test
// machine's scheduling rather than the server's arithmetic.
func TestSessionExpiryFollowsTheConfiguredTTL(t *testing.T) {
	e := setup(t)
	for _, tc := range []struct {
		name string
		ttl  time.Duration
	}{
		{"24h", 24 * time.Hour},
		{"15m", 15 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := e.withSessionTTL(t, tc.ttl)
			before := time.Now()
			sess := api.login(adminEmail, adminRotated, "")
			after := time.Now()

			if sess.ExpiresAt == "" {
				t.Fatalf("the login response carries no expires_at, so a portal has no lifetime to honour (AC-51)")
			}
			exp, err := time.Parse(time.RFC3339, sess.ExpiresAt)
			if err != nil {
				t.Fatalf("expires_at %q is not RFC 3339: %v", sess.ExpiresAt, err)
			}
			lo, hi := before.Add(tc.ttl).Add(-5*time.Second), after.Add(tc.ttl).Add(5*time.Second)
			if exp.Before(lo) || exp.After(hi) {
				t.Fatalf("expires_at = %s, want %s after the login (%s..%s): the session TTL is not the configured %s",
					exp.UTC(), tc.ttl, lo.UTC(), hi.UTC(), tc.ttl)
			}
			if tc.ttl == 15*time.Minute && exp.Sub(before) >= time.Hour {
				t.Fatalf("a 15 m session lifetime produced an expiry %s away - the default 12 h is leaking through", exp.Sub(before))
			}
		})
	}
}
