package httpapi

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/m7jay/pigeon/internal/ratelimit"
)

// logBuffer is a slog sink the tests can read. FR-108's AC-15 says the refusal is
// "404, logged", so the log line is part of the acceptance criterion and not an
// implementation detail.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// withLog returns a server over the same database whose log lands in buf.
func (e *env) withLog(t *testing.T) (*env, *logBuffer) {
	t.Helper()
	buf := &logBuffer{}
	api := New(Deps{
		Lookup:          e.st.LookupAPIKey,
		Sessions:        e.st.LookupSession,
		Data:            e.st,
		Version:         "test-log",
		SessionTTL:      time.Hour,
		KeyLimiter:      ratelimit.New(1000, 1000),
		ProducerLimiter: ratelimit.New(1000, 1000),
	}, slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	out := &env{admin: e.admin, app: e.app, st: e.st, api: api, adminToken: e.adminToken, tenants: e.tenants}
	out.srv = httptest.NewServer(api.Handler())
	t.Cleanup(out.srv.Close)
	return out, buf
}

// cpCase describes what a producer-scoped route must do when the caller aims it
// at another tenant's identifier. Exactly one of these exists per producer route,
// and TestCrossProducerSuiteCoversEveryRoute fails if that stops being true.
type cpCase struct {
	// build returns the request path for tenant B's identifiers, plus an optional
	// body.
	build func(a, b *tenant) (path string, body any)
	// want is the status the route must answer for a cross-tenant request.
	want int
	// leaks are substrings that must not appear in the response body (collection
	// routes have no id to 404, so they must prove absence instead).
	leaks func(a, b *tenant) []string
	// after runs an extra assertion, for routes that create rows: it proves the
	// row landed in A and is invisible to B.
	after func(t *testing.T, e *env, created []byte)
}

// crossProducerCases is AC-20's table. "Every resource" is literal: the coverage
// test below compares its keys against s.routes().
var crossProducerCases = map[string]cpCase{
	"GET /v1/producer/members": {
		build: func(a, b *tenant) (string, any) { return "/v1/producer/members", nil },
		want:  http.StatusOK,
		leaks: func(a, b *tenant) []string { return []string{b.OwnerEmail, b.DeveloperEmail, b.ID} },
	},
	"POST /v1/producer/invites": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/invites", map[string]string{"email": "cp-probe@example.test", "role": "developer"}
		},
		want: http.StatusCreated,
		after: func(t *testing.T, e *env, created []byte) {
			// The invitation must land in A, not in B: B's list cannot contain it.
			b := e.tenantFor(t, "cp-beta")
			code, raw := e.do(http.MethodGet, "/v1/producer/invites", b.OwnerToken, nil)
			mustStatus(t, code, raw, http.StatusOK)
			if strings.Contains(string(raw), "cp-probe@example.test") {
				t.Fatalf("an invitation created in producer A is visible in producer B: %s", raw)
			}
		},
	},
	"GET /v1/producer/invites": {
		build: func(a, b *tenant) (string, any) { return "/v1/producer/invites", nil },
		want:  http.StatusOK,
		leaks: func(a, b *tenant) []string { return []string{b.PendingInviteEmail, b.PendingInviteID} },
	},
	"DELETE /v1/producer/invites/{inviteID}": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/invites/" + b.PendingInviteID + "?env=live", nil
		},
		want: http.StatusNotFound,
	},
	"GET /v1/producer/consumers": {
		build: func(a, b *tenant) (string, any) { return "/v1/producer/consumers?env=live", nil },
		want:  http.StatusOK,
		leaks: func(a, b *tenant) []string { return []string{b.ConsumerID, b.ConsumerRef} },
	},
	"POST /v1/producer/consumers": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/consumers?env=live", map[string]string{"ref": "cp-probe-ref", "name": "Probe"}
		},
		want: http.StatusCreated,
		after: func(t *testing.T, e *env, created []byte) {
			b := e.tenantFor(t, "cp-beta")
			code, raw := e.do(http.MethodGet, "/v1/producer/consumers?env=live", b.OwnerToken, nil)
			mustStatus(t, code, raw, http.StatusOK)
			if strings.Contains(string(raw), "cp-probe-ref") {
				t.Fatalf("a consumer created in producer A is visible in producer B: %s", raw)
			}
		},
	},
	"GET /v1/producer/consumers/{consumerID}": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/consumers/" + b.ConsumerID + "?env=live", nil
		},
		want: http.StatusNotFound,
	},
	"POST /v1/producer/consumers/{consumerID}/invites": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/consumers/" + b.ConsumerID + "/invites?env=live",
				map[string]string{"email": "cp-probe-consumer@example.test"}
		},
		want: http.StatusNotFound,
	},
	"GET /v1/producer/consumers/{consumerID}/users": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/consumers/" + b.ConsumerID + "/users?env=live", nil
		},
		want: http.StatusNotFound,
	},
	"DELETE /v1/producer/consumers/{consumerID}/users/{userID}": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/consumers/" + b.ConsumerID + "/users/" + b.ConsumerUser + "?env=live", nil
		},
		want: http.StatusNotFound,
	},
	"POST /v1/producer/keys": {
		build: func(a, b *tenant) (string, any) { return "/v1/producer/keys?env=live", nil },
		want:  http.StatusCreated,
		after: func(t *testing.T, e *env, created []byte) {
			minted := decode[keyJSON](t, created)
			b := e.tenantFor(t, "cp-beta")
			code, raw := e.do(http.MethodGet, "/v1/producer/keys?env=live", b.OwnerToken, nil)
			mustStatus(t, code, raw, http.StatusOK)
			if strings.Contains(string(raw), minted.ID) {
				t.Fatalf("a key minted for producer A is visible in producer B: %s", raw)
			}
			// And the minted key must not authenticate as anything but A.
			code, raw = e.do(http.MethodGet, "/v1/whoami", *minted.Key, nil)
			mustStatus(t, code, raw, http.StatusOK)
			alpha := e.tenantFor(t, "cp-alpha")
			if who := decode[struct {
				ProducerID string `json:"producer_id"`
			}](t, raw); who.ProducerID != alpha.ID {
				t.Fatalf("key minted in A authenticated as %s", who.ProducerID)
			}
		},
	},
	"GET /v1/producer/keys": {
		build: func(a, b *tenant) (string, any) { return "/v1/producer/keys?env=live", nil },
		want:  http.StatusOK,
		leaks: func(a, b *tenant) []string { return []string{b.KeyID, b.KeyPlain} },
	},
	"GET /v1/producer/keys/{keyID}": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/keys/" + b.KeyID + "?env=live", nil
		},
		want: http.StatusNotFound,
	},
	"DELETE /v1/producer/keys/{keyID}": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/keys/" + b.KeyID + "?env=live", nil
		},
		want: http.StatusNotFound,
	},
}

// TestCrossProducerRequestsAreNotFound is AC-15 and AC-20: every producer-scoped
// route, addressed with another tenant's identifier, answers 404 and never 403,
// and the refusal is logged.
func TestCrossProducerRequestsAreNotFound(t *testing.T) {
	e, logs := e0(t)
	a := e.seedTenant(t, "cp-alpha")
	b := e.seedTenant(t, "cp-beta")

	// A pending invitation in B gives the invitation routes a real id to aim at.
	b.PendingInviteEmail = "cp-beta-pending@example.test"
	code, raw := e.do(http.MethodPost, "/v1/producer/invites", b.OwnerToken, map[string]string{
		"email": b.PendingInviteEmail, "role": "developer",
	})
	mustStatus(t, code, raw, http.StatusCreated)
	b.PendingInviteID = decode[inviteJSON](t, raw).ID
	e.remember(t, b)

	// A's owner session, aimed at every one of B's identifiers.
	for _, rt := range e.routes() {
		if rt.Kind != kindMember && rt.Kind != kindOwner {
			continue
		}
		key := rt.Method + " " + rt.Path
		tc, ok := crossProducerCases[key]
		if !ok {
			t.Fatalf("route %s has no cross-producer case; AC-20 says every resource (add it to crossProducerCases)", key)
		}
		t.Run(strings.ReplaceAll(key, " ", "_"), func(t *testing.T) {
			path, body := tc.build(a, b)
			code, raw := e.do(rt.Method, path, a.OwnerToken, body)
			mustStatus(t, code, raw, tc.want)
			if tc.want == http.StatusNotFound {
				if got := errorCode(t, raw); got != "not_found" {
					t.Fatalf("cross-producer error code = %q, want not_found (never forbidden)", got)
				}
			}
			if tc.leaks != nil {
				for _, leak := range tc.leaks(a, b) {
					if leak != "" && strings.Contains(string(raw), leak) {
						t.Fatalf("producer A's response contains producer B's %q: %s", leak, raw)
					}
				}
			}
			if tc.after != nil {
				tc.after(t, e, raw)
			}
		})
	}

	// AC-15: the refusal is logged, so enumeration is visible rather than
	// indistinguishable from a typo (MT-2's intent).
	logged := logs.String()
	if !strings.Contains(logged, "cross-producer attempt") {
		t.Fatalf("no cross-producer log line after the suite; log was:\n%s", logged)
	}
	if !strings.Contains(logged, b.ConsumerID) {
		t.Fatalf("the log line does not name the requested identifier; log was:\n%s", logged)
	}
}

// TestCrossProducerSuiteCoversEveryRoute is the anti-rot half of AC-20: it fails
// when a producer-scoped route exists without a cross-producer case, which is the
// only way "the suite covers every resource" stays true as routes are added.
func TestCrossProducerSuiteCoversEveryRoute(t *testing.T) {
	e := setup(t)
	seen := map[string]bool{}
	for _, rt := range e.routes() {
		if rt.Kind != kindMember && rt.Kind != kindOwner {
			continue
		}
		key := rt.Method + " " + rt.Path
		if _, ok := crossProducerCases[key]; !ok {
			t.Errorf("producer-scoped route %s has no cross-producer case (AC-20)", key)
		}
		seen[key] = true
	}
	for key := range crossProducerCases {
		if !seen[key] {
			t.Errorf("cross-producer case %s names no registered producer route", key)
		}
	}
	if len(seen) == 0 {
		t.Fatal("no producer-scoped routes found; the route table is empty or the kinds changed")
	}
}

// --- small helpers the table needs -----------------------------------------

// e0 is setup plus a log buffer.
func e0(t *testing.T) (*env, *logBuffer) {
	t.Helper()
	return setup(t).withLog(t)
}

// remember records a fixture so other tenants' hooks can find it.
func (e *env) remember(t *testing.T, tn *tenant) {
	t.Helper()
	if e.tenants == nil {
		e.tenants = map[string]*tenant{}
	}
	e.tenants[tn.Slug] = tn
}

// tenantFor returns the fixture this test created for a slug, so the "after"
// hooks can act as the other tenant.
func (e *env) tenantFor(t *testing.T, slug string) *tenant {
	t.Helper()
	if tn, ok := e.tenants[slug]; ok {
		return tn
	}
	t.Fatalf("no fixture recorded for %s", slug)
	return nil
}
