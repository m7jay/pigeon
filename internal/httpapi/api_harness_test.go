package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m7jay/pigeon/internal/atrest"
	"github.com/m7jay/pigeon/internal/db"
	"github.com/m7jay/pigeon/internal/dbtest"
	"github.com/m7jay/pigeon/internal/identity"
	"github.com/m7jay/pigeon/internal/ratelimit"
	"github.com/m7jay/pigeon/internal/store"
)

// The acceptance harness runs the real HTTP stack against a real Postgres, as
// the non-superuser application role. It is the only kind of test that can prove
// AC-15 and AC-20: a fake store would pass while the RLS policies were wrong, and
// a handler unit test cannot see a 404 that the database produced.
//
// It is gated on PIGEON_TEST_DATABASE_URL, which CI always sets. CI also fails on
// any `--- SKIP` line, so "the suite exists" cannot quietly become "the suite
// skipped" (that is the S0 lesson).
const (
	appRole     = "pigeon_app"
	appPassword = "pigeon_app"

	// adminEmail is what identity.EnsureInstanceAdmin creates (FR-101).
	adminEmail = "admin@pigeon.local"
	// adminPassword is the bootstrap password the harness starts with; the
	// first thing every run does is prove it must be changed.
	adminPassword = "bootstrap-horse-battery-staple"
	// adminRotated is the password after FR-101's forced change.
	adminRotated = "rotated-horse-battery-staple"
)

// env is one running API over one real database.
type env struct {
	admin *pgxpool.Pool // superuser: fixtures, and reading what the API wrote
	app   *pgxpool.Pool // the application role the server uses (no RLS bypass)
	st    *store.Store
	srv   *httptest.Server
	api   *Server
	// lastHeader holds the headers of the most recent request, so assertions
	// about Retry-After do not need a second request path.
	lastHeader http.Header

	// adminToken is an instance-admin session with the forced change completed.
	adminToken string

	// tenants holds the fixtures this run created, by slug.
	tenants map[string]*tenant
}

// routes exposes the server's route table to the tests: the AC-20 suite is
// generated from it, and the inventory test asserts nothing escapes it.
func (e *env) routes() []route { return e.api.routes() }

// lastRetryAfter is FR-207's header on the most recent response.
func (e *env) lastRetryAfter() string { return e.lastHeader.Get("Retry-After") }

// withLimits builds a second API over the same database with different rate
// limits, so the functional tests never have to run under a ceiling and the
// FR-207 test never has to guess.
func (e *env) withLimits(t *testing.T, keyRPS float64, keyBurst int, producerRPS float64, producerBurst int) *env {
	t.Helper()
	api := New(Deps{
		Lookup:          e.st.LookupAPIKey,
		Sessions:        e.st.LookupSession,
		Data:            e.st,
		Version:         "test-limits",
		SessionTTL:      time.Hour,
		KeyLimiter:      ratelimit.New(keyRPS, keyBurst),
		ProducerLimiter: ratelimit.New(producerRPS, producerBurst),
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	out := &env{admin: e.admin, app: e.app, st: e.st, api: api, adminToken: e.adminToken}
	out.srv = httptest.NewServer(api.Handler())
	t.Cleanup(out.srv.Close)
	return out
}

var (
	sharedOnce sync.Once
	sharedEnv  *env
	sharedErr  error

	// harnessHeadersKey is the at-rest key the shared harness's store seals header
	// values with (FR-1006), exposed so a test can seal or open a value itself.
	harnessHeadersKey atrest.Key
)

// setup returns the shared environment, provisioning it on first use.
func setup(t *testing.T) *env {
	t.Helper()
	if os.Getenv("PIGEON_TEST_DATABASE_URL") == "" {
		t.Skip("PIGEON_TEST_DATABASE_URL not set; run make test-integration for the database-backed acceptance suite")
	}
	sharedOnce.Do(func() { sharedEnv, sharedErr = buildEnv() })
	if sharedErr != nil {
		t.Fatalf("harness: %v", sharedErr)
	}
	return sharedEnv
}

func buildEnv() (*env, error) {
	dsn := os.Getenv("PIGEON_TEST_DATABASE_URL")
	if dsn == "" {
		return nil, fmt.Errorf("PIGEON_TEST_DATABASE_URL not set: refusing to skip, the AC-15/AC-20 suite must run")
	}
	ctx := context.Background()

	admin, err := db.Connect(ctx, dsn, 5)
	if err != nil {
		return nil, fmt.Errorf("connect admin: %w", err)
	}
	if err := db.Migrate(ctx, admin, 30*time.Second); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	if err := resetDatabase(ctx, admin, dsn); err != nil {
		return nil, err
	}
	// The runtime role is cluster-wide, so another suite on this cluster may be
	// creating it at the same instant: the shared fixture (internal/dbtest)
	// treats losing that race as the role now existing, and reports a genuine
	// CREATEROLE denial as one.
	if err := dbtest.ProvisionAppRole(ctx, admin, appRole, appPassword); err != nil {
		return nil, fmt.Errorf("provision %s: %w", appRole, err)
	}

	appDSN, err := withAppCredentials(dsn)
	if err != nil {
		return nil, err
	}
	app, err := db.Connect(ctx, appDSN, 10)
	if err != nil {
		return nil, fmt.Errorf("connect as %s: %w", appRole, err)
	}

	// FR-1006: the harness gives the store the at-rest key production always has,
	// so the endpoint registration path here is the same one that ships. The key is
	// per-run and protects nothing.
	headersKey, _, err := atrest.GenerateKey()
	if err != nil {
		return nil, fmt.Errorf("generate the at-rest headers key: %w", err)
	}
	st := store.New(app, store.WithHeadersKey(headersKey))
	harnessHeadersKey = headersKey
	e := &env{admin: admin, app: app, st: st}

	// FR-101: the bootstrap credential exists and demands a change.
	if _, err := identity.EnsureInstanceAdmin(ctx, admin, adminPassword); err != nil {
		return nil, fmt.Errorf("bootstrap admin: %w", err)
	}

	api := New(Deps{
		Lookup:     st.LookupAPIKey,
		Sessions:   st.LookupSession,
		Data:       st,
		Ready:      func(ctx context.Context) error { return app.Ping(ctx) },
		Version:    "test",
		SessionTTL: time.Hour,
		// The limits are generous here so that functional tests never trip them;
		// the rate-limit test builds its own server with tight numbers.
		KeyLimiter:      ratelimit.New(1000, 1000),
		ProducerLimiter: ratelimit.New(1000, 1000),
		TouchKey:        st.TouchKey,
		// The harness logs at warn and above to stderr: a failing test's server
		// error is the only diagnosis available in CI, and swallowing it costs a
		// round trip.
	}, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	e.api = api
	e.srv = httptest.NewServer(api.Handler())

	// Log in with the bootstrap password: it must work, and it must be blocked
	// from everything else until it is changed.
	sess := e.login(adminEmail, adminPassword, "")
	if !sess.User.MustChangePassword {
		return nil, fmt.Errorf("bootstrap admin session does not require a password change (FR-101)")
	}
	if code, _ := e.do(http.MethodGet, "/v1/admin/producers", sess.Token, nil); code != http.StatusForbidden {
		return nil, fmt.Errorf("admin route answered %d before the forced password change, want 403", code)
	}
	if code := e.doNoBody(http.MethodPost, "/v1/session/password", sess.Token, map[string]string{
		"old_password": adminPassword, "new_password": adminRotated,
	}); code != http.StatusNoContent {
		return nil, fmt.Errorf("forced password change answered %d, want 204", code)
	}
	e.adminToken = e.login(adminEmail, adminRotated, "").Token
	return e, nil
}

// resetDatabase empties the harness database so a re-run starts from the same
// state as CI's fresh container. It refuses to touch a database whose name does
// not look disposable: PIGEON_TEST_DATABASE_URL is a contract, but a typo in a
// developer's shell should not wipe a real one.
func resetDatabase(ctx context.Context, admin *pgxpool.Pool, dsn string) error {
	u, err := url.Parse(dsn)
	if err != nil {
		return fmt.Errorf("parse PIGEON_TEST_DATABASE_URL: %w", err)
	}
	if !strings.Contains(u.Path, "test") {
		return fmt.Errorf("refusing to truncate %q: the database name must contain \"test\"", strings.TrimPrefix(u.Path, "/"))
	}
	_, err = admin.Exec(ctx, `truncate table
		admin_audit_log, sessions, invites, consumer_users, producer_members,
		api_keys, consumers, event_types, events, outbox, idempotency_keys,
		replay_requests, producers, users restart identity cascade`)
	if err != nil {
		return fmt.Errorf("truncate: %w", err)
	}
	return nil
}

// withAppCredentials rewrites the DSN for the app role's credentials, so the
// server connects as the non-superuser runtime role rather than the test admin.
func withAppCredentials(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	u.User = url.UserPassword(appRole, appPassword)
	return u.String(), nil
}

// --- request helpers -------------------------------------------------------

// do performs a request and returns the status and the raw body. A JSON body is
// sent when body is not nil; a nil body sends no body at all, which the suspend
// and resume routes distinguish from an empty object.
func (e *env) do(method, path, token string, body any) (int, []byte) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			panic(err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, e.srv.URL+path, reader)
	if err != nil {
		panic(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := e.srv.Client().Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	e.lastHeader = resp.Header
	return resp.StatusCode, raw
}

// e.t is unused state today; kept so helpers can be called from anywhere without
// threading a *testing.T through every signature.
func (e *env) t(*testing.T) {}

func (e *env) doNoBody(method, path, token string, body any) int {
	code, _ := e.do(method, path, token, body)
	return code
}

// sessionResponse mirrors the login payload.
type sessionPayload struct {
	Token string `json:"token"`
	// ExpiresAt is what a portal turns into its cookie's Max-Age (AC-51).
	ExpiresAt string `json:"expires_at"`
	User      struct {
		ID                 string `json:"id"`
		Email              string `json:"email"`
		MustChangePassword bool   `json:"must_change_password"`
	} `json:"user"`
	Claims struct {
		InstanceAdmin bool   `json:"instance_admin"`
		ProducerID    string `json:"producer_id"`
		Role          string `json:"role"`
		ConsumerID    string `json:"consumer_id"`
		Env           string `json:"env"`
	} `json:"claims"`
}

func (e *env) login(email, password, producerSlug string) sessionPayload {
	body := map[string]string{"email": email, "password": password}
	if producerSlug != "" {
		body["producer_slug"] = producerSlug
	}
	code, raw := e.do(http.MethodPost, "/v1/session", "", body)
	if code != http.StatusCreated {
		panic(fmt.Sprintf("login %s: status %d body %s", email, code, raw))
	}
	var out sessionPayload
	if err := json.Unmarshal(raw, &out); err != nil {
		panic(err)
	}
	return out
}

// mustStatus fails the test with the body in the message: a bare status mismatch
// costs a debugging round trip that the body would have saved.
func mustStatus(t *testing.T, code int, raw []byte, want int) {
	t.Helper()
	if code != want {
		t.Fatalf("status = %d, want %d; body: %s", code, want, raw)
	}
}

func decode[T any](t *testing.T, raw []byte) T {
	t.Helper()
	var out T
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	return out
}

type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// errorCode extracts §11's machine-readable code, which is what the FRs are
// actually about.
func errorCode(t *testing.T, raw []byte) string {
	t.Helper()
	return decode[errorBody](t, raw).Error.Code
}
