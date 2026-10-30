package delivery

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m7jay/pigeon/internal/db"
	"github.com/m7jay/pigeon/internal/dbtest"
	"github.com/m7jay/pigeon/internal/store"
)

// FR-402's escape hatch, end to end, on the pool the worker actually runs on.
//
// The rest of this package's integration tests build the worker on the admin pool
// (harness), which owns the schema and is therefore exempt from row-level
// security: there, a read of `endpoints` by id succeeds whatever tenant scope is
// set. That is why they could not see the defect these two pin - FR-402's flag was
// unreadable by the process that needs it, so an endpoint the operator had marked
// internal was treated as public and every attempt ended terminally at the
// request-time SSRF check before a socket was opened. cmd/pigeon serves the
// delivery worker on the unprivileged runtime role (main's `MigrationsOn` comment:
// PostgreSQL exempts a table owner from row-level security, which is exactly why
// the runtime role exists), so these tests run the worker on that pool and leave
// only the seeding on the admin pool.

// runtimeStore returns a store on the unprivileged runtime role, which is what the
// delivery worker is handed in production. The provisioning is shared with the
// other DB-backed harnesses (internal/dbtest) because a role is cluster-wide: two
// suites on one cluster can be creating `pigeon_app` at the same moment, and the
// loser of that race must not be reported as anything - the role exists, which is
// all this harness needs.
func runtimeStore(t *testing.T, admin *pgxpool.Pool) *store.Store {
	t.Helper()
	ctx := context.Background()
	if err := dbtest.ProvisionAppRole(ctx, admin, appRole, appPassword); err != nil {
		if errors.Is(err, dbtest.ErrNeedsCreateRole) {
			t.Skipf("cannot provision %s (needs CREATEROLE, e.g. the CI postgres superuser): %v", appRole, err)
		}
		t.Fatalf("provision %s: %v", appRole, err)
	}
	dsn, err := runtimeDSN(os.Getenv("PIGEON_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatalf("build runtime url: %v", err)
	}
	pool, err := db.Connect(ctx, dsn, 5)
	if err != nil {
		t.Fatalf("connect as %s: %v", appRole, err)
	}
	t.Cleanup(pool.Close)
	return store.New(pool)
}

// runtimeDSN rewrites the harness DSN's credentials to the runtime role, keeping
// the host and the database path: that credential swap is the whole difference
// between the tenant-scoped pool under test and the privileged seeding pool.
func runtimeDSN(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	u.User = url.UserPassword(appRole, appPassword)
	return u.String(), nil
}

// TestInternalEndpointAtALoopbackAddressIsDelivered is FR-402's escape hatch with
// FR-1005's request-time check, as a receiver sees it: an endpoint stored with
// `internal = true` and a loopback URL has its delivery attempted and delivered.
//
// It is the positive case the defect removed. The flag reaches the worker in the
// claim's own row (0022), so nothing the tenant-scoped pool would have to answer
// stands between the operator's opt-in and the attempt, and the sink must actually
// receive the request - a delivery row that reached a terminal state would satisfy
// a weaker assertion while the receiver still got nothing (which is how the
// reference-box run's loopback sink could show delivery rows and no deliveries).
func TestInternalEndpointAtALoopbackAddressIsDelivered(t *testing.T) {
	st, admin := harness(t)

	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// seedHarnessWithHeaders registers a loopback httptest URL with internal = true
	// (the flag FR-402 requires for a private address), verifies it, subscribes it
	// and fans one event out to it.
	_, _, deliveryID := seedHarness(t, st, admin, srv.URL)

	w := New(runtimeStore(t, admin), testOptions(5*time.Second, 30*time.Second, time.Minute))
	did, err := w.ProcessOne(context.Background())
	if err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	if !did {
		t.Fatal("ProcessOne did no work; the delivery was not claimed")
	}

	if got := hits.Load(); got != 1 {
		t.Fatalf("the loopback sink received %d requests, want 1: FR-402's escape hatch did not reach the worker's request-time check", got)
	}
	status, dbAttempts, attemptRows, deliveredAt, _ := deliveryState(t, admin, deliveryID)
	if status != "delivered" {
		t.Fatalf("delivery status = %q, want delivered", status)
	}
	if dbAttempts != 1 {
		t.Fatalf("delivery.attempts = %d, want 1", dbAttempts)
	}
	if attemptRows != 0 {
		t.Fatalf("attempts table has %d rows for a successful first attempt, want 0 (AC-28)", attemptRows)
	}
	if !deliveredAt.Valid {
		t.Fatal("deliveries.delivered_at is null; a delivered row carries it (FR-811)")
	}
}

// TestEndpointWithoutTheInternalFlagIsRefusedAtRequestTime keeps the other half
// pinned: an endpoint at a private address without the flag is refused terminally,
// and nothing is dialled. This is FR-1005/AC-19 and it must not weaken as the flag
// moves into the claim.
//
// The flag is cleared after registration because FR-402 refuses a private URL at
// registration unless the endpoint is internal - so "private URL, no flag" is
// reached by the flag changing, not by the URL being registered that way. It is a
// reachable state and the one the request-time leg exists for: an operator turning
// the hatch off for an endpoint that already has deliveries queued, or a name that
// resolved publicly when the row was written and privately when the attempt runs.
func TestEndpointWithoutTheInternalFlagIsRefusedAtRequestTime(t *testing.T) {
	st, admin := harness(t)

	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	_, _, deliveryID := seedHarness(t, st, admin, srv.URL)

	ctx := context.Background()
	var endpointID string
	if err := admin.QueryRow(ctx,
		`select endpoint_id from deliveries where id=$1`, deliveryID).Scan(&endpointID); err != nil {
		t.Fatalf("resolve the endpoint id: %v", err)
	}
	if _, err := admin.Exec(ctx,
		`update endpoints set internal = false where id = $1`, endpointID); err != nil {
		t.Fatalf("clear the internal flag: %v", err)
	}

	w := New(runtimeStore(t, admin), testOptions(5*time.Second, 30*time.Second, time.Minute))
	did, err := w.ProcessOne(ctx)
	if err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	if !did {
		t.Fatal("ProcessOne did no work; the delivery was not claimed")
	}

	if got := hits.Load(); got != 0 {
		t.Fatalf("the refused address was dialled: the sink received %d requests (FR-1005's check runs before connect)", got)
	}
	status, dbAttempts, attemptRows, _, _ := deliveryState(t, admin, deliveryID)
	if status != "failed_terminal" {
		t.Fatalf("delivery status = %q, want failed_terminal (FR-806: a blocked address does not improve on retry)", status)
	}
	if dbAttempts != 1 {
		t.Fatalf("delivery.attempts = %d, want 1: a refused attempt is an attempt (FR-1209)", dbAttempts)
	}
	if attemptRows != 1 {
		t.Fatalf("attempts table has %d rows, want 1: the refusal is recorded evidence (FR-804)", attemptRows)
	}
	_, respStatus, outcome, errorClass, _, nextRetry := attemptRow(t, admin, deliveryID)
	if outcome != "terminal" {
		t.Errorf("attempt outcome = %q, want terminal", outcome)
	}
	if errorClass != classDialError {
		t.Errorf("attempt error_class = %q, want %q", errorClass, classDialError)
	}
	if respStatus.Valid {
		t.Errorf("attempt response_status = %d, want NULL: no peer answered, so there is no status to record (FR-804)", respStatus.Int32)
	}
	if nextRetry.Valid {
		t.Errorf("attempt next_retry_at = %v, want NULL: a terminal attempt names no next attempt", nextRetry.Time)
	}
}
