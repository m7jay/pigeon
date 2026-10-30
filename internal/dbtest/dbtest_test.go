package dbtest

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m7jay/pigeon/internal/db"
)

// These tests exercise the privilege boundary itself - creating a role is the
// thing under test - so they need the same superuser DSN every integration
// harness gets (PIGEON_TEST_DATABASE_URL; CI's postgres user).
//
// The probe roles are named per test and dropped again: a role is cluster-wide,
// so a leftover would be another worker's problem.

const (
	raceRole    = "pigeon_probe_race"
	targetRole  = "pigeon_probe_target"
	limitedRole = "pigeon_probe_nocreate"
	probePass   = "pigeon_probe"
	// limitedRoleFormat is the same create as the fixture's, minus CREATEROLE.
	limitedRoleFormat = `create role %I login password %L nocreatedb nocreaterole nosuperuser nobypassrls`
	grantUsageQ       = `select has_schema_privilege($1, 'public', 'USAGE')`
	waitersQuery      = `select count(*) from pg_stat_activity
		where datname = current_database() and wait_event_type = 'Lock'`
)

func adminPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("PIGEON_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PIGEON_TEST_DATABASE_URL not set; skipping the role-provisioning fixture tests")
	}
	pool, err := db.Connect(context.Background(), dsn, 5)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// TestProvisionAppRoleToleratesAConcurrentCreator is the regression test for the
// shared-cluster race this package exists for: two suites both read "no such
// role" and both create it, and the loser must treat the role existing as
// success. Both shapes the loser's create can report are covered, because they
// are produced by two different interleavings.
func TestProvisionAppRoleToleratesAConcurrentCreator(t *testing.T) {
	admin := adminPool(t)
	ctx := context.Background()

	// Each subtest owns the role: a subtest that fails must not leave the role
	// behind for the next one, whose seed would then report duplicate_object
	// instead of the thing it is testing.
	fresh := func(t *testing.T) {
		t.Helper()
		dropProbeRole(t, admin, raceRole)
		t.Cleanup(func() { dropProbeRole(t, admin, raceRole) })
	}

	// createStmt is the statement under test: the fixture's own, so the test
	// cannot drift from what the harnesses run.
	createStmt, err := createStatement(ctx, admin, raceRole, probePass)
	if err != nil {
		t.Fatalf("render the fixture's create: %v", err)
	}
	if mirror := render(t, admin, createRoleFormat, raceRole, probePass); mirror != createStmt {
		t.Fatalf("rendered %q, fixture builds %q", mirror, createStmt)
	}

	t.Run("the winner committed first", func(t *testing.T) {
		fresh(t)
		// The other suite's create landed before ours was planned. This is the
		// shape that used to reach the store harness's "needs CREATEROLE" skip.
		if _, err := admin.Exec(ctx, createStmt); err != nil {
			t.Fatalf("seed the winner's role: %v", err)
		}
		if _, err := admin.Exec(ctx, createStmt); codeOf(err) != "42710" {
			t.Fatalf("the second create reported %v (SQLSTATE %s), want duplicate_object 42710", err, codeOf(err))
		}
		if err := createRole(ctx, admin, raceRole, probePass); err != nil {
			t.Fatalf("a create that lost the race to a committed winner must be success, got: %v", err)
		}
	})

	t.Run("both creates are in flight at once", func(t *testing.T) {
		fresh(t)
		// Two suites booting together are genuinely simultaneous: the winner's
		// INSERT into pg_authid is still uncommitted when the loser's INSERT
		// runs, so CreateRole's own "already exists" check misses on both sides
		// and the unique index under pg_authid is what catches the loser (23505,
		// not 42710). Tolerating only duplicate_object would leave this open.
		conn, err := admin.Acquire(ctx)
		if err != nil {
			t.Fatalf("acquire the winner's connection: %v", err)
		}
		defer conn.Release()
		winner, err := conn.Begin(ctx)
		if err != nil {
			t.Fatalf("begin the winner's transaction: %v", err)
		}
		if _, err := winner.Exec(ctx, createStmt); err != nil {
			t.Fatalf("the winner's create: %v", err)
		}

		raw := make(chan error, 1)
		tolerated := make(chan error, 1)
		go func() {
			_, err := admin.Exec(ctx, createStmt)
			raw <- err
		}()
		go func() { tolerated <- createRole(ctx, admin, raceRole, probePass) }()

		// Only commit once both losers are provably parked inside their INSERT,
		// so the interleaving this subtest names is the one it exercised. If the
		// poll times out the run is still correct - both creates are tolerated -
		// it just observed the committed-winner ordering instead.
		if parked := waitForLockWaiters(ctx, admin, 2, 2*time.Second); parked < 2 {
			t.Logf("only %d of 2 losers were parked on the winner's uncommitted row; committing anyway", parked)
		}
		if err := winner.Commit(ctx); err != nil {
			t.Fatalf("commit the winner's create: %v", err)
		}

		rawErr := <-raw
		t.Logf("the simultaneous create reported %q (SQLSTATE %s)", rawErr, codeOf(rawErr))
		if code := codeOf(rawErr); code != "42710" && code != "23505" {
			t.Fatalf("the simultaneous create reported SQLSTATE %s (%v); the two shapes this race produces are 42710 and 23505, so a third one means the tolerance is incomplete", code, rawErr)
		}
		if !isCreateRace(rawErr) {
			t.Fatalf("isCreateRace does not recognise %v; the loser of a simultaneous create is not tolerated", rawErr)
		}
		if err := <-tolerated; err != nil {
			t.Fatalf("the create that lost the simultaneous race must be success, got: %v", err)
		}
	})

	t.Run("provisioning over an existing role is a no-op", func(t *testing.T) {
		fresh(t)
		if _, err := admin.Exec(ctx, createStmt); err != nil {
			t.Fatalf("seed the role: %v", err)
		}
		if err := ProvisionAppRole(ctx, admin, raceRole, probePass); err != nil {
			t.Fatalf("provisioning a role that is already there must succeed, got: %v", err)
		}
		var granted bool
		if err := admin.QueryRow(ctx, grantUsageQ, raceRole).Scan(&granted); err != nil {
			t.Fatalf("read has_schema_privilege: %v", err)
		}
		if !granted {
			t.Fatalf("ProvisionAppRole reported success without granting %s usage on the schema", raceRole)
		}
	})
}

// TestProvisionAppRoleReportsAGenuineCreateRoleDenial is the control: a session
// that really may not create roles must still be reported as that, not
// swallowed into the race tolerance. internal/store turns this into its skip and
// the other harnesses into a failure - both keep the honest label.
func TestProvisionAppRoleReportsAGenuineCreateRoleDenial(t *testing.T) {
	admin := adminPool(t)
	ctx := context.Background()
	dropProbeRole(t, admin, limitedRole)
	dropProbeRole(t, admin, targetRole)
	t.Cleanup(func() {
		dropProbeRole(t, admin, limitedRole)
		dropProbeRole(t, admin, targetRole)
	})

	if _, err := admin.Exec(ctx, render(t, admin, limitedRoleFormat, limitedRole, probePass)); err != nil {
		t.Fatalf("create the CREATEROLE-less probe role: %v", err)
	}
	limited := connectAs(t, limitedRole, probePass)

	err := ProvisionAppRole(ctx, limited, targetRole, probePass)
	if err == nil {
		t.Fatalf("%s may not create roles, but provisioning reported success", limitedRole)
	}
	if !errors.Is(err, ErrNeedsCreateRole) {
		t.Fatalf("a CREATEROLE denial must be reported as ErrNeedsCreateRole (a lost race is not an error at all), got: %v", err)
	}
	if !strings.Contains(err.Error(), "needs CREATEROLE") {
		t.Fatalf("the error does not label the genuine inability: %v", err)
	}
	exists, err := roleExists(ctx, admin, targetRole)
	if err != nil {
		t.Fatalf("read pg_catalog.pg_roles: %v", err)
	}
	if exists {
		t.Fatalf("%s exists, so the denial was not reported as a denial", targetRole)
	}
}

// connectAs opens a pool as role, over the same DSN the harness uses.
func connectAs(t *testing.T, role, password string) *pgxpool.Pool {
	t.Helper()
	u, err := url.Parse(os.Getenv("PIGEON_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatalf("parse PIGEON_TEST_DATABASE_URL: %v", err)
	}
	u.User = url.UserPassword(role, password)
	pool, err := db.Connect(context.Background(), u.String(), 2)
	if err != nil {
		t.Fatalf("connect as %s: %v", role, err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// dropProbeRole removes a role this test created. DROP ROLE refuses while a grant
// still depends on the role, so revoke its privileges first; a role that is not
// there is not an error.
func dropProbeRole(t *testing.T, admin *pgxpool.Pool, role string) {
	t.Helper()
	ctx := context.Background()
	exists, err := roleExists(ctx, admin, role)
	if err != nil {
		t.Errorf("cleanup: read pg_catalog.pg_roles: %v", err)
		return
	}
	if !exists {
		return
	}
	if _, err := admin.Exec(ctx, `drop owned by `+quoteRole(role)); err != nil {
		t.Errorf("cleanup: drop owned by %s: %v", role, err)
	}
	if _, err := admin.Exec(ctx, `drop role `+quoteRole(role)); err != nil {
		t.Errorf("cleanup: drop role %s: %v", role, err)
	}
}

// render builds a statement from a format string on the server, the same way the
// fixture builds its own: see createStatement. It lets a test run a variant of the
// fixture's SQL without hand-quoting.
func render(t *testing.T, admin *pgxpool.Pool, format, role, password string) string {
	t.Helper()
	var stmt string
	if err := admin.QueryRow(context.Background(),
		`select format($1::text, $2::text, $3::text)`, format, role, password).Scan(&stmt); err != nil {
		t.Fatalf("render %q: %v", format, err)
	}
	return stmt
}

// waitForLockWaiters polls until want sessions in this database are blocked on a
// lock, and returns how many it saw.
func waitForLockWaiters(ctx context.Context, admin *pgxpool.Pool, want int32, budget time.Duration) int32 {
	deadline := time.Now().Add(budget)
	for {
		var n int32
		if err := admin.QueryRow(ctx, waitersQuery).Scan(&n); err == nil && n >= want {
			return n
		}
		if time.Now().After(deadline) {
			return 0
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// codeOf returns err's SQLSTATE, or "" when it is not a Postgres error.
func codeOf(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}
