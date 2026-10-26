package identity

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m7jay/pigeon/internal/db"
)

// TestBootstrapIsIdempotentAndForcesAChange is FR-101 against a real database: the
// second boot finds the admin and does not overwrite it, and the row it creates
// carries the forced-change marker.
//
// The harness is in this package rather than store's because the property is about
// the bootstrap, not about tenant data.
func TestBootstrapIsIdempotentAndForcesAChange(t *testing.T) {
	dsn := os.Getenv("PIGEON_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PIGEON_TEST_DATABASE_URL not set; the bootstrap path needs a real users table")
	}
	ctx := context.Background()
	pool, err := db.Connect(ctx, dsn, 5)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(ctx, pool, 30*time.Second); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := clearAdmins(ctx, pool); err != nil {
		t.Fatalf("clear admins: %v", err)
	}

	created, err := EnsureInstanceAdmin(ctx, pool, "bootstrap-horse-battery-staple")
	if err != nil {
		t.Fatalf("first bootstrap: %v", err)
	}
	if !created {
		t.Fatal("first bootstrap reported that it created nothing")
	}

	var mustChange bool
	if err := pool.QueryRow(ctx, `
		select must_change_password from users where is_instance_admin limit 1`).Scan(&mustChange); err != nil {
		t.Fatalf("read admin: %v", err)
	}
	if !mustChange {
		t.Fatal("the bootstrap credential does not require a change (FR-101)")
	}

	// A second boot with a different password must not recreate or overwrite it:
	// otherwise anyone with the env file can take the instance at the next restart.
	created, err = EnsureInstanceAdmin(ctx, pool, "a-completely-different-password")
	if err != nil {
		t.Fatalf("second bootstrap: %v", err)
	}
	if created {
		t.Fatal("second bootstrap created another admin")
	}
	var count int
	if err := pool.QueryRow(ctx, `select count(*) from users where is_instance_admin`).Scan(&count); err != nil {
		t.Fatalf("count admins: %v", err)
	}
	if count != 1 {
		t.Fatalf("%d instance admins exist, want exactly 1", count)
	}
}

// clearAdmins removes the bootstrap account so the test starts from "first run".
// The dependent rows are deleted explicitly rather than by cascade: `invites` and
// `admin_audit_log` reference a user with NO ACTION on purpose, because a record of
// who invited whom and who suspended what must outlive the account.
func clearAdmins(ctx context.Context, pool *pgxpool.Pool) error {
	for _, stmt := range []string{
		`delete from admin_audit_log where actor_user_id in (select id from users where is_instance_admin)`,
		`delete from invites where invited_by in (select id from users where is_instance_admin)`,
		`delete from sessions where user_id in (select id from users where is_instance_admin)`,
		`delete from users where is_instance_admin`,
	} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}
