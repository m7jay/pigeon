package store

import (
	"context"
	"errors"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m7jay/pigeon/internal/apikey"
	"github.com/m7jay/pigeon/internal/db"
	"github.com/m7jay/pigeon/internal/dbtest"
)

// appRole is the non-superuser the application is meant to run as in production.
// It matters for this harness specifically: a superuser bypasses row-level
// security, so an isolation test that connects as `postgres` passes no matter how
// wrong the policies are. Every assertion that is *about* isolation runs as this
// role, never as the migration role.
const (
	appRole     = "pigeon_app"
	appPassword = "pigeon_app"
)

// testPool is the cross-producer harness (AC-20). It needs a real Postgres,
// because the property under test is enforced by row-level security and cannot
// be faked: a fake store would pass while the policies were wrong.
//
// Run with PIGEON_TEST_DATABASE_URL set to a disposable database. CI does this
// with a postgres:16 service container.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("PIGEON_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PIGEON_TEST_DATABASE_URL not set; skipping the cross-producer harness (AC-20)")
	}
	ctx := context.Background()
	admin, err := db.Connect(ctx, dsn, 5)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(admin.Close)
	if err := db.Migrate(ctx, admin, 30*time.Second); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return admin
}

// appPool provisions the non-superuser role (idempotently) and returns a pool
// connected as it.
//
// The provisioning is shared with the other DB-backed harnesses (internal/dbtest)
// because a role is cluster-wide: two suites on this cluster can be creating
// `pigeon_app` at the same moment, and the loser of that race must not be
// reported as anything - the role exists, which is all this harness needs. A
// session that genuinely may not create roles (a local database whose owner has
// no CREATEROLE) still skips rather than fails, so it is a skip, not a lie - and
// it is no longer indistinguishable from a lost race.
func appPool(t *testing.T, admin *pgxpool.Pool) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	err := dbtest.ProvisionAppRole(ctx, admin, appRole, appPassword)
	if err != nil {
		if errors.Is(err, dbtest.ErrNeedsCreateRole) {
			t.Skipf("cannot provision %s (needs CREATEROLE, e.g. the CI postgres superuser): %v", appRole, err)
		}
		t.Fatalf("provision %s: %v", appRole, err)
	}

	appDSN, err := withAppCredentials(os.Getenv("PIGEON_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatalf("build app url: %v", err)
	}
	pool, err := db.Connect(ctx, appDSN, 5)
	if err != nil {
		t.Fatalf("connect as %s: %v", appRole, err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func withAppCredentials(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	u.User = url.UserPassword(appRole, appPassword)
	return u.String(), nil
}

// seedProducer writes through the admin pool: seeding is the platform's own
// privileged path, and RLS deliberately forbids a tenant role from creating
// producers.
func seedProducer(t *testing.T, admin *pgxpool.Pool, slug string) (producerID string, plaintext string) {
	t.Helper()
	ctx := context.Background()
	producerID = "prd_" + slug
	if _, err := admin.Exec(ctx,
		`insert into producers (id, slug, name) values ($1, $2, $3) on conflict do nothing`,
		producerID, slug, "Producer "+slug); err != nil {
		t.Fatalf("insert producer: %v", err)
	}

	k, err := apikey.Generate("live")
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	if _, err := admin.Exec(ctx,
		`insert into api_keys (id, producer_id, env, key_hash, display_hint) values ($1,$2,$3,$4,$5)
		 on conflict (id) do update set key_hash = excluded.key_hash`,
		"key_"+slug, producerID, "live", k.Hash, k.Hint); err != nil {
		t.Fatalf("insert api key: %v", err)
	}
	return producerID, k.Plaintext
}

func TestLookupAPIKeyResolvesItsOwnProducer(t *testing.T) {
	admin := testPool(t)
	app := appPool(t, admin)
	ctx := context.Background()

	aID, aKey := seedProducer(t, admin, "alpha")
	bID, bKey := seedProducer(t, admin, "beta")

	for _, tc := range []struct{ key, want string }{{aKey, aID}, {bKey, bID}} {
		p, err := LookupAPIKey(ctx, app, tc.key)
		if err != nil {
			t.Fatalf("LookupAPIKey: %v", err)
		}
		if p.ProducerID != tc.want {
			t.Fatalf("key resolved to %s, want %s", p.ProducerID, tc.want)
		}
		if p.Env != "live" {
			t.Fatalf("env = %s, want live", p.Env)
		}
	}
}

func TestUnknownKeyIsNotFound(t *testing.T) {
	admin := testPool(t)
	app := appPool(t, admin)
	if _, err := LookupAPIKey(context.Background(), app,
		"pgn_live_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"); err != ErrNotFound {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

// TestCrossProducerReadsAreEmpty is the AC-15 property at the SQL layer: with the
// RLS scope set to producer A, a query that asks for producer B's row by id
// returns nothing. It is deliberately written as a raw query rather than through
// a repository method, because the point is that the database refuses even when
// the WHERE clause is wrong.
//
// The positive assertion is not decoration: a policy that unconditionally returns
// zero rows would make the negative assertion pass while breaking the product.
func TestCrossProducerReadsAreEmpty(t *testing.T) {
	admin := testPool(t)
	app := appPool(t, admin)
	ctx := context.Background()

	aID, _ := seedProducer(t, admin, "gamma")
	bID, _ := seedProducer(t, admin, "delta")

	for _, row := range []struct{ id, producer, ref string }{
		{"con_gamma", aID, "ref-gamma"},
		{"con_delta", bID, "ref-delta"},
	} {
		if _, err := admin.Exec(ctx,
			`insert into consumers (id, producer_id, env, ref, name) values ($1,$2,'live',$3,$4)
			 on conflict do nothing`,
			row.id, row.producer, row.ref, "Consumer "+row.id); err != nil {
			t.Fatalf("insert consumer: %v", err)
		}
	}

	countScoped := func(producerID, consumerID string) int {
		var n int
		err := db.ControlTx(ctx, app, func(tx pgx.Tx) error {
			if err := db.SetProducerScope(ctx, tx, producerID); err != nil {
				return err
			}
			if err := db.SetEnvScope(ctx, tx, "live"); err != nil {
				return err
			}
			return tx.QueryRow(ctx, `select count(*) from consumers where id = $1`, consumerID).Scan(&n)
		})
		if err != nil {
			t.Fatalf("scoped read: %v", err)
		}
		return n
	}

	if got := countScoped(aID, "con_gamma"); got != 1 {
		t.Fatalf("producer %s cannot see its own consumer (got %d); the RLS scope is broken, not strict", aID, got)
	}
	if got := countScoped(aID, "con_delta"); got != 0 {
		t.Fatalf("producer %s can see %d of producer %s's rows; RLS is not enforcing isolation", aID, got, bID)
	}

	// FR-103, enforced by the same policy: the producer scope without an
	// environment scope sees nothing at all, so a query that forgets the
	// environment fails closed instead of reading both planes.
	var withoutEnv int
	if err := db.ControlTx(ctx, app, func(tx pgx.Tx) error {
		if err := db.SetProducerScope(ctx, tx, aID); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `select count(*) from consumers where id = $1`, "con_gamma").Scan(&withoutEnv)
	}); err != nil {
		t.Fatalf("read without env scope: %v", err)
	}
	if withoutEnv != 0 {
		t.Fatalf("a transaction with a producer scope but no environment scope saw %d rows; FR-103 is not enforced in SQL", withoutEnv)
	}
}

// TestProducersAreAdminOnly proves both halves of the producers policy from
// 0003: the tenant role sees nothing by default (fail closed), and the explicit
// instance-admin path sees the rows it is meant to manage. Without the second
// assertion, `using (false)` would pass this test while making the admin API
// impossible to build.
func TestProducersAreAdminOnly(t *testing.T) {
	admin := testPool(t)
	app := appPool(t, admin)
	ctx := context.Background()

	aID, _ := seedProducer(t, admin, "epsilon")
	bID, _ := seedProducer(t, admin, "zeta")

	countProducers := func(setScope func(pgx.Tx) error) (int, error) {
		var n int
		err := db.ControlTx(ctx, app, func(tx pgx.Tx) error {
			if err := setScope(tx); err != nil {
				return err
			}
			return tx.QueryRow(ctx,
				`select count(*) from producers where id in ($1, $2)`, aID, bID).Scan(&n)
		})
		return n, err
	}

	// Tenant scope only: producers stay invisible, because no producer id can
	// isolate a producer list from itself (0001).
	if n, err := countProducers(func(tx pgx.Tx) error {
		return db.SetProducerScope(ctx, tx, aID)
	}); err != nil {
		t.Fatalf("tenant-scoped read: %v", err)
	} else if n != 0 {
		t.Fatalf("tenant role saw %d producers under tenant scope; producers must fail closed", n)
	}

	// Unset marker: still invisible. This is the "forgot to opt in" case.
	if n, err := countProducers(func(pgx.Tx) error { return nil }); err != nil {
		t.Fatalf("unmarked read: %v", err)
	} else if n != 0 {
		t.Fatalf("tenant role saw %d producers without the admin marker; the default must be no rows", n)
	}

	// Admin path: both rows are visible.
	if n, err := countProducers(func(tx pgx.Tx) error {
		return db.SetInstanceAdmin(ctx, tx)
	}); err != nil {
		t.Fatalf("admin-scoped read: %v", err)
	} else if n != 2 {
		t.Fatalf("instance-admin path saw %d of 2 producers; the admin policy is not working", n)
	}

	// Writes fail closed too: the tenant role cannot create a producer without
	// the marker. A write path that only guards reads would be a hole.
	_, err := admin.Exec(ctx, `delete from producers where id = 'prd_eta'`)
	if err != nil {
		t.Fatalf("cleanup prd_eta: %v", err)
	}
	if err := db.ControlTx(ctx, app, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `insert into producers (id, slug, name) values ('prd_eta','eta','Producer eta')`)
		return err
	}); err == nil {
		t.Fatalf("tenant role inserted a producer without the admin marker; RLS write path is open")
	}
	if err := db.ControlTx(ctx, app, func(tx pgx.Tx) error {
		if err := db.SetInstanceAdmin(ctx, tx); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `insert into producers (id, slug, name) values ('prd_eta','eta','Producer eta') on conflict (id) do nothing`)
		return err
	}); err != nil {
		t.Fatalf("instance-admin insert failed: %v", err)
	}
}

// TestAuditLogIsAppendOnly is MT-5's integrity half: the application role can
// insert and read audit rows on the admin path, and cannot rewrite them. A log a
// compromised app role can edit records nothing.
func TestAuditLogIsAppendOnly(t *testing.T) {
	admin := testPool(t)
	app := appPool(t, admin)
	ctx := context.Background()

	producerID, _ := seedProducer(t, admin, "auditlog")
	var actorID string
	if err := admin.QueryRow(ctx, `
		insert into users (id, email, password_hash) values ('usr_auditor', 'auditor@example.test', 'x')
		on conflict (id) do update set email = excluded.email returning id`).Scan(&actorID); err != nil {
		t.Fatalf("seed actor: %v", err)
	}

	insert := func() error {
		return db.ControlTx(ctx, app, func(tx pgx.Tx) error {
			if err := db.SetInstanceAdmin(ctx, tx); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `
				insert into admin_audit_log (id, actor_user_id, action, target_producer_id, reason)
				values ('aud_t1', $1, 'producer.suspended', $2, 'testing')`, actorID, producerID)
			return err
		})
	}
	// The audit row carries a fixed id, and the disposable test database is reused
	// across local runs: clear it so the row under test is this run's, not a
	// previous run's leftover.
	if _, err := admin.Exec(ctx, `delete from admin_audit_log where id = 'aud_t1'`); err != nil {
		t.Fatalf("reset aud_t1: %v", err)
	}
	if err := insert(); err != nil {
		t.Fatalf("admin path could not write an audit row: %v", err)
	}

	// No update or delete policy exists, so RLS refuses both silently (0 rows)
	// rather than erroring: assert the row survives either way.
	for _, stmt := range []string{
		`update admin_audit_log set action = 'producer.resumed' where id = 'aud_t1'`,
		`delete from admin_audit_log where id = 'aud_t1'`,
	} {
		_ = db.ControlTx(ctx, app, func(tx pgx.Tx) error {
			if err := db.SetInstanceAdmin(ctx, tx); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, stmt)
			return err
		})
	}

	var action string
	if err := admin.QueryRow(ctx, `select action from admin_audit_log where id = 'aud_t1'`).Scan(&action); err != nil {
		t.Fatalf("audit row disappeared: %v", err)
	}
	if action != "producer.suspended" {
		t.Fatalf("audit row was rewritten to %q; the log must be append-only", action)
	}
}

// TestAuthLookupReportsSuspension is FR-107 at the authentication boundary: the
// key stays valid, and the lookup says the producer is suspended so the caller can
// answer 403 rather than 401.
func TestAuthLookupReportsSuspension(t *testing.T) {
	admin := testPool(t)
	app := appPool(t, admin)
	ctx := context.Background()

	producerID, key := seedProducer(t, admin, "suspendedco")
	// seedProducer inserts with `on conflict do nothing`, so a previous run against
	// this same database left the producer suspended. The assertion below is about a
	// fresh producer, so clear that state rather than inherit it.
	if _, err := admin.Exec(ctx, `update producers set suspended_at = null where id = $1`, producerID); err != nil {
		t.Fatalf("reset suspension of %s: %v", producerID, err)
	}

	p, err := LookupAPIKey(ctx, app, key)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if p.Suspended {
		t.Fatal("a fresh producer is reported suspended")
	}

	if _, err := admin.Exec(ctx, `update producers set suspended_at = now() where id = $1`, producerID); err != nil {
		t.Fatalf("suspend: %v", err)
	}
	p, err = LookupAPIKey(ctx, app, key)
	if err != nil {
		t.Fatalf("lookup after suspension: %v", err)
	}
	if !p.Suspended {
		t.Fatal("a suspended producer's key authenticates without the suspension flag (FR-107)")
	}
}

// TestRevokedKeyStopsAuthenticating is FR-205 at the SQL layer, with no cache and
// no delay: the very next lookup returns nothing.
func TestRevokedKeyStopsAuthenticating(t *testing.T) {
	admin := testPool(t)
	app := appPool(t, admin)
	ctx := context.Background()

	producerID, key := seedProducer(t, admin, "revokedco")
	if _, err := admin.Exec(ctx,
		`update api_keys set revoked_at = now() where producer_id = $1`, producerID); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if _, err := LookupAPIKey(ctx, app, key); err != ErrNotFound {
		t.Fatalf("a revoked key still authenticates: err = %v", err)
	}
}
