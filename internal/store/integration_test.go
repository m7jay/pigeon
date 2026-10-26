package store

import (
	"context"
	"net/url"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m7jay/pigeon/internal/apikey"
	"github.com/m7jay/pigeon/internal/db"
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
	if err := db.Migrate(ctx, admin, 30); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return admin
}

// appPool provisions the non-superuser role (idempotently) and returns a pool
// connected as it. Skips rather than fails when the connected role may not create
// roles, so a developer's local database without CREATEROLE is a skip, not a lie.
func appPool(t *testing.T, admin *pgxpool.Pool) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	_, err := admin.Exec(ctx, `
		do $$
		begin
			if not exists (select 1 from pg_roles where rolname = '`+appRole+`') then
				create role `+appRole+` login password '`+appPassword+`' nosuperuser nobypassrls;
			end if;
		end $$;`)
	if err != nil {
		t.Skipf("cannot provision %s (needs CREATEROLE, e.g. the CI postgres superuser): %v", appRole, err)
	}
	for _, grant := range []string{
		`grant usage on schema public to ` + appRole,
		`grant select, insert, update, delete on all tables in schema public to ` + appRole,
		`grant usage, select on all sequences in schema public to ` + appRole,
		`grant execute on all functions in schema public to ` + appRole,
	} {
		if _, err := admin.Exec(ctx, grant); err != nil {
			t.Fatalf("grant: %v", err)
		}
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
}
