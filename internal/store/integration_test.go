package store

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pigeon-hq/pigeon/internal/apikey"
	"github.com/pigeon-hq/pigeon/internal/db"
)

// testPool is the cross-producer harness (AC-20). It needs a real Postgres,
// because the property under test is enforced by row-level security and cannot
// be faked: a fake store would pass while the policies were wrong.
//
// Run with PIGEON_TEST_DATABASE_URL set to a disposable database:
//
//	docker run -d --rm -e POSTGRES_PASSWORD=pigeon -p 5432:5432 postgres:16
//	PIGEON_TEST_DATABASE_URL=postgres://postgres:pigeon@localhost/postgres?sslmode=disable go test ./internal/store/
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("PIGEON_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("PIGEON_TEST_DATABASE_URL not set; skipping the cross-producer harness (AC-20)")
	}
	ctx := context.Background()
	pool, err := db.Connect(ctx, url, 5)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(ctx, pool, 30); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

func seedProducer(t *testing.T, pool *pgxpool.Pool, slug string) (producerID string, plaintext string) {
	t.Helper()
	ctx := context.Background()
	producerID = "prd_" + slug
	if _, err := pool.Exec(ctx,
		`insert into producers (id, slug, name) values ($1, $2, $3) on conflict do nothing`,
		producerID, slug, "Producer "+slug); err != nil {
		t.Fatalf("insert producer: %v", err)
	}

	k, err := apikey.Generate("live")
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`insert into api_keys (id, producer_id, env, key_hash, display_hint) values ($1,$2,$3,$4,$5)`,
		"key_"+slug, producerID, "live", k.Hash, k.Hint); err != nil {
		t.Fatalf("insert api key: %v", err)
	}
	return producerID, k.Plaintext
}

func TestLookupAPIKeyResolvesItsOwnProducer(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	aID, aKey := seedProducer(t, pool, "alpha")
	bID, bKey := seedProducer(t, pool, "beta")

	for _, tc := range []struct{ key, want string }{{aKey, aID}, {bKey, bID}} {
		p, err := LookupAPIKey(ctx, pool, tc.key)
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
	pool := testPool(t)
	if _, err := LookupAPIKey(context.Background(), pool,
		"pgn_live_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"); err != ErrNotFound {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

// TestCrossProducerReadsAreEmpty is the AC-15 property at the SQL layer: with the
// RLS scope set to producer A, a query that asks for producer B's row by id
// returns nothing. It is deliberately written as a raw query rather than through
// a repository method, because the point is that the database refuses even when
// the WHERE clause is wrong.
func TestCrossProducerReadsAreEmpty(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	aID, _ := seedProducer(t, pool, "gamma")
	bID, _ := seedProducer(t, pool, "delta")

	if _, err := pool.Exec(ctx,
		`insert into consumers (id, producer_id, env, ref, name) values ($1,$2,'live',$3,$4)`,
		"con_delta", bID, "ref-delta", "Delta consumer"); err != nil {
		t.Fatalf("insert consumer: %v", err)
	}

	// Scope to producer A, then try to read producer B's consumer by id.
	var seen int
	err := db.ControlTx(ctx, pool, func(tx pgx.Tx) error {
		if err := db.SetProducerScope(ctx, tx, aID); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `select count(*) from consumers where id = $1`, "con_delta").Scan(&seen)
	})
	if err != nil {
		t.Fatalf("scoped read: %v", err)
	}
	if seen != 0 {
		t.Fatalf("producer %s can see %d of producer %s's rows; RLS is not enforcing isolation", aID, seen, bID)
	}
}
