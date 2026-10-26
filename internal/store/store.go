// Package store is the tenant-scoped data access layer.
//
// Every method either takes a producer id or derives one from an authenticated
// principal. There is no method that reads tenant-owned rows without naming the
// tenant (I-1), and the SQL it emits runs under the RLS policies in
// internal/db/migrations, so a forgotten WHERE clause returns nothing rather
// than another producer's data (MT-1).
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m7jay/pigeon/internal/apikey"
)

// ErrNotFound is the only "no such thing" answer the API may give for a
// tenant-scoped resource. FR-108 requires 404 rather than 403 so that a wrong
// producer id cannot be distinguished from a nonexistent one.
var ErrNotFound = errors.New("not found")

// Principal is an authenticated caller. It is what the bearer key proved, and
// nothing more: no role, no permissions, no consumer.
type Principal struct {
	KeyID      string
	ProducerID string
	Env        string
	ReplayOnly bool
}

// LookupAPIKey authenticates a plaintext key. It is the pre-tenant boundary and
// therefore the only function that reads tenant rows without a producer id; it
// goes through the SECURITY DEFINER function from migration 0002.
func LookupAPIKey(ctx context.Context, pool *pgxpool.Pool, plaintext string) (Principal, error) {
	if _, err := apikey.Env(plaintext); err != nil {
		// Reject before touching the database: a malformed key is not a lookup.
		return Principal{}, ErrNotFound
	}

	var p Principal
	err := pool.QueryRow(ctx, `
		select key_id, producer_id, env, replay_scope
		  from pigeon_auth_lookup($1)`, apikey.Hash(plaintext),
	).Scan(&p.KeyID, &p.ProducerID, &p.Env, &p.ReplayOnly)
	if errors.Is(err, pgx.ErrNoRows) {
		return Principal{}, ErrNotFound
	}
	if err != nil {
		return Principal{}, fmt.Errorf("lookup api key: %w", err)
	}
	return p, nil
}

// TouchKey uses FR-206's last_used_at. It runs inside the evidence class: a lost
// timestamp is not worth an fsync.
func TouchKey(ctx context.Context, pool *pgxpool.Pool, keyID string) error {
	_, err := pool.Exec(ctx,
		`update api_keys set last_used_at = now() where id = $1`, keyID)
	return err
}
