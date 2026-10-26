package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/m7jay/pigeon/internal/apikey"
	"github.com/m7jay/pigeon/internal/id"
)

// KeyMeta is everything about a key that may be shown after creation: the hint,
// never the key (FR-202).
type KeyMeta struct {
	ID          string
	Env         string
	Hint        string
	ReplayScope bool
	LastUsedAt  *time.Time
	RevokedAt   *time.Time
	CreatedAt   time.Time
}

// IssuedKey carries the plaintext exactly once, on the response to the request
// that created it.
type IssuedKey struct {
	KeyMeta
	Plaintext string
}

// IssueKey mints a key for the caller's environment (FR-203: one producer, one
// environment). Rotation without downtime (FR-204) falls out of this being
// additive: nothing here touches the producer's existing keys.
//
// replayScope is FR-208's optional scope: a publish-only credential, separate from
// an administrative one. It is stored at issuance and reported on every read, and
// it reaches the request as Principal.ReplayOnly. The route that enforces it (the
// publish path) arrives with S2; until then the scope is a property of the
// credential rather than a rule, which is stated in SPEC.md rather than implied.
func (s *Store) IssueKey(ctx context.Context, sc Scope, actor Actor, replayScope bool) (IssuedKey, error) {
	k, err := apikey.Generate(sc.Env)
	if err != nil {
		return IssuedKey{}, fmt.Errorf("%w: %s", ErrInvalid, err.Error())
	}

	var out IssuedKey
	err = s.scoped(ctx, sc, func(tx pgx.Tx) error {
		out.ID = id.New("key")
		out.Env, out.Hint, out.Plaintext = k.Env, k.Hint, k.Plaintext
		out.ReplayScope = replayScope
		err := tx.QueryRow(ctx, `
			insert into api_keys (id, producer_id, env, key_hash, display_hint, replay_scope)
			values ($1, $2, $3, $4, $5, $6)
			returning created_at`,
			out.ID, sc.ProducerID, sc.Env, k.Hash, k.Hint, replayScope,
		).Scan(&out.CreatedAt)
		return mapPgError(err, ErrConflict, ErrNotFound)
	})
	if err != nil {
		return IssuedKey{}, err
	}
	return out, nil
}

// ListKeys returns the caller's environment's keys, including revoked ones:
// "which key was used last Tuesday" is a question about history, and hiding
// revoked rows would make it unanswerable.
func (s *Store) ListKeys(ctx context.Context, sc Scope) ([]KeyMeta, error) {
	var out []KeyMeta
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			select id, env, display_hint, replay_scope, last_used_at, revoked_at, created_at
			  from api_keys
			 where producer_id = $1 and env = $2
			 order by created_at desc`, sc.ProducerID, sc.Env)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var k KeyMeta
			if err := rows.Scan(&k.ID, &k.Env, &k.Hint, &k.ReplayScope, &k.LastUsedAt, &k.RevokedAt, &k.CreatedAt); err != nil {
				return err
			}
			out = append(out, k)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RevokeKey is FR-205. There is no cache to invalidate: authentication reads
// `revoked_at is null` through pigeon_auth_lookup on every request, so the next
// request from any instance is refused. That is strictly stronger than the 60 s
// budget, and it is the reason the budget is easy to meet.
//
// A key id belonging to another producer, or the other environment, is
// ErrNotFound (FR-108 + FR-103).
func (s *Store) RevokeKey(ctx context.Context, sc Scope, keyID string) error {
	return s.scoped(ctx, sc, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			update api_keys set revoked_at = now()
			 where id = $1 and producer_id = $2 and env = $3 and revoked_at is null`,
			keyID, sc.ProducerID, sc.Env)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			// Either no such key in this scope, or it is already revoked. Both
			// answer "not found": a client retrying a revocation should not have
			// to distinguish them, and an attacker must not learn which.
			return ErrNotFound
		}
		return nil
	})
}

// GetKey reads one key's metadata within the caller's scope, for routes that
// take a key id. Missing and out-of-scope are the same answer.
func (s *Store) GetKey(ctx context.Context, sc Scope, keyID string) (KeyMeta, error) {
	var k KeyMeta
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			select id, env, display_hint, replay_scope, last_used_at, revoked_at, created_at
			  from api_keys
			 where producer_id = $1 and env = $2 and id = $3`,
			sc.ProducerID, sc.Env, keyID,
		).Scan(&k.ID, &k.Env, &k.Hint, &k.ReplayScope, &k.LastUsedAt, &k.RevokedAt, &k.CreatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	})
	if err != nil {
		return KeyMeta{}, err
	}
	return k, nil
}
