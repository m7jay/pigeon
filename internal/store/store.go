// Package store is the tenant-scoped data access layer.
//
// Every method either takes a Scope (a producer id plus an environment) or
// derives one from an authenticated principal. There is no method that reads
// tenant-owned rows without naming the tenant (I-1), and the SQL it emits runs
// under the RLS policies in internal/db/migrations, so a forgotten WHERE clause
// returns nothing rather than another producer's data (MT-1).
//
// Two properties are enforced twice on purpose:
//
//   - Producer isolation: the Scope is in the WHERE clause *and* in
//     `pigeon.producer_id` for RLS.
//   - Environment separation (FR-103): the Scope's Env is in the WHERE clause
//     *and* in `pigeon.env`, which since 0004 is part of the policy predicate.
//     A transaction that sets only one of the two scopes sees zero rows.
package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m7jay/pigeon/internal/apikey"
	"github.com/m7jay/pigeon/internal/db"
)

// ErrNotFound is the only "no such thing" answer the API may give for a
// tenant-scoped resource. FR-108 requires 404 rather than 403 so that a wrong
// producer id cannot be distinguished from a nonexistent one.
var ErrNotFound = errors.New("not found")

// ErrConflict is a uniqueness violation the caller can act on: a duplicate
// producer slug, a second pending invitation for the same address, a user
// already bound to a consumer.
var ErrConflict = errors.New("conflict")

// ErrInvalid marks a value the caller must fix (422 rather than 500).
var ErrInvalid = errors.New("invalid")

// ErrInvalidEnv rejects anything that is not `test` or `live` before it reaches
// SQL (FR-103). A typo must not become a third data plane.
var ErrInvalidEnv = fmt.Errorf("%w: environment must be test or live", ErrInvalid)

// ErrAmbiguousProducer is FR-105 seen from the login form: a user in several
// producers must say which one the session is for, rather than have the server
// pick one.
var ErrAmbiguousProducer = errors.New("user belongs to several producers; name one")

// ErrLastOwner refuses to remove the only owner of a producer: a producer with
// no owner has no one who can invite the replacement.
var ErrLastOwner = errors.New("producer must keep at least one owner")

// ErrSessionRequired marks an invitation addressed to an address that already has
// an account. The invitation token is handed to the inviter (no mailer yet), so it
// cannot prove control of the account it names: the invitee has to accept it with
// their own session, and acceptance never touches their credentials.
var ErrSessionRequired = errors.New("this invitation belongs to an existing account; accept it with that account's session")

// Envs are the two data planes FR-103 requires to stay separate.
const (
	EnvTest = "test"
	EnvLive = "live"
)

// ValidEnv reports whether env is one of the two data planes.
func ValidEnv(env string) bool { return env == EnvTest || env == EnvLive }

// Scope is the tenant a query runs in. Both halves are mandatory at every call
// site: there is no "all environments" scope, because a query that spans test
// and live is the leak FR-103 exists to prevent.
type Scope struct {
	ProducerID string
	Env        string
}

// Actor is the authenticated party performing a write, carried so that audit
// rows name a person or a key rather than a service.
type Actor struct {
	UserID    string
	KeyID     string
	RequestID string
}

// Principal is an authenticated API-key caller. It is what the bearer key
// proved, and nothing more: no role, no permissions, no consumer.
type Principal struct {
	KeyID      string
	ProducerID string
	Env        string
	ReplayOnly bool
	Suspended  bool
}

// Scope returns the tenant this key may touch.
func (p Principal) Scope() Scope { return Scope{ProducerID: p.ProducerID, Env: p.Env} }

// Store is the whole data-access surface. It holds only the pool; every method
// opens its own transaction so the scope variables are set on the same
// connection that runs the query (a pooled connection reused without them would
// fail closed, which is safe but useless).
type Store struct {
	pool *pgxpool.Pool
	// attemptBodyCap is SB-4's truncation applied on the database side of the
	// evidence insert: 0 stores no body at all, a negative value stores it as
	// given. It defaults to the 2 KB SB-4 default; main wires the configured
	// PIGEON_DELIVERY_MAX_BODY_BYTES through WithAttemptBodyCap so the cap is a
	// setting rather than a constant.
	attemptBodyCap int

	// ensured caches which daily partitions this process has already confirmed
	// exist, so fan-out does not call the partition DDL primitive once per event
	// (SB-1). Entries expire quickly; the sweeper keeps partitions created ahead,
	// so a miss here is the rare backlog case, not the common path.
	ensureMu sync.Mutex
	ensured  map[string]time.Time
}

// DefaultAttemptBodyCap is SB-4's default per-row cost control: the first 2 KB
// of a response body, configurable down to 0.
const DefaultAttemptBodyCap = 2048

// Option configures a Store at construction.
type Option func(*Store)

// WithAttemptBodyCap sets SB-4's database-side body cap. The worker also
// truncates at capture with the same configured value; this is the second half,
// so a row cannot exceed the cap even if a future caller forgets.
func WithAttemptBodyCap(n int) Option {
	return func(s *Store) { s.attemptBodyCap = n }
}

// New wraps a pool. A nil pool is a programming error, not a runtime condition.
func New(pool *pgxpool.Pool, opts ...Option) *Store {
	s := &Store{pool: pool, attemptBodyCap: DefaultAttemptBodyCap, ensured: map[string]time.Time{}}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Pool exposes the pool for the few callers that run their own transaction
// (migrations, readiness probes). It is not a licence to write SQL elsewhere:
// SPEC.md's layering rule still applies.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// scoped runs fn inside a transaction with the tenant scope variables set.
// ControlTx (synchronous_commit=on) is correct for everything S1 writes: these
// rows are identity and configuration, where losing a commit after a 201 would
// be a lie the client already acted on.
//
// An empty Env is legal and means "producer-wide": the producer scope is set,
// the environment scope is not, and since 0004's policies require both, every
// environment-bearing table returns zero rows. Only `producers`,
// `producer_members` and producer-wide invitations (env is null) are visible, so
// a forgotten environment fails closed rather than spanning both planes.
func (s *Store) scoped(ctx context.Context, sc Scope, fn func(pgx.Tx) error) error {
	if sc.ProducerID == "" {
		return fmt.Errorf("store: missing producer scope: %w", ErrInvalid)
	}
	if sc.Env != "" && !ValidEnv(sc.Env) {
		return fmt.Errorf("%w: %q", ErrInvalidEnv, sc.Env)
	}
	return db.ControlTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := db.SetProducerScope(ctx, tx, sc.ProducerID); err != nil {
			return fmt.Errorf("set producer scope: %w", err)
		}
		if sc.Env != "" {
			if err := db.SetEnvScope(ctx, tx, sc.Env); err != nil {
				return fmt.Errorf("set env scope: %w", err)
			}
		}
		return fn(tx)
	})
}

// admin runs fn inside a transaction marked as the instance-admin path (0003).
// Every method that uses it is reached only from the admin routes, which are
// themselves gated on a session whose `is_instance_admin` came from the
// database.
func (s *Store) admin(ctx context.Context, fn func(pgx.Tx) error) error {
	return db.ControlTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := db.SetInstanceAdmin(ctx, tx); err != nil {
			return fmt.Errorf("set instance admin: %w", err)
		}
		return fn(tx)
	})
}

// LookupAPIKey authenticates a plaintext key. It is the pre-tenant boundary and
// therefore the only function that reads tenant rows without a producer id; it
// goes through the SECURITY DEFINER function from migration 0004 (originally
// 0002).
func LookupAPIKey(ctx context.Context, pool *pgxpool.Pool, plaintext string) (Principal, error) {
	if _, err := apikey.Env(plaintext); err != nil {
		// Reject before touching the database: a malformed key is not a lookup.
		return Principal{}, ErrNotFound
	}

	var p Principal
	err := pool.QueryRow(ctx, `
		select key_id, producer_id, env, replay_scope, suspended
		  from pigeon_auth_lookup($1)`, apikey.Hash(plaintext),
	).Scan(&p.KeyID, &p.ProducerID, &p.Env, &p.ReplayOnly, &p.Suspended)
	if errors.Is(err, pgx.ErrNoRows) {
		return Principal{}, ErrNotFound
	}
	if err != nil {
		return Principal{}, fmt.Errorf("lookup api key: %w", err)
	}
	return p, nil
}

// LookupAPIKey is the method form of the package function, for callers that hold
// a Store (the HTTP layer does).
func (s *Store) LookupAPIKey(ctx context.Context, plaintext string) (Principal, error) {
	return LookupAPIKey(ctx, s.pool, plaintext)
}

// TouchKey records FR-206's last_used_at. It goes through a definer function
// because the api_keys policy needs both scopes and this write has neither: it
// happens while authenticating, before the request's scope exists.
//
// Evidence class: a lost timestamp is not worth an fsync, but the caller still
// runs it off the request path (see cmd/pigeon/main.go).
func TouchKey(ctx context.Context, pool *pgxpool.Pool, keyID string) error {
	_, err := pool.Exec(ctx, `select pigeon_key_touch($1)`, keyID)
	return err
}

// TouchKey is the method form of the package function.
func (s *Store) TouchKey(ctx context.Context, keyID string) error {
	return TouchKey(ctx, s.pool, keyID)
}

// mapPgError turns the SQLSTATEs the API can explain into sentinel errors and
// leaves the rest alone.
func mapPgError(err error, conflict, notFound error) error {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return err
	}
	switch pgErr.Code {
	case "23505": // unique_violation
		return fmt.Errorf("%w: %s", conflict, pgErr.ConstraintName)
	case "23503": // foreign_key_violation: a reference to a row outside this
		// tenant, or inside another environment. FR-108 says that must look
		// nonexistent rather than forbidden.
		return notFound
	case "23514", "23502": // check / not-null violation: a rejected value
		return fmt.Errorf("%w: %s", ErrInvalid, pgErr.Message)
	}
	return err
}
