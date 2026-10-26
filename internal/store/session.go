package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/m7jay/pigeon/internal/id"
	"github.com/m7jay/pigeon/internal/identity"
)

// SessionPrefix marks a human session token. It is deliberately different from
// apikey.KeyPrefix ("pgn_") so a leaked token in a log can be told apart from a
// leaked producer key, and so a producer key can never be replayed as a session
// or the reverse.
const SessionPrefix = "pgns_"

// InvitePrefix marks an invitation token. Invitation tokens are accepted once
// and then burned (0004's pigeon_invite_accept), so they are the most dangerous
// short-lived secret in the tree and get their own namespace.
const InvitePrefix = "pgni_"

// Session is an authenticated human. Exactly one of the three claim sets is
// present:
//
//	instance admin — InstanceAdmin true, no producer, no consumer
//	producer member — ProducerID + Role (owner|developer)
//	consumer user  — ConsumerID + Env (FR-110's claim)
//
// The claims are re-read from the database on every request (0004's
// pigeon_session_lookup), so they cannot go stale: removing a membership or a
// consumer binding invalidates the session without a revocation step.
type Session struct {
	ID                 string
	UserID             string
	Email              string
	InstanceAdmin      bool
	MustChangePassword bool
	ProducerID         string
	Role               string
	ConsumerID         string
	Env                string
	ExpiresAt          time.Time
}

// IsProducerMember reports whether the session may act on a producer.
func (s Session) IsProducerMember() bool { return s.ProducerID != "" && s.Role != "" }

// IsConsumer reports whether the session carries FR-110's consumer claim.
func (s Session) IsConsumer() bool { return s.ConsumerID != "" && s.Env != "" }

// LoginInput is what POST /v1/session accepts. ProducerSlug is required only
// when the user belongs to more than one producer (FR-105): the server never
// guesses which tenant a session is for.
type LoginInput struct {
	Email        string
	Password     string
	ProducerSlug string
}

// Membership is one row of FR-105: a user's role in one producer.
type Membership struct {
	ProducerID string
	Slug       string
	Name       string
	Role       string
	Suspended  bool
}

// ConsumerBinding is FR-109's binding, resolved for the login path.
type ConsumerBinding struct {
	ConsumerID   string
	ProducerID   string
	Env          string
	ConsumerName string
	ConsumerRef  string
}

// CreateSession verifies a password and mints a session token. The plaintext
// token is returned once and never stored; the row holds a SHA-256 of it, for
// the same reason API keys are hashed (FR-202): the input is uniform randomness,
// so stretching defends against nothing.
//
// Every failure returns ErrNotFound so the caller cannot tell "no such user"
// from "wrong password" — the same reasoning as the key lookup.
func (s *Store) CreateSession(ctx context.Context, in LoginInput, ttl time.Duration) (Session, string, error) {
	var (
		userID     string
		email      string
		hash       string
		isAdmin    bool
		mustChange bool
	)
	err := s.pool.QueryRow(ctx, `
		select id, email, password_hash, is_instance_admin, must_change_password
		  from users where lower(email) = lower($1)`, in.Email,
	).Scan(&userID, &email, &hash, &isAdmin, &mustChange)
	if errors.Is(err, pgx.ErrNoRows) {
		return Session{}, "", ErrNotFound
	}
	if err != nil {
		return Session{}, "", fmt.Errorf("load user: %w", err)
	}
	if !identity.VerifyPassword(hash, in.Password) {
		return Session{}, "", ErrNotFound
	}

	sess := Session{
		UserID:             userID,
		Email:              email,
		InstanceAdmin:      isAdmin,
		MustChangePassword: mustChange,
		ExpiresAt:          time.Now().Add(ttl),
	}

	memberships, err := s.userMemberships(ctx, userID)
	if err != nil {
		return Session{}, "", err
	}
	if len(memberships) > 0 {
		m, err := pickMembership(memberships, in.ProducerSlug)
		if err != nil {
			return Session{}, "", err
		}
		sess.ProducerID, sess.Role = m.ProducerID, m.Role
	} else if !isAdmin {
		// A user with neither a producer membership nor an admin flag may still
		// be a consumer user (FR-109). Anything else has no claims at all and
		// gets no session: an authenticated identity that can do nothing is a
		// support ticket waiting to happen.
		binding, err := s.userConsumerBinding(ctx, userID)
		if err != nil {
			return Session{}, "", err
		}
		sess.ConsumerID, sess.Env = binding.ConsumerID, binding.Env
		sess.ProducerID = binding.ProducerID
	}

	token := newToken(SessionPrefix)
	sess.ID = id.New("ses")
	if _, err := s.pool.Exec(ctx, `
		select pigeon_session_create($1, $2, $3, $4, $5, $6, $7)`,
		sess.ID, userID, hashToken(token), sess.ProducerID, sess.ConsumerID, sess.Env, sess.ExpiresAt,
	); err != nil {
		return Session{}, "", mapPgError(err, ErrConflict, ErrNotFound)
	}
	return sess, token, nil
}

// LookupSession resolves a token to its claims. The token's hash is the only
// lookup key; an unknown, revoked or expired token is ErrNotFound, so a caller
// cannot distinguish them.
func (s *Store) LookupSession(ctx context.Context, token string) (Session, error) {
	if !wellFormedToken(token, SessionPrefix) {
		return Session{}, ErrNotFound
	}
	var sess Session
	err := s.pool.QueryRow(ctx, `
		select session_id, user_id, email, is_instance_admin, must_change_password,
		       coalesce(producer_id, ''), coalesce(role, ''),
		       coalesce(consumer_id, ''), coalesce(env, ''), expires_at
		  from pigeon_session_lookup($1)`, hashToken(token),
	).Scan(&sess.ID, &sess.UserID, &sess.Email, &sess.InstanceAdmin, &sess.MustChangePassword,
		&sess.ProducerID, &sess.Role, &sess.ConsumerID, &sess.Env, &sess.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Session{}, ErrNotFound
	}
	if err != nil {
		return Session{}, fmt.Errorf("lookup session: %w", err)
	}
	// A session that names a producer but resolves no role means the membership
	// was removed after login - unless it is a consumer session, where the
	// producer id is only there to scope the consumer's own reads (FR-110).
	if sess.ProducerID != "" && sess.Role == "" && sess.ConsumerID == "" {
		return Session{}, ErrNotFound
	}
	return sess, nil
}

// RevokeSession ends one session. It is idempotent: revoking twice is not an
// error, because a client retrying a logout must not see a failure.
func (s *Store) RevokeSession(ctx context.Context, token string) error {
	if !wellFormedToken(token, SessionPrefix) {
		return ErrNotFound
	}
	var revoked bool
	if err := s.pool.QueryRow(ctx, `select pigeon_session_revoke($1)`, hashToken(token)).Scan(&revoked); err != nil {
		return fmt.Errorf("revoke session: %w", err)
	}
	return nil
}

// TouchSession records last_seen_at. Evidence class, called off the request
// path like TouchKey.
func (s *Store) TouchSession(ctx context.Context, token string) error {
	if !wellFormedToken(token, SessionPrefix) {
		return ErrNotFound
	}
	_, err := s.pool.Exec(ctx, `select pigeon_session_touch($1)`, hashToken(token))
	return err
}

// ChangePassword is FR-101's forced first change and the ordinary password
// change. The old password is required even for the forced case: a stolen
// session token alone must not be enough to take the account.
func (s *Store) ChangePassword(ctx context.Context, userID, oldPassword, newPassword string) error {
	var hash string
	err := s.pool.QueryRow(ctx, `select password_hash from users where id = $1`, userID).Scan(&hash)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("load password: %w", err)
	}
	if !identity.VerifyPassword(hash, oldPassword) {
		return ErrNotFound
	}
	newHash, err := identity.HashPassword(newPassword)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrInvalid, err.Error())
	}
	_, err = s.pool.Exec(ctx, `
		update users set password_hash = $2, must_change_password = false, password_changed_at = now()
		 where id = $1`, userID, newHash)
	if err != nil {
		return fmt.Errorf("update password: %w", err)
	}
	return nil
}

// AcceptedInvite is what an invitation becomes once it is consumed.
type AcceptedInvite struct {
	InviteID   string
	UserID     string
	Email      string
	Role       string
	ProducerID string
	ConsumerID string
	Env        string
}

// AcceptInvite consumes an invitation token and sets the user's password. It is
// the only path that can add a user to a producer (FR-111: no self-registration)
// and the only path that can bind a consumer user (FR-109).
func (s *Store) AcceptInvite(ctx context.Context, token, password string) (AcceptedInvite, error) {
	if !wellFormedToken(token, InvitePrefix) {
		return AcceptedInvite{}, ErrNotFound
	}
	hash, err := identity.HashPassword(password)
	if err != nil {
		return AcceptedInvite{}, fmt.Errorf("%w: %s", ErrInvalid, err.Error())
	}

	var out AcceptedInvite
	// `select *` rather than a column list: the function's OUT parameters exist to
	// name the values for humans, and the store reads them positionally. Keeping a
	// list here would mean a rename in SQL breaks the Go scan with a runtime error
	// instead of a compile error - and `select *` is checked at plan time anyway.
	err = s.pool.QueryRow(ctx, `
		select * from pigeon_invite_accept($1, $2, $3)`,
		hashToken(token), id.New("usr"), hash,
	).Scan(&out.InviteID, &out.UserID, &out.Email, &out.Role, &out.ProducerID, &out.ConsumerID, &out.Env)
	if errors.Is(err, pgx.ErrNoRows) {
		// Unknown, expired, already accepted or revoked: one answer, because
		// distinguishing them is only useful to someone guessing tokens.
		return AcceptedInvite{}, ErrNotFound
	}
	if err != nil {
		return AcceptedInvite{}, mapPgError(err, ErrConflict, ErrNotFound)
	}
	return out, nil
}

func (s *Store) userMemberships(ctx context.Context, userID string) ([]Membership, error) {
	rows, err := s.pool.Query(ctx, `
		select producer_id, slug, name, role, suspended
		  from pigeon_user_memberships($1)`, userID)
	if err != nil {
		return nil, fmt.Errorf("load memberships: %w", err)
	}
	defer rows.Close()

	var out []Membership
	for rows.Next() {
		var m Membership
		if err := rows.Scan(&m.ProducerID, &m.Slug, &m.Name, &m.Role, &m.Suspended); err != nil {
			return nil, fmt.Errorf("scan membership: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *Store) userConsumerBinding(ctx context.Context, userID string) (ConsumerBinding, error) {
	var b ConsumerBinding
	err := s.pool.QueryRow(ctx, `
		select consumer_id, producer_id, env, consumer_name, consumer_ref
		  from pigeon_user_consumers($1)`, userID,
	).Scan(&b.ConsumerID, &b.ProducerID, &b.Env, &b.ConsumerName, &b.ConsumerRef)
	if errors.Is(err, pgx.ErrNoRows) {
		return ConsumerBinding{}, ErrNotFound
	}
	if err != nil {
		return ConsumerBinding{}, fmt.Errorf("load consumer binding: %w", err)
	}
	return b, nil
}

// pickMembership implements FR-105 without guessing: with one membership the
// choice is forced, with several the caller must name one, and a named producer
// the user is not a member of is ErrNotFound (never "forbidden", FR-108).
func pickMembership(all []Membership, slug string) (Membership, error) {
	if slug == "" {
		if len(all) == 1 {
			return all[0], nil
		}
		return Membership{}, ErrAmbiguousProducer
	}
	for _, m := range all {
		if m.Slug == slug {
			return m, nil
		}
	}
	return Membership{}, ErrNotFound
}

// newToken mints `<prefix><43 base64url chars>`: 256 bits of randomness, the
// same shape as an API key.
func newToken(prefix string) string {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("store: crypto/rand unavailable: " + err.Error())
	}
	return prefix + base64.RawURLEncoding.EncodeToString(b[:])
}

// hashToken is what the database stores. SHA-256 for the same reason as
// apikey.Hash.
func hashToken(token string) []byte {
	sum := sha256.Sum256([]byte(token))
	return sum[:]
}

// wellFormedToken rejects a token that cannot be one of ours before hashing it,
// so a garbage Authorization header is not a database round trip.
func wellFormedToken(token, prefix string) bool {
	if len(token) < len(prefix)+16 || token[:len(prefix)] != prefix {
		return false
	}
	_, err := base64.RawURLEncoding.DecodeString(token[len(prefix):])
	return err == nil
}
