package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/m7jay/pigeon/internal/id"
	"github.com/m7jay/pigeon/internal/identity"
)

// FR-111's password reset (S9a). This is the second half of FR-111: the tree
// already ships the invitation half, and this is what an account uses when it
// cannot use a password it knows any more.
//
// Two properties are load-bearing and both are enforced in SQL rather than here:
//
//   - Issuing cannot be used to enumerate accounts. The function answers only
//     "was there an account", and the API answers the same 202 either way, so the
//     public form is not a list of every address the instance knows.
//   - Accepting grants nothing but a new password: it writes one account's
//     `password_hash`, revokes that account's sessions, and burns the token. It
//     creates no membership, attaches no consumer and redeems no invitation, so
//     FR-111's "MUST NOT be able to self-register into a producer" is a property
//     of the write rather than a check that could be forgotten.
//
// The token is stored as a SHA-256 hash only, like a session or an invitation
// token; the plaintext exists in one process for the length of one request,
// where it is handed to the notifier (the operator's channel in v1.0 - there is
// no mailer in this release, which `docs/` and SPEC.md state rather than imply).

// ResetPrefix marks a password-reset token, in its own namespace so a leaked
// value in a log is tellable apart from a session, an invitation or a key.
const ResetPrefix = "pgnr_"

// DefaultResetTTL is how long an issued reset stays usable. One hour is the
// conventional window for a single-use credential that travels out of band, and
// it is short enough that the hash-only row is retired quickly.
const DefaultResetTTL = time.Hour

// PasswordReset is an accepted reset: the account whose password moved and the
// row that was spent.
type PasswordReset struct {
	ID     string
	UserID string
	Email  string
}

// CreatePasswordReset mints one single-use reset token for email. found reports
// whether an account exists for that address - the caller must not tell anyone
// else, and the API answers the same either way.
//
// A second request for the same address retires the first token inside the same
// transaction (pigeon_password_reset_create), so the newest link is the only live
// one.
func (s *Store) CreatePasswordReset(ctx context.Context, email string, ttl time.Duration) (resetID, token string, found bool, err error) {
	email = strings.TrimSpace(email)
	if err := validateEmail(email); err != nil {
		return "", "", false, err
	}
	if ttl <= 0 {
		ttl = DefaultResetTTL
	}
	resetID = id.New("rst")
	token = newToken(ResetPrefix)
	err = s.pool.QueryRow(ctx, `
		select pigeon_password_reset_create($1, $2, $3, $4::interval)`,
		resetID, email, hashToken(token), ttl.String()).Scan(&found)
	if err != nil {
		return "", "", false, fmt.Errorf("create password reset: %w", err)
	}
	if !found {
		// No row was written, so the id and the token are discarded rather than
		// returned: a caller must not be able to mint a credential for an account
		// that does not exist.
		return "", "", false, nil
	}
	return resetID, token, true, nil
}

// AcceptPasswordReset spends a reset token and sets the account's new password.
// An unknown, expired, spent or superseded token is ErrNotFound - the same answer
// for every one of them, so a caller learns only that the value it holds does not
// work. A password below FR-111's floor is ErrInvalid.
func (s *Store) AcceptPasswordReset(ctx context.Context, token, newPassword string) (PasswordReset, error) {
	if !wellFormedToken(token, ResetPrefix) {
		return PasswordReset{}, ErrNotFound
	}
	if len(newPassword) < 8 {
		return PasswordReset{}, fmt.Errorf("%w: new_password must be at least 8 characters", ErrInvalid)
	}
	hash, err := identity.HashPassword(newPassword)
	if err != nil {
		return PasswordReset{}, fmt.Errorf("%w: %s", ErrInvalid, err.Error())
	}
	var out PasswordReset
	err = s.pool.QueryRow(ctx, `
		select o_reset_id, o_user_id, o_email from pigeon_password_reset_accept($1, $2)`,
		hashToken(token), hash).Scan(&out.ID, &out.UserID, &out.Email)
	if errors.Is(err, pgx.ErrNoRows) {
		return PasswordReset{}, ErrNotFound
	}
	if err != nil {
		return PasswordReset{}, fmt.Errorf("accept password reset: %w", err)
	}
	return out, nil
}
