package store

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m7jay/pigeon/internal/identity"
)

// S9a's password reset (FR-111, migration 0021). The two properties the SQL is
// written around are asserted here from the outside: the token is only ever
// stored as a hash and is single-use, and accepting one rewrites exactly one
// account's credential - no membership, no consumer binding, no invitation.

// s9aUnique is a per-run suffix. internal/store's shared helpers only insert, so
// a rerun against the same disposable database must not collide on a unique key
// (users_email_lower_key here).
func s9aUnique() int64 { return time.Now().UnixNano() }

func s9aEmail(prefix string) string {
	return fmt.Sprintf("%s-%d@example.test", prefix, s9aUnique())
}

// seedResetUser creates the account a reset acts on. The password is hashed with
// the store's own hasher, so the "new password verifies" assertion exercises the
// real verification path rather than a string comparison.
func seedResetUser(t *testing.T, admin *pgxpool.Pool, userID, email, password string) string {
	t.Helper()
	hash, err := identity.HashPassword(password)
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	if _, err := admin.Exec(context.Background(),
		`insert into users (id, email, password_hash) values ($1,$2,$3)`,
		userID, email, hash); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	return hash
}

func countPasswordResets(t *testing.T, admin *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := admin.QueryRow(context.Background(), `select count(*) from password_resets`).Scan(&n); err != nil {
		t.Fatalf("count password_resets: %v", err)
	}
	return n
}

// countLiveSessions counts the account's sessions that are still usable. The
// table is RLS-hidden from the runtime role, so this reads through the admin
// pool, which is what a revocation assertion needs.
func countLiveSessions(t *testing.T, admin *pgxpool.Pool, userID string) int {
	t.Helper()
	var n int
	if err := admin.QueryRow(context.Background(),
		`select count(*) from sessions where user_id = $1 and revoked_at is null`, userID).Scan(&n); err != nil {
		t.Fatalf("count live sessions: %v", err)
	}
	return n
}

// scanMemberships reads the user's producer memberships as comparable strings,
// so "identical before and after" is an assertion about concrete rows.
func scanMemberships(t *testing.T, admin *pgxpool.Pool, userID string) []string {
	t.Helper()
	rows, err := admin.Query(context.Background(),
		`select producer_id || ':' || role from producer_members where user_id = $1 order by producer_id`, userID)
	if err != nil {
		t.Fatalf("scan memberships: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan membership row: %v", err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("membership rows: %v", err)
	}
	return out
}

// TestPasswordResetTokenIsStoredHashedAndBurnable is the storage half of FR-111:
// the row holds a SHA-256 of the token and never the token itself, the token is
// good exactly once, and an address with no account yields no token and no row -
// which is what keeps the public form from being an account-enumeration oracle.
func TestPasswordResetTokenIsStoredHashedAndBurnable(t *testing.T) {
	admin := testPool(t)
	st := newAppStore(t, admin)
	ctx := context.Background()

	n := s9aUnique()
	email := s9aEmail("reset-hashed")
	userID := fmt.Sprintf("usr_reset_hashed_%d", n)
	seedResetUser(t, admin, userID, email, "original-password-1")
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), `delete from users where id = $1`, userID)
	})

	resetID, token, found, err := st.CreatePasswordReset(ctx, email, 0)
	if err != nil {
		t.Fatalf("CreatePasswordReset: %v", err)
	}
	if !found {
		t.Fatal("CreatePasswordReset for a registered address reported found=false")
	}
	if resetID == "" || token == "" {
		t.Fatalf("CreatePasswordReset returned id=%q token=%q, want both", resetID, token)
	}
	if !strings.HasPrefix(token, ResetPrefix) {
		t.Fatalf("token %q does not carry the reset namespace %q", token, ResetPrefix)
	}

	// The stored row: the hash of this token, and the token nowhere in it.
	var storedHash []byte
	var rowText string
	if err := admin.QueryRow(ctx,
		`select token_hash, password_resets::text from password_resets where id = $1`,
		resetID).Scan(&storedHash, &rowText); err != nil {
		t.Fatalf("read the password_resets row: %v", err)
	}
	if !bytes.Equal(storedHash, hashToken(token)) {
		t.Fatalf("token_hash is not sha256(token); the column must be the same hash the accept path looks up")
	}
	if bytes.Equal(storedHash, []byte(token)) {
		t.Fatal("token_hash holds the raw token")
	}
	if strings.Contains(rowText, token) {
		t.Fatalf("the raw token appears in the password_resets row: %s", rowText)
	}

	// Single use: the first accept spends it, the second finds nothing.
	if _, err := st.AcceptPasswordReset(ctx, token, "new-password-1"); err != nil {
		t.Fatalf("first AcceptPasswordReset: %v", err)
	}
	if _, err := st.AcceptPasswordReset(ctx, token, "new-password-2"); err != ErrNotFound {
		t.Fatalf("a spent token was accepted again: err = %v, want ErrNotFound", err)
	}

	// An unknown address: found=false, no token, and no row written.
	before := countPasswordResets(t, admin)
	id, tok, found, err := st.CreatePasswordReset(ctx, s9aEmail("reset-unknown"), 0)
	if err != nil {
		t.Fatalf("CreatePasswordReset(unknown address): %v", err)
	}
	if found || id != "" || tok != "" {
		t.Fatalf("an unknown address returned id=%q token=%q found=%v, want empty/false: issuing must not mint a credential for a nonexistent account", id, tok, found)
	}
	if after := countPasswordResets(t, admin); after != before {
		t.Fatalf("an unknown address wrote a password_resets row (%d -> %d)", before, after)
	}
}

// TestPasswordResetAcceptChangesOnlyTheCredential is FR-111's second clause at
// the write: an accepted reset sets the account's password (verified through the
// store's own login path), revokes every session that account had, burns the
// token, and grants nothing else - the memberships and consumer bindings are the
// same rows afterwards, which is what makes self-registration into a producer
// impossible rather than merely unhandled.
func TestPasswordResetAcceptChangesOnlyTheCredential(t *testing.T) {
	admin := testPool(t)
	st := newAppStore(t, admin)
	ctx := context.Background()

	n := s9aUnique()
	email := s9aEmail("reset-credential")
	userID := fmt.Sprintf("usr_reset_credential_%d", n)
	const (
		oldPassword = "original-password-1"
		newPassword = "replacement-password-1"
	)
	seedResetUser(t, admin, userID, email, oldPassword)

	// A membership, so the account is a real producer member: the login path
	// resolves claims, and the boundary assertion has concrete rows to compare.
	producerID, _ := seedProducer(t, admin, fmt.Sprintf("resetcred%d", n))
	if _, err := admin.Exec(ctx,
		`insert into producer_members (producer_id, user_id, role) values ($1,$2,'owner')`,
		producerID, userID); err != nil {
		t.Fatalf("seed membership: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), `delete from users where id = $1`, userID)
		_, _ = admin.Exec(context.Background(), `delete from producers where id = $1`, producerID)
	})

	// Two live sessions, so revocation has something to revoke.
	for i := 0; i < 2; i++ {
		sess, tok, err := st.CreateSession(ctx, LoginInput{Email: email, Password: oldPassword}, time.Hour)
		if err != nil {
			t.Fatalf("CreateSession #%d: %v", i+1, err)
		}
		if sess.UserID != userID || tok == "" {
			t.Fatalf("CreateSession #%d = %+v, want user %s with a token", i+1, sess, userID)
		}
	}
	if got := countLiveSessions(t, admin, userID); got != 2 {
		t.Fatalf("seeded %d live sessions, want 2", got)
	}

	var consumerBindingsBefore int
	if err := admin.QueryRow(ctx,
		`select count(*) from consumer_users where user_id = $1`, userID).Scan(&consumerBindingsBefore); err != nil {
		t.Fatalf("count consumer_users before: %v", err)
	}
	if consumerBindingsBefore != 0 {
		t.Fatalf("fixture account unexpectedly has %d consumer bindings", consumerBindingsBefore)
	}
	membershipsBefore := scanMemberships(t, admin, userID)

	_, token, found, err := st.CreatePasswordReset(ctx, email, 0)
	if err != nil || !found {
		t.Fatalf("CreatePasswordReset: found=%v err=%v", found, err)
	}
	accepted, err := st.AcceptPasswordReset(ctx, token, newPassword)
	if err != nil {
		t.Fatalf("AcceptPasswordReset: %v", err)
	}
	if accepted.UserID != userID || accepted.Email != email {
		t.Fatalf("AcceptPasswordReset = %+v, want user %s (%s)", accepted, userID, email)
	}

	// Every session the account had is revoked: a session minted with the old
	// password must not survive the change.
	if got := countLiveSessions(t, admin, userID); got != 0 {
		t.Fatalf("%d sessions survived the reset, want 0", got)
	}
	// The token is burnt.
	if _, err := st.AcceptPasswordReset(ctx, token, newPassword); err != ErrNotFound {
		t.Fatalf("the reset token was accepted twice: err = %v, want ErrNotFound", err)
	}

	// The old password no longer authenticates and the new one does, through the
	// store's own login path (CreateSession verifies with identity.VerifyPassword)
	// rather than by comparing hashes here.
	if _, _, err := st.CreateSession(ctx, LoginInput{Email: email, Password: oldPassword}, time.Hour); err != ErrNotFound {
		t.Fatalf("login with the OLD password after a reset = %v, want ErrNotFound", err)
	}
	sess, _, err := st.CreateSession(ctx, LoginInput{Email: email, Password: newPassword}, time.Hour)
	if err != nil {
		t.Fatalf("login with the NEW password after a reset: %v", err)
	}
	if sess.UserID != userID {
		t.Fatalf("the new password authenticated as %s, want %s", sess.UserID, userID)
	}

	// The boundary: a reset grants no claim. No consumer binding appeared, no
	// producer was joined, and the membership rows are the same ones.
	var consumerBindingsAfter int
	if err := admin.QueryRow(ctx,
		`select count(*) from consumer_users where user_id = $1`, userID).Scan(&consumerBindingsAfter); err != nil {
		t.Fatalf("count consumer_users after: %v", err)
	}
	if consumerBindingsAfter != consumerBindingsBefore {
		t.Fatalf("consumer bindings changed across the reset: %d -> %d", consumerBindingsBefore, consumerBindingsAfter)
	}
	membershipsAfter := scanMemberships(t, admin, userID)
	if strings.Join(membershipsAfter, ",") != strings.Join(membershipsBefore, ",") {
		t.Fatalf("memberships changed across the reset: %v -> %v", membershipsBefore, membershipsAfter)
	}
	var producers int
	if err := admin.QueryRow(ctx,
		`select count(*) from producer_members where user_id = $1`, userID).Scan(&producers); err != nil {
		t.Fatalf("count producers after: %v", err)
	}
	if producers != 1 {
		t.Fatalf("the account is in %d producers after a reset, want the 1 it was already in", producers)
	}
}
