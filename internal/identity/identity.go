// Package identity covers the human side of authentication: the bootstrap
// instance-admin credential (FR-101) and password storage.
package identity

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"

	"github.com/m7jay/pigeon/internal/id"
)

// bcryptCost is 12: the admin credential is checked rarely (at login), so a
// ~250 ms hash costs nothing and raises the floor for a stolen database.
const bcryptCost = 12

// HashPassword returns a bcrypt hash suitable for users.password_hash.
func HashPassword(password string) (string, error) {
	if len(password) < 8 {
		return "", fmt.Errorf("password must be at least 8 characters")
	}
	h, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		return "", fmt.Errorf("hash password: %w", err)
	}
	return string(h), nil
}

// VerifyPassword compares a login attempt against a stored hash.
func VerifyPassword(hash, password string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
}

// EnsureInstanceAdmin creates the bootstrap admin on first run and is idempotent
// afterwards: an existing admin is never overwritten, because resetting it from
// an environment variable would let anyone with the env file take the instance.
//
// It returns true only when it created the row, so the caller can log the
// one-time warning FR-101 requires.
func EnsureInstanceAdmin(ctx context.Context, pool *pgxpool.Pool, password string) (bool, error) {
	var existing string
	err := pool.QueryRow(ctx, `select id from users where is_instance_admin limit 1`).Scan(&existing)
	if err == nil {
		return false, nil
	}
	if err != pgx.ErrNoRows {
		return false, fmt.Errorf("look for existing instance admin: %w", err)
	}

	hash, err := HashPassword(password)
	if err != nil {
		return false, err
	}

	// ON CONFLICT DO NOTHING makes two replicas booting together safe: exactly
	// one insert wins and neither errors. must_change_password is FR-101's forced
	// change: the bootstrap credential arrives through an environment variable,
	// which means it also sits in an operator's shell history and possibly a
	// process listing, so it must not survive first login.
	tag, err := pool.Exec(ctx, `
		insert into users (id, email, password_hash, is_instance_admin, must_change_password)
		values ($1, $2, $3, true, true)
		on conflict (id) do nothing`,
		id.New("usr"), "admin@pigeon.local", hash)
	if err != nil {
		return false, fmt.Errorf("insert instance admin: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}
