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

// DefaultPasswordCost is the bcrypt cost production hashes with. It is 12
// because a password is hashed once per invitation and verified once per login,
// so ~250 ms costs nothing next to the floor it puts under a stolen database.
//
// It is a constant on purpose: the only thing that may move the *effective* cost
// is SetPasswordCostForTests, and nothing in production calls it.
const DefaultPasswordCost = 12

// passwordCost is what HashPassword hands to bcrypt. It starts at the production
// default and is only ever moved by SetPasswordCostForTests (a test binary's
// TestMain); production has no code path that touches it.
var passwordCost = DefaultPasswordCost

// SetPasswordCostForTests moves the bcrypt cost this process hashes new
// passwords with. It is a test-harness knob, not a configuration surface:
//
//   - the shipped default stays DefaultPasswordCost, and no production code calls
//     this - the only callers in this tree are the TestMain of the DB-backed
//     acceptance suites (internal/httpapi), which seed hundreds of identities;
//   - bcrypt at cost 12 costs ~3.5 s per call under -race on a 2-vCPU runner, so
//     the fixture cost of an acceptance package is dominated by hashing rather
//     than by anything the tests are actually about;
//   - it refuses a cost bcrypt cannot use, so a typo cannot make every hash fail
//     at login time instead of here;
//   - verification follows the stored hash, so a hash written at this cost
//     verifies at this cost too - the whole `seed then log in` fixture path
//     shrinks by the same factor, and the *assertions* about it do not change.
//
// Raising the cost is allowed (so a test could pin a specific production cost);
// lowering it in a process that also serves real users would weaken password
// storage, which is why nothing outside a test binary may call it.
func SetPasswordCostForTests(cost int) error {
	if cost < bcrypt.MinCost || cost > bcrypt.MaxCost {
		return fmt.Errorf("bcrypt cost %d is outside bcrypt's range [%d, %d]", cost, bcrypt.MinCost, bcrypt.MaxCost)
	}
	passwordCost = cost
	return nil
}

// HashPassword returns a bcrypt hash suitable for users.password_hash.
func HashPassword(password string) (string, error) {
	if len(password) < 8 {
		return "", fmt.Errorf("password must be at least 8 characters")
	}
	h, err := bcrypt.GenerateFromPassword([]byte(password), passwordCost)
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
