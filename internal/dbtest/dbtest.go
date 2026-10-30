// Package dbtest holds the Postgres fixtures the database-backed test harnesses
// share. Nothing in the shipped binary imports it: it is reachable only from
// *_test.go files.
//
// It exists for one shared-cluster fact. Every pigeon suite on a box runs against
// the same Postgres cluster, and a role is cluster-wide rather than per-database,
// so the non-superuser runtime role each harness provisions (`pigeon_app`) is a
// singleton that two suites starting at once both try to create. Reading
// pg_roles and then creating the role is not atomic, so one of them loses - and
// the three harnesses each reported that loss differently: one as a misleading
// "needs CREATEROLE" skip, two as a failure.
package dbtest

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNeedsCreateRole marks the genuine inability to provision the runtime role:
// the session is connected as a role that may not create roles, so a local
// database whose owner has no CREATEROLE cannot run these harnesses. It is
// deliberately NOT how a lost creation race is reported - ProvisionAppRole
// tolerates that race and returns nil - so a caller can keep the two apart:
// internal/store skips on this and fails on anything else.
var ErrNeedsCreateRole = errors.New("needs CREATEROLE")

// ProvisionAppRole creates role if it does not already exist and grants it the
// baseline every harness needs: usage on the schema, DML on its tables, usage on
// its sequences, and execute on its functions.
//
// It is safe to call from two suites on one cluster at the same time. The
// promise is "the role exists and is usable when this returns", not "this call
// created it": a role another worker's suite created a moment ago is found by
// the read, and one created *while* this call is racing it is tolerated by
// createRole.
func ProvisionAppRole(ctx context.Context, admin *pgxpool.Pool, role, password string) error {
	if err := ensureRole(ctx, admin, role, password); err != nil {
		return err
	}
	quoted := quoteRole(role)
	for _, grant := range []string{
		`grant usage on schema public to ` + quoted,
		`grant select, insert, update, delete on all tables in schema public to ` + quoted,
		`grant usage, select on all sequences in schema public to ` + quoted,
		`grant execute on all functions in schema public to ` + quoted,
	} {
		if _, err := admin.Exec(ctx, grant); err != nil {
			return fmt.Errorf("grant: %w", err)
		}
	}
	return nil
}

// ensureRole is the read-then-create the harnesses used to do inline. The read is
// only an optimisation - it keeps the common case (the role is already there) off
// the create path - so losing the race between the two statements is not a
// failure: createRole owns that case.
func ensureRole(ctx context.Context, admin *pgxpool.Pool, role, password string) error {
	exists, err := roleExists(ctx, admin, role)
	if err != nil {
		return fmt.Errorf("read pg_catalog.pg_roles: %w", err)
	}
	if exists {
		return nil
	}
	return createRole(ctx, admin, role, password)
}

// createRole creates the role, tolerates a create that lost to a concurrent
// creator, and verifies the role is really there before the caller connects as
// it.
func createRole(ctx context.Context, admin *pgxpool.Pool, role, password string) error {
	stmt, err := createStatement(ctx, admin, role, password)
	if err != nil {
		return err
	}
	if _, err := admin.Exec(ctx, stmt); err != nil {
		if !isCreateRace(err) {
			if isInsufficientPrivilege(err) {
				return fmt.Errorf("create role %s: %w: %v", role, ErrNeedsCreateRole, err)
			}
			return fmt.Errorf("create role %s: %w", role, err)
		}
		// Another suite on this cluster created the role while this call was
		// creating it. The role exists, which is all this function promises.
	}
	return verifyRoleExists(ctx, admin, role)
}

// createRoleFormat is the CREATE ROLE the fixture renders: %I and %L are filled
// in by the server (see createStatement), so neither the role name nor the
// password is assembled into SQL by hand.
const createRoleFormat = `create role %I login password %L nosuperuser nobypassrls`

// createStatement renders the CREATE ROLE the fixture runs. PostgreSQL accepts $n
// placeholders only in SELECT/INSERT/UPDATE/DELETE/VALUES, so a utility statement
// like CREATE ROLE cannot take the role name or the password as a bind
// parameter - it answers `syntax error at or near "$1"` (42601). The statement is
// therefore rendered by the server itself, with %I/%L doing the quoting, rather
// than assembled in Go where a quote in either value would be a bug.
func createStatement(ctx context.Context, admin *pgxpool.Pool, role, password string) (string, error) {
	var stmt string
	err := admin.QueryRow(ctx,
		`select format($1::text, $2::text, $3::text)`, createRoleFormat, role, password).Scan(&stmt)
	if err != nil {
		return "", fmt.Errorf("render create role %s: %w", role, err)
	}
	return stmt, nil
}

// verifyRoleExists re-reads the catalog after a create, so a caller never gets
// nil and then fails to connect as a role that is not there.
func verifyRoleExists(ctx context.Context, admin *pgxpool.Pool, role string) error {
	exists, err := roleExists(ctx, admin, role)
	if err != nil {
		return fmt.Errorf("verify role %s after create: %w", role, err)
	}
	if !exists {
		return fmt.Errorf("role %s does not exist after create", role)
	}
	return nil
}

// roleExists reads the catalog explicitly rather than through the unqualified
// `pg_roles`, so a test that changes search_path cannot make this read a
// different relation.
func roleExists(ctx context.Context, admin *pgxpool.Pool, role string) (bool, error) {
	var exists bool
	err := admin.QueryRow(ctx,
		`select exists(select 1 from pg_catalog.pg_roles where rolname = $1)`, role).Scan(&exists)
	return exists, err
}

// isCreateRace reports whether err is a create that lost to a concurrent creator
// of the same role, i.e. "the role is there now, somebody else put it there".
// PostgreSQL reports that two ways and both must be tolerated:
//
//   - duplicate_object (42710, `role "x" already exists`): the winner committed
//     before this create was planned, so CreateRole's own check caught it.
//   - unique_violation (23505) on pg_authid_rolname_index: the two creates were
//     genuinely simultaneous, both passed that check, and the unique index under
//     pg_authid is what caught ours. This is the COMMON case for two suites
//     booting together - measured on PG16 (35 simultaneous `create role` pairs
//     for one name): 25 reported 23505 and 10 reported 42710. Tolerating only
//     duplicate_object would leave the race open.
func isCreateRace(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	switch pgErr.Code {
	case "42710": // duplicate_object
		return true
	case "23505": // unique_violation, but only on the role name's own index
		return pgErr.ConstraintName == "pg_authid_rolname_index" || pgErr.TableName == "pg_authid"
	}
	return false
}

// isInsufficientPrivilege reports 42501, what CREATE ROLE answers a session whose
// role has no CREATEROLE attribute. PostgreSQL checks the privilege before it
// checks for the role, so this is exactly "cannot create roles here" and never a
// lost race.
func isInsufficientPrivilege(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42501"
}

// quoteRole renders role as a quoted identifier, so a harness cannot build broken
// SQL out of a role name.
func quoteRole(role string) string { return pgx.Identifier{role}.Sanitize() }
