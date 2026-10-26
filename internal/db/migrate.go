package db

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// migratorLockID serialises migrations across replicas (OPS-9). It is a fixed
// constant, not a hash of the database name, so every instance of this binary
// contends for the same lock in the same database.
const migratorLockID int64 = 0x70696765_6e5f6d67 // "pigen_mg"

// Migration is one embedded file: NN_name.sql, applied in ascending order.
type Migration struct {
	Version  string // "0001"
	Name     string // "init"
	SQL      string
	Checksum string // sha256 of the file, so an edited applied migration is caught
}

// LoadMigrations reads the embedded set. Exported because the cross-producer CI
// harness and `pigeon doctor` both need to assert ordering without a database.
func LoadMigrations() ([]Migration, error) {
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}

	var out []Migration
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		body, err := migrationsFS.ReadFile("migrations/" + e.Name())
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", e.Name(), err)
		}
		version, name, ok := strings.Cut(strings.TrimSuffix(e.Name(), ".sql"), "_")
		if !ok || version == "" || name == "" {
			return nil, fmt.Errorf("migration %q must be named NN_name.sql", e.Name())
		}
		sum := sha256.Sum256(body)
		out = append(out, Migration{
			Version:  version,
			Name:     name,
			SQL:      string(body),
			Checksum: hex.EncodeToString(sum[:]),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })

	for i, m := range out {
		if i > 0 && out[i-1].Version == m.Version {
			return nil, fmt.Errorf("duplicate migration version %q (%s, %s)", m.Version, out[i-1].Name, m.Name)
		}
	}
	return out, nil
}

// Migrate applies every pending migration, holding a Postgres advisory lock so
// two replicas booting together cannot both migrate (OPS-9). The lock lives on
// one dedicated connection: an advisory lock is session-scoped and would be
// silently released by a pool checkout.
func Migrate(ctx context.Context, pool *pgxpool.Pool, waitFor time.Duration) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migrator connection: %w", err)
	}
	defer conn.Release()

	lockCtx, cancel := context.WithTimeout(ctx, waitFor)
	defer cancel()
	if _, err := conn.Exec(lockCtx, "select pg_advisory_lock($1)", migratorLockID); err != nil {
		return fmt.Errorf("acquire migrator advisory lock after %s (another replica migrating?): %w", waitFor, err)
	}
	defer func() {
		_, _ = conn.Exec(context.WithoutCancel(ctx), "select pg_advisory_unlock($1)", migratorLockID)
	}()

	if _, err := conn.Exec(ctx, `
		create table if not exists schema_migrations (
			version    text        primary key,
			name       text        not null,
			checksum   text        not null,
			applied_at timestamptz not null default now()
		)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	migrations, err := LoadMigrations()
	if err != nil {
		return err
	}

	applied := map[string]string{}
	rows, err := conn.Query(ctx, "select version, checksum from schema_migrations")
	if err != nil {
		return fmt.Errorf("read schema_migrations: %w", err)
	}
	for rows.Next() {
		var v, sum string
		if err := rows.Scan(&v, &sum); err != nil {
			rows.Close()
			return fmt.Errorf("scan schema_migrations: %w", err)
		}
		applied[v] = sum
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate schema_migrations: %w", err)
	}

	for _, m := range migrations {
		if prev, ok := applied[m.Version]; ok {
			if prev != m.Checksum {
				return fmt.Errorf("migration %s_%s was applied with a different checksum: the file changed after it ran", m.Version, m.Name)
			}
			continue
		}
		// Simple protocol: a migration file is many statements in one Exec.
		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin %s_%s: %w", m.Version, m.Name, err)
		}
		if _, err := tx.Exec(ctx, m.SQL, pgx.QueryExecModeSimpleProtocol); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply %s_%s: %w", m.Version, m.Name, err)
		}
		if _, err := tx.Exec(ctx,
			"insert into schema_migrations (version, name, checksum) values ($1, $2, $3)",
			m.Version, m.Name, m.Checksum); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("record %s_%s: %w", m.Version, m.Name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit %s_%s: %w", m.Version, m.Name, err)
		}
	}
	return nil
}
