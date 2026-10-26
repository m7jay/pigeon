// Package db owns the Postgres connection pool, the embedded migrations, and the
// two write classes the durability contract distinguishes (§6, FR-812).
package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Connect opens the pool and proves it works before the process claims to be up.
// A pool that has never minted a connection turns a startup error into a runtime
// error, so ping here (NFR-11 wants a bounded cold start, not an optimistic one).
func Connect(ctx context.Context, url string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse PIGEON_DATABASE_URL: %w", err)
	}
	cfg.MaxConns = maxConns
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.ConnConfig.RuntimeParams["application_name"] = "pigeon-core"

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres unreachable: %w", err)
	}
	return pool, nil
}

// ControlTx runs fn inside a transaction whose commit is fsynced
// (synchronous_commit=on). Event, outbox, delivery and terminal-status writes
// MUST use this: they are the ones §6 promises cannot be lost.
func ControlTx(ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) error) error {
	return txWith(ctx, pool, "on", fn)
}

// EvidenceTx runs fn with synchronous_commit=off. Attempt rows and truncated
// response bodies use this: §6 already declares a ≤1 s tail of loss acceptable,
// and paying an fsync for data whose loss is pre-declared is incoherent (L3).
func EvidenceTx(ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) error) error {
	return txWith(ctx, pool, "off", fn)
}

func txWith(ctx context.Context, pool *pgxpool.Pool, syncCommit string, fn func(pgx.Tx) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	// set_config is transaction-local, so the setting cannot leak to the next
	// borrower of this pooled connection (FR-812).
	if _, err := tx.Exec(ctx, "select set_config('synchronous_commit', $1, true)", syncCommit); err != nil {
		return fmt.Errorf("set synchronous_commit=%s: %w", syncCommit, err)
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// SetProducerScope sets the session variable the row-level security policies in
// 0001_init.sql read. It is called at the start of every request-scoped
// transaction and MUST NOT be skipped: with it unset the policies match nothing
// (fail closed, MT-1).
func SetProducerScope(ctx context.Context, tx pgx.Tx, producerID string) error {
	_, err := tx.Exec(ctx, "select set_config('pigeon.producer_id', $1, true)", producerID)
	return err
}

// SetInstanceAdmin marks the transaction as the instance-admin path. It is what
// the producers_admin policy in 0003 reads: without it, producers stay invisible
// even to the application role (fail closed).
//
// It is transaction-local, like SetProducerScope, so the marker cannot leak to
// the next borrower of a pooled connection. Callers must be admin routes; this
// function cannot tell, so the gate is only as good as its call sites (see the
// scope note in 0003_producers_admin.sql).
func SetInstanceAdmin(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, "select set_config('pigeon.instance_admin', 'on', true)")
	return err
}
