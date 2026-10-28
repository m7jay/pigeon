package store

import (
	"context"
	"fmt"
	"time"

	"github.com/m7jay/pigeon/internal/retention"
)

// This file is the SQL side of retention: the partition catalogue and the two
// SECURITY DEFINER primitives from 0012_partitioning.sql. The policy — when to
// drop, what counts as over budget, what to log — lives in internal/retention so
// it can be unit-tested without a database. Here there is only SQL.
//
// Every identifier that reaches DDL is either a constant in this file or a date
// formatted by time.Format; nothing caller-supplied is interpolated. The drop
// primitive additionally validates the name against the parent's leaf partitions
// and refuses any day that has not fully elapsed.

// retentionTables is the closed set the manager may touch. The SQL primitives
// enforce the same set, so a bug here fails loudly rather than dropping an
// unrelated relation. `delivery_dedupe` is FR-905's archive: it is dropped by
// partition like the rest (SB-2), and it is what an ordinary `deliveries` drop is
// copied into before it is dropped.
var retentionTables = map[string]bool{"deliveries": true, "attempts": true, "delivery_dedupe": true}

func checkRetentionTable(table string) error {
	if !retentionTables[table] {
		return fmt.Errorf("retention: unknown partitioned table %q", table)
	}
	return nil
}

// EnsurePartition creates the day's partition if it is missing and returns its
// name. Idempotent: the SQL function checks to_regclass first. The call runs as
// pigeon_app against a SECURITY DEFINER function, because the runtime role owns
// nothing and must not hold CREATE on the schema.
func (s *Store) EnsurePartition(ctx context.Context, table string, day time.Time) (string, error) {
	if err := checkRetentionTable(table); err != nil {
		return "", err
	}
	var name string
	if err := s.pool.QueryRow(ctx,
		`select pigeon_ensure_partition($1, ($2)::date)`,
		table, day.UTC().Format("2006-01-02"),
	).Scan(&name); err != nil {
		return "", fmt.Errorf("ensure %s partition: %w", table, err)
	}
	return name, nil
}

// ensureCacheTTL is how long a process trusts its own memory that a day's
// partition exists. The sweeper creates partitions days ahead, so this only
// covers the rare fan-out that lands on a day the sweeper has not reached yet
// (a relay backlog, or the first request after boot). Re-checking costs one cheap
// existence query, so a short TTL is enough to keep the ingest path quiet.
const ensureCacheTTL = time.Minute

// EnsurePartitionCached creates the day's partition if it is missing, checking at
// most once per TTL per process. Fan-out calls this before its transaction so a
// missing partition is created ahead of the insert rather than aborting it.
func (s *Store) EnsurePartitionCached(ctx context.Context, table string, day time.Time) error {
	if err := checkRetentionTable(table); err != nil {
		return err
	}
	day = day.UTC()
	key := table + "|" + day.Format("2006-01-02")

	s.ensureMu.Lock()
	until, ok := s.ensured[key]
	s.ensureMu.Unlock()
	if ok && time.Now().Before(until) {
		return nil
	}

	if _, err := s.EnsurePartition(ctx, table, day); err != nil {
		return err
	}

	s.ensureMu.Lock()
	s.ensured[key] = time.Now().Add(ensureCacheTTL)
	// Bound the map: a long-lived process that fans out events from many days
	// would otherwise accumulate keys. Expired entries carry no information.
	if len(s.ensured) > 64 {
		now := time.Now()
		for k, v := range s.ensured {
			if now.After(v) {
				delete(s.ensured, k)
			}
		}
	}
	s.ensureMu.Unlock()
	return nil
}

// ListPartitions returns the live leaf partitions of one retained table, oldest
// first. Sizes come from pg_total_relation_size (indexes included) and row counts
// from pg_class.reltuples — an estimate refreshed by autovacuum, which is the
// right cost trade-off for a monitoring payload that may run every few seconds.
// A negative estimate (never analysed) is reported as zero rather than a
// negative row count.
//
// A partition that another session dropped between this statement's snapshot and
// the size read is skipped, not reported and not an error. The size functions read
// the live catalogue rather than the snapshot, so they return NULL for a relation
// that has just gone (PostgreSQL answers NULL for an unknown OID — verified on
// PG 16; a missing privilege answers 0, so NULL here is unambiguous). This is the
// normal case for this reader rather than an edge case: retention drops partitions
// while an operator's dashboard scrapes GET /v1/meta/usage, and the sweeper lists
// the catalogue while the previous tick's drop is committing. Returning an error
// instead would turn a healthy drop into a 500 on the page that monitors disk
// pressure.
func (s *Store) ListPartitions(ctx context.Context, table string) ([]retention.Partition, error) {
	if err := checkRetentionTable(table); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
		select c.relname,
		       to_date(substring(c.relname from '[0-9]{8}$'), 'YYYYMMDD'),
		       pg_total_relation_size(c.oid),
		       greatest(coalesce(c.reltuples, 0), 0)::bigint
		from pg_inherits i
		join pg_class c on c.oid = i.inhrelid
		where i.inhparent = to_regclass($1)
		  and c.relkind = 'r'
		order by c.relname`, table)
	if err != nil {
		return nil, fmt.Errorf("list %s partitions: %w", table, err)
	}
	defer rows.Close()

	var out []retention.Partition
	for rows.Next() {
		p := retention.Partition{Table: table}
		// Nullable on purpose: NULL means the partition was dropped while this
		// statement was running (see above).
		var size *int64
		if err := rows.Scan(&p.Name, &p.Day, &size, &p.Rows); err != nil {
			return nil, fmt.Errorf("scan %s partition: %w", table, err)
		}
		if size == nil {
			continue
		}
		p.Bytes = *size
		p.Day = p.Day.UTC()
		out = append(out, p)
	}
	return out, rows.Err()
}

// DropPartition drops one leaf partition by name and reports whether this call
// removed it. The SQL primitive serialises drops with a transaction-scoped
// advisory lock (so no drop can run concurrently with itself, D-7), refuses a
// day that has not ended, and returns false when a writer held the partition past
// its lock_timeout so the sweeper can try again next tick.
//
// reason is FR-905's: an ordinary (age) drop of a `deliveries` partition copies
// the day's dedupe keys into the archive and drops it - both commit or neither
// does - while an SB-3 pressure drop may skip the copy and warns with the day it
// covered (FR-903).
//
// The transaction is explicit rather than left to the driver's implicit one: the
// copy and the drop live in one statement, and committing only after the primitive
// reports the drop is what keeps "neither" from meaning "the copy without the
// drop". A false return - already gone, or a writer held it - rolls back, so a
// copy the primitive could have made cannot outlive the partition it came from.
func (s *Store) DropPartition(ctx context.Context, table, name string, reason retention.DropReason) (bool, error) {
	if err := checkRetentionTable(table); err != nil {
		return false, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("drop %s partition %s: begin: %w", table, name, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var dropped bool
	if err := tx.QueryRow(ctx,
		`select pigeon_drop_partition($1, $2, $3)`, table, name, string(reason),
	).Scan(&dropped); err != nil {
		return false, fmt.Errorf("drop %s partition %s: %w", table, name, err)
	}
	if !dropped {
		return false, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("drop %s partition %s: commit: %w", table, name, err)
	}
	return true, nil
}

// DedupeArchiveSize is FR-905's gauge: the archive's on-disk size, measured from
// the catalogue with pg_total_relation_size rather than assumed from a row count.
//
// The sum is over the archive's PARTITIONS, and that is not a detail: a
// partitioned table has no storage of its own, so pg_total_relation_size on the
// parent answers 0 (measured on 16.2 - and the same is true of pg_relation_size),
// which would publish a calm zero for a multi-gigabyte archive. Each partition is
// measured with pg_total_relation_size, so its heap and its primary-key index are
// both counted, and a partition another session dropped mid-read contributes 0
// rather than a NULL the caller has to handle.
func (s *Store) DedupeArchiveSize(ctx context.Context) (int64, error) {
	var n int64
	if err := s.pool.QueryRow(ctx, `
		select coalesce(sum(pg_total_relation_size(c.oid)), 0)
		  from pg_inherits i
		  join pg_class c on c.oid = i.inhrelid
		 where i.inhparent = to_regclass('delivery_dedupe')
		   and c.relkind = 'r'`,
	).Scan(&n); err != nil {
		return 0, fmt.Errorf("dedupe archive size: %w", err)
	}
	return n, nil
}

// DatabaseSize is how many bytes pigeon's own data occupies in this database. It
// is the fallback signal when the application cannot see the volume the database
// lives on (split containers), and it is reported beside the volume reading in
// GET /v1/meta/usage so an operator can separate "the host is full" from "pigeon
// is big".
func (s *Store) DatabaseSize(ctx context.Context) (int64, error) {
	var n int64
	if err := s.pool.QueryRow(ctx, `select pg_database_size(current_database())`).Scan(&n); err != nil {
		return 0, fmt.Errorf("database size: %w", err)
	}
	return n, nil
}
