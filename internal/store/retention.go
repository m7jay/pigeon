package store

import (
	"context"
	"fmt"
	"time"

	"github.com/m7jay/pigeon/internal/retention"
)

// This file is the SQL side of retention: the partition catalogue and the two
// SECURITY DEFINER primitives from 0009_partitioning.sql. The policy — when to
// drop, what counts as over budget, what to log — lives in internal/retention so
// it can be unit-tested without a database. Here there is only SQL.
//
// Every identifier that reaches DDL is either a constant in this file or a date
// formatted by time.Format; nothing caller-supplied is interpolated. The drop
// primitive additionally validates the name against the parent's leaf partitions
// and refuses any day that has not fully elapsed.

// retentionTables is the closed set the manager may touch. The SQL primitives
// enforce the same set, so a bug here fails loudly rather than dropping an
// unrelated relation.
var retentionTables = map[string]bool{"deliveries": true, "attempts": true}

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
		if err := rows.Scan(&p.Name, &p.Day, &p.Bytes, &p.Rows); err != nil {
			return nil, fmt.Errorf("scan %s partition: %w", table, err)
		}
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
func (s *Store) DropPartition(ctx context.Context, table, name string) (bool, error) {
	if err := checkRetentionTable(table); err != nil {
		return false, err
	}
	var dropped bool
	if err := s.pool.QueryRow(ctx,
		`select pigeon_drop_partition($1, $2)`, table, name,
	).Scan(&dropped); err != nil {
		return false, fmt.Errorf("drop %s partition %s: %w", table, name, err)
	}
	return dropped, nil
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
