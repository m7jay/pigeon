package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// The relay is a system worker, not a tenant: it has no producer scope, so it
// reaches the outbox only through the SECURITY DEFINER functions in 0006, the
// same sanctioned bypass the S1 auth functions use. These three are its whole
// surface.

// LeaseOutbox is the relay's unit of work. It claims the next pending row with
// SKIP LOCKED (FR-702) and returns the full event so the §10 envelope can be
// built from one round trip. The boolean is false when nothing was claimable
// (the outbox is empty, or every pending row is still under lease), which tells
// the relay to wait rather than spin.
type LeasedOutbox struct {
	OutboxID       int64
	EventID        string
	ProducerID     string
	Env            string
	EventType      string
	PayloadVersion int
	RoutingMode    string
	EventClass     string
	ConsumerRef    string
	OccurredAt     time.Time
	Payload        json.RawMessage
}

// LeaseOutbox is the relay's unit of work in its one-row form. It claims the next
// pending row with SKIP LOCKED (FR-702) and returns the full event so the §10
// envelope can be built from one round trip. The boolean is false when nothing was
// claimable (the outbox is empty, or every pending row is still under lease), which
// tells the relay to wait rather than spin.
//
// It is LeaseOutboxBatch with a limit of one, and nothing else: the S2 primitive is
// stated once, so a caller that wants a single row cannot drift from the batch the
// relay drains through.
func (s *Store) LeaseOutbox(ctx context.Context, lease time.Duration) (LeasedOutbox, bool, error) {
	rows, err := s.LeaseOutboxBatch(ctx, lease, 1)
	if err != nil || len(rows) == 0 {
		return LeasedOutbox{}, false, err
	}
	return rows[0], true, nil
}

// LeaseOutboxBatch claims up to limit pending rows in ONE round trip and returns
// them in id order with their events, which is what makes the relay's drain loop a
// batch loop instead of one round trip per event: the lease, the §10 envelope, the
// fan-out and the mark all become per-BATCH costs (FR-818, §8.1's L1), and the
// fan-out insert reaches FR-815's rows per transaction.
//
// limit is clamped by the function itself (a NULL or non-positive limit reads as
// one, and there is a ceiling), so a caller cannot accidentally claim the whole
// outbox into memory. An empty slice means nothing was claimable: the outbox is
// empty, or every pending row is still under lease, which is the caller's cue to
// wait rather than spin.
func (s *Store) LeaseOutboxBatch(ctx context.Context, lease time.Duration, limit int) ([]LeasedOutbox, error) {
	rows, err := s.pool.Query(ctx, `select outbox_id, event_id, producer_id, env, event_type,
		payload_version, routing_mode, event_class, coalesce(consumer_ref, ''), occurred_at, payload
		from pigeon_lease_outbox_batch($1, $2)`, lease, limit)
	if err != nil {
		return nil, fmt.Errorf("lease outbox batch: %w", err)
	}
	defer rows.Close()
	var out []LeasedOutbox
	for rows.Next() {
		var row LeasedOutbox
		if err := rows.Scan(&row.OutboxID, &row.EventID, &row.ProducerID, &row.Env, &row.EventType,
			&row.PayloadVersion, &row.RoutingMode, &row.EventClass, &row.ConsumerRef,
			&row.OccurredAt, &row.Payload); err != nil {
			return nil, fmt.Errorf("lease outbox batch: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("lease outbox batch: %w", err)
	}
	return out, nil
}

// MarkOutboxPublished records FR-701's success: the row is marked published only
// after the broker acknowledged. It is idempotent - it transitions a still-pending
// row - so a relay that double-marks, or re-publishes an already-published event
// after a crash, is a no-op rather than an error.
func (s *Store) MarkOutboxPublished(ctx context.Context, outboxID int64) error {
	if _, err := s.pool.Exec(ctx, `select pigeon_mark_outbox_published($1)`, outboxID); err != nil {
		return fmt.Errorf("mark outbox published: %w", err)
	}
	return nil
}

// OutboxLagSeconds is OBS-2: the age in seconds of the oldest unpublished row, or
// 0 when the outbox is empty. The relay alarms on it; AC-09 is that it grows while
// the relay is stopped yet nothing is lost.
func (s *Store) OutboxLagSeconds(ctx context.Context) (float64, error) {
	var lag float64
	if err := s.pool.QueryRow(ctx, `select pigeon_outbox_lag_seconds()`).Scan(&lag); err != nil {
		return 0, fmt.Errorf("outbox lag: %w", err)
	}
	return lag, nil
}

// NewRelayPool builds the dedicated connection the relay LISTENs on. LISTEN/NOTIFY
// is connection-scoped, so it must not share a pooled connection that a borrow and
// return would silently reset (D-4's fast path depends on the subscription
// surviving). One connection, one replica's worth of notifications.
func NewRelayPool(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse relay DSN: %w", err)
	}
	// A single connection: the relay holds one LISTEN subscription for the life
	// of the process, and a pool would hand the subscription to a borrower and
	// take it back without the relay ever seeing the notification.
	cfg.MaxConns = 1
	cfg.MaxConnLifetime = 0
	cfg.ConnConfig.RuntimeParams["application_name"] = "pigeon-relay"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open relay pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("relay postgres unreachable: %w", err)
	}
	return pool, nil
}

// ListenOutbox subscribes to the relay channel and reports whether the
// subscription is live. It is a no-op success when the relay is disabled or the
// DSN is empty, so a build without a relay still starts.
func ListenOutbox(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return nil
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire listen connection: %w", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `listen pigeon_outbox`); err != nil {
		return fmt.Errorf("listen pigeon_outbox: %w", err)
	}
	return nil
}

// NotifyOutbox is the ingest side of the fast path: after the accept commits, the
// API tells the relay a row is ready. It is called off the request path (see the
// handler) so a listener's absence can never delay a 202.
func NotifyOutbox(ctx context.Context, pool *pgxpool.Pool, eventID string) {
	if pool == nil {
		return
	}
	_, _ = pool.Exec(context.WithoutCancel(ctx), `select pg_notify('pigeon_outbox', $1)`, eventID)
}
