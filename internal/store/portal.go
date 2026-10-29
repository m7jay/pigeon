package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// Reads the producer portal needs and no other caller has (S9a, M2a).
//
// Both of them are producer-member reads over rows that already exist: UI-2's
// endpoint health is derived from the delivery rows the worker writes (S4-S7),
// and UI-11's "unroutable events" is a read of the fan-out outcome S3 records on
// the event. Neither invents state: the only thing that happens here is that a
// value the platform already keeps becomes answerable through the API, which is
// what §14 requires before a portal may render it.

// EndpointHealth is UI-2's health for one endpoint, and UI-11's failure streak
// for a consumer's endpoint. It is derived at read time from the delivery rows
// rather than kept as a counter, because a counter is one more thing that can
// disagree with the evidence the producer can see.
//
// FailureStreak is the number of the endpoint's deliveries that ended without a
// delivery - `failed_terminal` (FR-1201's window expired) or `skipped` (a
// matched endpoint that could not take the event) - since its most recent
// `delivered` one. A delivery still `pending` neither breaks nor extends the
// streak: it is in flight, so counting it as a failure would report a failure
// that has not happened, and counting it as a success would hide one that has.
// Zero therefore means "the most recent delivery either succeeded or has not been
// attempted yet", and LastStatus tells those two apart.
type EndpointHealth struct {
	EndpointID     string
	FailureStreak  int
	LastStatus     string // "" when the endpoint has never had a delivery
	LastDeliveryAt *time.Time
}

// EndpointHealth reads health for a set of endpoints in one query (UI-2 lists
// every endpoint of every consumer, so a per-endpoint round trip would be a
// request per row). Every requested id appears in the map: an endpoint with no
// deliveries carries the zero value (streak 0, empty last status, nil timestamp),
// which the caller renders as "no delivery yet" rather than as a healthy endpoint
// that has been delivering quietly.
func (s *Store) EndpointHealth(ctx context.Context, sc Scope, endpointIDs []string) (map[string]EndpointHealth, error) {
	out := make(map[string]EndpointHealth, len(endpointIDs))
	if len(endpointIDs) == 0 {
		return out, nil
	}
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			select e.id,
			       coalesce(streak.n, 0)::int,
			       coalesce(last.status, ''),
			       last.created_at
			  from unnest($3::text[]) as e(id)
			  -- The streak: non-deliveries newer than the newest delivered row.
			  -- '-infinity' is the "never delivered" case, so the whole history
			  -- counts.
			  left join lateral (
			      select count(*) filter (where f.status in ('failed_terminal', 'skipped')) as n
			        from deliveries f
			       where f.producer_id = $1 and f.env = $2 and f.endpoint_id = e.id
			         and f.created_at > coalesce((
			              select max(s.created_at) from deliveries s
			               where s.producer_id = $1 and s.env = $2 and s.endpoint_id = e.id
			                 and s.status = 'delivered'), '-infinity'::timestamptz)
			  ) streak on true
			  -- The most recent delivery whatever its status, for LastStatus.
			  left join lateral (
			      select d.status, d.created_at
			        from deliveries d
			       where d.producer_id = $1 and d.env = $2 and d.endpoint_id = e.id
			       order by d.created_at desc, d.id desc
			       limit 1
			  ) last on true`,
			sc.ProducerID, sc.Env, endpointIDs)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var h EndpointHealth
			if err := rows.Scan(&h.EndpointID, &h.FailureStreak, &h.LastStatus, &h.LastDeliveryAt); err != nil {
				return err
			}
			out[h.EndpointID] = h
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ConsumerEvent is one event addressed to a consumer that produced no delivery
// to it: FR-508's `unroutable:consumer_not_registered` (the ref matched no
// consumer) or §7.5a's `not_delivered:no_matching_subscription` (the consumer
// exists and none of its endpoints subscribes to the type). It is UI-11's
// "unroutable events" panel, and it carries the reason rather than a single
// label because the two have different fixes: register the consumer, or have
// somebody subscribe.
type ConsumerEvent struct {
	EventID      string
	EventType    string
	RoutingState string // routable | unroutable
	Reason       string // the fan-out's own reason (non_delivery_reason)
	OccurredAt   time.Time
	CreatedAt    time.Time
	IsTest       bool
}

// ConsumerEvents lists the events addressed to one consumer - by ref, which is
// what the ingest resolves a `consumer_id` to before the row is written - that
// ended with a non-delivery reason, newest first. An unknown or foreign consumer
// id is ErrNotFound (FR-108), so the panel cannot be aimed at another producer's
// consumer to see whether it exists.
func (s *Store) ConsumerEvents(ctx context.Context, sc Scope, consumerID string, limit int) ([]ConsumerEvent, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	out := []ConsumerEvent{}
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		// The consumer's ref, resolved inside the caller's scope: a foreign id is
		// ErrNotFound rather than an empty panel.
		var ref string
		err := tx.QueryRow(ctx, `select ref from consumers
			where producer_id = $1 and env = $2 and id = $3`,
			sc.ProducerID, sc.Env, consumerID).Scan(&ref)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			select id, event_type, coalesce(routing_state, ''), coalesce(non_delivery_reason, ''),
			       occurred_at, created_at, is_test
			  from events
			 where producer_id = $1 and env = $2
			   and non_delivery_reason is not null
			   and consumer_ref = $3
			 order by created_at desc, id desc
			 limit $4`,
			sc.ProducerID, sc.Env, ref, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var e ConsumerEvent
			if err := rows.Scan(&e.EventID, &e.EventType, &e.RoutingState, &e.Reason,
				&e.OccurredAt, &e.CreatedAt, &e.IsTest); err != nil {
				return err
			}
			out = append(out, e)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
