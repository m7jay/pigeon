package store

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/m7jay/pigeon/internal/id"
)

// A subscription is the (endpoint, event_type) pair a consumer opts into
// (FR-501), independently creatable and deletable. Fan-out intersects these with
// the event's type (FR-502). It is soft-deleted, never hard-removed, so the
// history of what an endpoint was subscribed to is retained.
type Subscription struct {
	ID         string
	EndpointID string
	EventType  string
	Status     string // active | paused | deleted
	CreatedAt  time.Time
}

// NewSubscription is a POST to an endpoint's subscription collection.
type NewSubscription struct {
	EndpointID string
	EventType  string
}

// CreateSubscription adds an (endpoint, event_type) pair (FR-501). The two FKs do
// the validation: an unknown endpoint is ErrNotFound, an unknown event type is
// ErrNotFound, and re-adding an existing pair is ErrConflict (the pair is the unit).
func (s *Store) CreateSubscription(ctx context.Context, sc Scope, in NewSubscription) (Subscription, error) {
	eventType := strings.TrimSpace(in.EventType)
	if eventType == "" {
		return Subscription{}, errors.New("event_type is required")
	}
	var sub Subscription
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			insert into subscriptions (id, producer_id, env, endpoint_id, event_type)
			values ($1,$2,$3,$4,$5)
			returning id, endpoint_id, event_type, status, created_at`,
			id.New("sub"), sc.ProducerID, sc.Env, in.EndpointID, eventType,
		).Scan(&sub.ID, &sub.EndpointID, &sub.EventType, &sub.Status, &sub.CreatedAt)
		return mapPgError(err, ErrConflict, ErrNotFound)
	})
	if err != nil {
		return Subscription{}, err
	}
	return sub, nil
}

// ListSubscriptionsForEndpoint returns an endpoint's non-deleted subscriptions,
// newest pair first. The consumer portal and the producer both read it.
func (s *Store) ListSubscriptionsForEndpoint(ctx context.Context, sc Scope, endpointID string) ([]Subscription, error) {
	var out []Subscription
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			select id, endpoint_id, event_type, status, created_at
			from subscriptions
			where producer_id = $1 and env = $2 and endpoint_id = $3 and status <> 'deleted'
			order by created_at desc`,
			sc.ProducerID, sc.Env, endpointID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var sub Subscription
			if err := rows.Scan(&sub.ID, &sub.EndpointID, &sub.EventType, &sub.Status, &sub.CreatedAt); err != nil {
				return err
			}
			out = append(out, sub)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// DeleteSubscription soft-deletes one pair (FR-501). A pair a consumer has opted
// out of stops receiving new events at the next fan-out; the row is kept.
func (s *Store) DeleteSubscription(ctx context.Context, sc Scope, endpointID, eventType string) error {
	return s.scoped(ctx, sc, func(tx pgx.Tx) error {
		res, err := tx.Exec(ctx, `
			update subscriptions set status = 'deleted'
			where producer_id = $1 and env = $2 and endpoint_id = $3 and event_type = $4
			  and status <> 'deleted'`,
			sc.ProducerID, sc.Env, endpointID, eventType)
		if err != nil {
			return mapPgError(err, ErrConflict, ErrNotFound)
		}
		if res.RowsAffected() == 0 {
			return ErrNotFound
		}
		return nil
	})
}
