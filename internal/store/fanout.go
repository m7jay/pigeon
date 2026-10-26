package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Fan-out is where a resolved event becomes N deliveries (FR-7.7). It is the
// intersection of §7.5a's two decisions: the producer declared which consumer an
// event is about (consumer_ref / consumer_id), and each consumer declared which
// event types it wants on which endpoints (subscriptions). Fan-out computes that
// intersection and writes one delivery row per match BEFORE any attempt is made
// (FR-703), in one transaction per event, idempotent on (event_id, endpoint_id)
// (FR-704), batched so N rows are one round trip (FR-706). The lease is set here,
// at creation, never at claim (FR-813).
//
// The four ways an event delivers to nothing are recorded distinctly (§7.5a,
// FR-509), each with its own reason code, which the metric layer uses as its
// label (FR-508):
const (
	ReasonUnroutableNotRegistered = "unroutable:consumer_not_registered"
	ReasonNoMatchingSubscription  = "not_delivered:no_matching_subscription"
	ReasonEndpointUnverified      = "not_delivered:endpoint_unverified"
	ReasonEndpointPaused          = "not_delivered:endpoint_paused"
)

// deliveryLease is how long a freshly fanned-out delivery is leased for (FR-813).
// A worker that dies inside it lets the lease lapse and the sweeper re-queues the
// row (D-6, FR-809). It must outlast a full attempt (5 s ceiling) plus backoff.
const deliveryLease = 40 * time.Second

// FanOutResult is the outcome fan-out recorded, so the caller can advance the
// metric by reason label without re-reading. EventReason is set only for the two
// event-level cases (no consumer, or no subscription matched); endpoint-level
// reasons come back per skipped row in Skipped.
type FanOutResult struct {
	EventID      string
	RoutingMode  string
	RoutingState string // "routable", "unroutable", or "" for a broadcast event
	EventReason  string // the event-level §7.5a reason, "" when the event routed
	Deliveries   int    // pending delivery rows created
	Skipped      []SkippedDelivery
	Idle         bool // nothing was written (a replay, or a broadcast event)
}

// SkippedDelivery is a matched endpoint that could not take the event at fan-out
// time: unverified in live, or paused. It is recorded as a skipped delivery row.
type SkippedDelivery struct {
	EndpointID string
	Reason     string
}

type fanOutMatch struct {
	endpointID string
	status     string // endpoint status: active | paused
	verified   bool
	subStatus  string // subscription status: active | paused
}

// FanOutEvent resolves one event to its deliveries. It is a no-op for an event
// with no matching subscription beyond recording the reason, and a no-op on
// replay (idempotent). It runs in one synchronous_commit=on transaction (FR-703):
// the delivery rows are control state §6 promises cannot be lost.
func (s *Store) FanOutEvent(ctx context.Context, sc Scope, eventID string) (FanOutResult, error) {
	result := FanOutResult{EventID: eventID}
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		// Read the event under the caller's scope: a foreign event id is ErrNotFound
		// (FR-108), and the routing fields are what fan-out intersects.
		var routingMode, eventType, consumerRef string
		if err := tx.QueryRow(ctx, `
			select routing_mode, event_type, coalesce(consumer_ref, '')
			from events
			where producer_id = $1 and env = $2 and id = $3`,
			sc.ProducerID, sc.Env, eventID,
		).Scan(&routingMode, &eventType, &consumerRef); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		result.RoutingMode = routingMode

		// Broadcast events carry no consumer (§7.5a). The M0 endpoint model is a
		// consumer-owned endpoint, so there is nothing to intersect; broadcast
		// fan-out writes no rows. (Consumer-less endpoints land where broadcast
		// endpoints are modelled; noted in SPEC.md.)
		if routingMode != RoutingConsumer {
			result.Idle = true
			return nil
		}

		// Resolve the ref to a consumer (FR-508). A miss is the unroutable case:
		// accepted (the event is already committed), recorded, zero deliveries.
		var consumerID string
		err := tx.QueryRow(ctx, `
			select id from consumers
			where producer_id = $1 and env = $2 and ref = $3`,
			sc.ProducerID, sc.Env, consumerRef,
		).Scan(&consumerID)
		if errors.Is(err, pgx.ErrNoRows) {
			result.RoutingState = "unroutable"
			result.EventReason = ReasonUnroutableNotRegistered
			return recordEventReason(ctx, tx, sc, eventID, "unroutable", ReasonUnroutableNotRegistered)
		}
		if err != nil {
			return err
		}

		// Intersect: the consumer's non-deleted endpoints that carry a non-deleted
		// subscription for this type. Paused and unverified are sub-states read
		// here, not filtered out, so each produces its own reason rather than
		// vanishing (FR-509).
		matches, err := queryFanOutMatches(ctx, tx, sc, eventType, consumerID)
		if err != nil {
			return err
		}
		if len(matches) == 0 {
			// The consumer exists but nothing is subscribed to this type (AC-24).
			result.RoutingState = "routable"
			result.EventReason = ReasonNoMatchingSubscription
			return recordEventReason(ctx, tx, sc, eventID, "routable", ReasonNoMatchingSubscription)
		}

		// Classify each match: deliverable, or skipped with an endpoint-level
		// reason (paused, or unverified in live).
		var deliverable []string
		for _, m := range matches {
			switch {
			case m.subStatus == "paused" || m.status == "paused":
				result.Skipped = append(result.Skipped, SkippedDelivery{EndpointID: m.endpointID, Reason: ReasonEndpointPaused})
			case !m.verified && sc.Env == EnvLive:
				result.Skipped = append(result.Skipped, SkippedDelivery{EndpointID: m.endpointID, Reason: ReasonEndpointUnverified})
			default:
				deliverable = append(deliverable, m.endpointID)
			}
		}

		if n, err := insertDeliveries(ctx, tx, sc, eventID, consumerID, deliverable); err != nil {
			return err
		} else {
			result.Deliveries = n
		}
		if n, err := insertSkippedDeliveries(ctx, tx, sc, eventID, consumerID, result.Skipped); err != nil {
			return err
		} else {
			// Skipped rows are terminal; they advance nothing but the record.
			_ = n
		}

		// The event routed (a delivery or a recorded skip exists); clear any
		// stale non-delivery reason so a re-fan-out of a previously-unroutable
		// event that has since been onboarded reads back as routed.
		result.RoutingState = "routable"
		_, err = tx.Exec(ctx, `
			update events set routing_state = 'routable', non_delivery_reason = null
			where producer_id = $1 and env = $2 and id = $3`,
			sc.ProducerID, sc.Env, eventID)
		return err
	})
	return result, err
}

// queryFanOutMatches returns the (endpoint, subscription) pairs for one event's
// consumer and type that are not soft-deleted. Paused rows are included so the
// caller can record endpoint_paused rather than drop the match silently.
func queryFanOutMatches(ctx context.Context, tx pgx.Tx, sc Scope, eventType, consumerID string) ([]fanOutMatch, error) {
	rows, err := tx.Query(ctx, `
		select e.id, e.status, (e.verified_at is not null), s.status
		from subscriptions s
		join endpoints e on e.producer_id = s.producer_id and e.env = s.env and e.id = s.endpoint_id
		where s.producer_id = $1 and s.env = $2 and s.event_type = $3
		  and e.consumer_id = $4
		  and s.status <> 'deleted' and e.status <> 'deleted'`,
		sc.ProducerID, sc.Env, eventType, consumerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []fanOutMatch
	for rows.Next() {
		var m fanOutMatch
		if err := rows.Scan(&m.endpointID, &m.status, &m.verified, &m.subStatus); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// insertDeliveries batch-inserts one pending delivery per endpoint (FR-706: a
// single round trip, never one per row), setting the lease at creation
// (FR-813). ON CONFLICT (event_id, endpoint_id) DO NOTHING makes a re-fan-out a
// no-op (FR-704). Returns how many rows were actually inserted (0 on a replay,
// when every row already exists), which the caller reports as the delivery count.
func insertDeliveries(ctx context.Context, tx pgx.Tx, sc Scope, eventID, consumerID string, endpoints []string) (int, error) {
	if len(endpoints) == 0 {
		return 0, nil
	}
	res, err := tx.Exec(ctx, `
		insert into deliveries
			(event_id, endpoint_id, producer_id, env, consumer_id, status, lease_expires_at)
		select $1, ep, $2, $3, $4, 'pending', now() + interval '40 seconds'
		from unnest($5::text[]) as ep
		on conflict (event_id, endpoint_id) do nothing`,
		eventID, sc.ProducerID, sc.Env, consumerID, endpoints)
	if err != nil {
		return 0, fmt.Errorf("insert deliveries: %w", err)
	}
	return int(res.RowsAffected()), nil
}

// insertSkippedDeliveries batch-inserts the skipped (matched-but-undeliverable)
// rows with their endpoint-level reason, same batching and idempotency rules.
func insertSkippedDeliveries(ctx context.Context, tx pgx.Tx, sc Scope, eventID, consumerID string, skipped []SkippedDelivery) (int, error) {
	if len(skipped) == 0 {
		return 0, nil
	}
	endpoints := make([]string, len(skipped))
	reasons := make([]string, len(skipped))
	for i, s := range skipped {
		endpoints[i] = s.EndpointID
		reasons[i] = s.Reason
	}
	// The endpoint id and its reason are two columns of ONE set-returning
	// source: unnest($5, $6) zips the arrays row-by-row, so each endpoint is
	// paired with its own reason. Two comma-separated unnest() would be a CROSS
	// JOIN — every endpoint paired with every reason — and ON CONFLICT would
	// then keep an arbitrary reason per endpoint, corrupting both the row and
	// the FR-508 metric label derived from it.
	res, err := tx.Exec(ctx, `
		insert into deliveries
			(event_id, endpoint_id, producer_id, env, consumer_id, status, non_delivery_reason, lease_expires_at)
		select $1, x.endpoint_id, $2, $3, $4, 'skipped', x.reason, null
		from unnest($5::text[], $6::text[]) as x(endpoint_id, reason)
		on conflict (event_id, endpoint_id) do nothing`,
		eventID, sc.ProducerID, sc.Env, consumerID, endpoints, reasons)
	if err != nil {
		return 0, fmt.Errorf("insert skipped deliveries: %w", err)
	}
	return int(res.RowsAffected()), nil
}

// recordEventReason sets the event-level routing outcome: routing_state plus the
// §7.5a reason, so the UI-facing event view distinguishes an unroutable ref from
// an un-subscribed consumer (AC-23 vs AC-24) without joining the deliveries.
func recordEventReason(ctx context.Context, tx pgx.Tx, sc Scope, eventID, routingState, reason string) error {
	_, err := tx.Exec(ctx, `
		update events set routing_state = $4, non_delivery_reason = $5
		where producer_id = $1 and env = $2 and id = $3`,
		sc.ProducerID, sc.Env, eventID, routingState, reason)
	return err
}
