package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/m7jay/pigeon/internal/db"
)

// The threshold pause and its resume.
//
// The delivery worker is a system worker (S4): it carries no producer scope, so
// these reach the tenant tables through the SECURITY DEFINER functions in 0009
// exactly as the claim does. Both halves are CONTROL state and therefore run with
// synchronous_commit=on (FR-812, D-8): a pause that was reported but not written
// would leave the platform hammering a receiver it just told the operator it had
// stopped, and that is the one failure mode a pause exists to prevent.
//
// The Redis counters that DECIDE a pause are not here on purpose. The decision is
// a Redis read, the state is a Postgres column, and losing the first can only
// delay the second (D-8).

// SubscriptionPause is the outcome of a subscription threshold trip: what the
// notification must name (FR-1204), and whether this call was the one that
// performed the transition. Transitioned=false means the subscription was already
// paused, so nothing is owed - which is how "notify once" is enforced rather than
// hoped for.
type SubscriptionPause struct {
	Transitioned   bool
	ProducerID     string
	Env            string
	SubscriptionID string
	EventType      string
	FirstResponse  string
	LastResponse   string
	PausedAt       time.Time
}

// EndpointPause is the outcome of an endpoint breadth trip (FR-1203). It carries
// the failing-subscription count because that count is what the aggregated
// notification names (FR-1204, FR-1210).
type EndpointPause struct {
	Transitioned         bool
	ProducerID           string
	Env                  string
	EndpointID           string
	FailingSubscriptions int
	FirstResponse        string
	LastResponse         string
	PausedAt             time.Time
}

// ResumeResult reports whether a resume did anything and how long the scope was
// paused, which is the period FR-1207 adds back to every held delivery's window.
type ResumeResult struct {
	Resumed    bool
	ProducerID string
	Env        string
	PausedFor  time.Duration
}

// EndpointSubscriptionStats returns how many of an endpoint's subscriptions are
// unpaused, which is FR-1203's `active` - the number the breadth threshold scales
// with. The worker reads it at most once per newly failing subscription, never
// per attempt.
func (s *Store) EndpointSubscriptionStats(ctx context.Context, endpointID string) (active, paused, total int, err error) {
	err = s.pool.QueryRow(ctx, `select active, paused, total
		from pigeon_endpoint_subscription_stats($1)`, endpointID).Scan(&active, &paused, &total)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("endpoint subscription stats: %w", err)
	}
	return active, paused, total, nil
}

// PauseSubscriptionForFailures trips FR-1202's subscription pause for one
// (endpoint, event_type). It is one-shot: a second call inside the same pause
// episode reports Transitioned=false and writes nothing, including the
// notification row, which is inserted in the same statement as the transition.
//
// The function refuses to pause while the ENDPOINT is paused (FR-1210), so an
// endpoint resume restores exactly the subscriptions that had not tripped their
// own threshold.
func (s *Store) PauseSubscriptionForFailures(ctx context.Context, endpointID, eventType string) (SubscriptionPause, error) {
	var out SubscriptionPause
	var pausedAt *time.Time
	err := db.ControlTx(ctx, s.pool, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `select paused, producer_id, env, subscription_id,
			event_type, first_response, last_response, paused_at
			from pigeon_pause_subscription($1,$2,'failure_threshold')`,
			endpointID, eventType,
		).Scan(&out.Transitioned, &out.ProducerID, &out.Env, &out.SubscriptionID,
			&out.EventType, &out.FirstResponse, &out.LastResponse, &pausedAt)
	})
	if err != nil {
		return SubscriptionPause{}, fmt.Errorf("pause subscription: %w", err)
	}
	if pausedAt != nil {
		out.PausedAt = *pausedAt
	}
	return out, nil
}

// PauseEndpointForFailures trips FR-1203's endpoint pause. failingSubscriptions
// is the breadth count the caller measured, carried through to the notification
// because FR-1204 requires the message to name it.
func (s *Store) PauseEndpointForFailures(ctx context.Context, endpointID string, failingSubscriptions int) (EndpointPause, error) {
	var out EndpointPause
	var pausedAt *time.Time
	err := db.ControlTx(ctx, s.pool, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `select paused, producer_id, env, endpoint_id,
			failing_subscriptions, first_response, last_response, paused_at
			from pigeon_pause_endpoint($1,'failure_threshold',$2)`,
			endpointID, failingSubscriptions,
		).Scan(&out.Transitioned, &out.ProducerID, &out.Env, &out.EndpointID,
			&out.FailingSubscriptions, &out.FirstResponse, &out.LastResponse, &pausedAt)
	})
	if err != nil {
		return EndpointPause{}, fmt.Errorf("pause endpoint: %w", err)
	}
	if pausedAt != nil {
		out.PausedAt = *pausedAt
	}
	return out, nil
}

// ResumeEndpoint is FR-1205's one action for an endpoint: clear the flag and
// extend every held delivery's window by the paused period (FR-1207). It does NOT
// replay anything (FR-1205) - the caller clears the Redis counters that decide a
// pause, which is FR-1211's other half.
func (s *Store) ResumeEndpoint(ctx context.Context, endpointID string) (ResumeResult, error) {
	var out ResumeResult
	err := db.ControlTx(ctx, s.pool, func(tx pgx.Tx) error {
		var pausedFor *float64
		if err := tx.QueryRow(ctx, `select resumed, producer_id, env,
			(extract(epoch from paused_for))::double precision
			from pigeon_resume_endpoint($1)`, endpointID,
		).Scan(&out.Resumed, &out.ProducerID, &out.Env, &pausedFor); err != nil {
			return err
		}
		if pausedFor != nil {
			out.PausedFor = seconds(*pausedFor)
		}
		return nil
	})
	if err != nil {
		return ResumeResult{}, fmt.Errorf("resume endpoint: %w", err)
	}
	return out, nil
}

// ResumeSubscription is FR-1205's one action for a subscription.
func (s *Store) ResumeSubscription(ctx context.Context, endpointID, eventType string) (ResumeResult, error) {
	var out ResumeResult
	err := db.ControlTx(ctx, s.pool, func(tx pgx.Tx) error {
		var pausedFor *float64
		if err := tx.QueryRow(ctx, `select resumed, producer_id, env,
			(extract(epoch from paused_for))::double precision
			from pigeon_resume_subscription($1,$2)`, endpointID, eventType,
		).Scan(&out.Resumed, &out.ProducerID, &out.Env, &pausedFor); err != nil {
			return err
		}
		if pausedFor != nil {
			out.PausedFor = seconds(*pausedFor)
		}
		return nil
	})
	if err != nil {
		return ResumeResult{}, fmt.Errorf("resume subscription: %w", err)
	}
	return out, nil
}

// PauseNotification is one FR-1204 notification as recorded (0009). It is
// evidence, not the pause itself: the pause is the flag on the endpoint or
// subscription, and this row is what was said about it.
type PauseNotification struct {
	Kind                 string
	EndpointID           string
	EventType            string
	FailingSubscriptions int
	FirstResponse        string
	LastResponse         string
	PausedAt             time.Time
	OccurredAt           time.Time
}

// PauseNotifications reads an endpoint's pause notifications, newest first. The
// portal and an operator both need it: v1.0 has no mailer, so this row is the
// notification (0009).
func (s *Store) PauseNotifications(ctx context.Context, endpointID string, limit int) ([]PauseNotification, error) {
	rows, err := s.pool.Query(ctx, `select kind, endpoint_id, coalesce(event_type, ''),
		coalesce(failing_subscriptions, 0), coalesce(first_response, ''),
		coalesce(last_response, ''), paused_at, occurred_at
		from pigeon_pause_notifications($1,$2)`, endpointID, limit)
	if err != nil {
		return nil, fmt.Errorf("pause notifications: %w", err)
	}
	defer rows.Close()
	var out []PauseNotification
	for rows.Next() {
		var n PauseNotification
		if err := rows.Scan(&n.Kind, &n.EndpointID, &n.EventType,
			&n.FailingSubscriptions, &n.FirstResponse, &n.LastResponse,
			&n.PausedAt, &n.OccurredAt); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}
