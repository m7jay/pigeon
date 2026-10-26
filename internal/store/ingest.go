package store

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/m7jay/pigeon/internal/db"
	"github.com/m7jay/pigeon/internal/id"
	"github.com/m7jay/pigeon/internal/schema"
)

// PublishValidationError is FR-303's contract surfaced to the API: the payload
// failed the event type's JSON Schema, and this names the exact pointer that
// failed so the 422 can point at "/amount" rather than a bare "invalid". It
// unwraps to ErrInvalid, so the storeError map still renders it as a 422; the
// handler unwraps it further to add the structured pointer field.
type PublishValidationError struct {
	Pointer string // RFC 6901 pointer into the payload, "" for the document root
	Message string
	Keyword string // the schema keyword that rejected it ("type", "required", ...)
}

func (e *PublishValidationError) Error() string {
	if e.Pointer == "" {
		return "payload " + e.Message
	}
	return fmt.Sprintf("payload at %s %s", e.Pointer, e.Message)
}

// Unwrap keeps ErrInvalid in the chain so errors.Is(err, store.ErrInvalid) stays
// true for the generic 422 path; the handler prefers errors.As for the pointer.
func (e *PublishValidationError) Unwrap() error { return ErrInvalid }

// PublishInput is one POST /v1/events. OccurredAt is optional (FR-605: the caller
// may back- or forward-dating within a bounded window; zero means "now").
// ConsumerID and ConsumerRef are the two forms of the routing key (§7.5a); the
// handler enforces that at most one is set and the store enforces the routing
// rule against the event type.
type PublishInput struct {
	EventType      string
	Payload        json.RawMessage
	OccurredAt     time.Time
	ConsumerID     *string
	ConsumerRef    *string
	IdempotencyKey string
}

// PublishedEvent is what a 202 reports back, and the record of what was accepted.
// ConsumerRef is always the resolved form: a consumer_id input is looked up to
// the consumer's registered ref at ingest, so the envelope and the routing key
// are the same string (§10, FR-506).
type PublishedEvent struct {
	ID             string
	EventType      string
	ProducerID     string
	Env            string
	OccurredAt     time.Time
	RoutingMode    string
	EventClass     string
	ConsumerRef    string
	PayloadVersion int
	Payload        json.RawMessage
}

const (
	// idempotencyTTL is FR-603's 24 h window after which a key is reusable.
	idempotencyTTL = 24 * time.Hour
	// occurredAtBackdate and occurredAtForwardDate are FR-605's window.
	occurredAtBackdate    = 7 * 24 * time.Hour
	occurredAtForwardDate = 5 * time.Minute
)

// PublishEvent is FR-601..FR-607. The event row and the outbox row are written in
// ONE synchronous_commit=on transaction (FR-604): a caller that received 202 can
// never have an event without a publish intent, and the relay can never lose one.
// The schema is validated (FR-303) and the consumer resolved inside that same
// transaction, so a concurrent event-type version bump or disable is seen
// atomically with the accept.
func (s *Store) PublishEvent(ctx context.Context, sc Scope, in PublishInput) (PublishedEvent, error) {
	// The routing keys are validated for shape before any database work: a
	// consumer-routed event needs exactly one, a broadcast event needs neither
	// (FR-505, AC-25). Which one it is decides the branch below.
	var consumerRef string
	var ev PublishedEvent
	err := db.ControlTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := db.SetProducerScope(ctx, tx, sc.ProducerID); err != nil {
			return err
		}
		if err := db.SetEnvScope(ctx, tx, sc.Env); err != nil {
			return err
		}

		et, err := readEventTypeForPublish(ctx, tx, sc, in.EventType, in.Payload)
		if err != nil {
			return err
		}

		// FR-505 / AC-25: broadcast takes neither key, consumer takes exactly one.
		hasID, hasRef := in.ConsumerID != nil, in.ConsumerRef != nil
		if et.RoutingMode == RoutingBroadcast {
			if hasID || hasRef {
				return fmt.Errorf("%w: broadcast event types take no consumer key", ErrInvalid)
			}
		} else {
			if hasID == hasRef {
				return fmt.Errorf("%w: consumer event types require exactly one of consumer_id or consumer_ref", ErrInvalid)
			}
			switch {
			case in.ConsumerRef != nil:
				consumerRef = *in.ConsumerRef
			case in.ConsumerID != nil:
				r, err := resolveConsumerRef(ctx, tx, sc, *in.ConsumerID)
				if err != nil {
					return err
				}
				consumerRef = r
			}
		}

		occurredAt := in.OccurredAt
		if occurredAt.IsZero() {
			occurredAt = time.Now()
		} else if err := validateOccurredAt(occurredAt); err != nil {
			return err
		}

		// FR-603 idempotency. A live key short-circuits to the original event;
		// there is no second event, no second outbox row, no second publish.
		if in.IdempotencyKey != "" {
			replay, err := idempotentReplay(ctx, tx, sc, in.IdempotencyKey)
			if err != nil {
				return err
			}
			if replay != nil {
				ev = *replay
				return nil
			}
		}

		eventID := id.NewULID("evt")
		if _, err := tx.Exec(ctx, `
			insert into events
				(id, producer_id, env, event_type, payload_version, routing_mode, event_class,
				 consumer_id, consumer_ref, occurred_at, payload)
			values ($1, $2, $3, $4, $5, $6, $7, null, nullif($8, ''), $9, $10)`,
			eventID, sc.ProducerID, sc.Env, et.Name, et.PayloadVersion, et.RoutingMode, et.EventClass,
			consumerRef, occurredAt, in.Payload,
		); err != nil {
			return mapEventInsertError(err)
		}

		// The outbox row is the publish intent. It is in this transaction, so the
		// 202 below cannot outlive a missing intent (FR-604).
		if _, err := tx.Exec(ctx, `insert into outbox (event_id, producer_id, env) values ($1, $2, $3)`,
			eventID, sc.ProducerID, sc.Env); err != nil {
			return fmt.Errorf("insert outbox: %w", err)
		}

		// FR-603: claim the key. A unique-violation here means a concurrent publish
		// with the same key committed first; that is a replay, handled after the
		// rollback below.
		if in.IdempotencyKey != "" {
			if _, err := tx.Exec(ctx, `
				insert into idempotency_keys (producer_id, env, key_hash, event_id, expires_at)
				values ($1, $2, $3, $4, now() + interval '24 hours')`,
				sc.ProducerID, sc.Env, hashIdempotencyKey(in.IdempotencyKey), eventID,
			); err != nil {
				return mapIdempotencyError(err)
			}
		}

		// D-4 fast path: wake the relay. NOTIFY is unrestricted, so the runtime
		// role may send it; the payload is the event id for a targeted lease.
		if _, err := tx.Exec(ctx, `select pg_notify('pigeon_outbox', $1)`, eventID); err != nil {
			return fmt.Errorf("notify relay: %w", err)
		}

		ev = PublishedEvent{
			ID: eventID, EventType: et.Name, ProducerID: sc.ProducerID, Env: sc.Env,
			OccurredAt: occurredAt, RoutingMode: et.RoutingMode, EventClass: et.EventClass,
			ConsumerRef: consumerRef, PayloadVersion: et.PayloadVersion, Payload: in.Payload,
		}
		return nil
	})
	if err != nil {
		// A replay race (unique violation on the idempotency key) is not an error:
		// roll back the half-written event and return the winner's original id.
		if errors.Is(err, errIdempotencyConflict) {
			replay, rerr := s.idempotentReplayRead(ctx, sc, in.IdempotencyKey)
			if rerr != nil {
				return PublishedEvent{}, err
			}
			if replay != nil {
				return *replay, nil
			}
		}
		return PublishedEvent{}, err
	}
	return ev, nil
}

// readEventTypeForPublish loads the type under the caller's scope and enforces
// the accept-time rules: it must exist, be enabled (FR-305), and its payload
// schema must validate the payload (FR-303). All three live in one row read
// inside the transaction, so a concurrent PATCH that disables or bumps the
// version is seen before the event is written.
func readEventTypeForPublish(ctx context.Context, tx pgx.Tx, sc Scope, name string, payload json.RawMessage) (eventTypeRow, error) {
	var et eventTypeRow
	err := tx.QueryRow(ctx, `
		select name, routing_mode, event_class, payload_schema, payload_version, enabled
		from event_types
		where producer_id = $1 and env = $2 and name = $3`,
		sc.ProducerID, sc.Env, name,
	).Scan(&et.Name, &et.RoutingMode, &et.EventClass, &et.PayloadSchema, &et.PayloadVersion, &et.Enabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return et, fmt.Errorf("%w: no event type named %q in this producer", ErrInvalid, name)
	}
	if err != nil {
		return et, err
	}
	if !et.Enabled {
		return et, fmt.Errorf("%w: event type %q is disabled", ErrInvalid, name)
	}
	if err := schema.Validate(et.PayloadSchema, payload); err != nil {
		// FR-303: the failing JSON pointer must reach the API as structured data,
		// not be swallowed into a generic message. A *schema.ValidationError becomes
		// a PublishValidationError (which still unwraps to ErrInvalid, so the
		// generic 422 mapping keeps working); a non-validation error means the
		// producer's own schema is malformed, which is a different fault.
		var ve *schema.ValidationError
		if errors.As(err, &ve) {
			return et, &PublishValidationError{Pointer: ve.Pointer, Message: ve.Message, Keyword: ve.Keyword}
		}
		return et, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	return et, nil
}

// eventTypeRow is the slice of an event type this package needs.
type eventTypeRow struct {
	Name           string
	RoutingMode    string
	EventClass     string
	PayloadSchema  json.RawMessage
	PayloadVersion int
	Enabled        bool
}

// resolveConsumerRef maps a producer-supplied consumer_id to the consumer's
// registered ref. The id is ours and opaque, so an unknown one is a caller error
// (422), not an unroutable event - the producer's own external identifier is
// consumer_ref, which is allowed to name a not-yet-onboarded consumer (FR-508,
// handled at fan-out).
func resolveConsumerRef(ctx context.Context, tx pgx.Tx, sc Scope, consumerID string) (string, error) {
	var ref string
	err := tx.QueryRow(ctx, `
		select ref from consumers
		where producer_id = $1 and env = $2 and id = $3`,
		sc.ProducerID, sc.Env, consumerID,
	).Scan(&ref)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("%w: no consumer with id %q in this producer", ErrInvalid, consumerID)
	}
	if err != nil {
		return "", err
	}
	return ref, nil
}

func validateOccurredAt(t time.Time) error {
	now := time.Now()
	if t.Before(now.Add(-occurredAtBackdate)) || t.After(now.Add(occurredAtForwardDate)) {
		return fmt.Errorf("%w: occurred_at must be within the last 7 days and no more than 5 minutes in the future (FR-605)", ErrInvalid)
	}
	return nil
}

// idempotentReplay looks for a live key inside the transaction. It returns the
// original event when the key was already used within its 24 h window, nil when
// the caller may proceed, or an error. Expired keys are removed first so they do
// not block reuse (FR-603).
func idempotentReplay(ctx context.Context, tx pgx.Tx, sc Scope, key string) (*PublishedEvent, error) {
	if _, err := tx.Exec(ctx, `
		delete from idempotency_keys
		where producer_id = $1 and env = $2 and key_hash = $3 and expires_at <= now()`,
		sc.ProducerID, sc.Env, hashIdempotencyKey(key),
	); err != nil {
		return nil, err
	}
	var eventID string
	err := tx.QueryRow(ctx, `
		select event_id from idempotency_keys
		where producer_id = $1 and env = $2 and key_hash = $3 and expires_at > now()`,
		sc.ProducerID, sc.Env, hashIdempotencyKey(key),
	).Scan(&eventID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return readPublishedEventTx(ctx, tx, sc, eventID)
}

// idempotentReplayRead is the post-rollback form of the replay lookup, used when
// a concurrent publish won the idempotency key under us.
func (s *Store) idempotentReplayRead(ctx context.Context, sc Scope, key string) (*PublishedEvent, error) {
	var eventID string
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `
			select event_id from idempotency_keys
			where producer_id = $1 and env = $2 and key_hash = $3 and expires_at > now()`,
			sc.ProducerID, sc.Env, hashIdempotencyKey(key),
		).Scan(&eventID)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return s.readPublishedEvent(ctx, sc, eventID)
}

// readPublishedEvent rebuilds a PublishedEvent from the events table.
func (s *Store) readPublishedEvent(ctx context.Context, sc Scope, eventID string) (*PublishedEvent, error) {
	var ev PublishedEvent
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		return readPublishedEventRow(ctx, tx, sc, eventID, &ev)
	})
	if err != nil {
		return nil, err
	}
	return &ev, nil
}

func readPublishedEventTx(ctx context.Context, tx pgx.Tx, sc Scope, eventID string) (*PublishedEvent, error) {
	var ev PublishedEvent
	if err := readPublishedEventRow(ctx, tx, sc, eventID, &ev); err != nil {
		return nil, err
	}
	return &ev, nil
}

func readPublishedEventRow(ctx context.Context, tx pgx.Tx, sc Scope, eventID string, out *PublishedEvent) error {
	return tx.QueryRow(ctx, `
		select id, event_type, occurred_at, routing_mode, event_class, coalesce(consumer_ref, ''),
			payload_version, payload
		from events
		where producer_id = $1 and env = $2 and id = $3`,
		sc.ProducerID, sc.Env, eventID,
	).Scan(&out.ID, &out.EventType, &out.OccurredAt, &out.RoutingMode, &out.EventClass,
		&out.ConsumerRef, &out.PayloadVersion, &out.Payload)
}

func hashIdempotencyKey(key string) []byte {
	sum := sha256.Sum256([]byte(key))
	return sum[:]
}

// mapEventInsertError turns the SQLSTATEs an event insert can raise into the
// store's sentinels. A disabled/unknown type is caught before the insert, so the
// insert itself only fails on a genuine constraint (a malformed payload that
// violates a column check) or a deadlocked/raced idempotency key.
func mapEventInsertError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23514", "23502":
			return fmt.Errorf("%w: %s", ErrInvalid, pgErr.Message)
		}
	}
	return err
}

// errIdempotencyConflict is the marker mapIdempotencyError attaches so PublishEvent
// can distinguish a replay race from a real failure.
var errIdempotencyConflict = errors.New("idempotency key already held by a concurrent publish")

func mapIdempotencyError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return fmt.Errorf("%w: %v", errIdempotencyConflict, err)
	}
	return err
}
