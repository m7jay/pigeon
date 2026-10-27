package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/m7jay/pigeon/internal/id"
)

// Event type names (FR-302). The regex is enforced in SQL (event_types_name_format)
// as well as here, so a bad name is a 422 with a reason rather than a 500 from a
// constraint the API cannot explain.
var eventTypeNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$`)

// Routing modes (FR-7.5a), declared on the type and immutable.
const (
	RoutingBroadcast = "broadcast"
	RoutingConsumer  = "consumer"
)

// Event classes (FR-7.7a), declared on the type. They pick the queue and the
// latency target; a payload cannot change its own class.
const (
	EventClassPriority = "priority"
	EventClassNormal   = "normal"
)

// EventType is one of a producer's published contracts. Name is the fully
// qualified form ("payment.succeeded"); it is immutable after creation (FR-302),
// which is structural here - there is no UpdateName path.
type EventType struct {
	Name           string
	ProducerID     string
	Env            string
	RoutingMode    string
	EventClass     string
	PayloadSchema  json.RawMessage
	PayloadVersion int
	Enabled        bool
	// PubliclySubscribable is UI-14's gate: whether a consumer user may subscribe
	// to this type from the consumer portal. Defaults true at creation (the
	// migration argues why) and is opted out by the producer with a PATCH.
	PubliclySubscribable bool
	Examples             []json.RawMessage
	CreatedAt            time.Time
}

// NewEventType is a create request. Zero values take their documented defaults:
// broadcast routing, normal class, version 1.
type NewEventType struct {
	Name           string
	RoutingMode    string
	EventClass     string
	PayloadSchema  json.RawMessage
	PayloadVersion int
	Examples       []json.RawMessage
}

// ValidateEventTypeName is exported so the handler can reject a bad name before
// opening a transaction, exactly as ValidateSlug does for producers.
func ValidateEventTypeName(name string) error {
	if !eventTypeNamePattern.MatchString(name) {
		return fmt.Errorf("%w: event type name must match %s", ErrInvalid, eventTypeNamePattern)
	}
	return nil
}

func normalizeRoutingMode(m string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(m)) {
	case "", RoutingBroadcast:
		return RoutingBroadcast, nil
	case RoutingConsumer:
		return RoutingConsumer, nil
	default:
		return "", fmt.Errorf("%w: routing_mode must be broadcast or consumer", ErrInvalid)
	}
}

func normalizeEventClass(c string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(c)) {
	case "", EventClassNormal:
		return EventClassNormal, nil
	case EventClassPriority:
		return EventClassPriority, nil
	default:
		return "", fmt.Errorf("%w: event_class must be priority or normal", ErrInvalid)
	}
}

// validateSchemaShape rejects a payload schema that is not even a JSON Schema
// object, so a producer cannot create a type whose schema would reject every
// payload (or, worse, accept garbage). The deeper validation - that a given
// payload conforms - happens at publish time (FR-303).
func validateSchemaShape(raw json.RawMessage) error {
	if len(strings.TrimSpace(string(raw))) == 0 {
		return fmt.Errorf("%w: payload_schema is required", ErrInvalid)
	}
	var node any
	if err := json.Unmarshal(raw, &node); err != nil {
		return fmt.Errorf("%w: payload_schema is not valid JSON: %v", ErrInvalid, err)
	}
	if _, ok := node.(map[string]any); !ok {
		return fmt.Errorf("%w: payload_schema must be a JSON object", ErrInvalid)
	}
	return nil
}

// CreateEventType registers a new event type in the caller's environment. The
// environment comes from the Scope, never the body (FR-103): a live member cannot
// accidentally define a test-only type and vice versa.
func (s *Store) CreateEventType(ctx context.Context, sc Scope, in NewEventType) (EventType, error) {
	if err := ValidateEventTypeName(in.Name); err != nil {
		return EventType{}, err
	}
	routing, err := normalizeRoutingMode(in.RoutingMode)
	if err != nil {
		return EventType{}, err
	}
	class, err := normalizeEventClass(in.EventClass)
	if err != nil {
		return EventType{}, err
	}
	if err := validateSchemaShape(in.PayloadSchema); err != nil {
		return EventType{}, err
	}
	version := in.PayloadVersion
	if version == 0 {
		version = 1
	}
	if version < 1 || version > 1000 {
		return EventType{}, fmt.Errorf("%w: payload_version must be between 1 and 1000", ErrInvalid)
	}

	var et EventType
	err = s.scoped(ctx, sc, func(tx pgx.Tx) error {
		examples := in.Examples
		if examples == nil {
			examples = []json.RawMessage{}
		}
		err := tx.QueryRow(ctx, `
			insert into event_types
				(id, producer_id, env, name, routing_mode, event_class, payload_schema, payload_version, examples)
			values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			returning name, producer_id, env, routing_mode, event_class, payload_schema,
				payload_version, enabled, examples, created_at, publicly_subscribable`,
			id.New("et"), sc.ProducerID, sc.Env, in.Name, routing, class,
			in.PayloadSchema, version, examples,
		).Scan(&et.Name, &et.ProducerID, &et.Env, &et.RoutingMode, &et.EventClass,
			&et.PayloadSchema, &et.PayloadVersion, &et.Enabled, &et.Examples, &et.CreatedAt, &et.PubliclySubscribable)
		return mapPgError(err, ErrConflict, ErrNotFound)
	})
	if err != nil {
		return EventType{}, err
	}
	return et, nil
}

// GetEventType reads one type in the caller's scope. An unknown name, or a name
// the caller's environment does not own, is ErrNotFound - the two are
// indistinguishable by design (FR-108 + FR-103).
func (s *Store) GetEventType(ctx context.Context, sc Scope, name string) (EventType, error) {
	var et EventType
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			select name, producer_id, env, routing_mode, event_class, payload_schema,
				payload_version, enabled, examples, created_at, publicly_subscribable
			from event_types
			where producer_id = $1 and env = $2 and name = $3`,
			sc.ProducerID, sc.Env, name,
		).Scan(&et.Name, &et.ProducerID, &et.Env, &et.RoutingMode, &et.EventClass,
			&et.PayloadSchema, &et.PayloadVersion, &et.Enabled, &et.Examples, &et.CreatedAt, &et.PubliclySubscribable)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	})
	if err != nil {
		return EventType{}, err
	}
	return et, nil
}

// ListEventTypes returns the caller's environment only, newest first.
func (s *Store) ListEventTypes(ctx context.Context, sc Scope) ([]EventType, error) {
	var out []EventType
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			select name, producer_id, env, routing_mode, event_class, payload_schema,
				payload_version, enabled, examples, created_at, publicly_subscribable
			from event_types
			where producer_id = $1 and env = $2
			order by created_at desc, name`, sc.ProducerID, sc.Env)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var et EventType
			if err := rows.Scan(&et.Name, &et.ProducerID, &et.Env, &et.RoutingMode, &et.EventClass,
				&et.PayloadSchema, &et.PayloadVersion, &et.Enabled, &et.Examples, &et.CreatedAt, &et.PubliclySubscribable); err != nil {
				return err
			}
			out = append(out, et)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// EventTypePatch is a PATCH to an event type. A nil field is left unchanged, so
// a body of {} is a no-op and a body of {"enabled": false} disables the type.
// FR-305 is disable-without-deleting-history: the row and every event published
// against it stay, only the enabled flag flips. A disabled type is refused at
// publish (the ingest query checks it, and TestPublishDisabledTypeIsInvalid
// pins that), so toggling enabled is the whole disable/enable contract; FR-304
// is the independent version bump on the same row. UI-14's
// publicly_subscribable is the third toggle: the producer's gate on consumer
// self-service subscription.
type EventTypePatch struct {
	Enabled        *bool
	PayloadVersion *int
	// PubliclySubscribable is UI-14's gate: the producer allows or refuses public
	// subscription to this type. A nil field is unchanged.
	PubliclySubscribable *bool
}

// PatchEventType applies an EventTypePatch to one type in the caller's scope and
// returns the updated row. An unknown name, or one the caller's environment does
// not own, is ErrNotFound (FR-108 + FR-103). It never changes name, producer,
// env, routing_mode or event_class - those are immutable at creation.
func (s *Store) PatchEventType(ctx context.Context, sc Scope, name string, p EventTypePatch) (EventType, error) {
	const selectCols = `name, producer_id, env, routing_mode, event_class, payload_schema,
		payload_version, enabled, examples, created_at, publicly_subscribable`
	var et EventType
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		scan := func(row pgx.Row) error {
			return row.Scan(&et.Name, &et.ProducerID, &et.Env, &et.RoutingMode, &et.EventClass,
				&et.PayloadSchema, &et.PayloadVersion, &et.Enabled, &et.Examples, &et.CreatedAt, &et.PubliclySubscribable)
		}
		if err := scan(tx.QueryRow(ctx,
			`select `+selectCols+` from event_types where producer_id = $1 and env = $2 and name = $3`,
			sc.ProducerID, sc.Env, name)); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return mapPgError(err, ErrConflict, ErrNotFound)
		}

		var sets []string
		var args []any
		if p.Enabled != nil {
			sets = append(sets, fmt.Sprintf("enabled = $%d", len(sets)+1))
			args = append(args, *p.Enabled)
		}
		if p.PayloadVersion != nil {
			sets = append(sets, fmt.Sprintf("payload_version = $%d", len(sets)+1))
			args = append(args, *p.PayloadVersion)
		}
		if p.PubliclySubscribable != nil {
			// UI-14: the producer allows or refuses public subscription (the
			// consumer portal's gate). It is a PATCH, not a create-time field, so a
			// producer can close a type that consumers already see.
			sets = append(sets, fmt.Sprintf("publicly_subscribable = $%d", len(sets)+1))
			args = append(args, *p.PubliclySubscribable)
		}
		if len(sets) > 0 {
			args = append(args, sc.ProducerID, sc.Env, name)
			q := fmt.Sprintf(`update event_types set %s where producer_id = $%d and env = $%d and name = $%d`,
				strings.Join(sets, ", "), len(args)-2, len(args)-1, len(args))
			if _, err := tx.Exec(ctx, q, args...); err != nil {
				return mapPgError(err, ErrConflict, ErrNotFound)
			}
		}
		// Re-read so the returned row reflects the write, including the version the
		// caller bumped to.
		if err := scan(tx.QueryRow(ctx,
			`select `+selectCols+` from event_types where producer_id = $1 and env = $2 and name = $3`,
			sc.ProducerID, sc.Env, name)); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return EventType{}, err
	}
	return et, nil
}

// exampleEventType is one of the three example contracts seeded per producer
// (FR-306). A producer gets all three in both environments so its dashboard has
// a working contract to publish against in test and live from the first login.
type exampleEventType struct {
	name    string
	schema  json.RawMessage
	example json.RawMessage
}

// exampleEventTypes is the seeded set (FR-306). Names are deliberately distinct
// from the market's generic "user.created" so they read as a coherent product
// story (order → refund) rather than a placeholder.
var exampleEventTypes = []exampleEventType{
	{
		name:    "order.shipped",
		schema:  json.RawMessage(`{"type":"object","required":["order_id","tracking_number"],"properties":{"order_id":{"type":"string"},"tracking_number":{"type":"string"}}}`),
		example: json.RawMessage(`{"order_id":"ord_01HXAQ2","tracking_number":"1Z999AA10123456784"}`),
	},
	{
		name:    "order.refunded",
		schema:  json.RawMessage(`{"type":"object","required":["order_id","amount"],"properties":{"order_id":{"type":"string"},"amount":{"type":"integer"}}}`),
		example: json.RawMessage(`{"order_id":"ord_01HXAQ2","amount":49900}`),
	},
	{
		name:    "refund.processed",
		schema:  json.RawMessage(`{"type":"object","required":["refund_id","amount"],"properties":{"refund_id":{"type":"string"},"amount":{"type":"integer"}}}`),
		example: json.RawMessage(`{"refund_id":"ref_7F3091","amount":49900}`),
	},
}

// seedExampleEventTypes writes the three FR-306 example types for one environment.
// It runs inside the producer-creation transaction, after the producer row and
// its scope are set, so a producer and its examples commit or fail together. The
// caller must have set the producer scope on the transaction; this sets the env
// for the duration of the insert.
func seedExampleEventTypes(ctx context.Context, tx pgx.Tx, producerID, env string) error {
	if _, err := tx.Exec(ctx, `select set_config('pigeon.env', $1, true)`, env); err != nil {
		return err
	}
	for _, ex := range exampleEventTypes {
		if _, err := tx.Exec(ctx, `
			insert into event_types
				(id, producer_id, env, name, routing_mode, event_class, payload_schema, payload_version, examples)
			values ($1, $2, $3, $4, 'broadcast', 'normal', $5, 1, $6)`,
			id.New("et"), producerID, env, ex.name, ex.schema, []json.RawMessage{ex.example}); err != nil {
			return fmt.Errorf("seed example event type %s: %w", ex.name, err)
		}
	}
	return nil
}
