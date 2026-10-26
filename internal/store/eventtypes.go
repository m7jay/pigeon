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
	Examples       []json.RawMessage
	CreatedAt      time.Time
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
				payload_version, enabled, examples, created_at`,
			id.New("et"), sc.ProducerID, sc.Env, in.Name, routing, class,
			in.PayloadSchema, version, examples,
		).Scan(&et.Name, &et.ProducerID, &et.Env, &et.RoutingMode, &et.EventClass,
			&et.PayloadSchema, &et.PayloadVersion, &et.Enabled, &et.Examples, &et.CreatedAt)
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
				payload_version, enabled, examples, created_at
			from event_types
			where producer_id = $1 and env = $2 and name = $3`,
			sc.ProducerID, sc.Env, name,
		).Scan(&et.Name, &et.ProducerID, &et.Env, &et.RoutingMode, &et.EventClass,
			&et.PayloadSchema, &et.PayloadVersion, &et.Enabled, &et.Examples, &et.CreatedAt)
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
				payload_version, enabled, examples, created_at
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
				&et.PayloadSchema, &et.PayloadVersion, &et.Enabled, &et.Examples, &et.CreatedAt); err != nil {
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
