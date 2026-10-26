package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/m7jay/pigeon/internal/store"
)

// This file is the producer-facing publish and event-type surface (§11):
//   POST /v1/events        - validate, write event + outbox in one transaction, 202
//   POST /v1/event-types   - register a published contract (name, schema, version)
//   GET  /v1/event-types   - list the caller's environment
//
// The durability work lives in the store (FR-604's single transaction); the
// handler's jobs are the shape of the request/response, the 64 KB ceiling
// (FR-606), the Idempotency-Key header (FR-603), and turning the store's typed
// validation error into a 422 that names the failing JSON pointer (FR-303).

// publishRequest is the POST /v1/events body. Payload is raw so the store can
// validate it against the event type's JSON Schema (FR-303) and store the exact
// bytes the producer sent; the envelope delivers them back unchanged.
type publishRequest struct {
	EventType      string          `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`
	OccurredAt     *time.Time      `json:"occurred_at"`
	ConsumerID     *string         `json:"consumer_id"`
	ConsumerRef    *string         `json:"consumer_ref"`
	IdempotencyKey string          `json:"idempotency_key"`
}

// publishResponse is the 202 body: enough of the accepted event for the caller to
// record the id (its own idempotency anchor) and confirm the routing that was
// resolved server-side.
type publishResponse struct {
	ID             string          `json:"id"`
	EventType      string          `json:"event_type"`
	ProducerID     string          `json:"producer_id"`
	Env            string          `json:"env"`
	OccurredAt     time.Time       `json:"occurred_at"`
	RoutingMode    string          `json:"routing_mode"`
	EventClass     string          `json:"event_class"`
	ConsumerRef    string          `json:"consumer_ref,omitempty"`
	PayloadVersion int             `json:"payload_version"`
	Payload        json.RawMessage `json:"payload"`
}

func toPublishResponse(ev store.PublishedEvent) publishResponse {
	return publishResponse{
		ID: ev.ID, EventType: ev.EventType, ProducerID: ev.ProducerID, Env: ev.Env,
		OccurredAt: ev.OccurredAt, RoutingMode: ev.RoutingMode, EventClass: ev.EventClass,
		ConsumerRef: ev.ConsumerRef, PayloadVersion: ev.PayloadVersion, Payload: ev.Payload,
	}
}

// handlePublish is POST /v1/events. FR-601..FR-607.
func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	p, ok := principalFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "no producer credential on request")
		return
	}

	var in publishRequest
	if !s.decodePublish(w, r, &in) {
		return
	}
	// The Idempotency-Key header is the documented place for it (FR-603); a body
	// copy is accepted too, but the header wins when both are present.
	if h := r.Header.Get("Idempotency-Key"); h != "" {
		in.IdempotencyKey = h
	}

	sc := p.Scope()
	input := store.PublishInput{
		EventType:      in.EventType,
		Payload:        in.Payload,
		ConsumerID:     in.ConsumerID,
		ConsumerRef:    in.ConsumerRef,
		IdempotencyKey: in.IdempotencyKey,
	}
	if in.OccurredAt != nil {
		input.OccurredAt = *in.OccurredAt
	}

	ev, err := s.deps.Data.PublishEvent(r.Context(), sc, input)
	if err != nil {
		// FR-303: a payload that fails the event type's schema is a 422 that names
		// the exact pointer, so a producer learns "payload at /amount expected
		// integer" rather than a bare "invalid". The pointer and keyword ride
		// beside code/message inside the §11 error envelope.
		var pve *store.PublishValidationError
		if errors.As(err, &pve) {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]any{
				"error": map[string]any{"code": "invalid", "message": pve.Message, "pointer": pve.Pointer, "keyword": pve.Keyword},
			})
			return
		}
		s.storeError(w, r, err)
		return
	}

	// 202 only if the event row and the outbox row both committed (FR-604): the
	// store returns here after that transaction, so the accept is the intent.
	writeJSON(w, http.StatusAccepted, toPublishResponse(ev))
}

// decodePublish reads the publish body and enforces FR-606's 64 KB ceiling with a
// 413, kept distinct from a malformed body's 400. The two failures have different
// remediations - send less, or send valid JSON - so they must not share a code.
func (s *Server) decodePublish(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBody)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeError(w, http.StatusRequestEntityTooLarge, "payload_too_large",
				fmt.Sprintf("event body must be at most %d bytes (FR-606)", maxBody))
			return false
		}
		writeError(w, http.StatusBadRequest, "invalid_json", "request body must be a JSON object: "+err.Error())
		return false
	}
	return true
}

// --- event types ---------------------------------------------------------

// createEventTypeRequest is the POST /v1/event-types body. Schema is required;
// routing, class and version take their documented defaults when omitted.
type createEventTypeRequest struct {
	Name           string            `json:"name"`
	RoutingMode    string            `json:"routing_mode"`
	EventClass     string            `json:"event_class"`
	PayloadSchema  json.RawMessage   `json:"payload_schema"`
	PayloadVersion int               `json:"payload_version"`
	Examples       []json.RawMessage `json:"examples"`
}

// eventTypeJSON is the wire view of a registered event type. It is deliberately
// the same shape for create and list so a producer reads one thing.
type eventTypeJSON struct {
	Name           string            `json:"name"`
	ProducerID     string            `json:"producer_id"`
	Env            string            `json:"env"`
	RoutingMode    string            `json:"routing_mode"`
	EventClass     string            `json:"event_class"`
	PayloadSchema  json.RawMessage   `json:"payload_schema"`
	PayloadVersion int               `json:"payload_version"`
	Enabled        bool              `json:"enabled"`
	Examples       []json.RawMessage `json:"examples"`
	CreatedAt      time.Time         `json:"created_at"`
}

func toEventTypeJSON(et store.EventType) eventTypeJSON {
	examples := et.Examples
	if examples == nil {
		examples = []json.RawMessage{}
	}
	return eventTypeJSON{
		Name: et.Name, ProducerID: et.ProducerID, Env: et.Env, RoutingMode: et.RoutingMode,
		EventClass: et.EventClass, PayloadSchema: et.PayloadSchema, PayloadVersion: et.PayloadVersion,
		Enabled: et.Enabled, Examples: examples, CreatedAt: et.CreatedAt,
	}
}

// handleCreateEventType is POST /v1/event-types. The name is immutable after this
// (FR-302); there is no update-name path - only version bump and disable, both
// PATCH /v1/event-types/{name} (FR-304, FR-305).
func (s *Server) handleCreateEventType(w http.ResponseWriter, r *http.Request) {
	p, ok := principalFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "no producer credential on request")
		return
	}
	var in createEventTypeRequest
	if !s.decodeJSON(w, r, &in) {
		return
	}
	et, err := s.deps.Data.CreateEventType(r.Context(), p.Scope(), store.NewEventType{
		Name: in.Name, RoutingMode: in.RoutingMode, EventClass: in.EventClass,
		PayloadSchema: in.PayloadSchema, PayloadVersion: in.PayloadVersion, Examples: in.Examples,
	})
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, toEventTypeJSON(et))
}

// handleListEventTypes is GET /v1/event-types.
func (s *Server) handleListEventTypes(w http.ResponseWriter, r *http.Request) {
	p, ok := principalFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "no producer credential on request")
		return
	}
	types, err := s.deps.Data.ListEventTypes(r.Context(), p.Scope())
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	out := make([]eventTypeJSON, len(types))
	for i, et := range types {
		out[i] = toEventTypeJSON(et)
	}
	writeJSON(w, http.StatusOK, out)
}

// patchEventTypeRequest is the PATCH /v1/event-types/{name} body. An omitted (nil)
// field is left unchanged; both are optional so {"enabled": false} and
// {"payload_version": 2} are each valid single-field patches.
type patchEventTypeRequest struct {
	Enabled        *bool `json:"enabled"`
	PayloadVersion *int  `json:"payload_version"`
}

// handlePatchEventType is PATCH /v1/event-types/{name}: disable/enable (FR-305)
// and version bump (FR-304) on one type in the caller's environment. Name,
// producer, env, routing_mode and event_class are immutable and cannot be named
// here. An unknown name in the caller's environment is a 404, never a 403
// (FR-108 + FR-103).
func (s *Server) handlePatchEventType(w http.ResponseWriter, r *http.Request) {
	p, ok := principalFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "no producer credential on request")
		return
	}
	var in patchEventTypeRequest
	if !s.decodeJSON(w, r, &in) {
		return
	}
	et, err := s.deps.Data.PatchEventType(r.Context(), p.Scope(), r.PathValue("name"), store.EventTypePatch{
		Enabled:        in.Enabled,
		PayloadVersion: in.PayloadVersion,
	})
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toEventTypeJSON(et))
}
