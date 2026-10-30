package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/m7jay/pigeon/internal/ssrf"
	"github.com/m7jay/pigeon/internal/store"
)

// This file is the consumer portal's own surface (M2b, UI-13, UI-14, FR-110,
// FR-1105):
//
//	consumer/event-types                  - GET the types open to public subscription
//	consumer/endpoints                    - GET list, POST register (SSRF-checked)
//	consumer/endpoints/{eid}              - GET, PATCH edit/pause/email, DELETE soft-delete
//	consumer/endpoints/{eid}/rotate       - POST rotate the endpoint's own signing secret
//	consumer/endpoints/{eid}/verify       - POST answer the ownership challenge
//	consumer/endpoints/{eid}/subscriptions- GET list, POST add, DELETE opt out
//
// Every route is kindConsumer: the credential is the consumer user's session,
// which carries FR-110's consumer_id claim. The consumer id is taken from the
// session, never from the path or body, so a consumer user has exactly one
// consumer they can address. The store filters by it on every read and write, so
// guessing another consumer's endpoint id answers 404 (AC-39). The portal is a
// thin client: everything here is also directly callable with the same scoped
// session, and the answer is identical whether it comes from the portal or from
// a script (FR-110, §14's "authorization is server-side")
//
// UI-14's gate lives in the store, not here: POST subscriptions asks
// CreateConsumerSubscription, which refuses a type the producer has not left
// publicly subscribable, and GET consumer/event-types answers with the same set
// the write accepts. A UI that offered a free-text field would be offering a
// control the API does not honour.

// consumerSelfScope is the (producer, environment, consumer) a consumer user may
// act within. The producer and environment come from the session claim and the
// consumer id is the one the session is bound to; nothing in the request can move
// any of the three (FR-110). ok is false when the session is not a consumer,
// which the authorize() kind check already forbids, so it is a defence in depth.
func consumerSelfScope(r *http.Request) (store.Scope, string, bool) {
	sess, ok := sessionFrom(r.Context())
	if !ok || !sess.IsConsumer() {
		return store.Scope{}, "", false
	}
	return store.Scope{ProducerID: sess.ProducerID, Env: sess.Env}, sess.ConsumerID, true
}

// consumerScopeOrNotFound resolves the consumer self-service scope or answers the
// uniform 404 a consumer user never expects to see a 403 for (FR-108).
func (s *Server) consumerScopeOrNotFound(w http.ResponseWriter, r *http.Request) (store.Scope, string) {
	sc, cid, ok := consumerSelfScope(r)
	if !ok {
		s.writeNotFound(w, r)
		return store.Scope{}, ""
	}
	return sc, cid
}

type createConsumerEndpointRequest struct {
	Name              string `json:"name"`
	URL               string `json:"url"`
	NotificationEmail string `json:"notification_email"`
	// FR-405 applies here too: a consumer configures the same headers on the same
	// endpoint type, and the same names are refused (see customHeaderRequest).
	CustomHeaders []customHeaderRequest `json:"custom_headers"`
}

// handleConsumerListEventTypes is GET /v1/consumer/event-types: the event types
// this producer has left open to public subscription (UI-14), so the portal can
// offer a list rather than a free-text field. It is the read side of the gate
// CreateConsumerSubscription enforces, so the two can never disagree about what a
// consumer may subscribe to.
func (s *Server) handleConsumerListEventTypes(w http.ResponseWriter, r *http.Request) {
	sc, cid := s.consumerScopeOrNotFound(w, r)
	if cid == "" {
		return
	}
	types, err := s.deps.Data.ListSubscribableEventTypes(r.Context(), sc)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	out := make([]subscribableEventTypeJSON, 0, len(types))
	for _, et := range types {
		out = append(out, subscribableEventTypeJSON{
			Name: et.Name, PayloadVersion: et.PayloadVersion, PayloadSchema: et.PayloadSchema,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"event_types": out})
}

// subscribableEventTypeJSON is the name UI-14's control offers, plus the contract
// detail UI-9's consumer-side integration guide renders. The schema was originally
// left out because UI-14 only needed a list; UI-9 is what makes it a consumer's
// business, and one read satisfies both rather than a second route that would have
// to be kept in step with this one.
type subscribableEventTypeJSON struct {
	Name           string          `json:"name"`
	PayloadVersion int             `json:"payload_version"`
	PayloadSchema  json.RawMessage `json:"payload_schema"`
}

// handleConsumerListEndpoints is GET /v1/consumer/endpoints: the caller's own
// non-deleted endpoints (UI-13).
func (s *Server) handleConsumerListEndpoints(w http.ResponseWriter, r *http.Request) {
	sc, cid := s.consumerScopeOrNotFound(w, r)
	if cid == "" {
		return
	}
	eps, err := s.deps.Data.ListEndpointsForConsumer(r.Context(), sc, cid)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	out := make([]endpointJSON, 0, len(eps))
	for _, ep := range eps {
		out = append(out, toEndpointJSON(ep))
	}
	writeJSON(w, http.StatusOK, map[string]any{"endpoints": out})
}

// handleConsumerCreateEndpoint is POST /v1/consumer/endpoints. The endpoint is
// registered under the caller's own consumer, never another (the consumer id is
// the session claim, not a body field), and the SSRF check runs before the row
// exists (AC-19). The signing secret and verification challenge are shown once.
func (s *Server) handleConsumerCreateEndpoint(w http.ResponseWriter, r *http.Request) {
	sc, cid := s.consumerScopeOrNotFound(w, r)
	if cid == "" {
		return
	}
	var req createConsumerEndpointRequest
	if !s.decodeJSON(w, r, &req) {
		return
	}
	created, err := s.deps.Data.CreateConsumerEndpoint(r.Context(), sc, cid, store.NewConsumerEndpoint{
		Name: req.Name, URL: req.URL, NotificationEmail: req.NotificationEmail,
		CustomHeaders: customHeadersToStore(req.CustomHeaders),
	})
	if err != nil {
		// A blocked URL is invalid input, not a 500 (FR-402, AC-19): the reason is
		// what the consumer must fix.
		if errors.Is(err, ssrf.ErrBlocked) {
			writeError(w, http.StatusUnprocessableEntity, "invalid", err.Error())
			return
		}
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, endpointCreatedJSON{
		endpointJSON:      toEndpointJSON(created.Endpoint),
		SigningSecret:     created.Secret,
		SigningSecretHint: created.SecretHint,
		VerificationToken: created.VerificationToken,
		VerificationHint:  created.VerificationHint,
	})
}

func (s *Server) handleConsumerGetEndpoint(w http.ResponseWriter, r *http.Request) {
	sc, cid := s.consumerScopeOrNotFound(w, r)
	if cid == "" {
		return
	}
	ep, err := s.deps.Data.GetConsumerEndpoint(r.Context(), sc, cid, r.PathValue("endpointID"))
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toEndpointJSON(ep))
}

// patchConsumerEndpointRequest is the PATCH /v1/consumer/endpoints/{endpointID}
// body: UI-13's own-endpoint edits. A nil field is left unchanged, so the portal's
// pause button sends {"paused": true} and nothing else moves; sending
// {"notification_email": ""} clears the address.
type patchConsumerEndpointRequest struct {
	Name              *string `json:"name"`
	URL               *string `json:"url"`
	NotificationEmail *string `json:"notification_email"`
	Paused            *bool   `json:"paused"`
}

// handleConsumerPatchEndpoint is PATCH /v1/consumer/endpoints/{endpointID}: edit
// the endpoint, set or clear its notification email, or pause/resume it (UI-13,
// FR-406). A foreign or deleted id is a 404, and an edited URL is re-checked
// against FR-402 before the row moves.
func (s *Server) handleConsumerPatchEndpoint(w http.ResponseWriter, r *http.Request) {
	sc, cid := s.consumerScopeOrNotFound(w, r)
	if cid == "" {
		return
	}
	var req patchConsumerEndpointRequest
	if !s.decodeJSON(w, r, &req) {
		return
	}
	ep, err := s.deps.Data.UpdateConsumerEndpoint(r.Context(), sc, cid, r.PathValue("endpointID"),
		store.ConsumerEndpointPatch{
			Name:              req.Name,
			URL:               req.URL,
			NotificationEmail: req.NotificationEmail,
			Paused:            req.Paused,
		})
	if err != nil {
		// FR-402: a URL that resolves into a private range is invalid input, not a
		// 500 - the same answer registration gives.
		if errors.Is(err, ssrf.ErrBlocked) {
			writeError(w, http.StatusUnprocessableEntity, "invalid", err.Error())
			return
		}
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toEndpointJSON(ep))
}

// handleConsumerRotateEndpointSecret is POST
// /v1/consumer/endpoints/{endpointID}/rotate: mint a fresh signing secret for one
// of the caller's own endpoints and show it once (UI-13, FR-404). It is a POST and
// not a PATCH field because it is not idempotent - each call mints a new secret -
// and because the response body carries a secret exactly once, like registration.
// A foreign or deleted id is a 404 (AC-39).
func (s *Server) handleConsumerRotateEndpointSecret(w http.ResponseWriter, r *http.Request) {
	sc, cid := s.consumerScopeOrNotFound(w, r)
	if cid == "" {
		return
	}
	sec, err := s.deps.Data.RotateConsumerEndpointSecret(r.Context(), sc, cid, r.PathValue("endpointID"))
	if err != nil {
		s.storeError(w, r, err)
		return
	}
		writeJSON(w, http.StatusCreated, endpointRotatedJSON{
			SigningSecret:     sec.Secret,
			SigningSecretHint: sec.Hint,
		})
	}

	// handleConsumerReissueEndpointChallenge is POST
	// /v1/consumer/endpoints/{endpointID}/challenge (FR-409, SPEC.md A-12). A member
	// re-issues the ownership challenge for one of the caller's own endpoints. Each
	// call mints a fresh token and hint, and answers 201 with the creation
	// response's own field names (verification_token, verification_hint). The route
	// is therefore not idempotent. A foreign or deleted id is a 404 (AC-39); an
	// already-verified endpoint is a 409 (AC-54).
	func (s *Server) handleConsumerReissueEndpointChallenge(w http.ResponseWriter, r *http.Request) {
		sc, cid := s.consumerScopeOrNotFound(w, r)
		if cid == "" {
			return
		}
		challenge, err := s.deps.Data.ReissueEndpointChallenge(r.Context(), sc, cid, r.PathValue("endpointID"))
		if err != nil {
			s.storeError(w, r, err)
			return
		}
		writeJSON(w, http.StatusCreated, endpointReissuedJSON{
			VerificationToken: challenge.Token,
			VerificationHint:  challenge.Hint,
		})
	}

	// endpointRotatedJSON is the 201 body of a rotation: the new secret and its safe
	// hint, returned exactly once. It is deliberately not an endpointJSON: the row did
	// not change, only the credential did.
	type endpointRotatedJSON struct {
	SigningSecret     string `json:"signing_secret"`
	SigningSecretHint string `json:"signing_secret_hint"`
}

// handleConsumerDeleteEndpoint is DELETE /v1/consumer/endpoints/{endpointID}:
// FR-407's soft delete, consumer-scoped. The row is retained, never hard-removed.
func (s *Server) handleConsumerDeleteEndpoint(w http.ResponseWriter, r *http.Request) {
	sc, cid := s.consumerScopeOrNotFound(w, r)
	if cid == "" {
		return
	}
	if err := s.deps.Data.DeleteConsumerEndpoint(r.Context(), sc, cid, r.PathValue("endpointID")); err != nil {
		s.storeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type verifyConsumerEndpointRequest struct {
	Token string `json:"token"`
}

// handleConsumerVerifyEndpoint answers the ownership challenge (FR-403) for one of
// the caller's endpoints, so it may receive live events. A wrong token is a 422
// and a foreign/unknown id is a 404, told apart exactly as the producer path does
// so the challenge cannot be probed.
func (s *Server) handleConsumerVerifyEndpoint(w http.ResponseWriter, r *http.Request) {
	sc, cid := s.consumerScopeOrNotFound(w, r)
	if cid == "" {
		return
	}
	var req verifyConsumerEndpointRequest
	if !s.decodeJSON(w, r, &req) {
		return
	}
	if err := s.deps.Data.VerifyConsumerEndpoint(r.Context(), sc, cid, r.PathValue("endpointID"), req.Token); err != nil {
		s.storeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleConsumerListSubscriptions(w http.ResponseWriter, r *http.Request) {
	sc, cid := s.consumerScopeOrNotFound(w, r)
	if cid == "" {
		return
	}
	subs, err := s.deps.Data.ListConsumerSubscriptions(r.Context(), sc, cid, r.PathValue("endpointID"))
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	out := make([]subscriptionJSON, 0, len(subs))
	for _, sub := range subs {
		out = append(out, toSubscriptionJSON(sub))
	}
	writeJSON(w, http.StatusOK, map[string]any{"subscriptions": out})
}

// handleConsumerCreateSubscription is POST
// /v1/consumer/endpoints/{endpointID}/subscriptions (UI-14). The endpoint must be
// the caller's own; the event type is the producer's, and UI-14's gate - the type
// must be enabled and publicly subscribable - is enforced inside
// CreateConsumerSubscription, so a script holding the consumer's own token gets
// exactly the refusal the portal shows.
func (s *Server) handleConsumerCreateSubscription(w http.ResponseWriter, r *http.Request) {
	sc, cid := s.consumerScopeOrNotFound(w, r)
	if cid == "" {
		return
	}
	var req createSubscriptionRequest
	if !s.decodeJSON(w, r, &req) {
		return
	}
	sub, err := s.deps.Data.CreateConsumerSubscription(r.Context(), sc, cid, r.PathValue("endpointID"), req.EventType)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, toSubscriptionJSON(sub))
}

// handleConsumerDeleteSubscription is DELETE
// /v1/consumer/endpoints/{endpointID}/subscriptions/{eventType}: opt out (UI-14).
// A pair against an endpoint that is not the caller's is a 404.
func (s *Server) handleConsumerDeleteSubscription(w http.ResponseWriter, r *http.Request) {
	sc, cid := s.consumerScopeOrNotFound(w, r)
	if cid == "" {
		return
	}
	if err := s.deps.Data.DeleteConsumerSubscription(r.Context(), sc, cid, r.PathValue("endpointID"), r.PathValue("eventType")); err != nil {
		s.storeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
