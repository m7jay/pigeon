package httpapi

import (
	"errors"
	"net/http"

	"github.com/m7jay/pigeon/internal/ssrf"
	"github.com/m7jay/pigeon/internal/store"
)

// This file is the consumer portal's own surface (M2b, UI-13, UI-14, FR-110,
// FR-1105):
//
//	consumer/endpoints                    - GET list, POST register (SSRF-checked)
//	consumer/endpoints/{eid}              - GET, PATCH pause/resume, DELETE soft-delete
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
// a script (FR-110, §14's "authorization is server-side").

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
	Name string `json:"name"`
	URL  string `json:"url"`
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
	created, err := s.deps.Data.CreateConsumerEndpoint(r.Context(), sc, cid, req.Name, req.URL)
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

// handleConsumerPatchEndpoint is PATCH /v1/consumer/endpoints/{endpointID}: pause
// or resume (UI-13, FR-406). A foreign or deleted id is a 404.
func (s *Server) handleConsumerPatchEndpoint(w http.ResponseWriter, r *http.Request) {
	sc, cid := s.consumerScopeOrNotFound(w, r)
	if cid == "" {
		return
	}
	var req patchEndpointRequest
	if !s.decodeJSON(w, r, &req) {
		return
	}
	paused := false
	if req.Paused != nil {
		paused = *req.Paused
	}
	ep, err := s.deps.Data.SetConsumerEndpointPaused(r.Context(), sc, cid, r.PathValue("endpointID"), paused)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toEndpointJSON(ep))
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
// the caller's own; the event type is the producer's, and subscribing to one the
// producer did not make publicly subscribable is settled in front of this route
// (a producer-visible gate, not yet modelled on event_types).
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
