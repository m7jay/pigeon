package httpapi

import (
	"errors"
	"net/http"

	"github.com/m7jay/pigeon/internal/ssrf"
	"github.com/m7jay/pigeon/internal/store"
)

// This file is the consumer-facing routing surface (§7.5a, FR-401..FR-408,
// FR-501..FR-512):
//
//	consumers/{id}/rekey                       - move the immutable routing key (FR-506)
//	consumers/{id}/endpoints                   - GET list, POST register (SSRF-checked)
//	consumers/{id}/endpoints/{eid}             - GET, PATCH pause/resume, DELETE soft-delete
//	consumers/{id}/endpoints/{eid}/verify      - POST answer the ownership challenge (FR-403)
//	endpoints/{eid}/subscriptions              - GET list, POST add (FR-501)
//	endpoints/{eid}/subscriptions/{type}       - DELETE opt out
//
// A producer registers endpoints UNDER one of its consumers; it never names a URL
// at a consumer itself (FR-7.5a). The signing secret and the ownership-challenge
// token are shown exactly once, at creation, and never read back.

// --- consumers: re-key ---------------------------------------------------

type rekeyRequest struct {
	Ref string `json:"ref"`
}

// handleRekeyConsumer is POST /v1/producer/consumers/{consumerID}/rekey. The ref
// is immutable everywhere except this one operation (FR-506): it is the routing
// key and the §10 consumer_ref, so moving it is explicit and audited, never a
// field on a generic update. A duplicate ref is a 409; an unknown id a 404.
func (s *Server) handleRekeyConsumer(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	var req rekeyRequest
	if !s.decodeJSON(w, r, &req) {
		return
	}
	c, err := s.deps.Data.RekeyConsumer(r.Context(), sc, r.PathValue("consumerID"), req.Ref)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, consumerJSON{ID: c.ID, Ref: c.Ref, Name: c.Name, Env: c.Env, CreatedAt: ts(c.CreatedAt)})
}

// --- endpoints -----------------------------------------------------------

type createEndpointRequest struct {
	Name     string `json:"name"`
	URL      string `json:"url"`
	Internal bool   `json:"internal"`
}

// endpointJSON is the wire view of an endpoint, shared by list, get and create
// (minus the one-time secrets). The signing secret and challenge token are never
// part of it.
type endpointJSON struct {
	ID         string `json:"id"`
	ConsumerID string `json:"consumer_id"`
	Name       string `json:"name"`
	URL        string `json:"url"`
	// NotificationEmail is UI-13's consumer-configured failure-notification
	// address; null until the consumer user sets one.
	NotificationEmail *string `json:"notification_email"`
	Internal          bool    `json:"internal"`
	Status            string  `json:"status"`
	Verified          bool    `json:"verified"`
	CreatedAt         string  `json:"created_at"`
}

func toEndpointJSON(ep store.Endpoint) endpointJSON {
	return endpointJSON{
		ID: ep.ID, ConsumerID: ep.ConsumerID, Name: ep.Name, URL: ep.URL,
		NotificationEmail: ep.NotificationEmail,
		Internal:          ep.Internal, Status: ep.Status, Verified: ep.Verified, CreatedAt: ts(ep.CreatedAt),
	}
}

// endpointCreatedJSON is the 201 body. The signing secret and the ownership
// challenge ride here exactly once (FR-403, FR-404); a producer must persist them
// before the next response, because they are never returned again.
type endpointCreatedJSON struct {
	endpointJSON
	SigningSecret     string `json:"signing_secret"`
	SigningSecretHint string `json:"signing_secret_hint"`
	VerificationToken string `json:"verification_token"`
	VerificationHint  string `json:"verification_hint"`
}

func (s *Server) handleListEndpoints(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	eps, err := s.deps.Data.ListEndpointsForConsumer(r.Context(), sc, r.PathValue("consumerID"))
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

// handleCreateEndpoint is POST /v1/producer/consumers/{consumerID}/endpoints. It
// rejects an SSRF-blocked URL with a 422 that names the reason (FR-402, AC-19),
// mints the signing secret and challenge token, and returns all of them once.
func (s *Server) handleCreateEndpoint(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	var req createEndpointRequest
	if !s.decodeJSON(w, r, &req) {
		return
	}
	created, err := s.deps.Data.CreateEndpoint(r.Context(), sc, store.NewEndpoint{
		Name: req.Name, URL: req.URL, ConsumerID: r.PathValue("consumerID"), Internal: req.Internal,
	})
	if err != nil {
		// FR-402: a blocked URL is invalid input, not a 500. The SSRF reason (a
		// private address, or http in live) is what the producer must fix.
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

func (s *Server) handleGetEndpoint(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	ep, err := s.deps.Data.GetEndpoint(r.Context(), sc, r.PathValue("endpointID"))
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toEndpointJSON(ep))
}

type verifyEndpointRequest struct {
	Token string `json:"token"`
}

// handleVerifyEndpoint is POST .../endpoints/{endpointID}/verify (FR-403). Answer
// the ownership challenge to stamp verified_at; from then the endpoint may receive
// live events. A wrong token and an unknown endpoint are both refused without
// saying which (a 404 for the id, a 422 for a bad token), so the challenge cannot
// be probed.
func (s *Server) handleVerifyEndpoint(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	var req verifyEndpointRequest
	if !s.decodeJSON(w, r, &req) {
		return
	}
	if err := s.deps.Data.VerifyEndpoint(r.Context(), sc, r.PathValue("endpointID"), req.Token); err != nil {
		s.storeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type patchEndpointRequest struct {
	Paused *bool `json:"paused"`
}

// handlePatchEndpoint is PATCH .../endpoints/{endpointID}: pause or resume
// (FR-406). A paused endpoint collects no new deliveries and holds in-flight ones
// without spending retry budget.
func (s *Server) handlePatchEndpoint(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
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
	ep, err := s.deps.Data.SetEndpointPaused(r.Context(), sc, r.PathValue("endpointID"), paused)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toEndpointJSON(ep))
}

// handleDeleteEndpoint is DELETE .../endpoints/{endpointID}: FR-407's soft delete.
// The row is retained (history, the deliveries FK) and never hard-removed.
func (s *Server) handleDeleteEndpoint(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	if err := s.deps.Data.DeleteEndpoint(r.Context(), sc, r.PathValue("endpointID")); err != nil {
		s.storeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- subscriptions -------------------------------------------------------

type createSubscriptionRequest struct {
	EventType string `json:"event_type"`
}

type subscriptionJSON struct {
	ID         string `json:"id"`
	EndpointID string `json:"endpoint_id"`
	EventType  string `json:"event_type"`
	Status     string `json:"status"`
	CreatedAt  string `json:"created_at"`
}

func toSubscriptionJSON(sub store.Subscription) subscriptionJSON {
	return subscriptionJSON{
		ID: sub.ID, EndpointID: sub.EndpointID, EventType: sub.EventType,
		Status: sub.Status, CreatedAt: ts(sub.CreatedAt),
	}
}

func (s *Server) handleListSubscriptions(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	subs, err := s.deps.Data.ListSubscriptionsForEndpoint(r.Context(), sc, r.PathValue("endpointID"))
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

// handleCreateSubscription is POST /v1/producer/endpoints/{endpointID}/subscriptions.
// The event type comes from the body; the endpoint id from the path. The store's
// FKs reject an unknown endpoint or type (404) and a duplicate pair (409).
func (s *Server) handleCreateSubscription(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	var req createSubscriptionRequest
	if !s.decodeJSON(w, r, &req) {
		return
	}
	sub, err := s.deps.Data.CreateSubscription(r.Context(), sc, store.NewSubscription{
		EndpointID: r.PathValue("endpointID"), EventType: req.EventType,
	})
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, toSubscriptionJSON(sub))
}

// handleDeleteSubscription is DELETE .../subscriptions/{eventType}: opt an endpoint
// out of a type (FR-501). The pair is soft-deleted and kept.
func (s *Server) handleDeleteSubscription(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	if err := s.deps.Data.DeleteSubscription(r.Context(), sc, r.PathValue("endpointID"), r.PathValue("eventType")); err != nil {
		s.storeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
