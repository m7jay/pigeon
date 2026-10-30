package producer

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/m7jay/pigeon/internal/portal"
)

// The page handlers. One handler per row of PLAN.md:29's M2a list, each performing
// the documented core API calls that row is made of and rendering what came back.
// There is no portal-side data model to get out of step with the API, which is
// what §14 asks for and what makes each row testable by asserting the calls.

// --- shared reads ---------------------------------------------------------

func (h *Handler) listEventTypes(r *http.Request) ([]eventTypeRow, error) {
	resp, err := h.coreReq(r, http.MethodGet, h.envPath(r, "/v1/producer/event-types"), nil)
	if err != nil {
		return nil, errCoreUnreachable
	}
	if resp.Status != http.StatusOK {
		return nil, fmt.Errorf("%s", coreError(resp))
	}
	var raw []eventTypeJSON
	if err := json.Unmarshal(resp.Body, &raw); err != nil {
		return nil, fmt.Errorf("the API's event-type list could not be read")
	}
	out := make([]eventTypeRow, 0, len(raw))
	for _, et := range raw {
		out = append(out, eventTypeRow{
			Name: et.Name, RoutingMode: et.RoutingMode, EventClass: et.EventClass,
			PayloadVersion: et.PayloadVersion, Enabled: et.Enabled, Schema: prettyJSON(et.PayloadSchema),
		})
	}
	return out, nil
}

func (h *Handler) listConsumers(r *http.Request) ([]consumerRow, error) {
	resp, err := h.coreReq(r, http.MethodGet, h.envPath(r, "/v1/producer/consumers"), nil)
	if err != nil {
		return nil, errCoreUnreachable
	}
	if resp.Status != http.StatusOK {
		return nil, fmt.Errorf("%s", coreError(resp))
	}
	var parsed listConsumersResponse
	if err := json.Unmarshal(resp.Body, &parsed); err != nil {
		return nil, fmt.Errorf("the API's consumer list could not be read")
	}
	out := make([]consumerRow, 0, len(parsed.Consumers))
	for _, c := range parsed.Consumers {
		out = append(out, consumerRow{ID: c.ID, Ref: c.Ref, Name: c.Name, Env: c.Env, CreatedAt: c.CreatedAt})
	}
	return out, nil
}

func (h *Handler) listEndpoints(r *http.Request, consumerID, consumerRef string) ([]endpointRow, error) {
	resp, err := h.coreReq(r, http.MethodGet,
		h.envPath(r, "/v1/producer/consumers/"+consumerID+"/endpoints"), nil)
	if err != nil {
		return nil, errCoreUnreachable
	}
	if resp.Status != http.StatusOK {
		return nil, fmt.Errorf("%s", coreError(resp))
	}
	var parsed listEndpointsResponse
	if err := json.Unmarshal(resp.Body, &parsed); err != nil {
		return nil, fmt.Errorf("the API's endpoint list could not be read")
	}
	out := make([]endpointRow, 0, len(parsed.Endpoints))
	for _, e := range parsed.Endpoints {
		out = append(out, toEndpointRow(e, consumerRef))
	}
	return out, nil
}

func toEndpointRow(e endpointJSON, consumerRef string) endpointRow {
	row := endpointRow{
		ID: e.ID, ConsumerID: e.ConsumerID, ConsumerRef: consumerRef, Name: e.Name, URL: e.URL,
		Status: e.Status, Verified: e.Verified,
	}
	if e.NotificationEmail != nil {
		row.NotificationEmail = *e.NotificationEmail
	}
	if e.Health != nil {
		row.FailureStreak = e.Health.FailureStreak
		row.LastStatus = e.Health.LastStatus
		if e.Health.LastDeliveryAt != nil {
			row.LastDeliveryAt = *e.Health.LastDeliveryAt
		}
	}
	return row
}

// allEndpoints is UI-2's "health across all consumers": the API lists endpoints
// per consumer, so the portal asks per consumer and joins. The consumer count is
// the page size of one list call, so the number of calls is bounded by the
// producer's own customers - and the alternative (an aggregate route) would be a
// new API surface invented for a page rather than for a caller.
func (h *Handler) allEndpoints(r *http.Request, consumers []consumerRow) ([]endpointRow, error) {
	var out []endpointRow
	for i := range consumers {
		eps, err := h.listEndpoints(r, consumers[i].ID, consumers[i].Ref)
		if err != nil {
			return nil, err
		}
		consumers[i].EndpointCnt = len(eps)
		out = append(out, eps...)
	}
	return out, nil
}

func (h *Handler) listDeliveryPage(r *http.Request, f deliveryFilterView) (listDeliveriesResponse, error) {
	resp, err := h.coreReq(r, http.MethodGet,
		"/v1/producer/deliveries"+f.coreQuery(h.env(r), true), nil)
	if err != nil {
		return listDeliveriesResponse{}, errCoreUnreachable
	}
	if resp.Status != http.StatusOK {
		return listDeliveriesResponse{}, fmt.Errorf("%s", coreError(resp))
	}
	var parsed listDeliveriesResponse
	if err := json.Unmarshal(resp.Body, &parsed); err != nil {
		return listDeliveriesResponse{}, fmt.Errorf("the API's delivery list could not be read")
	}
	return parsed, nil
}

func (h *Handler) listConsumerUsers(r *http.Request, consumerID string) ([]consumerUserJSON, error) {
	resp, err := h.coreReq(r, http.MethodGet,
		h.envPath(r, "/v1/producer/consumers/"+consumerID+"/users"), nil)
	if err != nil || resp.Status != http.StatusOK {
		return nil, errCoreUnreachable
	}
	var parsed listConsumerUsersResponse
	_ = json.Unmarshal(resp.Body, &parsed)
	return parsed.Users, nil
}

func (h *Handler) listConsumerEvents(r *http.Request, consumerID string) ([]consumerEventJSON, error) {
	resp, err := h.coreReq(r, http.MethodGet,
		h.envPath(r, "/v1/producer/consumers/"+consumerID+"/events"), nil)
	if err != nil || resp.Status != http.StatusOK {
		return nil, errCoreUnreachable
	}
	var parsed listConsumerEventsResponse
	_ = json.Unmarshal(resp.Body, &parsed)
	return parsed.Events, nil
}

// --- UI-1: the onboarding checklist ---------------------------------------

// onboarding is UI-1: producer -> event type -> endpoint -> verify -> test event ->
// see delivery, with each step's state read from the API. The checklist is
// *derived*: "an endpoint exists" is a consumer's endpoint list being non-empty,
// "a test event was sent" is a delivery carrying the synthetic marker (S9a's
// is_test column), and "a delivery was seen" is a delivered row. Nothing here
// remembers a click, so the checklist cannot disagree with the platform.
func (h *Handler) onboarding(w http.ResponseWriter, r *http.Request, sess memberSession) {
	view := onboardingView{Env: h.env(r)}
	types, err := h.listEventTypes(r)
	if err != nil {
		h.coreFail(w, r, sess, "The checklist could not be read", err)
		return
	}
	consumers, err := h.listConsumers(r)
	if err != nil {
		h.coreFail(w, r, sess, "The checklist could not be read", err)
		return
	}
	endpoints, err := h.allEndpoints(r, consumers)
	if err != nil {
		h.coreFail(w, r, sess, "The checklist could not be read", err)
		return
	}
	deliveries, err := h.listDeliveryPage(r, deliveryFilterView{Limit: "100"})
	if err != nil {
		h.coreFail(w, r, sess, "The checklist could not be read", err)
		return
	}

	verified := false
	for _, ep := range endpoints {
		if ep.Verified {
			verified = true
		}
	}
	testEvent, delivered := false, false
	for _, d := range deliveries.Deliveries {
		if d.IsTest {
			testEvent = true
		}
		if d.Status == "delivered" {
			delivered = true
		}
	}
	// A test event that matched no subscription leaves no delivery, so the
	// consumer-scoped event list is the second place the marker can be seen.
	if !testEvent {
		for i := range consumers {
			events, err := h.listConsumerEvents(r, consumers[i].ID)
			if err != nil {
				continue
			}
			for _, e := range events {
				if e.IsTest {
					testEvent = true
				}
			}
		}
	}

	view.EventTypes = types
	view.Consumers = consumers
	view.Endpoints = endpoints
	view.EventTypesCount, view.EndpointsCount = len(types), len(endpoints)
	if len(types) > 0 {
		view.TestEventBody = testBodyFor(types[0])
	} else {
		view.TestEventBody = "{}"
	}
	view.Steps = []stepView{
		{N: 1, Title: "Producer", Detail: "You are signed in as a member of " + sess.Claims.ProducerID + ".", Done: true},
		{N: 2, Title: "Register an event type", Detail: "The contract you publish against: a name, a JSON Schema and a routing mode.", Done: len(types) > 0},
		{N: 3, Title: "Register a consumer and an endpoint", Detail: "A consumer is your customer; an endpoint is a URL it receives your events at.", Done: len(endpoints) > 0},
		{N: 4, Title: "Verify the endpoint", Detail: "Answer the ownership challenge so the endpoint can receive live events.", Done: verified},
		{N: 5, Title: "Send a test event", Detail: "A real event, marked synthetic, through the same path as production.", Done: testEvent},
		{N: 6, Title: "See the delivery", Detail: "The delivery log with every attempt, for the event you just sent.", Done: delivered},
	}
	h.renderPage(w, r, sess, "onboarding", "Get started", "onboarding", view)
}

// onboardingEventType is UI-1 step 2: POST /v1/producer/event-types.
func (h *Handler) onboardingEventType(w http.ResponseWriter, r *http.Request, sess memberSession) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	body := map[string]any{
		"name":           strings.TrimSpace(r.FormValue("name")),
		"routing_mode":   r.FormValue("routing_mode"),
		"payload_schema": json.RawMessage(orDefault(r.FormValue("payload_schema"), `{"type":"object"}`)),
	}
	if v := r.FormValue("payload_version"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			body["payload_version"] = n
		}
	}
	if !json.Valid(body["payload_schema"].(json.RawMessage)) {
		h.setFlash(w, "error", "payload_schema must be a JSON document")
		http.Redirect(w, r, "/onboarding", http.StatusSeeOther)
		return
	}
	resp, err := h.coreReq(r, http.MethodPost, h.envPath(r, "/v1/producer/event-types"), body)
	if err != nil {
		h.setFlash(w, "error", "The event type was not created: the API could not be reached.")
	} else if resp.Status != http.StatusCreated {
		h.setFlash(w, "error", "The event type was not created: "+coreError(resp))
	} else {
		h.setFlash(w, "success", "Event type "+fmt.Sprint(body["name"])+" registered.")
	}
	http.Redirect(w, r, "/onboarding", http.StatusSeeOther)
}

// onboardingEndpoint is UI-1 step 3, which is two API calls: create the consumer
// (if the form named a new one) and create the endpoint inside it.
func (h *Handler) onboardingEndpoint(w http.ResponseWriter, r *http.Request, sess memberSession) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	consumerID := strings.TrimSpace(r.FormValue("consumer_id"))
	if consumerID == "" {
		ref := strings.TrimSpace(r.FormValue("consumer_ref"))
		name := strings.TrimSpace(r.FormValue("consumer_name"))
		if ref == "" || name == "" {
			h.setFlash(w, "error", "A consumer needs both a ref and a name.")
			http.Redirect(w, r, "/onboarding", http.StatusSeeOther)
			return
		}
		resp, err := h.coreReq(r, http.MethodPost, h.envPath(r, "/v1/producer/consumers"),
			map[string]string{"ref": ref, "name": name})
		if err != nil {
			h.setFlash(w, "error", "The consumer was not created: the API could not be reached.")
			http.Redirect(w, r, "/onboarding", http.StatusSeeOther)
			return
		}
		if resp.Status != http.StatusCreated {
			h.setFlash(w, "error", "The consumer was not created: "+coreError(resp))
			http.Redirect(w, r, "/onboarding", http.StatusSeeOther)
			return
		}
		var c consumerJSON
		_ = json.Unmarshal(resp.Body, &c)
		consumerID = c.ID
	}
	resp, err := h.coreReq(r, http.MethodPost,
		h.envPath(r, "/v1/producer/consumers/"+consumerID+"/endpoints"),
		map[string]any{
			"name":     orDefault(r.FormValue("name"), "Endpoint"),
			"url":      strings.TrimSpace(r.FormValue("url")),
			"internal": r.FormValue("internal") != "",
		})
	if err != nil {
		h.setFlash(w, "error", "The endpoint was not created: the API could not be reached.")
		http.Redirect(w, r, "/onboarding", http.StatusSeeOther)
		return
	}
	if resp.Status != http.StatusCreated {
		h.setFlash(w, "error", "The endpoint was not created: "+coreError(resp))
		http.Redirect(w, r, "/onboarding", http.StatusSeeOther)
		return
	}
	// The challenge token is returned exactly once (FR-403), so the endpoint page
	// is where it is shown - along with the snippet for the new secret.
	h.setFlash(w, "success", "Endpoint created. Verify it, or copy its signing secret into a snippet.")
	http.Redirect(w, r, "/endpoints/"+decodeID(resp.Body), http.StatusSeeOther)
}

// --- UI-2 / UI-10: consumers and their endpoints --------------------------

// consumersList is UI-2's endpoint list across every consumer, plus UI-10's
// management surface.
func (h *Handler) consumersList(w http.ResponseWriter, r *http.Request, sess memberSession) {
	view := consumersView{Env: h.env(r)}
	consumers, err := h.listConsumers(r)
	if err != nil {
		h.coreFail(w, r, sess, "Consumers could not be listed", err)
		return
	}
	endpoints, err := h.allEndpoints(r, consumers)
	if err != nil {
		h.coreFail(w, r, sess, "Endpoints could not be listed", err)
		return
	}
	view.Consumers, view.Endpoints = consumers, endpoints
	h.renderPage(w, r, sess, "consumers", "Consumers and endpoints", "consumers", view)
}

// consumerCreate is UI-10's first control: POST /v1/producer/consumers.
func (h *Handler) consumerCreate(w http.ResponseWriter, r *http.Request, sess memberSession) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	resp, err := h.coreReq(r, http.MethodPost, h.envPath(r, "/v1/producer/consumers"),
		map[string]string{
			"ref":  strings.TrimSpace(r.FormValue("ref")),
			"name": strings.TrimSpace(r.FormValue("name")),
		})
	switch {
	case err != nil:
		h.setFlash(w, "error", "The consumer was not created: the API could not be reached.")
	case resp.Status != http.StatusCreated:
		h.setFlash(w, "error", "The consumer was not created: "+coreError(resp))
	default:
		h.setFlash(w, "success", "Consumer created.")
	}
	http.Redirect(w, r, "/consumers", http.StatusSeeOther)
}

// consumerDetail is UI-10 and UI-11 together: that consumer's users (invite,
// revoke), its endpoints with their failure streaks, and its unroutable events.
func (h *Handler) consumerDetail(w http.ResponseWriter, r *http.Request, sess memberSession) {
	view, ok := h.consumerViewFor(w, r, sess, r.PathValue("id"))
	if !ok {
		return
	}
	h.renderPage(w, r, sess, "consumer", view.Consumer.Name, "consumers", view)
}

// consumerViewFor builds the UI-11 page for one consumer, or renders the 404 page
// and reports false. It is shared by the page handler and by the invite handler,
// which renders the SAME page with the one-time invitation token on it: the token
// is returned exactly once by the API, so it has to be shown in the response that
// created it rather than round-tripped through a cookie.
func (h *Handler) consumerViewFor(w http.ResponseWriter, r *http.Request, sess memberSession, id string) (consumerView, bool) {
	view := consumerView{Env: h.env(r), IsOwner: sess.isOwner()}
	consumers, err := h.listConsumers(r)
	if err != nil {
		h.coreFail(w, r, sess, "That consumer could not be read", err)
		return consumerView{}, false
	}
	found := false
	for _, c := range consumers {
		if c.ID == id {
			view.Consumer, found = c, true
		}
	}
	if !found {
		// A consumer that is not in the caller's own list is indistinguishable from
		// one that does not exist - the same 404 the API gives (FR-108), rendered
		// as a page instead of a JSON body.
		h.renderPageStatus(w, r, http.StatusNotFound, "problem", "No such consumer", "consumers", problemView{
			Title:   "No such consumer",
			Message: "This consumer is not one of yours, or no longer exists.",
		})
		return consumerView{}, false
	}
	if view.Endpoints, err = h.listEndpoints(r, id, view.Consumer.Ref); err != nil {
		h.coreFail(w, r, sess, "That consumer's endpoints could not be read", err)
		return consumerView{}, false
	}
	if view.Users, err = h.listConsumerUsers(r, id); err != nil {
		h.coreFail(w, r, sess, "That consumer's users could not be read", err)
		return consumerView{}, false
	}
	if view.Events, err = h.listConsumerEvents(r, id); err != nil {
		view.Events = nil // an empty panel is not worth failing the page for
	}
	return view, true
}

// consumerInvite is UI-10's "invite consumer users by email":
// POST /v1/producer/consumers/{consumerID}/invites (owner only).
func (h *Handler) consumerInvite(w http.ResponseWriter, r *http.Request, sess memberSession) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	id := r.PathValue("id")
	email := strings.TrimSpace(r.FormValue("email"))
	resp, err := h.coreReq(r, http.MethodPost, h.envPath(r, "/v1/producer/consumers/"+id+"/invites"),
		map[string]string{"email": email})
	if err != nil {
		h.setFlash(w, "error", "The invitation was not created: the API could not be reached.")
		http.Redirect(w, r, "/consumers/"+id, http.StatusSeeOther)
		return
	}
	if resp.Status != http.StatusCreated {
		h.setFlash(w, "error", "The invitation was not created: "+coreError(resp))
		http.Redirect(w, r, "/consumers/"+id, http.StatusSeeOther)
		return
	}
	// The token comes back once. There is no mailer in v1.0, so the portal shows
	// it to the inviter and says what it is: an invitation addressed to an address
	// that ALREADY has an account can only be redeemed by that account's own
	// session (0005, SPEC.md item 7), so the inviter has to know that too.
	//
	// It is rendered on the page rather than carried in the flash cookie: a token
	// is opaque base64url and a cookie value may not contain every byte such a
	// token can hold, so a flash round trip could hand the inviter a mangled token
	// that no one can redeem.
	var inv inviteJSON
	_ = json.Unmarshal(resp.Body, &inv)
	view, ok := h.consumerViewFor(w, r, sess, id)
	if !ok {
		return
	}
	view.NewInviteEmail = email
	if inv.Token != nil {
		view.NewInviteToken = *inv.Token
	}
	h.renderPageStatus(w, r, http.StatusCreated, "consumer", view.Consumer.Name, "consumers", view)
}

// consumerRevokeUser is UI-10's "revoke access":
// DELETE /v1/producer/consumers/{consumerID}/users/{userID} (owner only).
func (h *Handler) consumerRevokeUser(w http.ResponseWriter, r *http.Request, sess memberSession) {
	id, userID := r.PathValue("id"), r.PathValue("userID")
	resp, err := h.coreReq(r, http.MethodDelete, h.envPath(r, "/v1/producer/consumers/"+id+"/users/"+userID), nil)
	switch {
	case err != nil:
		h.setFlash(w, "error", "Access was not revoked: the API could not be reached.")
	case resp.Status != http.StatusNoContent:
		h.setFlash(w, "error", "Access was not revoked: "+coreError(resp))
	default:
		h.setFlash(w, "success", "Access revoked. The consumer's history and endpoints are untouched.")
	}
	http.Redirect(w, r, "/consumers/"+id, http.StatusSeeOther)
}

// --- UI-2 / UI-6: one endpoint --------------------------------------------

// endpointDetail is UI-2's health for one endpoint plus UI-6's rotation control.
func (h *Handler) endpointDetail(w http.ResponseWriter, r *http.Request, sess memberSession) {
	id := r.PathValue("id")
	view, ok := h.endpointView(w, r, sess, id)
	if !ok {
		return
	}
	h.renderPage(w, r, sess, "endpoint", view.Endpoint.Name, "consumers", view)
}

// endpointView finds an endpoint by id the way the portal can: through its
// consumer's list. The API has no producer-wide "get endpoint by id" for a
// member... it does (GET /v1/producer/consumers/{consumerID}/endpoints/{id}), but
// the consumer id is part of the path, and the portal does not store it. So it
// walks the producer's consumers, which is bounded by the producer's own customer
// count and keeps the page a thin client rather than a place that caches ids.
func (h *Handler) endpointView(w http.ResponseWriter, r *http.Request, sess memberSession, endpointID string) (endpointView, bool) {
	consumers, err := h.listConsumers(r)
	if err != nil {
		h.coreFail(w, r, sess, "That endpoint could not be read", err)
		return endpointView{}, false
	}
	for _, c := range consumers {
		eps, err := h.listEndpoints(r, c.ID, c.Ref)
		if err != nil {
			h.coreFail(w, r, sess, "That endpoint could not be read", err)
			return endpointView{}, false
		}
		for _, ep := range eps {
			if ep.ID == endpointID {
				return endpointView{Env: h.env(r), Endpoint: ep, Consumer: c}, true
			}
		}
	}
	h.renderPageStatus(w, r, http.StatusNotFound, "problem", "No such endpoint", "consumers", problemView{
		Title:   "No such endpoint",
		Message: "This endpoint is not one of yours, or no longer exists.",
	})
	return endpointView{}, false
}

// endpointVerify is UI-1 step 4: answer the ownership challenge (FR-403).
func (h *Handler) endpointVerify(w http.ResponseWriter, r *http.Request, sess memberSession) {
	view, ok := h.endpointView(w, r, sess, r.PathValue("id"))
	if !ok {
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	resp, err := h.coreReq(r, http.MethodPost,
		h.envPath(r, "/v1/producer/consumers/"+view.Endpoint.ConsumerID+"/endpoints/"+view.Endpoint.ID+"/verify"),
		map[string]string{"token": strings.TrimSpace(r.FormValue("token"))})
	switch {
	case err != nil:
		view.Error = "The challenge could not be answered: the API could not be reached."
	case resp.Status != http.StatusOK:
		view.Error = coreError(resp)
	default:
		view.Endpoint.Verified = true
	}
	h.renderPage(w, r, sess, "endpoint", view.Endpoint.Name, "consumers", view)
}

// endpointRotate is UI-6: rotate the signing secret and explain the overlap.
//
// The overlap window is S10's semantics (FR-1004) and the API reports it; the
// portal renders the API's own numbers rather than its own idea of how long a
// receiver has. It also carries the new secret straight into UI-7's snippet, so a
// producer rotating a secret sees where to use it in the same breath.
func (h *Handler) endpointRotate(w http.ResponseWriter, r *http.Request, sess memberSession) {
	view, ok := h.endpointView(w, r, sess, r.PathValue("id"))
	if !ok {
		return
	}
	resp, err := h.coreReq(r, http.MethodPost,
		h.envPath(r, "/v1/producer/consumers/"+view.Endpoint.ConsumerID+"/endpoints/"+view.Endpoint.ID+"/rotate"), nil)
	if err != nil {
		view.Error = "The secret was not rotated: the API could not be reached."
		h.renderPage(w, r, sess, "endpoint", view.Endpoint.Name, "consumers", view)
		return
	}
	if resp.Status != http.StatusCreated {
		view.Error = coreError(resp)
		h.renderPage(w, r, sess, "endpoint", view.Endpoint.Name, "consumers", view)
		return
	}
	var rot endpointRotatedJSON
	if err := json.Unmarshal(resp.Body, &rot); err != nil {
		view.Error = "The API's rotation response could not be read."
		h.renderPage(w, r, sess, "endpoint", view.Endpoint.Name, "consumers", view)
		return
	}
	view.Endpoint.RotatedSecret = rot.SigningSecret
	view.Endpoint.RotatedHint = rot.SigningSecretHint
	view.Endpoint.RotatedOverlap = humanSeconds(rot.OverlapSeconds)
	if rot.PreviousSecretExpiresAt != nil {
		view.Endpoint.RotatedUntil = *rot.PreviousSecretExpiresAt
	}
	h.renderPage(w, r, sess, "endpoint", view.Endpoint.Name, "consumers", view)
}

// endpointReissue is UI-11's membership re-issue: it asks the API for a fresh
// ownership-challenge token and hint for one of its endpoints, and renders the
// endpoint page from that response so the token is shown exactly once, with a
// copy warning and a Verify now button.
func (h *Handler) endpointReissue(w http.ResponseWriter, r *http.Request, sess memberSession) {
	view, ok := h.endpointView(w, r, sess, r.PathValue("id"))
	if !ok {
		return
	}
	resp, err := h.coreReq(r, http.MethodPost,
		h.envPath(r, "/v1/producer/consumers/"+view.Endpoint.ConsumerID+"/endpoints/"+view.Endpoint.ID+"/challenge"),
		nil)
	if err != nil {
		view.Error = "The token could not be re-issued: the API could not be reached."
		h.renderPage(w, r, sess, "endpoint", view.Endpoint.Name, "consumers", view)
		return
	}
	if resp.Status != http.StatusCreated {
		view.Error = coreError(resp)
		h.renderPage(w, r, sess, "endpoint", view.Endpoint.Name, "consumers", view)
		return
	}
	var chal endpointReissuedJSON
	if err := json.Unmarshal(resp.Body, &chal); err != nil {
		view.Error = "The re-issued token could not be read."
		h.renderPage(w, r, sess, "endpoint", view.Endpoint.Name, "consumers", view)
		return
	}
	view.Endpoint.ReissuedToken = chal.VerificationToken
	view.Endpoint.ReissuedHint = chal.VerificationHint
	h.renderPage(w, r, sess, "endpoint", view.Endpoint.Name, "consumers", view)
}

// endpointSetPaused is UI-2's pause/resume control (FR-408 and S5's pause).
func (h *Handler) endpointSetPaused(paused bool) memberHandler {
	return func(w http.ResponseWriter, r *http.Request, sess memberSession) {
		view, ok := h.endpointView(w, r, sess, r.PathValue("id"))
		if !ok {
			return
		}
		resp, err := h.coreReq(r, http.MethodPatch,
			h.envPath(r, "/v1/producer/consumers/"+view.Endpoint.ConsumerID+"/endpoints/"+view.Endpoint.ID),
			map[string]bool{"paused": paused})
		switch {
		case err != nil:
			h.setFlash(w, "error", "The endpoint was not updated: the API could not be reached.")
		case resp.Status != http.StatusOK:
			h.setFlash(w, "error", "The endpoint was not updated: "+coreError(resp))
		default:
			h.setFlash(w, "success", "Endpoint updated.")
		}
		http.Redirect(w, r, "/endpoints/"+view.Endpoint.ID, http.StatusSeeOther)
	}
}

// --- UI-3 / UI-5: the delivery log and replay ------------------------------

// deliveryLog is UI-3: every delivery with every attempt, filterable by consumer.
func (h *Handler) deliveryLog(w http.ResponseWriter, r *http.Request, sess memberSession) {
	f := readDeliveryFilters(r.URL.Query().Get)
	view := deliveriesView{Env: h.env(r), Filter: f, Statuses: deliveryStatuses}
	page, err := h.listDeliveryPage(r, f)
	if err != nil {
		h.coreFail(w, r, sess, "The delivery log could not be read", err)
		return
	}
	for _, d := range page.Deliveries {
		view.Deliveries = append(view.Deliveries, toDeliveryRow(d))
	}
	view.NextCursor = page.NextCursor
	// The filter controls are fed from the API too: a consumer the API does not
	// list is not offered, so the control cannot select nothing.
	view.Consumers, _ = h.listConsumers(r)
	view.Endpoints, _ = h.allEndpoints(r, view.Consumers)
	view.Replay = h.replayPanel(r, f, replayView{})
	h.renderPage(w, r, sess, "deliveries", "Deliveries", "deliveries", view)
}

// deliveryDetail is UI-3's detail: FR-1102's ordered attempts and FR-1110's
// never-empty trail, rendered from what the API sent.
func (h *Handler) deliveryDetail(w http.ResponseWriter, r *http.Request, sess memberSession) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	resp, err := h.coreReq(r, http.MethodGet, h.envPath(r, "/v1/producer/deliveries/"+strconv.FormatInt(id, 10)), nil)
	if err != nil {
		h.coreFail(w, r, sess, "That delivery could not be read", errCoreUnreachable)
		return
	}
	if resp.Status == http.StatusNotFound {
		h.renderPageStatus(w, r, http.StatusNotFound, "problem", "No such delivery", "deliveries", problemView{
			Title:   "No such delivery",
			Message: "This delivery is not one of yours, or no longer exists.",
		})
		return
	}
	if resp.Status != http.StatusOK {
		h.coreFail(w, r, sess, "That delivery could not be read", fmt.Errorf("%s", coreError(resp)))
		return
	}
	var d deliveryJSON
	if err := json.Unmarshal(resp.Body, &d); err != nil {
		h.coreFail(w, r, sess, "That delivery could not be read", fmt.Errorf("the API's delivery could not be parsed"))
		return
	}
	h.renderPage(w, r, sess, "delivery", fmt.Sprintf("Delivery %d", d.ID), "deliveries",
		deliveryView{Env: h.env(r), Delivery: toDeliveryRow(d)})
}

// replayConfirm is UI-5's first step: the API's own dry run, which is where the
// count and the rate the confirmation states come from.
func (h *Handler) replayConfirm(w http.ResponseWriter, r *http.Request, sess memberSession) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	f := readDeliveryFilters(r.FormValue)
	panel := h.replayPanel(r, f, replayView{})
	resp, err := h.coreReqHeaders(r, http.MethodPost, "/v1/producer/deliveries/replay"+h.envQuery(r),
		replayBody(f, true), replayKeyHeader(panel.Key))
	switch {
	case err != nil:
		panel.Error = "The preview could not be run: the API could not be reached."
	case resp.Status != http.StatusOK:
		panel.Error = coreError(resp)
	default:
		var res replayResponse
		_ = json.Unmarshal(resp.Body, &res)
		panel.Confirmed, panel.Matched, panel.Rate = true, res.Matched, res.ReleasePerSecond
	}
	view := deliveriesView{Env: h.env(r), Filter: f, Statuses: deliveryStatuses, Replay: panel}
	page, _ := h.listDeliveryPage(r, f)
	for _, d := range page.Deliveries {
		view.Deliveries = append(view.Deliveries, toDeliveryRow(d))
	}
	view.NextCursor = page.NextCursor
	view.Consumers, _ = h.listConsumers(r)
	view.Endpoints, _ = h.allEndpoints(r, view.Consumers)
	h.renderPage(w, r, sess, "deliveries", "Deliveries", "deliveries", view)
}

// replay is UI-5's second step: the same filter set, the same request key, and an
// explicit commit. It is a second POST rather than a parameter on the first, so
// "replay" is never a side effect of looking.
func (h *Handler) replay(w http.ResponseWriter, r *http.Request, sess memberSession) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	f := readDeliveryFilters(r.FormValue)
	key := r.FormValue("idempotency_key")
	resp, err := h.coreReqHeaders(r, http.MethodPost, "/v1/producer/deliveries/replay"+h.envQuery(r),
		replayBody(f, false), replayKeyHeader(key))
	switch {
	case err != nil:
		h.setFlash(w, "error", "The replay was not started: the API could not be reached.")
	case resp.Status != http.StatusOK:
		h.setFlash(w, "error", "The replay was refused: "+coreError(resp))
	default:
		var res replayResponse
		_ = json.Unmarshal(resp.Body, &res)
		h.setFlash(w, "success", replayOutcome(res))
	}
	http.Redirect(w, r, "/deliveries"+f.pageQuery(false), http.StatusSeeOther)
}

// --- UI-1's last step, UI-4's landing page, UI-11's rows -------------------

// eventView is GET /v1/producer/events/{eventID}: the event, its fan-out outcome
// and its rollup. It is where UI-1's "see the delivery" lands and where a test
// event's synthetic marker is shown.
func (h *Handler) eventView(w http.ResponseWriter, r *http.Request, sess memberSession) {
	resp, err := h.coreReq(r, http.MethodGet,
		h.envPath(r, "/v1/producer/events/"+r.PathValue("id")), nil)
	if err != nil {
		h.coreFail(w, r, sess, "That event could not be read", errCoreUnreachable)
		return
	}
	if resp.Status == http.StatusNotFound {
		h.renderPageStatus(w, r, http.StatusNotFound, "problem", "No such event", "", problemView{
			Title:   "No such event",
			Message: "This event is not one of yours, or no longer exists.",
		})
		return
	}
	if resp.Status != http.StatusOK {
		h.coreFail(w, r, sess, "That event could not be read", fmt.Errorf("%s", coreError(resp)))
		return
	}
	var parsed eventViewResponse
	if err := json.Unmarshal(resp.Body, &parsed); err != nil {
		h.coreFail(w, r, sess, "That event could not be read", fmt.Errorf("the API's event could not be parsed"))
		return
	}
	row := eventRowView{
		ID: parsed.Event.ID, EventType: parsed.Event.EventType, RoutingMode: parsed.Event.RoutingMode,
		OccurredAt: parsed.Event.OccurredAt, CreatedAt: parsed.Event.CreatedAt,
		IsTest: parsed.Event.IsTest, Rollup: parsed.Rollup,
	}
	if parsed.Event.ConsumerRef != nil {
		row.ConsumerRef = *parsed.Event.ConsumerRef
	}
	if parsed.Event.RoutingState != nil {
		row.RoutingState = *parsed.Event.RoutingState
	}
	if parsed.Event.NonDeliveryReason != nil {
		row.NonDeliveryReason = *parsed.Event.NonDeliveryReason
	}
	for _, d := range parsed.Deliveries {
		row.Deliveries = append(row.Deliveries, toDeliveryRow(d))
	}
	h.renderPage(w, r, sess, "event", "Event "+row.ID, "", eventView{Env: h.env(r), Event: row})
}

// testEventForm is UI-4: the "Send test event" control, clearly marked synthetic.
func (h *Handler) testEventForm(w http.ResponseWriter, r *http.Request, sess memberSession) {
	view := testEventView{Env: h.env(r), Body: "{}"}
	types, err := h.listEventTypes(r)
	if err != nil {
		h.coreFail(w, r, sess, "The test-event form could not be built", err)
		return
	}
	view.EventTypes = types
	view.Consumers, _ = h.listConsumers(r)
	if q := r.URL.Query().Get("event_type"); q != "" {
		view.EventType = q
	} else if len(types) > 0 {
		view.EventType = types[0].Name
	}
	for _, t := range types {
		if t.Name == view.EventType {
			view.Body = testBodyFor(t)
		}
	}
	h.renderPage(w, r, sess, "testevent", "Send a test event", "onboarding", view)
}

// testEventSubmit is UI-4's action: POST /v1/producer/events with test: true.
//
// The marker is a field of the publish (S9a's `test`), so the event is ordinary -
// validated, fanned out, delivered, retried - and the fact that it was a test
// lives on the row, where the delivery log and the event page can both show it.
func (h *Handler) testEventSubmit(w http.ResponseWriter, r *http.Request, sess memberSession) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	view := testEventView{
		Env:         h.env(r),
		EventType:   strings.TrimSpace(r.FormValue("event_type")),
		ConsumerRef: strings.TrimSpace(r.FormValue("consumer_ref")),
		Body:        strings.TrimSpace(r.FormValue("payload")),
	}
	if view.Body == "" {
		view.Body = "{}"
	}
	if !json.Valid([]byte(view.Body)) {
		view.Error = "The payload must be a JSON document."
		view.EventTypes, _ = h.listEventTypes(r)
		view.Consumers, _ = h.listConsumers(r)
		h.renderPageStatus(w, r, http.StatusBadRequest, "testevent", "Send a test event", "onboarding", view)
		return
	}
	body := map[string]any{
		"event_type": view.EventType,
		"payload":    json.RawMessage(view.Body),
		"test":       true,
	}
	if view.ConsumerRef != "" {
		body["consumer_ref"] = view.ConsumerRef
	}
	resp, err := h.coreReq(r, http.MethodPost, h.envPath(r, "/v1/producer/events"), body)
	if err != nil {
		view.Error = "The test event was not accepted: the API could not be reached."
		view.EventTypes, _ = h.listEventTypes(r)
		view.Consumers, _ = h.listConsumers(r)
		h.renderPageStatus(w, r, http.StatusBadGateway, "testevent", "Send a test event", "onboarding", view)
		return
	}
	if resp.Status != http.StatusAccepted {
		view.Error = coreError(resp)
		view.EventTypes, _ = h.listEventTypes(r)
		view.Consumers, _ = h.listConsumers(r)
		h.renderPageStatus(w, r, resp.Status, "testevent", "Send a test event", "onboarding", view)
		return
	}
	var pub publishResponseJSON
	_ = json.Unmarshal(resp.Body, &pub)
	h.setFlash(w, "success", "Test event accepted (synthetic). It is a real event: watch it land in the delivery log below.")
	http.Redirect(w, r, "/events/"+pub.ID, http.StatusSeeOther)
}

// --- UI-7: snippets -------------------------------------------------------

// snippetPage is UI-7: the four copy-paste verification snippets, rendered from a
// secret the user holds. The secret is POSTed (never a query parameter) and is
// never sent to the core API: the snippet's rules live in internal/portal, shared
// with the consumer portal's UI-18 page so the two cannot drift.
func (h *Handler) snippetPage(w http.ResponseWriter, r *http.Request, sess memberSession) {
	view := snippetView{Note: portal.SnippetTemplateNote}
	if r.Method == http.MethodPost {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		view.EndpointID = strings.TrimSpace(r.FormValue("endpoint_id"))
		view.Whsec = strings.TrimSpace(r.FormValue("whsec"))
	} else {
		view.EndpointID = r.URL.Query().Get("endpoint")
	}
	if view.Whsec != "" {
		for _, s := range portal.Snippets(view.Whsec) {
			view.Snippet = append(view.Snippet, snippetRow{
				Language: string(s.Language), Label: s.Label, Source: s.Source, Note: s.Note,
			})
		}
	}
	h.renderPage(w, r, sess, "snippet", "Verification snippets", "snippet", view)
}

// --- UI-9: the integration guide -----------------------------------------

// guide is UI-9: the integration guide rendered from the LIVE event-type schemas.
//
// It renders from GET /v1/producer/event-types, which is the same table the
// consumer portal reads through GET /v1/consumer/event-types: "publishable to the
// consumer portal" is that - one source, two renders - rather than a copy of the
// guide stored somewhere. A producer who bumps a schema changes both.
func (h *Handler) guide(w http.ResponseWriter, r *http.Request, sess memberSession) {
	view := guideView{Env: h.env(r), BaseURL: h.cfg.CoreURL}
	types, err := h.listEventTypes(r)
	if err != nil {
		h.coreFail(w, r, sess, "The integration guide could not be built", err)
		return
	}
	view.EventTypes = types
	h.renderPage(w, r, sess, "guide", "Integration guide", "guide", view)
}

// --- UI-8: disk and retention pressure -----------------------------------

// usage is UI-8: SB-6's pressure panel, read from GET /v1/producer/usage (S9a's
// member half of the usage surface).
func (h *Handler) usage(w http.ResponseWriter, r *http.Request, sess memberSession) {
	view := usageView{Env: h.env(r)}
	// The plane travels in the query string, like every other member call this
	// portal makes: memberScope (internal/httpapi/auth.go) reads it from `?env=`
	// and answers 400 env_required without it, so a call that omits it renders
	// the API's refusal instead of the reading.
	resp, err := h.coreReq(r, http.MethodGet, h.envPath(r, "/v1/producer/usage"), nil)
	switch {
	case err != nil:
		view.Error = "The pressure reading could not be fetched: the API could not be reached."
	case resp.Status == http.StatusServiceUnavailable:
		// SB-6: "we cannot tell you how full the disk is" is its own answer, and
		// the panel says it rather than drawing an empty gauge at 0%.
		view.Error = coreError(resp)
	case resp.Status != http.StatusOK:
		view.Error = coreError(resp)
	default:
		if err := json.Unmarshal(resp.Body, &view.Usage); err != nil {
			view.Error = "The API's pressure reading could not be read."
		} else {
			view.Available = true
		}
	}
	h.renderPage(w, r, sess, "usage", "Disk and retention", "usage", view)
}

// --- account --------------------------------------------------------------

// account is the account page: FR-101's forced password change, and FR-111's way
// back in when the password is not merely stale but unknown.
func (h *Handler) account(w http.ResponseWriter, r *http.Request, sess memberSession) {
	h.renderPage(w, r, sess, "account", "Account", "account", accountView{
		Email: sess.User.Email, Role: sess.Claims.Role, ProducerID: sess.Claims.ProducerID,
		MustChangePassword: sess.User.MustChangePassword,
	})
}

// accountPassword is FR-101's change, performed by the core:
// POST /v1/session/password. The portal passes the old password through and never
// stores either.
func (h *Handler) accountPassword(w http.ResponseWriter, r *http.Request, sess memberSession) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	view := accountView{
		Email: sess.User.Email, Role: sess.Claims.Role, ProducerID: sess.Claims.ProducerID,
	}
	resp, err := h.coreReq(r, http.MethodPost, "/v1/session/password", map[string]string{
		"old_password": r.FormValue("old_password"),
		"new_password": r.FormValue("new_password"),
	})
	switch {
	case err != nil:
		view.Error = "The password was not changed: the API could not be reached."
	case resp.Status != http.StatusNoContent && resp.Status != http.StatusOK:
		view.Error = coreError(resp)
	default:
		view.Changed = true
	}
	h.renderPageStatus(w, r, http.StatusOK, "account", "Account", "account", view)
}

// --- helpers --------------------------------------------------------------

// coreFail renders the problem page for a core failure. A page that cannot read
// its own data must say so; a blank dashboard would look like a calm zero.
func (h *Handler) coreFail(w http.ResponseWriter, r *http.Request, sess memberSession, title string, err error) {
	h.renderPageStatus(w, r, http.StatusBadGateway, "problem", title, "", problemView{
		Title:   title,
		Message: err.Error(),
		Detail:  "The core API is the source of every panel here: when it cannot be reached, this portal has nothing true to show.",
	})
}

func (h *Handler) envQuery(r *http.Request) string {
	return "?env=" + h.env(r)
}

// replayPanel completes a replay view from the filter set it acts on, minting a
// request key per confirmation: the key is a request identifier the portal owns,
// never a value the user types and never a credential.
func (h *Handler) replayPanel(r *http.Request, f deliveryFilterView, v replayView) replayView {
	if v.Key == "" {
		v.Key = newRequestKey()
	}
	v.Selection = f.selectionWords()
	v.Hidden = hiddenFrom(f, map[string]string{"idempotency_key": v.Key})
	return v
}

// replayBody is the replay request: the API's own fields, with the dry-run flag as
// the only difference between the two steps.
func replayBody(f deliveryFilterView, dryRun bool) map[string]any {
	body := map[string]any{"dry_run": dryRun}
	if f.ConsumerID != "" {
		body["consumer_id"] = f.ConsumerID
	}
	if f.EndpointID != "" {
		body["endpoint_id"] = f.EndpointID
	}
	if f.EventType != "" {
		body["event_type"] = f.EventType
	}
	if f.Status != "" {
		body["status"] = f.Status
	}
	if f.From != "" {
		body["from"] = f.From
	}
	if f.To != "" {
		body["to"] = f.To
	}
	return body
}

func replayKeyHeader(key string) map[string]string {
	if key == "" {
		return nil
	}
	return map[string]string{"Idempotency-Key": key}
}

// replayOutcome says what a committed replay did, in the API's numbers.
func replayOutcome(res replayResponse) string {
	msg := fmt.Sprintf("Replay started: %d delivered, releasing at %d/s.", res.Created, res.ReleasePerSecond)
	if res.Repeated {
		msg += " This request key had already been used, so the existing deliveries were returned rather than replayed again."
	}
	return msg
}

// toDeliveryRow is the one place the core's delivery JSON becomes a rendered row,
// so the log, the detail page and an event's delivery list all agree.
func toDeliveryRow(d deliveryJSON) deliveryRow {
	row := deliveryRow{
		ID: d.ID, EventID: d.EventID, EventType: d.EventType, EndpointID: d.EndpointID,
		ConsumerID: d.ConsumerID, Status: d.Status, Attempts: d.Attempts,
		CreatedAt: d.CreatedAt, IsTest: d.IsTest,
	}
	if d.DurationMs != nil {
		row.DurationMs = fmt.Sprintf("%d ms", *d.DurationMs)
	}
	if d.NonDeliveryReason != nil {
		row.NonDeliveryReason = *d.NonDeliveryReason
	}
	if d.HeldBackReason != nil {
		row.HeldBackReason = *d.HeldBackReason
	}
	if d.ReplayOfDeliveryID != nil {
		row.ReplayOfDeliveryID = *d.ReplayOfDeliveryID
	}
	for _, a := range d.AttemptsRows {
		t := trailRow{
			Attempt: a.Attempt, OccurredAt: a.OccurredAt, DurationMs: fmt.Sprintf("%d ms", a.DurationMs),
			ErrorClass: a.ErrorClass, Outcome: a.Outcome,
		}
		if a.ResponseStatus != nil {
			t.ResponseStatus = strconv.Itoa(*a.ResponseStatus)
		}
		if a.ResponseBody != nil {
			t.ResponseBody = *a.ResponseBody
		}
		if a.NextRetryAt != nil {
			t.NextRetryAt = *a.NextRetryAt
		}
		row.Trail = append(row.Trail, t)
	}
	if e := d.EffectiveAttempt; e != nil {
		t := trailRow{
			Attempt: e.Attempt, OccurredAt: e.OccurredAt, ErrorClass: "—",
			Outcome: e.Outcome, Synthesized: true,
		}
		if e.DurationMs != nil {
			t.DurationMs = fmt.Sprintf("%d ms", *e.DurationMs)
		}
		// FR-1110: a successful first attempt has no attempt row, and its status is
		// the delivery's own. The row is rendered because the API sent it, not
		// because the portal decided a trail looked empty.
		t.ResponseStatus = e.Status
		row.Trail = append(row.Trail, t)
	}
	row.RollupNote = rollupNote(row)
	return row
}

// rollupNote is the one-line reading of a delivery's outcome, derived from the
// status the API reports.
func rollupNote(row deliveryRow) string {
	switch row.Status {
	case "delivered":
		return "Delivered."
	case "pending":
		return "Not delivered yet: in flight, held back, or awaiting a retry."
	case "failed_terminal":
		return "Failed: the retry window or count is exhausted. It stays replayable."
	case "skipped":
		return "Skipped: the endpoint matched but could not take the event."
	default:
		return ""
	}
}

// testBodyFor is the payload a test-event form starts from: the type's own first
// example when it has one. A producer should not have to guess their contract.
func testBodyFor(t eventTypeRow) string {
	return orDefault(strings.TrimSpace(t.Schema), "{}")
}

func orDefault(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}

// prettyJSON re-indents a JSON document for display, and returns it unchanged (as
// a string) when it is not JSON.
func prettyJSON(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var buf any
	if err := json.Unmarshal(raw, &buf); err != nil {
		return string(raw)
	}
	out, err := json.MarshalIndent(buf, "", "  ")
	if err != nil {
		return string(raw)
	}
	return string(out)
}

// decodeID reads an `id` out of a core response body, for the redirect after a
// create. A body without one leaves the caller at the list page.
func decodeID(body []byte) string {
	var v struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &v); err != nil {
		return ""
	}
	return v.ID
}

// humanSeconds renders FR-1004's overlap window the way the page explains it.
func humanSeconds(n int) string {
	if n <= 0 {
		return ""
	}
	d := time.Duration(n) * time.Second
	if d%time.Hour == 0 {
		return fmt.Sprintf("%d h", int(d.Hours()))
	}
	return d.String()
}
