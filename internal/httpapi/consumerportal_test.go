package httpapi

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

// bodyOf runs a request and returns only the response body, for the assertions
// that decode it directly (e.do returns both the status and the body).
func bodyOf(t *testing.T, e *env, method, path, token string, body any) []byte {
	t.Helper()
	_, raw := e.do(method, path, token, body)
	return raw
}

// seedSubscribableEventType registers an event type the consumer can subscribe
// to. Event types are ingest-credential routes (FR-201), so this uses the tenant
// API key, exactly as the producer would. Pigeon does not yet model a per-type
// subscribable flag, so a registered type is subscribable.
func seedSubscribableEventType(t *testing.T, e *env, tn *tenant) string {
	t.Helper()
	name := strings.ReplaceAll(tn.Slug, "-", "") + ".order"
	code, raw := e.do(http.MethodPost, "/v1/event-types", tn.KeyPlain, map[string]any{
		"name":           name,
		"payload_schema": map[string]any{"type": "object"},
	})
	mustStatus(t, code, raw, http.StatusCreated)
	return name
}

// TestConsumerPortalSelfService is AC-38: a consumer user is a self-service
// surface. Every operation below is theirs alone - their own endpoints, their own
// subscriptions - and there is no producer action among them.
func TestConsumerPortalSelfService(t *testing.T) {
	e := setup(t)
	b := e.seedTenant(t, "m2b-self-b")
	tok := b.ConsumerToken

	// Register an endpoint (UI-13): the signing secret and the verification
	// challenge ride back once, and the row is the caller's own consumer (FR-110).
	code, raw := e.do(http.MethodPost, "/v1/consumer/endpoints", tok, map[string]string{
		"name": "my-hook", "url": "https://1.1.1.1/hook",
	})
	mustStatus(t, code, raw, http.StatusCreated)
	created := decode[endpointCreatedJSON](t, raw)
	if created.ConsumerID != b.ConsumerID {
		t.Fatalf("endpoint registered under consumer %q, want %q (FR-110)", created.ConsumerID, b.ConsumerID)
	}
	if created.SigningSecret == "" || created.VerificationToken == "" {
		t.Fatalf("register did not reveal the one-time signing secret and verification challenge")
	}
	if created.Status != "active" || created.Verified {
		t.Fatalf("freshly registered endpoint = %q verified=%v, want active/unverified", created.Status, created.Verified)
	}
	endpointID := created.ID

	// Verify with the challenge: the endpoint may now receive live events (FR-403).
	e.do(http.MethodPost, "/v1/consumer/endpoints/"+endpointID+"/verify", tok, map[string]string{
		"token": created.VerificationToken,
	})
	if got := decode[endpointJSON](t, bodyOf(t, e, http.MethodGet, "/v1/consumer/endpoints/"+endpointID, tok, nil)); !got.Verified || got.Status != "active" {
		t.Fatalf("endpoint after verify = %q verified=%v, want active/verified", got.Status, got.Verified)
	}

	// Pause and resume (UI-13): status is the observable of the toggle.
	if paused := decode[endpointJSON](t, bodyOf(t, e, http.MethodPatch, "/v1/consumer/endpoints/"+endpointID, tok, map[string]bool{"paused": true})); paused.Status != "paused" {
		t.Fatalf("pause did not take: status = %q", paused.Status)
	}
	if resumed := decode[endpointJSON](t, bodyOf(t, e, http.MethodPatch, "/v1/consumer/endpoints/"+endpointID, tok, map[string]bool{"paused": false})); resumed.Status != "active" {
		t.Fatalf("resume did not take: status = %q", resumed.Status)
	}

	// Subscribe, list, opt out (UI-14).
	name := seedSubscribableEventType(t, e, b)
	sub := decode[subscriptionJSON](t, bodyOf(t, e, http.MethodPost, "/v1/consumer/endpoints/"+endpointID+"/subscriptions", tok, map[string]string{
		"event_type": name,
	}))
	if sub.EventType != name || sub.EndpointID != endpointID {
		t.Fatalf("subscription = %+v, want event %q on the caller's endpoint", sub, name)
	}
	list := decode[struct {
		Subscriptions []subscriptionJSON `json:"subscriptions"`
	}](t, bodyOf(t, e, http.MethodGet, "/v1/consumer/endpoints/"+endpointID+"/subscriptions", tok, nil))
	if len(list.Subscriptions) != 1 || list.Subscriptions[0].ID != sub.ID {
		t.Fatalf("subscription list = %+v, want the one just created", list.Subscriptions)
	}
	e.do(http.MethodDelete, "/v1/consumer/endpoints/"+endpointID+"/subscriptions/"+name, tok, nil)
	list = decode[struct {
		Subscriptions []subscriptionJSON `json:"subscriptions"`
	}](t, bodyOf(t, e, http.MethodGet, "/v1/consumer/endpoints/"+endpointID+"/subscriptions", tok, nil))
	if len(list.Subscriptions) != 0 {
		t.Fatalf("subscription not removed: %d remain", len(list.Subscriptions))
	}

	// Soft delete (FR-407): the row is retained but invisible to the API, the same
	// as the producer path - a deleted endpoint drops off the list and a direct get
	// is a 404, while the row survives for history and the delivery FK.
	e.do(http.MethodDelete, "/v1/consumer/endpoints/"+endpointID, tok, nil)
	if code, _ := e.do(http.MethodGet, "/v1/consumer/endpoints/"+endpointID, tok, nil); code != http.StatusNotFound {
		t.Fatalf("get after delete: status = %d, want 404 (deleted endpoints are invisible, as the producer path is)", code)
	}
	ctx := context.Background()
	var retained bool
	if err := e.admin.QueryRow(ctx,
		`select true from endpoints where id = $1 and consumer_id = $2 and status = 'deleted'`,
		endpointID, b.ConsumerID).Scan(&retained); err != nil {
		t.Fatalf("row not retained after soft delete: %v", err)
	}
	if !retained {
		t.Fatalf("soft delete did not retain the row")
	}

	// AC-38's other half: a consumer token has no producer action. The route that
	// would register an endpoint is absent for it (FR-111, AC-38).
	code, raw = e.do(http.MethodPost, "/v1/producer/endpoints", tok, map[string]any{
		"name": "no", "url": "https://1.1.1.1/x", "internal": false,
	})
	mustStatus(t, code, raw, http.StatusNotFound)
}

// TestConsumerPortalForeignResourcesAreNotFound is AC-39: a consumer user calling
// the API directly gets exactly the filtered set, and a guessed foreign id is a
// 404 on every resource - including by guessing ids.
func TestConsumerPortalForeignResourcesAreNotFound(t *testing.T) {
	e := setup(t)
	a := e.seedTenant(t, "m2b-fx-a")
	b := e.seedTenant(t, "m2b-fx-b")
	// Each tenant already has one endpoint under its own consumer (seedTenant), so
	// a.EndpointID is a real, guessable foreign id for b's session.
	name := seedSubscribableEventType(t, e, b)
	tok := b.ConsumerToken

	// Every direct operation on A's endpoint id is a 404, told apart from a missing
	// token so the challenge cannot be probed (FR-108).
	for _, tc := range []struct {
		method string
		path   string
		body   any
	}{{http.MethodGet, "/v1/consumer/endpoints/" + a.EndpointID, nil},
		{http.MethodPatch, "/v1/consumer/endpoints/" + a.EndpointID, map[string]bool{"paused": true}},
		{http.MethodDelete, "/v1/consumer/endpoints/" + a.EndpointID, nil},
		{http.MethodPost, "/v1/consumer/endpoints/" + a.EndpointID + "/verify", map[string]string{"token": a.EndpointVerifyToken}},
		{http.MethodPost, "/v1/consumer/endpoints/" + a.EndpointID + "/subscriptions", map[string]string{"event_type": name}},
		{http.MethodDelete, "/v1/consumer/endpoints/" + a.EndpointID + "/subscriptions/" + name, nil},
	} {
		code, raw := e.do(tc.method, tc.path, tok, tc.body)
		mustStatus(t, code, raw, http.StatusNotFound)
	}
	// Listing subscriptions of a foreign endpoint yields an empty set, never a
	// 404-with-data and never another consumer's pairs.
	code, raw := e.do(http.MethodGet, "/v1/consumer/endpoints/"+a.EndpointID+"/subscriptions", tok, nil)
	mustStatus(t, code, raw, http.StatusOK)
	if subs := decode[struct {
		Subscriptions []subscriptionJSON `json:"subscriptions"`
	}](t, raw); len(subs.Subscriptions) != 0 {
		t.Fatalf("a consumer saw %d subscriptions on a foreign endpoint, want 0", len(subs.Subscriptions))
	}

	// The list is exactly B's own set: A's endpoint and consumer are absent.
	list := decode[struct {
		Endpoints []endpointJSON `json:"endpoints"`
	}](t, bodyOf(t, e, http.MethodGet, "/v1/consumer/endpoints", tok, nil))
	if len(list.Endpoints) != 1 {
		t.Fatalf("B's endpoint list has %d entries, want 1 (only its own)", len(list.Endpoints))
	}
	if list.Endpoints[0].ID == a.EndpointID || list.Endpoints[0].ConsumerID == a.ConsumerID {
		t.Fatalf("B's list leaked A's consumer resource id %q", list.Endpoints[0].ID)
	}
}
