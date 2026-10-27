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
// API key, exactly as the producer would. A freshly registered type is publicly
// subscribable by default (migration 0008), which is what makes AC-38 - a consumer
// subscribing with no producer action - possible; the producer opts a type out with
// PATCH /v1/event-types/{name} (see TestConsumerPortalSubscribableGate).
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

	// Edit the endpoint (UI-13): rename it, move it, and set the notification
	// email. All three are one PATCH - the same request the portal posts.
	edited := decode[endpointJSON](t, bodyOf(t, e, http.MethodPatch, "/v1/consumer/endpoints/"+endpointID, tok, map[string]any{
		"name": "my-hook-renamed", "url": "https://1.0.0.1/hook2", "notification_email": "consumer-ops@example.test",
	}))
	if edited.Name != "my-hook-renamed" || edited.URL != "https://1.0.0.1/hook2" {
		t.Fatalf("edit did not take: %+v", edited)
	}
	if edited.NotificationEmail == nil || *edited.NotificationEmail != "consumer-ops@example.test" {
		t.Fatalf("notification email = %v, want consumer-ops@example.test (UI-13)", edited.NotificationEmail)
	}
	// The email is validated at that edge, and "" is how it is cleared.
	if cleared := decode[endpointJSON](t, bodyOf(t, e, http.MethodPatch, "/v1/consumer/endpoints/"+endpointID, tok, map[string]any{
		"notification_email": "",
	})); cleared.NotificationEmail != nil {
		t.Fatalf("clearing the notification email left %q", *cleared.NotificationEmail)
	}
	if code, raw := e.do(http.MethodPatch, "/v1/consumer/endpoints/"+endpointID, tok, map[string]any{
		"notification_email": "not-an-address",
	}); code != http.StatusUnprocessableEntity {
		t.Fatalf("malformed notification email: status = %d (%s), want 422", code, raw)
	}
	// Moving an endpoint onto a private address is refused by the same FR-402 check
	// registration runs - and refused before the row moves.
	if code, raw := e.do(http.MethodPatch, "/v1/consumer/endpoints/"+endpointID, tok, map[string]any{
		"url": "https://169.254.169.254/latest/meta-data/",
	}); code != http.StatusUnprocessableEntity {
		t.Fatalf("private-range URL on edit: status = %d (%s), want 422", code, raw)
	}

	// Rotate the signing secret (UI-13). The new secret comes back once, the stored
	// one changes, and the endpoint needs no re-verification to keep delivering.
	rotated := decode[endpointRotatedJSON](t, bodyOf(t, e, http.MethodPost, "/v1/consumer/endpoints/"+endpointID+"/rotate", tok, nil))
	if !strings.HasPrefix(rotated.SigningSecret, "whsec_") || rotated.SigningSecretHint == "" {
		t.Fatalf("rotate did not return a whsec_ secret and hint: %+v", rotated)
	}
	if rotated.SigningSecret == created.SigningSecret {
		t.Fatal("rotate returned the secret that was already in use")
	}
	var stored string
	if err := e.admin.QueryRow(context.Background(),
		`select signing_secret from endpoints where id = $1`, endpointID).Scan(&stored); err != nil {
		t.Fatalf("read the stored signing secret: %v", err)
	}
	if stored != rotated.SigningSecret {
		t.Fatalf("the rotated secret was not persisted (stored %q)", stored)
	}
	if got := decode[endpointJSON](t, bodyOf(t, e, http.MethodGet, "/v1/consumer/endpoints/"+endpointID, tok, nil)); !got.Verified || got.Status != "active" {
		t.Fatalf("rotation disturbed the endpoint: %+v", got)
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
		{http.MethodPost, "/v1/consumer/endpoints/" + a.EndpointID + "/rotate", nil},
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

// TestConsumerPortalSubscribableGate is UI-14's gate from both ends: the producer
// decides whether a type may be subscribed to, and both the read the portal's
// subscribe control uses (GET /v1/consumer/event-types) and the write it posts
// (POST .../subscriptions) answer with that decision, so a control the API would
// refuse can never be offered and a script gets the same answer as the portal.
func TestConsumerPortalSubscribableGate(t *testing.T) {
	e := setup(t)
	b := e.seedTenant(t, "m2b-gate-b")
	tok := b.ConsumerToken
	name := seedSubscribableEventType(t, e, b)

	created := decode[endpointCreatedJSON](t, bodyOf(t, e, http.MethodPost, "/v1/consumer/endpoints", tok, map[string]string{
		"name": "gate-hook", "url": "https://1.1.1.1/hook",
	}))
	endpointID := created.ID

	// offers reports whether the consumer's subscribable list currently names the
	// type: the read side of the gate, exactly what the portal renders as options.
	offers := func() bool {
		list := decode[struct {
			EventTypes []struct {
				Name string `json:"name"`
			} `json:"event_types"`
		}](t, bodyOf(t, e, http.MethodGet, "/v1/consumer/event-types", tok, nil))
		for _, et := range list.EventTypes {
			if et.Name == name {
				return true
			}
		}
		return false
	}
	// subscribe reports the status and body of the write side of the gate.
	subscribe := func() (int, []byte) {
		return e.do(http.MethodPost, "/v1/consumer/endpoints/"+endpointID+"/subscriptions", tok,
			map[string]string{"event_type": name})
	}

	// A freshly registered type is open, so it is offered and accepted. UI-14's gate
	// is not a wall in front of a consumer's own self-service (AC-38).
	if !offers() {
		t.Fatalf("a newly registered event type is not offered to the consumer: %s", name)
	}
	if code, raw := subscribe(); code != http.StatusCreated {
		t.Fatalf("subscribing to an open type: status = %d (%s), want 201", code, raw)
	}
	// Opt straight back out, so the later re-open assertion starts from no pair and
	// cannot pass or fail on a duplicate (409) instead of the gate.
	if code, raw := e.do(http.MethodDelete, "/v1/consumer/endpoints/"+endpointID+"/subscriptions/"+name, tok, nil); code != http.StatusNoContent {
		t.Fatalf("opting out: status = %d (%s), want 204", code, raw)
	}

	// The producer closes the gate. No consumer-side action is needed for the
	// refusal to take: the decision is the producer's, enforced server-side.
	patchEventType(t, e, b, name, map[string]any{"publicly_subscribable": false})
	if offers() {
		t.Fatalf("a closed event type is still offered to the consumer: %s", name)
	}
	code, raw := subscribe()
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("subscribing to a closed type: status = %d (%s), want 422", code, raw)
	}
	if !strings.Contains(string(raw), "publicly subscribable") {
		t.Fatalf("the refusal does not name UI-14's reason: %s", raw)
	}

	// A disabled type is refused for the other reason (FR-305): it can never be
	// published, so a subscription to it would set up a delivery that cannot happen.
	patchEventType(t, e, b, name, map[string]any{"publicly_subscribable": true, "enabled": false})
	if offers() {
		t.Fatalf("a disabled event type is still offered to the consumer: %s", name)
	}
	code, raw = subscribe()
	if code != http.StatusUnprocessableEntity || !strings.Contains(string(raw), "disabled") {
		t.Fatalf("subscribing to a disabled type: status = %d (%s), want 422 naming the type is disabled", code, raw)
	}

	// Re-opening the gate restores self-service without touching the consumer's
	// endpoint: the two sides of UI-14 are one decision, read and enforced once.
	patchEventType(t, e, b, name, map[string]any{"enabled": true})
	if !offers() {
		t.Fatalf("re-opened event type is not offered to the consumer: %s", name)
	}
	if code, raw := subscribe(); code != http.StatusCreated {
		t.Fatalf("subscribing after re-opening the gate: status = %d (%s), want 201", code, raw)
	}
}

// patchEventType applies a producer-side PATCH to one event type with the tenant's
// own ingest credential, asserting it took. It is the producer's half of UI-14.
func patchEventType(t *testing.T, e *env, tn *tenant, name string, body map[string]any) {
	t.Helper()
	code, raw := e.do(http.MethodPatch, "/v1/event-types/"+name, tn.KeyPlain, body)
	mustStatus(t, code, raw, http.StatusOK)
}
