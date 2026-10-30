package httpapi

import (
	"net/http"
	"testing"
)

// TestEndpointCreateVerifySubscribeRoundTrip is the producer's happy path for the
// routing surface: register an endpoint (getting the signing secret and challenge
// once), answer the challenge so it is deliverable, subscribe it to a type, and
// read it back verified. It proves the handlers wire the store's one-time returns
// into the response, which the cross-producer 404 suite cannot.
func TestEndpointCreateVerifySubscribeRoundTrip(t *testing.T) {
	e := setup(t)
	a := e.seedTenant(t, "routing-alpha")
	// A live event type to subscribe to (FR-501). seedTenant does not create one.
	seedCrossProducerEventType(t, e, a)

	// Create an endpoint under the seeded consumer. The response carries the
	// one-time signing secret and verification token.
	code, raw := e.do(http.MethodPost,
		"/v1/producer/consumers/"+a.ConsumerID+"/endpoints?env=live", a.OwnerToken,
		map[string]string{"name": "primary", "url": "https://8.8.8.8/hook"})
	created := decode[endpointCreatedJSON](t, raw)
	if code != http.StatusCreated {
		t.Fatalf("create endpoint: %d, want 201 (%s)", code, raw)
	}
	if created.SigningSecret == "" || created.VerificationToken == "" {
		t.Fatalf("create endpoint did not return the one-time secret and token: %s", raw)
	}
	if created.Verified {
		t.Fatalf("new endpoint is verified before the challenge is answered: %s", raw)
	}

	// Answer the ownership challenge (FR-403).
	code, raw = e.do(http.MethodPost,
		"/v1/producer/consumers/"+a.ConsumerID+"/endpoints/"+created.ID+"/verify?env=live",
		a.OwnerToken, map[string]string{"token": created.VerificationToken})
	if code != http.StatusNoContent {
		t.Fatalf("verify: %d, want 204 (%s)", code, raw)
	}

	// It now reads back verified.
	code, raw = e.do(http.MethodGet,
		"/v1/producer/consumers/"+a.ConsumerID+"/endpoints/"+created.ID+"?env=live", a.OwnerToken, nil)
	got := decode[endpointJSON](t, raw)
	if code != http.StatusOK || !got.Verified {
		t.Fatalf("get endpoint after verify: %d verified=%v (%s)", code, got.Verified, raw)
	}

	// Subscribe it to the tenant's event type (FR-501).
	code, raw = e.do(http.MethodPost,
		"/v1/producer/endpoints/"+created.ID+"/subscriptions?env=live", a.OwnerToken,
		map[string]string{"event_type": a.EventTypeName})
	if code != http.StatusCreated {
		t.Fatalf("create subscription: %d, want 201 (%s)", code, raw)
	}
	code, raw = e.do(http.MethodGet,
		"/v1/producer/endpoints/"+created.ID+"/subscriptions?env=live", a.OwnerToken, nil)
	listed := decode[struct {
		Subscriptions []subscriptionJSON `json:"subscriptions"`
	}](t, raw)
	found := false
	for _, s := range listed.Subscriptions {
		if s.EventType == a.EventTypeName {
			found = true
		}
	}
	if !found {
		t.Fatalf("subscription list does not contain %q: %s", a.EventTypeName, raw)
	}
}
