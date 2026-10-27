package delivery

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/m7jay/pigeon/internal/store"
)

// validSig independently verifies the worker's signature the way a consumer
// would (FR-1001/FR-1002): recompute base64(HMAC-SHA256(secret,
// "<webhook-id>.<webhook-timestamp>.<body>")) and compare. It does not call the
// signer, so the worker and this verifier agreeing IS the property under test.
func validSig(secret []byte, sr signedRequest) bool {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(sr.webhookID + "." + sr.webhookTime + "."))
	_, _ = mac.Write(sr.body)
	want := "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(sr.webhookSig), []byte(want))
}

func sampleDelivery() store.LeasedDelivery {
	return store.LeasedDelivery{
		DeliveryID:     42,
		EventID:        "evt_01HZXWQK3M5N6P7Q8R9S0T1U2V",
		ProducerID:     "prd_01",
		Env:            "live",
		EndpointID:     "ep_01",
		EndpointURL:    "https://consumer.example/webhooks",
		SigningSecret:  "whsec_MlcXwqGz4Rk1S8HvZqF2bN0cV7dW9aY=",
		EventType:      "order.created",
		PayloadVersion: 3,
		ConsumerRef:    "ref-99",
		Payload:        []byte(`{"id":"o1","total":100}`),
	}
}

// TestBuildSignedRequestMatchesStandardWebhooks: the worker signs with the
// endpoint's own secret, sets webhook-id to the event id, prefixes "v1,", and
// carries the raw body verbatim so the consumer can recompute the signature.
func TestBuildSignedRequestMatchesStandardWebhooks(t *testing.T) {
	secret := []byte(sampleDelivery().SigningSecret)
	sr := buildSignedRequest(sampleDelivery(), 2, "pigeon/test")

	if sr.url != sampleDelivery().EndpointURL {
		t.Fatalf("url = %q", sr.url)
	}
	if sr.webhookID != sampleDelivery().EventID {
		t.Fatalf("webhook-id = %q, want event id %q", sr.webhookID, sampleDelivery().EventID)
	}
	if sr.userAgent != "pigeon/test" {
		t.Fatalf("user-agent = %q", sr.userAgent)
	}
	if !strings.HasPrefix(sr.webhookSig, "v1,") {
		t.Fatalf("signature %q missing v1, prefix (FR-1001)", sr.webhookSig)
	}
	if !validSig(secret, sr) {
		t.Fatalf("signature does not verify against secret over <id>.<ts>.<body>")
	}
	// The body must be the exact bytes the signature covers, and carry the
	// delivery id and attempt (FR-902).
	if !strings.Contains(string(sr.body), "\"delivery_id\":\"42\"") {
		t.Fatalf("body missing delivery_id: %s", sr.body)
	}
	if !strings.Contains(string(sr.body), "\"attempt\":2") {
		t.Fatalf("body missing attempt: %s", sr.body)
	}
}

// TestSignatureFailsForTamperedBody: flipping any byte of the body invalidates
// the signature — the consumer's tamper/replay defense (FR-1002).
func TestSignatureFailsForTamperedBody(t *testing.T) {
	secret := []byte(sampleDelivery().SigningSecret)
	sr := buildSignedRequest(sampleDelivery(), 1, "pigeon/test")
	if !validSig(secret, sr) {
		t.Fatal("baseline signature should verify")
	}
	sr.body = append(append([]byte{}, sr.body...), 'x') // byte the signer never covered
	if validSig(secret, sr) {
		t.Fatal("tampered body verified; signature does not cover the whole body")
	}
}

// TestSignatureFailsForWrongSecret: a consumer holding a different secret must
// not accept the request, which is why each endpoint carries its own secret.
func TestSignatureFailsForWrongSecret(t *testing.T) {
	sr := buildSignedRequest(sampleDelivery(), 1, "pigeon/test")
	wrong := []byte("whsec_0000000000000000000000000000000000000000000==")
	if validSig(wrong, sr) {
		t.Fatal("signature verified under the wrong secret")
	}
}
