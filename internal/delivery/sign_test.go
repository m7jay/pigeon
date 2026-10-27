package delivery

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/m7jay/pigeon/internal/store"
)

// decodeSecret re-derives the HMAC key from a stored Standard Webhooks secret the
// way a spec verifier does: strip the "whsec_" wrapper, then standard base64. It is
// deliberately a second implementation of the decode, not a call to
// envelope.SecretBytes: a test that shares the signer's decode would pass even if
// both sides were wrong about the key (the round-1 defect: the wrapper was used as
// the key and every test agreed with it).
func decodeSecret(t *testing.T, secret string) []byte {
	t.Helper()
	enc := strings.TrimPrefix(secret, "whsec_")
	if enc == secret {
		t.Fatalf("secret %q is missing the whsec_ prefix (FR-1001)", secret)
	}
	if pad := len(enc) % 4; pad != 0 {
		enc += strings.Repeat("=", 4-pad)
	}
	key, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		t.Fatalf("secret %q is not standard base64: %v", secret, err)
	}
	if len(key) == 0 {
		t.Fatalf("secret %q decodes to zero bytes", secret)
	}
	return key
}

// validSig independently verifies the worker's signature the way a consumer
// would (FR-1001/FR-1002): recompute base64(HMAC-SHA256(key,
// "<webhook-id>.<webhook-timestamp>.<body>")) and compare. It does not call the
// signer, so the worker and this verifier agreeing IS the property under test.
func validSig(key []byte, sr signedRequest) bool {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(sr.webhookID + "." + sr.webhookTime + "."))
	_, _ = mac.Write(sr.body)
	want := "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(sr.webhookSig), []byte(want))
}

// sampleOccurredAt is a fixed event time so the produced body and signature are
// deterministic and can be frozen against an externally computed vector.
var sampleOccurredAt = time.Date(2026, 10, 26, 9, 14, 2, 412000000, time.UTC)

func sampleDelivery() store.LeasedDelivery {
	return store.LeasedDelivery{
		DeliveryID:     42,
		EventID:        "01HZXWQK3M5N6P7Q8R9S0T1U2V",
		ProducerID:     "prd_01",
		Env:            "live",
		EndpointID:     "ep_01",
		EndpointURL:    "https://consumer.example/webhooks",
		SigningSecret:  "whsec_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=",
		EventType:      "order.created",
		PayloadVersion: 3,
		ConsumerRef:    "ref-99",
		OccurredAt:     sampleOccurredAt,
		Payload:        []byte(`{"id":"o1","total":100}`),
	}
}

// attemptTime is the fixed clock the signature tests sign at.
var attemptTime = time.Unix(1793088000, 0)

// TestBuildSignedRequestMatchesStandardWebhooks: the worker signs with the
// endpoint's own secret's DECODED bytes, sets webhook-id to the event id, stamps
// webhook-timestamp with the attempt time as integer Unix seconds, prefixes "v1,",
// and carries the raw body verbatim so the consumer can recompute the signature.
func TestBuildSignedRequestMatchesStandardWebhooks(t *testing.T) {
	d := sampleDelivery()
	key := decodeSecret(t, d.SigningSecret)
	sr, err := buildSignedRequest(d, 2, "pigeon/test", attemptTime)
	if err != nil {
		t.Fatalf("buildSignedRequest: %v", err)
	}

	if sr.url != d.EndpointURL {
		t.Fatalf("url = %q", sr.url)
	}
	if sr.webhookID != d.EventID {
		t.Fatalf("webhook-id = %q, want event id %q", sr.webhookID, d.EventID)
	}
	if sr.userAgent != "pigeon/test" {
		t.Fatalf("user-agent = %q", sr.userAgent)
	}
	if !strings.HasPrefix(sr.webhookSig, "v1,") {
		t.Fatalf("signature %q missing v1, prefix (FR-1001)", sr.webhookSig)
	}
	if !validSig(key, sr) {
		t.Fatalf("signature does not verify against the decoded secret over <id>.<ts>.<body>")
	}
	// The body must be the exact bytes the signature covers, and carry the
	// delivery id and attempt (FR-902).
	if !strings.Contains(string(sr.body), `"delivery_id":"42"`) {
		t.Fatalf("body missing delivery_id: %s", sr.body)
	}
	if !strings.Contains(string(sr.body), `"attempt":2`) {
		t.Fatalf("body missing attempt: %s", sr.body)
	}
}

// TestSignedRequestFreezesTheWireVector pins the whole wire contract — the exact
// body bytes, the exact timestamp string, and the exact signature — against values
// computed OUTSIDE this repository (Python's hmac/hashlib/base64 over the same
// bytes). A regression in the key, the alphabet, the signed-content order or the
// header rendering changes this value and fails here, which is what stops the
// signer and its own tests from agreeing on a wrong contract.
func TestSignedRequestFreezesTheWireVector(t *testing.T) {
	const (
		wantBody = `{"id":"01HZXWQK3M5N6P7Q8R9S0T1U2V","type":"order.created","version":"3.0","producer_id":"prd_01","consumer_ref":"ref-99","created_at":"2026-10-26T09:14:02.412Z","delivery_id":"42","attempt":2,"data":{"id":"o1","total":100}}`
		wantSig  = "v1,LuvxOrVDcdKO7ktbhF1hM3yr/+Y2Y58v5LwlLwktjPQ="
		wantTS   = "1793088000"
	)
	sr, err := buildSignedRequest(sampleDelivery(), 2, "pigeon/test", attemptTime)
	if err != nil {
		t.Fatalf("buildSignedRequest: %v", err)
	}
	if string(sr.body) != wantBody {
		t.Fatalf("wire body changed:\n got %s\nwant %s", sr.body, wantBody)
	}
	if sr.webhookTime != wantTS {
		t.Fatalf("webhook-timestamp = %q, want %q (integer Unix seconds, FR-1001)", sr.webhookTime, wantTS)
	}
	if sr.webhookSig != wantSig {
		t.Fatalf("signature = %q, want %q (the frozen Standard Webhooks vector)", sr.webhookSig, wantSig)
	}
}

// TestWebhookTimestampIsTheAttemptTime: webhook-timestamp is integer Unix seconds
// of the ATTEMPT, not the event's created_at (FR-1001/FR-1003). §9's schedule
// crosses FR-1003's 300 s tolerance at attempt 4, so an event-time stamp would make
// every later retry unverifiable by a conforming consumer.
func TestWebhookTimestampIsTheAttemptTime(t *testing.T) {
	d := sampleDelivery()
	sr, err := buildSignedRequest(d, 4, "pigeon/test", attemptTime)
	if err != nil {
		t.Fatalf("buildSignedRequest: %v", err)
	}
	got, err := strconv.ParseInt(sr.webhookTime, 10, 64)
	if err != nil {
		t.Fatalf("webhook-timestamp = %q, want an integer Unix-seconds string (FR-1001)", sr.webhookTime)
	}
	if got != attemptTime.Unix() {
		t.Fatalf("webhook-timestamp = %d, want the attempt time %d", got, attemptTime.Unix())
	}
	// And it is emphatically not the event time: an event stamped 450 s ago must
	// still verify inside a consumer's 300 s window.
	if delta := attemptTime.Unix() - d.OccurredAt.Unix(); delta < 301 {
		t.Fatalf("fixture is weak: the attempt is only %d s after the event", delta)
	}
}

// TestSignatureKeyIsTheDecodedSecret: the "whsec_..." string is a wrapper, not the
// key (FR-1001). Signing with the wrapper must fail, which is exactly what the
// official Standard Webhooks verifier reported in review round 1.
func TestSignatureKeyIsTheDecodedSecret(t *testing.T) {
	d := sampleDelivery()
	sr, err := buildSignedRequest(d, 1, "pigeon/test", attemptTime)
	if err != nil {
		t.Fatalf("buildSignedRequest: %v", err)
	}
	if validSig([]byte(d.SigningSecret), sr) {
		t.Fatal("signature verified against the raw whsec_ wrapper; the key must be the decoded bytes")
	}
	if !validSig(decodeSecret(t, d.SigningSecret), sr) {
		t.Fatal("signature does not verify against the decoded secret")
	}
}

// TestUndecodableSecretIsReportedAndStillSigned: a stored secret that is not
// decodable base64 is reported so the worker can log it, but the attempt still goes
// out signed with the value as it stands — the delivery then ends through the
// normal path (the peer rejects it, FR-806) instead of looping on a lease forever.
func TestUndecodableSecretIsReportedAndStillSigned(t *testing.T) {
	d := sampleDelivery()
	d.SigningSecret = "whsec_!!!not base64!!!"
	sr, err := buildSignedRequest(d, 1, "pigeon/test", attemptTime)
	if err == nil {
		t.Fatal("buildSignedRequest accepted an undecodable secret without reporting it")
	}
	if !strings.HasPrefix(sr.webhookSig, "v1,") {
		t.Fatalf("signature %q missing v1, prefix; the request must still be signable", sr.webhookSig)
	}
	if !validSig([]byte(d.SigningSecret), sr) {
		t.Fatal("fallback request is not signed with the stored value as it stands")
	}
	if !strings.Contains(err.Error(), "base64") {
		t.Fatalf("error %q should name the decode failure without carrying the secret (FR-1008)", err)
	}
	if strings.Contains(err.Error(), d.SigningSecret) {
		t.Fatal("the error carries the secret value (FR-1008)")
	}
}

// TestSignatureFailsForTamperedBody: flipping any byte of the body invalidates
// the signature — the consumer's tamper/replay defense (FR-1002).
func TestSignatureFailsForTamperedBody(t *testing.T) {
	key := decodeSecret(t, sampleDelivery().SigningSecret)
	sr, err := buildSignedRequest(sampleDelivery(), 1, "pigeon/test", attemptTime)
	if err != nil {
		t.Fatalf("buildSignedRequest: %v", err)
	}
	if !validSig(key, sr) {
		t.Fatal("baseline signature should verify")
	}
	sr.body = append(append([]byte{}, sr.body...), 'x') // byte the signer never covered
	if validSig(key, sr) {
		t.Fatal("tampered body verified; signature does not cover the whole body")
	}
}

// TestSignatureFailsForTamperedTimestamp: the timestamp is inside the signed
// content (FR-1002), so a replay with a re-stamped timestamp must not verify. The
// header string and the signed string are the same value, which is what makes this
// hold.
func TestSignatureFailsForTamperedTimestamp(t *testing.T) {
	key := decodeSecret(t, sampleDelivery().SigningSecret)
	sr, err := buildSignedRequest(sampleDelivery(), 1, "pigeon/test", attemptTime)
	if err != nil {
		t.Fatalf("buildSignedRequest: %v", err)
	}
	sr.webhookTime = strconv.FormatInt(attemptTime.Add(time.Hour).Unix(), 10)
	if validSig(key, sr) {
		t.Fatal("a re-stamped timestamp verified; the timestamp is not covered by the signature")
	}
}

// TestSignatureFailsForWrongSecret: a consumer holding a different secret must
// not accept the request, which is why each endpoint carries its own secret.
func TestSignatureFailsForWrongSecret(t *testing.T) {
	sr, err := buildSignedRequest(sampleDelivery(), 1, "pigeon/test", attemptTime)
	if err != nil {
		t.Fatalf("buildSignedRequest: %v", err)
	}
	wrong := decodeSecret(t, "whsec_ICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj8=")
	if validSig(wrong, sr) {
		t.Fatal("signature verified under the wrong secret")
	}
}
