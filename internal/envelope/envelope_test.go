package envelope

import (
	"strings"
	"testing"
	"time"
)

// TestSecretBytesDecodesTheKey: the "whsec_" prefix is a wrapper, not part of the
// HMAC key (FR-1001). The decoded bytes are what a spec verifier derives.
func TestSecretBytesDecodesTheKey(t *testing.T) {
	key, err := SecretBytes("whsec_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=")
	if err != nil {
		t.Fatalf("SecretBytes: %v", err)
	}
	if len(key) != 32 {
		t.Fatalf("key is %d bytes, want 32", len(key))
	}
	if string(key) != string(mustBytes(0x00, 0x1f)) {
		t.Fatalf("key = %x, want the decoded payload 00..1f", key)
	}
}

// TestSecretBytesToleratesMissingPadding: a secret that has been through a shell,
// an env file or a URL loses its "=" padding; the decode must still derive the same
// key, because the padding carries no entropy.
func TestSecretBytesToleratesMissingPadding(t *testing.T) {
	padded, err := SecretBytes("whsec_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=")
	if err != nil {
		t.Fatalf("SecretBytes(padded): %v", err)
	}
	// 32 bytes of standard base64 is 44 characters with padding, 43 without -
	// which is exactly what internal/store mints.
	unpadded, err := SecretBytes("whsec_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8")
	if err != nil {
		t.Fatalf("SecretBytes(unpadded): %v", err)
	}
	if string(padded) != string(unpadded) {
		t.Fatal("padding changed the derived key")
	}
}

// mustBytes returns the byte range [from, to] as a string, for fixture comparisons.
func mustBytes(from, to byte) []byte {
	out := make([]byte, 0, int(to-from)+1)
	for b := from; b <= to; b++ {
		out = append(out, b)
	}
	return out
}

// TestSecretBytesRejectsGarbageWithoutEchoingIt: an undecodable value is an error,
// and the error never carries the secret (FR-1008).
func TestSecretBytesRejectsGarbageWithoutEchoingIt(t *testing.T) {
	const bad = "whsec_!!!not base64!!!"
	if _, err := SecretBytes(bad); err == nil {
		t.Fatal("SecretBytes accepted a non-base64 value")
	} else if strings.Contains(err.Error(), bad) {
		t.Fatalf("error %q echoes the secret (FR-1008)", err)
	}
	for _, empty := range []string{"", "whsec_", "   "} {
		if _, err := SecretBytes(empty); err == nil {
			t.Fatalf("SecretBytes(%q) accepted an empty secret", empty)
		}
	}
}

// TestWebhookTimestampIsUnixSeconds: the header value is an integer Unix-seconds
// string, never an ISO-8601 rendering (FR-1001/FR-1003).
func TestWebhookTimestampIsUnixSeconds(t *testing.T) {
	at := time.Date(2026, 10, 26, 9, 14, 2, 412000000, time.UTC)
	if got, want := WebhookTimestamp(at), "1793006042"; got != want {
		t.Fatalf("WebhookTimestamp = %q, want %q (integer Unix seconds)", got, want)
	}
	if s := WebhookTimestamp(at); strings.ContainsAny(s, "-:TZ") {
		t.Fatalf("WebhookTimestamp = %q looks like a date, not Unix seconds", s)
	}
}

// TestSignatureVectorMatchesAnIndependentImplementation freezes the wire contract
// (FR-1001) against a digest computed OUTSIDE this repository — Python's
// hmac/hashlib/base64 over the same key and signed content — so a regression in the
// key derivation, the alphabet, the signed-content order or the "v1," rendering
// cannot pass by agreeing with itself.
func TestSignatureVectorMatchesAnIndependentImplementation(t *testing.T) {
	const (
		secret = "whsec_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
		id     = "01HZXWQK3M5N6P7Q8R9S0T1U2V"
		ts     = "1793088000"
		body   = `{"id":"01HZXWQK3M5N6P7Q8R9S0T1U2V","type":"order.created","version":"3.0","producer_id":"prd_01","consumer_ref":"ref-99","created_at":"2026-10-26T09:14:02.412Z","delivery_id":"42","attempt":2,"data":{"id":"o1","total":100}}`
		want   = "v1,LuvxOrVDcdKO7ktbhF1hM3yr/+Y2Y58v5LwlLwktjPQ="
	)
	key, err := SecretBytes(secret)
	if err != nil {
		t.Fatalf("SecretBytes: %v", err)
	}
	env := Envelope{ID: id}
	if got := env.SignedContent(ts, body); got != id+"."+ts+"."+body {
		t.Fatalf("SignedContent = %q", got)
	}
	if got := env.Signature(key, ts, body); got != want {
		t.Fatalf("Signature = %q, want the independently computed %q", got, want)
	}
}
