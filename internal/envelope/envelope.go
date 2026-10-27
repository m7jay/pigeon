// Package envelope builds the wire shape a consumer receives (§10).
//
// It exists as its own package because the envelope is a contract, not an
// internal detail: the signature scheme (FR-1001) signs a rendering of it, the
// relay builds it, and the producer-facing inspection API reads it back. Keeping
// one type in one place is what stops the signed bytes and the stored bytes from
// drifting apart.
package envelope

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// secretPrefix is the Standard Webhooks secret namespace (FR-1001): secrets are
// issued as "whsec_" + base64, and the base64 half — not the whole string — is
// the HMAC key.
const secretPrefix = "whsec_"

// hmac256 is the Standard Webhooks primitive: base64(HMAC-SHA256) over the signed
// content. Kept local so the verifier (in the consumer portal, S2b) and this
// signer use the identical construction. The Standard Webhooks spec renders the
// digest as base64, not hex — hex would not verify against a real consumer.
func hmac256(key, content []byte) string {
	mac := hmac.New(sha256.New, key)
	mac.Write(content)
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// SecretBytes decodes a stored signing secret into the key the spec actually
// signs with (FR-1001): the "<base64>" half of a "whsec_<base64>" value, in the
// STANDARD alphabet. A verifier must derive the key exactly this way — using the
// wrapper as the key, or a URL-safe decoding of it, computes a different HMAC.
// Missing padding is tolerated, because a secret that has been through a shell, an
// env file or a URL often loses it; the URL alphabet is not, because a standard
// decoder does not fail on "-"/"_", it silently drops those bytes and derives a
// short key. The errors never carry the secret (FR-1008).
func SecretBytes(secret string) ([]byte, error) {
	enc := strings.TrimPrefix(strings.TrimSpace(secret), secretPrefix)
	if enc == "" {
		return nil, errors.New("envelope: empty signing secret")
	}
	if pad := len(enc) % 4; pad != 0 {
		enc += strings.Repeat("=", 4-pad)
	}
	raw, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		return nil, errors.New("envelope: signing secret is not standard base64")
	}
	if len(raw) == 0 {
		return nil, errors.New("envelope: signing secret decodes to zero bytes")
	}
	return raw, nil
}

// WebhookTimestamp renders a time as the `webhook-timestamp` header value
// (FR-1001): integer Unix seconds, the form FR-1003 has a consumer compare
// against its own clock. The header carries the ATTEMPT time, not the event time:
// §9's schedule (30 s, 60 s, 120 s, 240 s, …) crosses FR-1003's 300 s tolerance at
// attempt 4, so a retry stamped with the event time would be rejected by every
// conforming consumer.
func WebhookTimestamp(t time.Time) string {
	return strconv.FormatInt(t.Unix(), 10)
}

// Envelope is §10 exactly. Fields map one-to-one to the documented shape; the
// JSON tags ARE the contract, so they are written once here and never repeated.
//
// Class is not in §10: it is the event's queue class (FR-7.7a) and only picks
// which stream the relay publishes to. It is carried here so the relay and the
// signature do not each have to re-derive it, and it is excluded from the wire
// form with json:"-".
type Envelope struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Version     string          `json:"version"`
	ProducerID  string          `json:"producer_id"`
	ConsumerRef string          `json:"consumer_ref,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	DeliveryID  string          `json:"delivery_id"`
	Attempt     int             `json:"attempt"`
	Data        json.RawMessage `json:"data"`
	Class       string          `json:"-"`
}

// SignedContent is the exact string the signature covers (FR-1001, §10):
// "<webhook-id>.<webhook-timestamp>.<raw body>". timestamp is the value that goes
// in the `webhook-timestamp` header verbatim (integer Unix seconds of the attempt,
// WebhookTimestamp); the signer and the verifier must cover the identical string,
// so it is passed in rather than re-derived from the envelope here. The webhook-id
// is the event id, so a captured request cannot be replayed against a different
// resource (FR-1002).
func (e Envelope) SignedContent(timestamp, rawBody string) string {
	return e.ID + "." + timestamp + "." + rawBody
}

// Signature is base64(HMAC-SHA256(key, signed content)), rendered as the Standard
// Webhooks header value "v1,<b64>". key is the DECODED secret (SecretBytes), not
// the "whsec_..." wrapper string (FR-1001). It is a method so the signer and the
// verifier agree on the exact signed content.
func (e Envelope) Signature(key []byte, timestamp, rawBody string) string {
	return "v1," + hmac256(key, []byte(e.SignedContent(timestamp, rawBody)))
}

// Stream is the events stream this envelope publishes to. Priority and normal
// events use separate streams and worker pools (FR-705); the class, declared on
// the event type, chooses which.
func (e Envelope) Stream() string {
	if e.Class == "priority" {
		return "events:priority"
	}
	return "events:normal"
}

// Fingerprint is a stable digest of the parts that identify the event for dedupe
// and inspection: event id, type, version, producer, consumer ref. Two envelopes
// that differ only in attempt/delivery_id (a retry) share a fingerprint, which
// is what lets a UI roll up "all deliveries of this event".
func (e Envelope) Fingerprint() string {
	h := sha256.Sum256([]byte(e.ID + "|" + e.Type + "|" + e.Version + "|" + e.ProducerID + "|" + e.ConsumerRef))
	return hex.EncodeToString(h[:])
}

// Marshal returns the wire bytes. It is the single rendering the signature signs,
// so the stored envelope and the delivered envelope are the same bytes.
func (e Envelope) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

// MustMarshal is Marshal for callers that have already validated the payload
// (the relay only publishes rows whose event passed FR-303 at ingest); a
// marshalling error there is a programming bug, surfaced loudly rather than
// dropped silently on the delivery path.
func MustMarshal(e Envelope) []byte {
	b, err := e.Marshal()
	if err != nil {
		panic(fmt.Sprintf("envelope: marshal %s: %v", e.ID, err))
	}
	return b
}
