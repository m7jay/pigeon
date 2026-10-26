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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// hmac256 is the Standard Webhooks primitive: HMAC-SHA256 over the signed
// content. Kept local so the verifier (in the consumer portal, S2b) and this
// signer use the identical construction.
func hmac256(secret, content []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(content)
	return hex.EncodeToString(mac.Sum(nil))
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

// CreatedAtMillis is the §10 stability note: ISO-8601 UTC to the millisecond, so
// a timestamp is stable across time zones and never carries sub-ms noise.
func (e Envelope) CreatedAtMillis() string {
	return e.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z07:00")
}

// SignedContent is the exact string the signature covers (FR-1001, §10):
// "<webhook-id>.<webhook-timestamp>.<raw body>". The webhook-id is the event id
// and the timestamp is the envelope's, so a captured request cannot be replayed
// against a different resource or after the tolerance window (FR-1002).
func (e Envelope) SignedContent(rawBody string) string {
	return e.ID + "." + e.CreatedAtMillis() + "." + rawBody
}

// Signature is base64(HMAC-SHA256(secret, signed content)), rendered as the
// Standard Webhooks value "v1,<b64>". secret is the raw whsec_ bytes. It is a
// method so the signer and the verifier agree on the exact input.
func (e Envelope) Signature(secret []byte, rawBody string) string {
	mac := hmac256(secret, []byte(e.SignedContent(rawBody)))
	return "v1," + mac
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
