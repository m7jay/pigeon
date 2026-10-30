package delivery

import (
	"strconv"
	"time"

	"github.com/m7jay/pigeon/internal/envelope"
	"github.com/m7jay/pigeon/internal/store"
)

// Standard Webhooks header names (FR-1001). The consumer verifies these against
// the endpoint's own secret.
const (
	headerWebhookID   = "Webhook-Id"
	headerWebhookTime = "Webhook-Timestamp"
	headerWebhookSig  = "Webhook-Signature"
	headerContentType = "Content-Type"
	headerUserAgent   = "User-Agent"
	contentTypeJSON   = "application/json"
)

// signedRequest is the fully-formed header set the worker sends for one attempt.
// It is built once per attempt and is the unit the signature test asserts on, so
// the signing contract (FR-1001) is checked in one place rather than at the call
// site.
type signedRequest struct {
	url         string
	body        []byte
	webhookID   string // = event id, stable across retries and replays (FR-1001)
	webhookTime string // integer Unix seconds of the attempt (FR-1001, FR-1003)
	webhookSig  string
	userAgent   string
	// customHeaders are the endpoint's FR-405 headers, decrypted for this one
	// request (FR-1006). They are the only place a header plaintext exists
	// outside the seal, and they are never logged.
	customHeaders []store.CustomHeader
	// droppedHeaders names the stored entries that did not authenticate on this
	// claim - a value sealed under a different endpoint's id, or under a key this
	// process does not have. They are NOT sent; the name is kept so the worker can
	// say what it refused. Listing names, never values, is what keeps this
	// diagnosable without putting a secret in the log (FR-1006, FR-1008).
	droppedHeaders []string
	// endpointID keys the per-endpoint transport (FR-817). It is carried on the
	// request rather than looked up by URL so two endpoints that happen to share
	// a host still get their own connection pools and their own caps.
	endpointID string
}

// buildSignedRequest renders the leased delivery into the §10 envelope, signs it
// with the endpoint's own secret (FR-1001), and assembles the exact request the
// worker POSTs. The signed content is "<webhook-id>.<webhook-timestamp>.<body>"
// (FR-1002), where the timestamp is now — the ATTEMPT time — as an integer Unix
// seconds string, and the signed string is the one that goes in the header, byte
// for byte (FR-1003 has the consumer compare that header against its own clock).
// The signature header value is "v1,<b64 HMAC-SHA256>". attempt is the 1-based
// number for this attempt (FR-902), carried in the envelope so a consumer can see
// which attempt it received.
//
// The signing key is the DECODED secret — the base64 half of "whsec_..." — not the
// wrapper string (FR-1001). The secret is used only here and is never logged or
// stored (FR-1008).
//
// A non-nil error reports a stored secret that is not a decodable whsec_ value. The
// request is still returned, signed with the value as it stands: the attempt
// vocabulary (0008) has no class for a local signing fault, and handing the
// delivery to the normal path means a conforming consumer rejects it and FR-806
// ends the delivery terminally with a real status on the evidence row, instead of
// a row looping forever through leases that nothing can deliver.
func buildSignedRequest(d store.LeasedDelivery, attempt int, userAgent string, now time.Time) (signedRequest, error) {
	env := envelope.Envelope{
		ID:          d.EventID,
		Type:        d.EventType,
		Version:     strconv.Itoa(d.PayloadVersion) + ".0",
		ProducerID:  d.ProducerID,
		ConsumerRef: d.ConsumerRef,
		CreatedAt:   d.OccurredAt,
		DeliveryID:  strconv.FormatInt(d.DeliveryID, 10),
		Attempt:     attempt,
		Data:        d.Payload,
	}
	body := envelope.MustMarshal(env)
	key, keyErr := envelope.SecretBytes(d.SigningSecret)
	if keyErr != nil {
		key = []byte(d.SigningSecret)
	}
	ts := envelope.WebhookTimestamp(now)
	return signedRequest{
		url:            d.EndpointURL,
		endpointID:     d.EndpointID,
		body:           body,
		webhookID:      env.ID,
		webhookTime:    ts,
		webhookSig:     dualSignature(env, d, key, ts, string(body), now),
		userAgent:      userAgent,
		customHeaders:  d.CustomHeaders,
		droppedHeaders: d.DroppedCustomHeaders,
	}, keyErr
}

// dualSignature renders FR-1004's header value during a rotation's 24 h overlap:
// BOTH signatures, space-separated, so a receiver verifies against either one
// without a deploy of its own. Standard Webhooks' header is a space-separated
// list for exactly this reason, and the order carries no meaning - a verifier
// checks each entry and accepts the first that matches.
//
// The expiry is applied HERE rather than in the claim query, because the rule
// that has to hold is "both signatures while the overlap is open, one after", and
// a pure function is where a rule is easy to see and easy to test. An endpoint
// that has never rotated, or whose overlap has closed, gets the single signature
// the delivery has always carried.
func dualSignature(env envelope.Envelope, d store.LeasedDelivery, key []byte, ts, body string, now time.Time) string {
	sig := env.Signature(key, ts, body)
	if d.PreviousSigningSecret == "" || d.PreviousSecretExpiresAt == nil {
		return sig
	}
	if !now.Before(*d.PreviousSecretExpiresAt) {
		// The window has closed. The sweeper clears the pair shortly after, and
		// this branch covers the attempt that lands in between - the endpoint
		// must not be signed with a secret it may have retired.
		return sig
	}
	previousKey, err := envelope.SecretBytes(d.PreviousSigningSecret)
	if err != nil {
		// The stored previous value is not a decodable whsec_. The raw bytes are
		// signed and appended all the same: this header is a space-separated list
		// that a verifier checks entry by entry, so a second entry nobody can
		// verify costs nothing, and the current secret's signature - always the
		// first entry - still authenticates the delivery. The unreadable value is
		// reported by the caller's own error path on the attempt that uses it
		// (delivery.go, buildSignedRequest's signErr branch).
		previousKey = []byte(d.PreviousSigningSecret)
	}
	return sig + " " + env.Signature(previousKey, ts, body)
}
