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
		url:         d.EndpointURL,
		body:        body,
		webhookID:   env.ID,
		webhookTime: ts,
		webhookSig:  env.Signature(key, ts, string(body)),
		userAgent:   userAgent,
	}, keyErr
}
