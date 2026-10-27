package delivery

import (
	"strconv"

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
	webhookTime string
	webhookSig  string
	userAgent   string
}

// buildSignedRequest renders the leased delivery into the §10 envelope, signs it
// with the endpoint's own secret (FR-1001), and assembles the exact request the
// worker POSTs. The signed content is "<webhook-id>.<webhook-timestamp>.<body>"
// (FR-1002); the signature header value is "v1,<b64 HMAC-SHA256>". attempt is the
// 1-based number for this attempt (FR-902), carried in the envelope so a consumer
// can see which attempt it received.
//
// The secret is used only here and is never logged or stored (FR-1008).
func buildSignedRequest(d store.LeasedDelivery, attempt int, userAgent string) signedRequest {
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
	return signedRequest{
		url:         d.EndpointURL,
		body:        body,
		webhookID:   env.ID,
		webhookTime: env.CreatedAtMillis(),
		webhookSig:  env.Signature([]byte(d.SigningSecret), string(body)),
		userAgent:   userAgent,
	}
}
