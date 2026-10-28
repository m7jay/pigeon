// Package portal holds the plumbing shared by the two portal deployables: a thin
// HTTP client for the core API, and a signed/encrypted session cookie so each
// portal can hold a human's scoped core session server-side.
//
// §14's contract: a portal is a thin client of the same public API using scoped
// tokens, and "nothing the portal can do may be impossible from the API". This
// client therefore only ever calls core API routes, with the caller's scoped
// session token attached as FR-208's Bearer header. The browser never sees that
// token; it lives in an encrypted httpOnly cookie on the portal's own origin.
package portal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client is a thin HTTP client for one core API instance. The scoped session
// token (a producer member session or a consumer session, whichever the portal
// holds) is passed per request, so one Client is safe for many concurrent
// requests each carrying its own caller's token — no shared mutable state.
type Client struct {
	BaseURL string
	HTTP    *http.Client
}

// New returns a Client for the core API at baseURL (e.g. http://core:8080 inside
// the compose network, or http://localhost:8080 for local runs).
func New(baseURL string) *Client {
	return &Client{
		BaseURL: baseURL,
		HTTP:    &http.Client{Timeout: 15 * time.Second},
	}
}

// Response is one core API reply: the machine status code, the raw body, and a
// parsed machine-error code when the body is an error object.
type Response struct {
	Status int
	Body   []byte
	Code   string // the "code" field of an error body, when present
}

// Request issues one call to the core API. token is the caller's scoped session
// token (empty for anonymous routes like login and invitation acceptance); it is
// attached as FR-208's Bearer header. A non-nil body is JSON-encoded and sent
// with Content-Type: application/json; a nil body sends nothing. Network and
// timeout errors are returned; a core HTTP error status is reported in the
// Response, not as an error, so the caller can branch on 404/409/422 exactly as
// the API defines them.
func (c *Client) Request(ctx context.Context, token, method, path string, body any) (Response, error) {
	return c.RequestHeaders(ctx, token, method, path, body, nil)
}

// RequestHeaders is Request with extra request headers attached. It exists for
// the one core call that needs a header of its own: a replay is idempotent on
// FR-1107's request key, which the API reads from the `Idempotency-Key` header
// (the same place publish takes it, FR-603). The key is the portal's own - the
// user does not type it - and the API is what enforces it, so this is still a
// thin client: no replay rule lives here.
func (c *Client) RequestHeaders(ctx context.Context, token, method, path string, body any,
	headers map[string]string) (Response, error) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return Response{}, fmt.Errorf("encode request body: %w", err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, reader)
	if err != nil {
		return Response{}, fmt.Errorf("build request: %w", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return Response{}, fmt.Errorf("core API %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return Response{}, fmt.Errorf("read core response: %w", err)
	}
	out := Response{Status: resp.StatusCode, Body: raw}
	out.Code, _ = errorCode(raw)
	return out, nil
}

// errorCode extracts the machine-readable "code" from a §11 error body, or "" if
// the body is not an error object.
func errorCode(raw []byte) (string, error) {
	var e struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &e); err != nil {
		return "", err
	}
	return e.Error.Code, nil
}

// Decode unmarshals the response body into v, for the callers that want the
// parsed payload.
func Decode(raw []byte, v any) error { return json.Unmarshal(raw, v) }
