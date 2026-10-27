package delivery

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"sync"
	"time"
)

// attemptResult is the outcome of one POST, before any database write.
type attemptResult struct {
	success    bool          // 2xx (delivered)
	retryable  bool          // FR-805 retry vs FR-806 terminal (only for non-success)
	outcome    outcome       // retry / terminal, for the attempt row
	errorClass string        // attempts.error_class
	status     int           // HTTP status; 0 for a transport error
	body       string        // first MaxBodyBytes of the response body (FR-804)
	duration   time.Duration // wall time of the attempt
	retryAfter time.Duration // Retry-After override parsed from a 429/503 (FR-807)
	sendErr    error         // transport-level error, for logging
}

// transportCache holds one *http.Transport per endpoint (FR-817), so a slow
// endpoint's connection pool cannot bleed into another's, and MaxConnsPerHost
// bounds that endpoint's in-flight deliveries to its concurrency cap. Clients are
// created lazily and cached; the map is only touched while building a client, so
// the common hot path (an endpoint already seen) is a single read after warm-up.
type transportCache struct {
	mu         sync.Mutex
	byEndpoint map[string]*http.Client
	maxConns   int
}

// newTransportCache is the constructor.
func newTransportCache(maxConns int) *transportCache {
	return &transportCache{byEndpoint: map[string]*http.Client{}, maxConns: maxConns}
}

// clientFor returns the cached client for an endpoint, creating it on first use.
// MaxConnsPerHost equals the endpoint's concurrency cap (FR-817); keep-alives are
// on so a retried delivery to a healthy receiver reuses the connection; and
// CheckRedirect returns the redirect response unmodified so redirects are never
// followed (FR-803). The client has no own Timeout: every attempt carries its own
// context.WithTimeout (FR-819), which bounds the attempt in both time and
// goroutine lifetime and is cancelled on shutdown.
func (c *transportCache) clientFor(endpointID string) *http.Client {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cl, ok := c.byEndpoint[endpointID]; ok {
		return cl
	}
	cl := &http.Client{
		Timeout: 0,
		// FR-803: do not follow redirects. Returning this sentinel makes
		// Client.Do return the redirect response itself, which the classifier
		// then treats as a non-2xx delivery failure.
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Transport: &http.Transport{
			MaxConnsPerHost:     c.maxConns,
			MaxIdleConns:        c.maxConns,
			MaxIdleConnsPerHost: c.maxConns,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true, // capture the raw body, not a decoded one (FR-804)
		},
	}
	c.byEndpoint[endpointID] = cl
	return cl
}

// deliver performs one signed POST and classifies the outcome. It always drains
// and closes the response body (FR-817), captures the first MaxBodyBytes for the
// attempt trail (FR-804), and never follows a redirect (FR-803). ctx carries the
// per-attempt timeout (FR-819).
func (w *Worker) deliver(ctx context.Context, req signedRequest, maxBody int) attemptResult {
	var body bytes.Reader
	body.Reset(req.body)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, req.url, &body)
	if err != nil {
		// A malformed URL or target is a permanent programming error for this row,
		// not a transient one: classify it terminal so it does not retry forever.
		return attemptResult{outcome: outcomeTerminal, errorClass: classHTTPTerminal, sendErr: err, duration: 0}
	}
	httpReq.Header.Set(headerContentType, contentTypeJSON) // FR-801
	httpReq.Header.Set(headerUserAgent, req.userAgent)     // FR-810
	httpReq.Header.Set(headerWebhookID, req.webhookID)     // FR-1001
	httpReq.Header.Set(headerWebhookTime, req.webhookTime) // FR-1001
	httpReq.Header.Set(headerWebhookSig, req.webhookSig)   // FR-1001

	start := time.Now()
	resp, err := w.clients.clientFor(req.url).Do(httpReq)
	res := attemptResult{duration: time.Since(start), sendErr: err}
	if err != nil {
		res.outcome, res.errorClass = classifySendErr(err)
		return res
	}
	// Every path below drains and closes the body, so a slow peer cannot leak a
	// connection (FR-817).
	res.status = resp.StatusCode
	res.body = readAndDrain(resp.Body, maxBody)
	res.retryAfter = parseRetryAfter(resp.Header, resp.StatusCode) // FR-807
	resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		res.success = true
		res.outcome = outcomeTerminal
		return res
	}
	res.outcome, res.errorClass = classifyStatus(resp.StatusCode)
	return res
}

// readAndDrain returns the first max bytes of the body and drains the remainder
// so the keep-alive connection can be reused, then closes it. max <= 0 captures
// nothing (SB-4: body capture configurable down to 0) but still drains and closes.
func readAndDrain(r io.ReadCloser, max int) string {
	defer r.Close()
	if max <= 0 {
		_, _ = io.Copy(io.Discard, r)
		return ""
	}
	data, _ := io.ReadAll(io.LimitReader(r, int64(max)))
	// Drain whatever is left beyond the captured window.
	_, _ = io.Copy(io.Discard, r)
	return string(data)
}
