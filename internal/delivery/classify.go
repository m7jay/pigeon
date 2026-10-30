package delivery

import (
	"context"
	"errors"
	"net"
)

// Error classes for the attempt trail (FR-1103) and the metric/UI surface. These
// are the only values the attempts.error_class column accepts.
const (
	classTimeout       = "timeout"
	classDialError     = "dial_error"
	classDNSError      = "dns_error"
	classHTTPRetryable = "http_retryable"
	classHTTPTerminal  = "http_terminal"
)

// outcome is whether a failed attempt earns another try.
type outcome int

const (
	outcomeRetry outcome = iota
	outcomeTerminal
)

// classifyStatus maps an HTTP status the peer returned to retry-or-terminal
// (FR-805 vs FR-806). FR-805 names exactly what is retried — 408, 425, 429 and
// all 5xx — and FR-806 names the immediate terminals; everything else that is
// not 2xx (every other 4xx, and every 3xx, since redirects are never followed,
// FR-803) is terminal, so a misconfigured receiver cannot burn the whole retry
// budget (§17's "retry on 4xx wasted three calls" anti-pattern). A 2xx is a
// success, not a failure, so this is only reached for non-2xx.
func classifyStatus(status int) (outcome, string) {
	switch {
	case status == 408, status == 425, status == 429, status >= 500 && status < 600:
		return outcomeRetry, classHTTPRetryable
	default:
		return outcomeTerminal, classHTTPTerminal
	}
}

// classifySendErr maps a transport-level error (no HTTP response was received) to
// retry-or-terminal and a class. Every transport failure is retryable (FR-805:
// "connection/DNS error"): the peer did not answer, so the delivery was never
// attempted at the application level. The class distinguishes a 5 s timeout
// (FR-802) from a DNS failure from a plain dial failure for the trail.
func classifySendErr(err error) (outcome, string) {
	if err == nil {
		return outcomeTerminal, ""
	}
	// A per-attempt timeout (FR-819) or a net timeout is a FR-802 timeout. A
	// context cancellation (a shutdown aborting the attempt) is the same class:
	// the peer never answered, so the delivery earns another try.
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return outcomeRetry, classTimeout
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return outcomeRetry, classTimeout
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return outcomeRetry, classDNSError
	}
	return outcomeRetry, classDialError
}
