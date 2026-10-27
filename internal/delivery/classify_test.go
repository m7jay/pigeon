package delivery

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
)

func TestClassifyStatus(t *testing.T) {
	cases := []struct {
		status    int
		wantOut   outcome
		wantClass string
		desc      string
	}{
		{408, outcomeRetry, classHTTPRetryable, "408 Request Timeout retries"},
		{425, outcomeRetry, classHTTPRetryable, "425 Too Early retries"},
		{429, outcomeRetry, classHTTPRetryable, "429 Too Many Requests retries"},
		{500, outcomeRetry, classHTTPRetryable, "500 retries"},
		{503, outcomeRetry, classHTTPRetryable, "503 retries"},
		{599, outcomeRetry, classHTTPRetryable, "599 retries"},
		{400, outcomeTerminal, classHTTPTerminal, "400 is terminal (FR-806)"},
		{401, outcomeTerminal, classHTTPTerminal, "401 is terminal (FR-806)"},
		{403, outcomeTerminal, classHTTPTerminal, "403 is terminal (FR-806)"},
		{404, outcomeTerminal, classHTTPTerminal, "404 is terminal (FR-806)"},
		{410, outcomeTerminal, classHTTPTerminal, "410 is terminal (FR-806)"},
		{422, outcomeTerminal, classHTTPTerminal, "422 is terminal (FR-806)"},
		{402, outcomeTerminal, classHTTPTerminal, "unlisted 4xx is terminal (§17: do not retry 4xx)"},
		{418, outcomeTerminal, classHTTPTerminal, "unlisted 4xx is terminal"},
		{301, outcomeTerminal, classHTTPTerminal, "redirect is not followed and not retried (FR-803)"},
		{302, outcomeTerminal, classHTTPTerminal, "redirect is terminal"},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			out, cls := classifyStatus(tc.status)
			if out != tc.wantOut || cls != tc.wantClass {
				t.Fatalf("classifyStatus(%d) = (%d,%q), want (%d,%q)", tc.status, out, cls, tc.wantOut, tc.wantClass)
			}
		})
	}
}

func TestClassifySendErr(t *testing.T) {
	dnsErr := &net.DNSError{Err: "no such host", Name: "dead.example"}

	cases := []struct {
		err       error
		wantOut   outcome
		wantClass string
		desc      string
	}{
		{context.DeadlineExceeded, outcomeRetry, classTimeout, "deadline is a retryable timeout"},
		{context.Canceled, outcomeRetry, classTimeout, "cancellation is a retryable timeout"},
		{dnsErr, outcomeRetry, classDNSError, "DNS failure is retryable"},
		{&net.OpError{Op: "dial", Err: errors.New("connection refused")}, outcomeRetry, classDialError, "dial failure is retryable"},
		{fmt.Errorf("dial tcp: %w", &net.OpError{Op: "dial", Err: errors.New("reset")}), outcomeRetry, classDialError, "wrapped dial failure is retryable"},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			out, cls := classifySendErr(tc.err)
			if out != tc.wantOut || cls != tc.wantClass {
				t.Fatalf("classifySendErr(%v) = (%d,%q), want (%d,%q)", tc.err, out, cls, tc.wantOut, tc.wantClass)
			}
		})
	}
}
