package delivery

import (
	"net/http"
	"testing"
	"time"
)

// TestBackoffScheduleAC04 is AC-04's "backoff visible (30 s, 1 m)": at the
// default base of 30 s, the delay after attempt 1 is 30 s and after attempt 2 is
// 60 s, the exponential schedule the acceptance scenario names.
func TestBackoffScheduleAC04(t *testing.T) {
	base, max := 30*time.Second, time.Hour
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 30 * time.Second},
		{2, 60 * time.Second},
		{3, 120 * time.Second},
		{4, 240 * time.Second},
	}
	for _, tc := range cases {
		if got := backoffFor(base, max, tc.attempt, 0); got != tc.want {
			t.Fatalf("backoffFor attempt %d = %v, want %v", tc.attempt, got, tc.want)
		}
	}
}

func TestBackoffCappedAndOverflowSafe(t *testing.T) {
	// A large attempt number must clamp to the max, not overflow to a negative
	// or absurd duration.
	if got := backoffFor(30*time.Second, 5*time.Minute, 100, 0); got != 5*time.Minute {
		t.Fatalf("backoff at attempt 100 = %v, want cap 5m", got)
	}
	if got := backoffFor(30*time.Second, time.Hour, 0, 0); got != 30*time.Second {
		t.Fatalf("backoff at attempt 0 = %v, want base 30s", got)
	}
}

func TestBackoffRetryAfterOverrideFR807(t *testing.T) {
	// A Retry-After on 429/503 within 300 s overrides the computed backoff.
	if got := backoffFor(30*time.Second, time.Hour, 1, 90*time.Second); got != 90*time.Second {
		t.Fatalf("Retry-After 90s = %v, want 90s override", got)
	}
	// A Retry-After over 300 s is capped at 300 s (FR-807).
	if got := backoffFor(30*time.Second, time.Hour, 1, 600*time.Second); got != 300*time.Second {
		t.Fatalf("Retry-After 600s = %v, want 300s cap", got)
	}
}

func TestParseRetryAfter(t *testing.T) {
	cases := []struct {
		status int
		header string
		want   time.Duration
		desc   string
	}{
		{429, "90", 90 * time.Second, "delta-seconds on 429"},
		{503, "120", 120 * time.Second, "delta-seconds on 503"},
		{429, "400", 300 * time.Second, "over 300 s is capped at 300 s (FR-807)"},
		{400, "90", 0, "Retry-After ignored on a non-429/503 status"},
		{429, "", 0, "absent header yields no override"},
		{429, "garbage", 0, "unparseable yields no override"},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			h := http.Header{}
			if tc.header != "" {
				h.Set("Retry-After", tc.header)
			}
			if got := parseRetryAfter(h, tc.status); got != tc.want {
				t.Fatalf("parseRetryAfter(status=%d, %q) = %v, want %v", tc.status, tc.header, got, tc.want)
			}
		})
	}
	// An HTTP-date in the future is honored; a past date yields no override.
	future := http.Header{}
	future.Set("Retry-After", time.Now().Add(45*time.Second).UTC().Format(http.TimeFormat))
	if got := parseRetryAfter(future, 503); got <= 0 || got > 60*time.Second {
		t.Fatalf("HTTP-date Retry-After = %v, want ~45s", got)
	}
}
