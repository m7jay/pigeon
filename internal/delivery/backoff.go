package delivery

import (
	"math"
	"net/http"
	"strconv"
	"time"
)

// Retry-After bounds (FR-807): a Retry-After on 429/503 up to 300 s overrides the
// computed backoff; anything longer is capped at 300 s so a receiver cannot pin a
// delivery idle for hours on a single header.
const maxRetryAfter = 300 * time.Second

// backoffFor computes the delay before the next attempt at §9's default factor
// of 2. The schedule is exponential in the attempt number — base, 2×base,
// 4×base, … — capped at BackoffMax, so a persistently failing receiver backs off
// rather than hammering it. FR-807: a Retry-After on 429/503 (≤ 300 s) wins over
// the computed value.
//
// AC-04's "backoff visible (30 s, 1 m)" is this function at the default base of
// 30 s: after attempt 1 the delay is 30 s, after attempt 2 it is 60 s.
func backoffFor(base, max time.Duration, attempt int, retryAfter time.Duration) time.Duration {
	if retryAfter > 0 {
		return cappedRetryAfter(retryAfter)
	}
	return exponential(base, max, 2, attempt)
}

// exponential is the family backoffFor and RetryPolicy.Delay both draw from:
// base × factor^(attempt-1), clamped to max. Factoring it out is what lets §9's
// per-endpoint "factor" override be a number rather than a rewrite.
//
// Computed in float to avoid an int-duration overflow that a left shift would hit
// around attempt 34; the cap then clamps it.
func exponential(base, max time.Duration, factor float64, attempt int) time.Duration {
	if base <= 0 {
		base = time.Second
	}
	if factor < 1 {
		factor = 1
	}
	shift := attempt - 1
	if shift < 0 {
		shift = 0
	}
	d := float64(base) * math.Pow(factor, float64(shift))
	if d <= 0 || d > float64(max) {
		return max
	}
	return time.Duration(d)
}

// parseRetryAfter reads a Retry-After header from a 429/503 response and returns
// the delay it names, or 0 when there is none or it is not one of those statuses.
// FR-807 only honours Retry-After on 429/503; the value may be delta-seconds (an
// integer) or an HTTP date, of which this parses the delta-seconds form, which is
// what receivers emit for a backoff. An unparseable or past date yields 0 (no
// override), and the computed backoff stands.
func parseRetryAfter(h http.Header, status int) time.Duration {
	if status != http.StatusTooManyRequests && status != http.StatusServiceUnavailable {
		return 0
	}
	raw := h.Get("Retry-After")
	if raw == "" {
		return 0
	}
	if secs, err := strconv.Atoi(raw); err == nil {
		return cappedRetryAfter(time.Duration(secs) * time.Second)
	}
	if t, err := http.ParseTime(raw); err == nil {
		return cappedRetryAfter(time.Until(t))
	}
	return 0
}

// cappedRetryAfter clamps a positive Retry-After to the FR-807 300 s ceiling and
// yields 0 for a non-positive value (no override). Both the delta-seconds and the
// HTTP-date forms route through it so the cap lives in one place.
func cappedRetryAfter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	if d > maxRetryAfter {
		return maxRetryAfter
	}
	return d
}
