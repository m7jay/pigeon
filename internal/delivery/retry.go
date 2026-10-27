package delivery

import (
	"math/rand"
	"time"
)

// RetryPolicy is §9's schedule as one value, so the per-endpoint override and the
// deployment default differ only in their numbers:
//
//	Max retries  15 (16 attempts including the first)   per endpoint 3-30
//	Backoff      exponential, base 30 s, factor 2, capped at 12 h, ±20% jitter
//	Total window 72 h (1 h - 7 d per endpoint)
//
// FR-1201 gives the WINDOW precedence over the count: an attempt scheduled
// beyond the deadline is not made, whatever retries remain. That is why Next
// checks the deadline before it checks the budget and again before it accepts a
// scheduled instant.
type RetryPolicy struct {
	// Base is the first retry's delay, doubling each attempt thereafter.
	Base time.Duration
	// Factor is the exponential base (2 in §9). It is a float so the shape stays
	// a parameter rather than an assumption baked into a shift.
	Factor float64
	// Max caps the exponential, so a receiver that is genuinely down for two days
	// sees a handful of probes rather than hundreds (§9).
	Max time.Duration
	// MaxRetries is §9's retry budget: 15 retries, i.e. at most 16 attempts
	// including the first.
	MaxRetries int
	// Window is the per-delivery retry window (§9: 72 h; 1 h - 7 d per endpoint).
	Window time.Duration
	// Jitter is the fractional spread applied to the computed delay: 0.2 is §9's
	// ±20%, and it is mandatory, because without it a burst of failures retries
	// in lockstep and produces a second spike against an endpoint that is already
	// down (§9).
	Jitter float64
	// Rand supplies a value in [0,1). Nil means the shared source; tests pin it
	// so a schedule is reproducible.
	Rand func() float64
}

// DefaultRetryPolicy is §9's row.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		Base:       30 * time.Second,
		Factor:     2,
		Max:        12 * time.Hour,
		MaxRetries: 15,
		Window:     72 * time.Hour,
		Jitter:     0.2,
	}
}

// Normalize fills a zero-valued policy with the defaults and repairs a policy
// that cannot be scheduled (a zero base, a factor below 1, a negative jitter).
func (p RetryPolicy) Normalize() RetryPolicy {
	d := DefaultRetryPolicy()
	if p.Base <= 0 {
		p.Base = d.Base
	}
	if p.Factor < 1 {
		p.Factor = d.Factor
	}
	if p.Max <= 0 {
		p.Max = d.Max
	}
	if p.Max < p.Base {
		p.Max = p.Base
	}
	if p.MaxRetries <= 0 {
		p.MaxRetries = d.MaxRetries
	}
	if p.Window <= 0 {
		p.Window = d.Window
	}
	if p.Jitter < 0 {
		p.Jitter = 0
	}
	if p.Rand == nil {
		p.Rand = rand.Float64
	}
	return p
}

// NominalDelay is the schedule without jitter: base, 2×base, 4×base, … capped at
// Max. §9's table is this function - 30 s, 1 m, 2 m, 4 m, 8 m, 16 m, 32 m,
// 1 h 4 m, 2 h 8 m, 4 h 16 m, 8 h 32 m, then 12 h.
//
// FR-807: a Retry-After on a 429/503 response (≤ 300 s, enforced by
// parseRetryAfter) replaces the computed value entirely. It is not jittered:
// the receiver named an instant, and second-guessing it is not this platform's
// call.
func (p RetryPolicy) NominalDelay(attempt int, retryAfter time.Duration) time.Duration {
	if retryAfter > 0 {
		if retryAfter > maxRetryAfter {
			retryAfter = maxRetryAfter
		}
		return retryAfter
	}
	return exponential(p.Base, p.Max, p.Factor, attempt)
}

// Delay is NominalDelay with §9's jitter applied, clamped to Max so the cap is
// still a cap and the upper steps of the schedule stay exactly 12 h.
func (p RetryPolicy) Delay(attempt int, retryAfter time.Duration) time.Duration {
	d := p.NominalDelay(attempt, retryAfter)
	if retryAfter > 0 || p.Jitter <= 0 {
		return d
	}
	// rand in [0,1) mapped to [-1,1): the spread is symmetric, so the mean delay
	// is the nominal one and the schedule keeps its shape.
	r := p.Rand
	if r == nil {
		r = rand.Float64
	}
	offset := (r()*2 - 1) * p.Jitter
	jittered := time.Duration(float64(d) * (1 + offset))
	if jittered < time.Second {
		jittered = time.Second
	}
	if jittered > p.Max {
		jittered = p.Max
	}
	return jittered
}

// RetryVerdict is why a delivery did or did not get another attempt.
type RetryVerdict string

const (
	// RetryScheduled means another attempt fits inside the window.
	RetryScheduled RetryVerdict = "scheduled"
	// RetryNonRetryable means FR-806: the response itself ended the delivery
	// (a 4xx other than 408/425/429, or a 3xx). No schedule is computed for it,
	// because retrying a receiver that answered "no" is the §17 anti-pattern
	// that burns the budget on calls that cannot succeed.
	RetryNonRetryable RetryVerdict = "non_retryable"
	// RetryExhausted means FR-1201's retry count is spent.
	RetryExhausted RetryVerdict = "retries_exhausted"
	// RetryWindowClosed means FR-1201's window is spent, or the next attempt
	// would fall beyond it. The window takes precedence over the count, so this
	// is reported even when retries remain.
	RetryWindowClosed RetryVerdict = "window_closed"
)

// Next decides what follows a failed attempt. `attempt` is the 1-based number of
// the attempt that just failed; `deadline` is the delivery's retry_deadline,
// which a pause extends (FR-1207). It returns the instant of the next attempt and
// whether there is one.
func (p RetryPolicy) Next(attempt int, now, deadline time.Time, retryAfter time.Duration) (time.Time, bool, RetryVerdict) {
	if !now.Before(deadline) {
		return time.Time{}, false, RetryWindowClosed
	}
	// The window is checked against the ATTEMPT'S OWN schedule before the count is
	// consulted, so when both are exhausted the reason reported is the window's -
	// which is what "the window takes precedence" has to mean if it means
	// anything: the attempt that would land beyond it is the one that ends the
	// delivery, whatever retries remain.
	next := now.Add(p.Delay(attempt, retryAfter))
	if next.After(deadline) {
		return time.Time{}, false, RetryWindowClosed
	}
	if attempt > p.MaxRetries {
		return time.Time{}, false, RetryExhausted
	}
	return next, true, RetryScheduled
}
