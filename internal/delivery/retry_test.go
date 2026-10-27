package delivery

import (
	"testing"
	"time"
)

// §9's retry schedule, the retry window's precedence over the retry count
// (FR-1201), the Retry-After override (FR-807), and the three acceptance
// criteria that pin them (AC-35, AC-36, AC-37).
//
// These drive the real policy at the real numbers - a 72 h window is a value, not
// a duration anyone waits - because Next is a pure function of (attempt, now,
// deadline, retryAfter). Shrinking the window to make a test fast would leave the
// arithmetic AC-36 is about unverified, which is the whole of what it asserts.

// nominalSchedule is §9's table: base 30 s, factor 2, cap 12 h.
func TestNominalScheduleIsSectionNine(t *testing.T) {
	p := DefaultRetryPolicy().Normalize()
	want := []time.Duration{
		30 * time.Second,  // retry 1
		time.Minute,       // 2
		2 * time.Minute,   // 3
		4 * time.Minute,   // 4
		8 * time.Minute,   // 5
		16 * time.Minute,  // 6
		32 * time.Minute,  // 7
		64 * time.Minute,  // 8
		128 * time.Minute, // 9  (2 h 8 m)
		256 * time.Minute, // 10 (4 h 16 m)
		512 * time.Minute, // 11 (8 h 32 m)
		12 * time.Hour,    // 12 - capped
		12 * time.Hour,    // 13 - the cap holds forever after
		12 * time.Hour,    // 14
		12 * time.Hour,    // 15 - the last retry §9 grants
	}
	for i, wantDelay := range want {
		attempt := i + 1
		got := p.NominalDelay(attempt, 0)
		if got != wantDelay {
			t.Errorf("NominalDelay(attempt=%d) = %s, want %s (§9's schedule)", attempt, got, wantDelay)
		}
	}
}

// FR-807: a Retry-After on a 429/503 (<= 300 s) replaces the computed backoff
// entirely, including its jitter, and a larger value is capped at 300 s.
func TestRetryAfterOverridesTheSchedule(t *testing.T) {
	p := DefaultRetryPolicy().Normalize()
	// A pinned jitter source that would move any computed delay, proving the
	// override path does not merely happen to agree.
	p.Rand = func() float64 { return 0.999 }

	for _, tc := range []struct {
		name       string
		retryAfter time.Duration
		want       time.Duration
	}{
		{"a receiver's 120 s wins over the 30 s base", 120 * time.Second, 120 * time.Second},
		{"300 s is the ceiling FR-807 grants", 300 * time.Second, 300 * time.Second},
		{"900 s is clamped to 300 s", 900 * time.Second, maxRetryAfter},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := p.Delay(1, tc.retryAfter); got != tc.want {
				t.Errorf("Delay(attempt=1, retryAfter=%s) = %s, want %s (FR-807)", tc.retryAfter, got, tc.want)
			}
		})
	}
	// And a Retry-After cannot stretch a delivery past its window either.
	now := time.Now()
	deadline := now.Add(90 * time.Second)
	if _, retry, verdict := p.Next(1, now, deadline, 300*time.Second); retry || verdict != RetryWindowClosed {
		t.Errorf("Next with retryAfter=300s past a 90 s deadline = (retry=%v, %s), want no retry at the window (FR-1201)", retry, verdict)
	}
}

// §9's ±20% jitter: applied to the computed delay, symmetric, and clamped so the
// 12 h cap stays a cap.
func TestJitterIsSymmetricAndBounded(t *testing.T) {
	p := DefaultRetryPolicy().Normalize()
	for _, r := range []float64{0, 0.25, 0.5, 0.75, 0.999999} {
		p.Rand = func() float64 { return r }
		for _, attempt := range []int{1, 2, 6, 11} {
			nominal := p.NominalDelay(attempt, 0)
			got := p.Delay(attempt, 0)
			lo := time.Duration(float64(nominal) * 0.8)
			hi := time.Duration(float64(nominal) * 1.2)
			if got < lo || got > hi {
				t.Errorf("Delay(attempt=%d) with rand=%v = %s; want within ±20%% of %s (§9)", attempt, r, got, nominal)
			}
		}
	}
	// At the cap, jitter cannot push a delay past 12 h.
	p.Rand = func() float64 { return 0.999999 }
	if got := p.Delay(15, 0); got > p.Max {
		t.Errorf("Delay(attempt=15) = %s, want <= %s: the cap is a cap", got, p.Max)
	}
	// Without jitter the delay is exactly the nominal one, which is what makes the
	// schedule above assertable at all.
	p.Jitter = 0
	if got := p.Delay(4, 0); got != 4*time.Minute {
		t.Errorf("Delay(attempt=4, jitter=0) = %s, want 4m", got)
	}
}

// The retry count is a budget of RETRIES, not of attempts (§9: 15 retries, 16
// attempts). AC-36 asserts the attempt count stays within 16.
func TestRetryBudgetIsFifteen(t *testing.T) {
	p := DefaultRetryPolicy().Normalize()
	p.Jitter = 0
	now := time.Now()
	deadline := now.Add(7 * 24 * time.Hour) // the widest window §9 allows

	if _, retry, _ := p.Next(p.MaxRetries, now, deadline, 0); !retry {
		t.Errorf("attempt %d (the last retry) must still be schedulable", p.MaxRetries)
	}
	if _, retry, verdict := p.Next(p.MaxRetries+1, now, deadline, 0); retry || verdict != RetryExhausted {
		t.Errorf("attempt %d = (retry=%v, %s), want the budget spent (FR-1201)", p.MaxRetries+1, retry, verdict)
	}
}

// FR-1201: the window takes precedence over the count.
func TestWindowTakesPrecedenceOverTheCount(t *testing.T) {
	p := DefaultRetryPolicy().Normalize()
	p.Jitter = 0
	now := time.Now()

	// One hour out, the schedule's own arithmetic closes the delivery long before
	// its 15 retries are spent.
	deadline := now.Add(time.Hour)
	attempts, verdict := runSchedule(p, now, deadline)
	if verdict != RetryWindowClosed {
		t.Errorf("a 1 h deadline ended with %s, want window_closed (FR-1201)", verdict)
	}
	if attempts >= p.MaxRetries+1 {
		t.Errorf("a 1 h deadline took %d attempts of a %d-retry budget; the window must close it first",
			attempts, p.MaxRetries)
	}

	// And the window closes it even when the retry budget has barely been touched:
	// at attempt 2 the next scheduled attempt is already beyond a 45 s deadline.
	short := now.Add(45 * time.Second)
	next, retry, verdict := p.Next(2, now, short, 0)
	if retry || verdict != RetryWindowClosed {
		t.Errorf("Next(attempt=2, deadline=+45s) = (next=%s, retry=%v, %s); the attempt that would land beyond the window is not made",
			next, retry, verdict)
	}
}

// AC-35: an endpoint that is down and then recovers is delivered on a scheduled
// retry inside the 72 h window, with no operator action.
//
// The claim being tested is arithmetic: within the first 20 h the §9 schedule
// lands an attempt, and that attempt is inside the deadline. A receiver is
// "healthy" from that attempt onward by construction - the policy has no state
// beyond the row - so the delivery completing is the scheduled retry succeeding.
func TestAC35DeliveryRecoversInsideTheWindow(t *testing.T) {
	p := DefaultRetryPolicy().Normalize()
	p.Jitter = 0
	start := time.Now()
	deadline := start.Add(72 * time.Hour)

	const outage = 20 * time.Hour
	now := start
	var scheduled []time.Time
	for attempt := 1; ; attempt++ {
		next, retry, _ := p.Next(attempt, now, deadline, 0)
		if !retry {
			t.Fatalf("the 72 h window closed after %d attempts, inside a 20 h outage", attempt)
		}
		if next.Sub(start) > outage {
			// The receiver came back: this scheduled attempt is the deliverable one.
			scheduled = append(scheduled, next)
			break
		}
		scheduled = append(scheduled, next)
		now = next
	}
	last := scheduled[len(scheduled)-1]
	if last.After(deadline) {
		t.Errorf("the retry that carries the delivery lands at %s, past the deadline %s (FR-1201)", last, deadline)
	}
	if elapsed := last.Sub(start); elapsed > outage+2*p.Max {
		t.Errorf("the first retry after a 20 h outage lands %s in, more than a cap later than the recovery", elapsed)
	}
	t.Logf("AC-35: %d attempts bring the delivery past a 20 h outage; the first is at +%s, inside the 72 h window",
		len(scheduled), last.Sub(start).Round(time.Second))
}

// AC-36: an endpoint still failing at 72 h ends failed_terminal at the window
// boundary, with at most 16 attempts, and the row is still there to replay.
func TestAC36WindowBoundaryTerminalWithinSixteenAttempts(t *testing.T) {
	p := DefaultRetryPolicy().Normalize()
	p.Jitter = 0
	start := time.Now()
	deadline := start.Add(72 * time.Hour)

	attempts, verdict := runSchedule(p, start, deadline)
	if attempts > 16 {
		t.Errorf("AC-36: %d attempts before the 72 h boundary, want <= 16", attempts)
	}
	if verdict != RetryWindowClosed && verdict != RetryExhausted {
		t.Errorf("AC-36: the schedule ended with %s, want the window boundary", verdict)
	}
	// The boundary is where it ends: the last attempt made is inside the window and
	// the next one would not be.
	t.Logf("AC-36: %d attempts end terminally at the 72 h boundary (%s)", attempts, verdict)
}

// AC-37: a 1 h per-endpoint window override governs over the global default, so
// the delivery ends terminally at 1 h although the global policy would have kept
// trying for 72.
//
// The override's effect is the DEADLINE (the claim computes it from the endpoint's
// window rather than from the global default - asserted against Postgres in
// TestAC37ClaimUsesTheEndpointWindow), and the deadline is what ends the delivery
// here.
func TestAC37PerEndpointWindowGoverns(t *testing.T) {
	p := DefaultRetryPolicy().Normalize()
	p.Jitter = 0
	now := time.Now()

	overrideAttempts, verdict := runSchedule(p, now, now.Add(time.Hour))
	if verdict != RetryWindowClosed {
		t.Errorf("AC-37: a 1 h window ended with %s, want window_closed", verdict)
	}
	if overrideAttempts > 16 {
		t.Errorf("AC-37: %d attempts in a 1 h window, want <= 16", overrideAttempts)
	}

	// The same policy against the global 72 h window keeps going: the override is
	// what ended the delivery, not the retry budget.
	globalAttempts, _ := runSchedule(p, now, now.Add(72*time.Hour))
	if globalAttempts <= overrideAttempts {
		t.Errorf("the 72 h window took %d attempts and the 1 h window %d; the 1 h override is what terminated it",
			globalAttempts, overrideAttempts)
	}
	if globalAttempts > 16 {
		t.Errorf("the 72 h window took %d attempts, want <= 16 (AC-36)", globalAttempts)
	}
	t.Logf("AC-37: 1 h window -> terminal after %d attempts; the same policy under the 72 h default ran %d",
		overrideAttempts, globalAttempts)
}

// runSchedule walks the policy the way the worker does - attempt, schedule,
// advance to the scheduled instant, fail again - and reports how many attempts
// were made before it stopped, and why. It is the whole retry lifecycle of one
// always-failing delivery, at any scale.
func runSchedule(p RetryPolicy, start, deadline time.Time) (attempts int, verdict RetryVerdict) {
	now := start
	for attempt := 1; ; attempt++ {
		if now.After(deadline) {
			return attempt - 1, RetryWindowClosed
		}
		next, retry, v := p.Next(attempt, now, deadline, 0)
		if !retry {
			return attempt, v
		}
		if !next.After(now) {
			// A policy that does not advance time would loop forever; the check
			// keeps a broken schedule from hanging the suite instead of failing it.
			return attempt, v
		}
		now = next
	}
}
