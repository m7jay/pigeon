package ratelimit

import (
	"sync"
	"testing"
	"time"
)

// frozenClock is a manually advanced clock, so the test asserts the arithmetic
// rather than sleeping and hoping.
type frozenClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *frozenClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *frozenClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newClock() *frozenClock {
	return &frozenClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

// TestBurstThenRetryAfter encodes FR-207's shape: a key may spend its burst
// immediately, the next request is refused, and the reported wait is the time
// until a token is actually available.
func TestBurstThenRetryAfter(t *testing.T) {
	clock := newClock()
	l := New(2, 3).WithClock(clock.Now)

	for i := 0; i < 3; i++ {
		if wait, ok := l.Allow("key_a"); !ok {
			t.Fatalf("call %d refused with %v wait; burst of 3 should allow it", i+1, wait)
		}
	}
	wait, ok := l.Allow("key_a")
	if ok {
		t.Fatal("fourth call allowed; the burst limit is not enforced")
	}
	if wait < 400*time.Millisecond || wait > time.Second {
		t.Fatalf("Retry-After = %v, want about 500ms at 2 tokens/s", wait)
	}

	clock.Advance(500 * time.Millisecond)
	if wait, ok := l.Allow("key_a"); !ok {
		t.Fatalf("after half a second at 2/s a token should exist (wait %v)", wait)
	}
}

// TestBucketsAreIndependent is the "per key" half of FR-207: one key exhausting
// its budget must not consume another key's.
func TestBucketsAreIndependent(t *testing.T) {
	clock := newClock()
	l := New(1, 1).WithClock(clock.Now)

	if _, ok := l.Allow("key_a"); !ok {
		t.Fatal("first key refused its first request")
	}
	if _, ok := l.Allow("key_a"); ok {
		t.Fatal("second request from the same key allowed at 1/s")
	}
	if _, ok := l.Allow("key_b"); !ok {
		t.Fatal("a different key was refused because another one was busy")
	}
}

// TestRetryAfterIsNeverZero guards the header value a client actually schedules
// on: returning 0 turns a 429 into a tight retry loop.
func TestRetryAfterIsNeverZero(t *testing.T) {
	clock := newClock()
	// 1000/s means the honest wait is 1ms, which must still report >= 1s.
	l := New(1000, 1).WithClock(clock.Now)
	l.Allow("k")
	wait, ok := l.Allow("k")
	if ok {
		t.Fatal("over-limit request allowed")
	}
	if wait < time.Second {
		t.Fatalf("Retry-After = %v, want at least 1s", wait)
	}
}

// TestDisabledLimiterAllowsEverything keeps the "no limit" configuration honest.
func TestDisabledLimiterAllowsEverything(t *testing.T) {
	var nilLimiter *Limiter
	if _, ok := nilLimiter.Allow("k"); !ok {
		t.Fatal("a nil limiter refused a request")
	}
	zero := New(0, 0)
	for i := 0; i < 100; i++ {
		if _, ok := zero.Allow("k"); !ok {
			t.Fatalf("a disabled limiter refused request %d", i)
		}
	}
}

// TestSweepKeepsRecentlyUsedBuckets makes sure the map does not grow without
// bound while a key is still being limited.
func TestSweepKeepsRecentlyUsedBuckets(t *testing.T) {
	clock := newClock()
	l := New(1, 1).WithClock(clock.Now)
	l.Allow("busy")
	l.Allow("busy") // now empty
	for i := 0; i < 1100; i++ {
		l.Allow("key_" + string(rune('a'+i%26)))
	}
	// "busy" was swept only if it had refilled; it has not (no time passed), so
	// it must still be limited.
	if _, ok := l.Allow("busy"); ok {
		t.Fatal("a bucket that is still empty was swept away, resetting its limit")
	}
}
