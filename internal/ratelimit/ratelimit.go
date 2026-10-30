// Package ratelimit is the in-process token bucket behind FR-207.
//
// Scope and honest limits: this limiter lives in one core process, so N replicas
// allow N times the configured rate. That is deliberate for S1 (the requirement
// is per key and per producer, and the enrolment path needs a ceiling before the
// ingest path exists) and it is written down in SPEC.md's S1 notes, because the
// distributed limiter that enforces one rate across replicas is a Redis concern
// and belongs with the per-endpoint throttles in S5.
package ratelimit

import (
	"sync"
	"time"
)

// Limiter is a token bucket per key. It is safe for concurrent use.
type Limiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	rate    float64 // tokens per second, refilled continuously
	burst   float64 // bucket capacity; the largest instantaneous burst
	now     func() time.Time
	calls   int // used to trigger a sweep without a background goroutine
}

type bucket struct {
	tokens float64
	last   time.Time
}

// New returns a limiter refilling at perSecond with a bucket of `burst` tokens.
// A non-positive rate or burst disables the limiter (nil is also accepted by the
// middleware as "no limit"), which is what the tests and a self-hoster with a
// single slow producer want.
func New(perSecond float64, burst int) *Limiter {
	l := &Limiter{
		buckets: make(map[string]*bucket),
		rate:    perSecond,
		burst:   float64(burst),
		now:     time.Now,
	}
	if perSecond <= 0 || burst <= 0 {
		l.rate, l.burst = 0, 0
	}
	return l
}

// WithClock replaces the time source. Tests use it to advance time instead of
// sleeping; production never calls it.
func (l *Limiter) WithClock(now func() time.Time) *Limiter {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.now = now
	return l
}

// Enabled reports whether the limiter does anything.
func (l *Limiter) Enabled() bool { return l != nil && l.rate > 0 && l.burst > 0 }

// Allow takes one token from key's bucket. When the bucket is empty it reports
// how long until one token is available, which is what FR-207's Retry-After
// header must state. The minimum is one second: "Retry-After: 0" invites exactly
// the retry storm the limit exists to prevent.
func (l *Limiter) Allow(key string) (retryAfter time.Duration, ok bool) {
	if !l.Enabled() {
		return 0, true
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	b, seen := l.buckets[key]
	if !seen {
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	} else {
		elapsed := now.Sub(b.last).Seconds()
		if elapsed > 0 {
			b.tokens += elapsed * l.rate
			if b.tokens > l.burst {
				b.tokens = l.burst
			}
			b.last = now
		}
	}

	l.calls++
	if l.calls%1024 == 0 {
		l.sweepLocked(now)
	}

	if b.tokens >= 1 {
		b.tokens--
		return 0, true
	}
	missing := 1 - b.tokens
	wait := time.Duration(missing / l.rate * float64(time.Second))
	if wait < time.Second {
		wait = time.Second
	}
	return wait, false
}

// sweepLocked drops buckets that have refilled to full: they are
// indistinguishable from a key that was never seen, so forgetting them changes
// nothing and bounds the map at the number of recently active keys.
func (l *Limiter) sweepLocked(now time.Time) {
	for k, b := range l.buckets {
		if now.Sub(b.last).Seconds()*l.rate+b.tokens >= l.burst {
			delete(l.buckets, k)
		}
	}
}
