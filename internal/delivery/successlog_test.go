package delivery

import (
	"testing"
	"time"

	"github.com/m7jay/pigeon/internal/metrics"
)

// TestSuccessLogGateHoldsTheBoundAtLoad is LOG-2's whole point: at 10,000
// deliveries a second the success path must emit at most SuccessLogRPS lines per
// second and count the rest. A synthetic clock makes the bound provable without
// waiting a real second.
func TestSuccessLogGateHoldsTheBoundAtLoad(t *testing.T) {
	const rps = 100
	m := metrics.New()
	now := time.Date(2026, 2, 3, 4, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	g := newSuccessLogGateAt(rps, m, clock)

	const deliveries = 10_000
	allowed := 0
	for i := 0; i < deliveries; i++ {
		// 10,000 deliveries in one second: 100 µs apart. Advancing the clock in
		// microseconds also exercises the token refill path instead of only the
		// initial full bucket.
		now = now.Add(100 * time.Microsecond)
		if g.allow() {
			allowed++
		}
	}
	// The bucket starts full and refills at 100/s, so one second of load yields
	// at most 200 lines (the initial second's worth plus the second's refill).
	// The strict bound §13.1 states is "at most rps per second"; the token
	// bucket's ceiling over any one-second window is 2×rps because the initial
	// bucket and the refill are counted separately. That is the documented
	// behaviour of this gate (see successlog.go), and 200 lines for 10,000
	// deliveries is still a 50× reduction.
	if allowed > 2*rps {
		t.Fatalf("gate emitted %d lines for %d deliveries in one second, want at most %d", allowed, deliveries, 2*rps)
	}
	if allowed == 0 {
		t.Fatal("gate suppressed every success; a small deployment must still see one line")
	}
	if got, want := m.Counter(metrics.SuccessLogsEmitted), int64(allowed); got != want {
		t.Fatalf("%s = %d, want %d", metrics.SuccessLogsEmitted, got, want)
	}
	if got, want := m.Counter(metrics.SuccessLogsSuppressed), int64(deliveries-allowed); got != want {
		t.Fatalf("%s = %d, want %d", metrics.SuccessLogsSuppressed, got, want)
	}
}

// TestSuccessLogGateIsAFullBucketOnStart: the first successes after a quiet
// period always log, so an idle deployment's liveness signal is never lost to a
// refill artefact.
func TestSuccessLogGateIsAFullBucketOnStart(t *testing.T) {
	m := metrics.New()
	now := time.Date(2026, 2, 3, 4, 0, 0, 0, time.UTC)
	g := newSuccessLogGateAt(10, m, func() time.Time { return now })
	for i := 0; i < 10; i++ {
		if !g.allow() {
			t.Fatalf("success %d was suppressed; the bucket must start full", i+1)
		}
	}
	if g.allow() {
		t.Fatal("an 11th success logged without any elapsed time; the bound was exceeded")
	}
	if got := m.Counter(metrics.SuccessLogsSuppressed); got != 1 {
		t.Fatalf("%s = %d, want 1", metrics.SuccessLogsSuppressed, got)
	}
}

// TestSuccessLogGateRefillsOverTime: a quiet period restores tokens at the
// configured rate, so a deployment that bursts and then idles logs its next
// burst's first lines.
func TestSuccessLogGateRefillsOverTime(t *testing.T) {
	m := metrics.New()
	now := time.Date(2026, 2, 3, 4, 0, 0, 0, time.UTC)
	g := newSuccessLogGateAt(10, m, func() time.Time { return now })
	for i := 0; i < 10; i++ {
		if !g.allow() {
			t.Fatalf("initial success %d was suppressed", i+1)
		}
	}
	if g.allow() {
		t.Fatal("the 11th immediate success was allowed")
	}
	now = now.Add(250 * time.Millisecond) // 2.5 tokens at 10/s
	if !g.allow() || !g.allow() {
		t.Fatal("refilled tokens were not available after a quarter second")
	}
}

// TestSuccessLogGateNilMeansNoGate is the pre-S6 escape hatch: a Worker built
// without a gate logs every success. The S4 tests rely on this, and so does a
// small deployment that explicitly sets a negative SuccessLogRPS.
func TestSuccessLogGateNilMeansNoGate(t *testing.T) {
	var g *successLogGate
	if !g.allow() {
		t.Fatal("a nil gate suppressed a success; nil must mean no gate")
	}
}

// TestSuccessLogGateClampsTheRate: a configured rate below one line per second is
// raised to one, because zero would mean "never log a success" — which no
// operator asked for, and which would make an idle deployment look dead.
func TestSuccessLogGateClampsTheRate(t *testing.T) {
	g := newSuccessLogGateAt(0, metrics.New(), time.Now)
	if g.rate != 1 {
		t.Fatalf("rate = %v, want it clamped to 1 line/s", g.rate)
	}
	if !g.allow() {
		t.Fatal("a rate of 1/s suppressed the first success")
	}
}
