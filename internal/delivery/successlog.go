package delivery

import (
	"sync"
	"time"

	"github.com/m7jay/pigeon/internal/metrics"
)

// successLogGate is LOG-2's bound on per-success log lines.
//
// §13.1 is explicit that a service delivering thousands of requests a second
// cannot log a line per success: that is thousands of lines a second of its own
// I/O problem, and it buries the failures the logs exist to surface. So the
// success path logs at most SuccessLogRPS lines per second (100 by default) and
// counts the rest — below the bound every success still logs, which is what a
// small deployment wants. Failures and state transitions are NOT gated: LOG-2
// requires those at warn, always.
type successLogGate struct {
	mu     sync.Mutex
	rate   float64   // lines per second
	tokens float64   // available lines
	last   time.Time // when tokens were last refilled
	now    func() time.Time
	// metrics counts both sides of the gate. The emitted/suppressed split is the
	// evidence that the bound held, which is what AC-33 asks for.
	metrics *metrics.Metrics
}

// newSuccessLogGate builds a gate with a full bucket, so the first successes
// after a quiet period always log.
func newSuccessLogGate(perSecond int, m *metrics.Metrics) *successLogGate {
	return newSuccessLogGateAt(perSecond, m, time.Now)
}

// newSuccessLogGateAt is the injectable-clock form the tests drive, so the bound
// can be asserted at a synthesized 10,000 deliveries/s without waiting for a
// real second to pass.
func newSuccessLogGateAt(perSecond int, m *metrics.Metrics, now func() time.Time) *successLogGate {
	if perSecond < 1 {
		perSecond = 1
	}
	t := now()
	return &successLogGate{
		rate:    float64(perSecond),
		tokens:  float64(perSecond),
		last:    t,
		now:     now,
		metrics: m,
	}
}

// allow reports whether this success may be logged, counting it either way. It
// is a token bucket: the bucket holds one second's worth of lines and refills at
// the configured rate, so a burst cannot exceed the bound over any second and an
// idle period restores a full second's worth.
func (g *successLogGate) allow() bool {
	if g == nil {
		// No gate means the operator asked for no light: every success logs. This
		// is the pre-S6 behaviour, kept for callers that construct a Worker
		// directly.
		return true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.now()
	if elapsed := now.Sub(g.last); elapsed > 0 {
		g.tokens += elapsed.Seconds() * g.rate
		if g.tokens > g.rate {
			g.tokens = g.rate
		}
		g.last = now
	}
	if g.tokens >= 1 {
		g.tokens--
		g.metrics.Inc(metrics.SuccessLogsEmitted)
		return true
	}
	g.metrics.Inc(metrics.SuccessLogsSuppressed)
	return false
}
