package delivery

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// TestAttemptFanOutIsBounded is AC-34's census, in three legs.
//
// AC-34 asks that 2,500 concurrent attempts on a 2 vCPU machine stay bounded by
// "the pool plus the per-endpoint caps", with file descriptors inside the
// documented ulimit and the CPU largely idle. The thing that makes that true is a
// bound, so that is what this asserts: the concurrent attempt count - and the
// goroutines, sockets and CPU behind it - is a function of the caps, not of the
// offered load.
//
// Legs 1 and 2 are the comparison: one endpoint capped at FR-1302's documented 10
// concurrent is offered two backlogs six times apart, and the peak goroutine
// count and peak open file descriptor count must not follow the load. A goroutine
// or a descriptor per offered delivery would make the second run's figures six
// times the first's, and that is what this fails on. The figures do track
// *concurrency* - more attempts in flight is more goroutines and more sockets, and
// that is correct - but concurrency is itself capped, so the growth is bounded by
// the cap rather than by the queue behind it.
//
// Leg 3 is the criterion's own number rather than a proxy for it: 2,500 due
// deliveries offered at once to the same capped endpoint. It exists because the
// earlier version of this test argued that the literal figure "needs the soft
// ulimit raised first, roughly 5,000 descriptors for 2,500 in-flight attempts" -
// which is the goroutine-per-offered-delivery model this test exists to refute.
// If the footprint is a function of the caps it needs no raise at all, and the
// measurement says so: the leg reports the process's RLIMIT_NOFILE, the peak
// descriptors and the CPU cores used. It also raises the soft limit to the hard
// limit itself (RLIMIT_NOFILE is self-raisable without privilege) so that "the
// limit was raised" is a fact in the log rather than a claim in a document - and
// then shows the peak is far under both.
//
// The peak in-flight count is asserted to stay at or under the endpoint's cap and to
// overlap more than one attempt, so a run cannot pass by never running concurrently
// at all. Whether the cap is *reached* is logged rather than asserted: on a loaded
// box the single claim loop, not the cap, is the binding constraint, and the cap's
// exact behaviour has its own test
// (TestAC41ConcurrencyCapIsIdenticalAtOneAndTwoReplicas).
//
// Why the figures are read from this process: the worker under test runs inside the
// test binary, which is where a real replica's goroutines and descriptors live.
// AC-34's 2,500-attempt figure describes concurrency, not throughput, so it does
// not need a machine that can serve 2,500 deliveries a second - the caps hold at any
// offered load, which is the point.
func TestAttemptFanOutIsBounded(t *testing.T) {
	const (
		concurrencyCap = 10 // FR-1302's documented per-endpoint default
		processCap     = 4 * concurrencyCap
		eventType      = "order.created"
		// One second of service time for the comparative legs, not 200 ms. The
		// worker's claim loop is serialised on database round-trips, and on a
		// loaded box those round-trips are tens of milliseconds each; with a short
		// service time the loop cannot refill the endpoint's slots fast enough and
		// the observed concurrency measures the box's latency rather than the cap.
		// A service time several times the claim latency is what lets the cap be
		// the binding constraint, which is the property under test. (The cap itself
		// is also asserted exactly by TestAC41ConcurrencyCapIsIdenticalAtOneAndTwoReplicas.)
		comparativeService = time.Second
		// The literal leg's service time is short because 2,500 deliveries are
		// offered: at the comparative second each delivery would take 250 s at the
		// endpoint's cap of 10 and the leg would be measuring the test's patience.
		// 50 ms keeps the run in the tens of seconds while leaving the service time
		// several times the claim latency, so the cap still binds.
		literalService = 50 * time.Millisecond
		smallBacklog   = 20   //  2x the endpoint's cap: enough to saturate it
		largeBacklog   = 120  // 12x the cap, and 6x the small run
		literalBacklog = 2500 // AC-34's own figure
		// Slack on the comparative assertions. Not zero because a low pass from the
		// sampler, a GC cycle and the connection pool warming up are all real, and
		// none of them scales with the backlog; a leak would blow past this by 6x
		// (leg 2) or 20x (leg 3).
		slack = 40
	)

	// The receiver's service time is per-leg state; the handler reads it atomically
	// because the legs share one server.
	var service atomic.Int64
	service.Store(int64(comparativeService))

	var counter inFlightCounter
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// The receiver holds each request for a fixed service time, so the number
		// of handlers running at once IS the number of attempts the endpoint is
		// serving at once. (Blocking until the test releases the handler instead
		// would keep counting after a client timeout tore a request down, and would
		// then be measuring the test rather than the cap.)
		counter.enter()
		time.Sleep(time.Duration(service.Load()))
		counter.leave()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	f := newS5Fixture(t, srv.URL, eventType)
	f.setEndpoint(t, `max_concurrency = 10`)

	baseline := runtime.NumGoroutine()

	census := func(backlog int, serviceTime time.Duration) figures {
		t.Helper()
		service.Store(int64(serviceTime))
		counter.reset()

		// Enqueue the WHOLE backlog before the worker starts. Publishing while it
		// drains would measure the arrival rate rather than the cap: the due set
		// would never be bigger than a handful of rows, so in-flight requests
		// would never reach the limit and the assertion below would be vacuous.
		//
		// The ingest half of the leg is counted separately: it is CPU the run
		// spends before any attempt exists, and AC-34's "CPU largely idle" is about
		// the attempt fan-out.
		for i := 0; i < backlog; i++ {
			f.publish(t, eventType, `{"id":"bound"}`)
		}

		// Sample this replica's own footprint while the load runs. Only the sampler
		// writes the maxima and the test reads them after sampler.Wait(), so the
		// handoff needs no lock.
		stop := make(chan struct{})
		var sampler sync.WaitGroup
		sampler.Add(1)
		maxGoroutines, maxFDs := 0, 0
		var cpuCores float64
		started := time.Now()
		cpuAtStart := processCPUSeconds()
		go func() {
			defer sampler.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if n := runtime.NumGoroutine(); n > maxGoroutines {
					maxGoroutines = n
				}
				if n := openFileDescriptors(); n > maxFDs {
					maxFDs = n
				}
				cpuCores = (processCPUSeconds() - cpuAtStart) / time.Since(started).Seconds()
				time.Sleep(10 * time.Millisecond)
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		w := New(f.st, f.options(func(o *Options) {
			// Backpressure well above the endpoint's cap: the assertion is that the
			// ENDPOINT's limit binds, and that the process cap is a ceiling rather
			// than the authority (FR-816).
			o.Concurrency = processCap
			// FR-817: the per-endpoint transport pool is the endpoint's cap. Leaving
			// testOptions' 1 here would make the transport, not the limiter, the
			// thing under measurement.
			o.MaxConnsPerHost = concurrencyCap
			o.Limits = f.limits
			o.PollInterval = time.Millisecond
		}))
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = w.Run(ctx)
		}()

		// Wait for the receiver to have served every attempt AND the worker to have
		// finished every delivery, so the maxima cover the whole load rather than
		// the moment the last publish returned.
		deadline := time.Now().Add(5 * time.Minute)
		for time.Now().Before(deadline) {
			cur, _, started := counter.snapshot()
			if cur == 0 && started >= backlog && f.pendingDeliveries(t) == 0 {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		close(stop)
		sampler.Wait()
		_, peak, attemptsServed := counter.snapshot()
		if pending := f.pendingDeliveries(t); pending != 0 {
			t.Fatalf("offered %d: %d deliveries still pending after 5m", backlog, pending)
		}
		cancel()
		wg.Wait()
		// An aborted attempt can tear the client side down while a handler is still
		// working; let the receiver quiesce so the next run's counter starts clean.
		drain := time.Now().Add(5 * time.Second)
		for time.Now().Before(drain) {
			if cur, _, _ := counter.snapshot(); cur == 0 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		return figures{
			backlog: backlog, inFlight: peak, goroutines: maxGoroutines,
			fds: maxFDs, attempts: attemptsServed, cpuCores: cpuCores,
		}
	}

	seen := map[int]figures{}
	record := func(fig figures) {
		seen[fig.backlog] = fig
		t.Logf("offered %d deliveries: peak in-flight %d, peak goroutines %d, peak fds %d, attempts served %d, CPU %.2f cores",
			fig.backlog, fig.inFlight, fig.goroutines, fig.fds, fig.attempts, fig.cpuCores)

		if fig.inFlight > concurrencyCap {
			t.Errorf("offered %d: %d attempts in flight for an endpoint capped at %d (FR-1302)",
				fig.backlog, fig.inFlight, concurrencyCap)
		}
		if fig.inFlight < 2 {
			t.Errorf("offered %d: peak in-flight was %d; the load never overlapped more than one attempt, so this run says nothing about concurrency",
				fig.backlog, fig.inFlight)
		}
		if fig.inFlight < concurrencyCap {
			// Not a failure: the cap is an upper bound, and a box slow enough that
			// the claim loop cannot refill it measures the box rather than the cap.
			// Logged rather than asserted, and the cap's exact behaviour has its own
			// test (TestAC41ConcurrencyCapIsIdenticalAtOneAndTwoReplicas).
			t.Logf("offered %d: peak in-flight %d did not reach the %d cap; the dispatch loop, not the cap, was the binding constraint in this run",
				fig.backlog, fig.inFlight, concurrencyCap)
		}
		if fig.attempts < fig.backlog {
			t.Errorf("offered %d: only %d attempts were served", fig.backlog, fig.attempts)
		}
		// An absolute ceiling on every leg, so a run in which BOTH footprints are
		// broken fails rather than agreeing with itself.
		if fig.goroutines > baseline+processCap+128 {
			t.Errorf("offered %d: %d goroutines with a process cap of %d and a baseline of %d; the pool bound is not holding",
				fig.backlog, fig.goroutines, processCap, baseline)
		}
		if fig.fds > 512 {
			t.Errorf("offered %d: %d open file descriptors, which a default ulimit -n of 1024 would not survive alongside the rest of a replica",
				fig.backlog, fig.fds)
		}
	}

	record(census(smallBacklog, comparativeService))
	record(census(largeBacklog, comparativeService))

	// AC-34's literal figure, and the limit AC-34's third clause is about. Both are
	// reported: nothing here turns on the raise succeeding, because the assertion is
	// that the footprint never approaches either value.
	before, after := raiseSoftNoFile(t)
	t.Logf("RLIMIT_NOFILE soft/hard: %d/%d before the literal leg, %d/%d for it", before.Cur, before.Max, after.Cur, after.Max)
	record(census(literalBacklog, literalService))

	small, large, literal := seen[smallBacklog], seen[largeBacklog], seen[literalBacklog]

	// Six times the work, the same footprint. This is AC-34's bound, comparatively.
	if large.goroutines > small.goroutines+slack {
		t.Errorf("AC-34: peak goroutines %d when offering %d deliveries against %d when offering %d; the count tracks the backlog instead of the caps (FR-816)",
			large.goroutines, largeBacklog, small.goroutines, smallBacklog)
	}
	if large.fds > small.fds+slack {
		t.Errorf("AC-34: peak open file descriptors %d when offering %d deliveries against %d when offering %d; descriptors track the backlog instead of the caps",
			large.fds, largeBacklog, small.fds, smallBacklog)
	}
	// Twenty times the work again, and still the same footprint: this is the leg
	// that refutes "2,500 in-flight attempts hold roughly 5,000 descriptors".
	if literal.goroutines > large.goroutines+slack {
		t.Errorf("AC-34: peak goroutines %d when offering the criterion's literal %d deliveries against %d when offering %d; the count tracks the backlog instead of the caps",
			literal.goroutines, literalBacklog, large.goroutines, largeBacklog)
	}
	if literal.fds > large.fds+slack {
		t.Errorf("AC-34: peak open file descriptors %d when offering the criterion's literal %d deliveries against %d when offering %d; descriptors track the backlog instead of the caps",
			literal.fds, literalBacklog, large.fds, largeBacklog)
	}
	// "FD usage within the documented ulimit": the literal leg's peak, against the
	// actual limit it ran under. This is the clause that is about the operator.
	if literal.fds > int(after.Cur) {
		t.Errorf("AC-34: %d open descriptors inside an RLIMIT_NOFILE of %d", literal.fds, after.Cur)
	}
	// "CPU largely idle". Half a core, against a 2 vCPU reference machine: the
	// fan-out is I/O-bound, so a figure near the cap would mean the run was
	// measuring the box's CPU rather than the delivery path.
	if literal.cpuCores > 0.5 {
		t.Errorf("AC-34: the attempt fan-out used %.2f cores on a 2 vCPU reference machine; the capped fan-out is supposed to leave the CPU largely idle",
			literal.cpuCores)
	}
}

// figures is one leg's census.
type figures struct {
	backlog, inFlight, goroutines, fds, attempts int
	cpuCores                                     float64
}

// raiseSoftNoFile lifts this process's soft RLIMIT_NOFILE to the hard limit and
// returns the limit before and after. It cannot fail usefully: an unprivileged
// process may always raise its soft limit up to the hard one, and if the raise is
// refused the measurement is still valid - the test then reports the limit it
// actually ran under.
func raiseSoftNoFile(t *testing.T) (before, after syscall.Rlimit) {
	t.Helper()
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &before); err != nil {
		t.Logf("RLIMIT_NOFILE could not be read: %v", err)
		return before, before
	}
	raised := before
	raised.Cur = raised.Max
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &raised); err != nil {
		t.Logf("RLIMIT_NOFILE could not be raised from %d to %d: %v", before.Cur, before.Max, err)
		return before, before
	}
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &after); err != nil {
		return before, raised
	}
	return before, after
}

// processCPUSeconds is this process's user+system CPU time in seconds, read from
// /proc/self/stat. A read failure (a non-Linux host) reports 0, which makes the
// CPU assertion vacuous rather than failing on the platform.
func processCPUSeconds() float64 {
	raw, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return 0
	}
	// The comm field (2) is parenthesised and can contain spaces, so the fields
	// after it are counted from the last ')'.
	s := string(raw)
	idx := strings.LastIndex(s, ")")
	if idx < 0 {
		return 0
	}
	fields := strings.Fields(s[idx+1:])
	// After comm, field 1 is state; utime and stime are fields 14 and 15 of the
	// whole line, so indexes 11 and 12 here (0-based, state = 0).
	if len(fields) < 13 {
		return 0
	}
	utime, err1 := strconv.ParseFloat(fields[11], 64)
	stime, err2 := strconv.ParseFloat(fields[12], 64)
	if err1 != nil || err2 != nil {
		return 0
	}
	const clockTicksPerSecond = 100 // USER_HZ on Linux/amd64
	return (utime + stime) / clockTicksPerSecond
}

// openFileDescriptors counts this process's open descriptors. AC-34's "FD usage
// within the documented ulimit" is about this number; a read failure (a non-Linux
// host) reports 0 rather than failing the test.
func openFileDescriptors() int {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0
	}
	return len(entries)
}

// pendingDeliveries counts the deliveries the worker has not finished, so the
// census can wait for its backlog to drain instead of guessing at a sleep.
func (f *s5Fixture) pendingDeliveries(t *testing.T) int {
	t.Helper()
	var n int
	if err := f.admin.QueryRow(context.Background(),
		`select count(*) from deliveries where status='pending'`).Scan(&n); err != nil {
		t.Fatalf("count pending deliveries: %v", err)
	}
	return n
}
