package delivery

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestAttemptFanOutIsBounded is AC-34's census.
//
// AC-34 asks that 2,500 concurrent attempts on a 2 vCPU machine stay bounded by
// "the pool plus the per-endpoint caps", with file descriptors inside the
// documented ulimit and the CPU largely idle. The thing that makes that true is a
// bound, so that is what this asserts: the concurrent attempt count - and the
// goroutines and sockets behind it - is a function of the caps, not of the offered
// load.
//
// Hence a comparison rather than a magic number. One endpoint capped at FR-1302's
// documented 10 concurrent is offered two backlogs six times apart, and the peak
// goroutine count and peak open file descriptor count must not follow the load: a
// goroutine or a descriptor per offered delivery would make the second run's
// figures six times the first's, and that is what this fails on. The figures do
// track *concurrency* - more attempts in flight is more goroutines and more sockets,
// and that is correct - but concurrency is itself capped, so the growth is bounded
// by the cap rather than by the queue behind it.
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
		// One second of service time, not 200 ms. The worker's claim loop is
		// serialised on database round-trips, and on a loaded box those round-trips
		// are tens of milliseconds each; with a short service time the loop cannot
		// refill the endpoint's slots fast enough and the observed concurrency
		// measures the box's latency rather than the cap. A service time several
		// times the claim latency is what lets the cap be the binding constraint,
		// which is the property under test. (The cap itself is also asserted
		// exactly by TestAC41ConcurrencyCapIsIdenticalAtOneAndTwoReplicas.)
		serviceTime  = time.Second
		smallBacklog = 20  //  2x the endpoint's cap: enough to saturate it
		largeBacklog = 120 // 12x the cap, and 6x the small run
		// Slack on the comparative assertions. Not zero because a low pass from the
		// sampler, a GC cycle and the connection pool warming up are all real, and
		// none of them scales with the backlog; a leak would blow past this by 6x.
		slack = 40
	)

	var counter inFlightCounter
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// The receiver holds each request for a fixed service time, so the number
		// of handlers running at once IS the number of attempts the endpoint is
		// serving at once. (Blocking until the test releases the handler instead
		// would keep counting after a client timeout tore a request down, and would
		// then be measuring the test rather than the cap.)
		counter.enter()
		time.Sleep(serviceTime)
		counter.leave()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	f := newS5Fixture(t, srv.URL, eventType)
	f.setEndpoint(t, `max_concurrency = 10`)

	baseline := runtime.NumGoroutine()

	census := func(backlog int) (inFlight, goroutines, fds, attempts int) {
		t.Helper()
		counter.reset()

		// Enqueue the WHOLE backlog before the worker starts. Publishing while it
		// drains would measure the arrival rate rather than the cap: the due set
		// would never be bigger than a handful of rows, so in-flight requests
		// would never reach the limit and the assertion below would be vacuous.
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
		deadline := time.Now().Add(3 * time.Minute)
		for time.Now().Before(deadline) {
			cur, _, started := counter.snapshot()
			if cur == 0 && started >= backlog && f.pendingDeliveries(t) == 0 {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		close(stop)
		sampler.Wait()
		_, peak, started := counter.snapshot()
		if pending := f.pendingDeliveries(t); pending != 0 {
			t.Fatalf("offered %d: %d deliveries still pending after 3m", backlog, pending)
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
		return peak, maxGoroutines, maxFDs, started
	}

	type figures struct{ inFlight, goroutines, fds, attempts int }
	seen := map[int]figures{}
	for _, backlog := range []int{smallBacklog, largeBacklog} {
		inFlight, goroutines, fds, attempts := census(backlog)
		seen[backlog] = figures{inFlight, goroutines, fds, attempts}
		t.Logf("offered %d deliveries: peak in-flight %d, peak goroutines %d, peak fds %d, attempts served %d",
			backlog, inFlight, goroutines, fds, attempts)

		if inFlight > concurrencyCap {
			t.Errorf("offered %d: %d attempts in flight for an endpoint capped at %d (FR-1302)",
				backlog, inFlight, concurrencyCap)
		}
		if inFlight < 2 {
			t.Errorf("offered %d: peak in-flight was %d; the load never overlapped more than one attempt, so this run says nothing about concurrency",
				backlog, inFlight)
		}
		if inFlight < concurrencyCap {
			// Not a failure: the cap is an upper bound, and a box slow enough that
			// the claim loop cannot refill it measures the box rather than the cap.
			// Logged rather than asserted, and the cap's exact behaviour has its own
			// test (TestAC41ConcurrencyCapIsIdenticalAtOneAndTwoReplicas).
			t.Logf("offered %d: peak in-flight %d did not reach the %d cap; the dispatch loop, not the cap, was the binding constraint in this run",
				backlog, inFlight, concurrencyCap)
		}
		if attempts < backlog {
			t.Errorf("offered %d: only %d attempts were served", backlog, attempts)
		}
	}

	small, large := seen[smallBacklog], seen[largeBacklog]

	// Six times the work, the same footprint. This is AC-34's bound.
	if large.goroutines > small.goroutines+slack {
		t.Errorf("AC-34: peak goroutines %d when offering %d deliveries against %d when offering %d; the count tracks the backlog instead of the caps (FR-816)",
			large.goroutines, largeBacklog, small.goroutines, smallBacklog)
	}
	if large.fds > small.fds+slack {
		t.Errorf("AC-34: peak open file descriptors %d when offering %d deliveries against %d when offering %d; descriptors track the backlog instead of the caps",
			large.fds, largeBacklog, small.fds, smallBacklog)
	}
	// An absolute ceiling too, so a run in which BOTH footprints are broken fails
	// rather than agreeing with itself.
	if large.goroutines > baseline+processCap+128 {
		t.Errorf("AC-34: %d goroutines with a process cap of %d and a baseline of %d; the pool bound is not holding",
			large.goroutines, processCap, baseline)
	}
	if large.fds > 512 {
		t.Errorf("AC-34: %d open file descriptors, which a default ulimit -n of 1024 would not survive alongside the rest of a replica", large.fds)
	}
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
