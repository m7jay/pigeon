// Command loadgen measures a Pigeon instance against the load protocol in §16 of
// REQUIREMENTS.md: 5 minutes of warm-up excluded from the measurement, 30 minutes
// measured, pass criteria evaluated over the last 10 minutes, the generator outside
// the machine under test, and the accepted count reconciled against `events` and
// `outbox` rows rather than taken from the generator's own counter.
//
// It reports BOTH units the capacity model is stated in - events/s accepted at the
// edge and deliveries/s written to the fan-out - because an event is not a message:
// with N subscriptions one accepted event becomes N deliveries, and a claim quoted
// in only one of the two units means whatever the reader assumes.
//
// Usage:
//
//	loadgen run   -base http://127.0.0.1:8080 -key "$KEY" -event-type order.created \
//	              -rate 250 -subs 10 -db "$DSN" -label 2vCPU-4GB -out report.json
//	loadgen compare -label 1-replica.json -label 2-replica.json -label 4-replica.json
//
// `run` prints a human report and, with -out, the same report as JSON. `compare`
// reads three or more reports and applies AC-43: throughput must scale at least 80%
// per doubling of replicas while the write path has headroom, and must stop improving
// once it does not.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	// §8.1's reference payload is 700 B on the wire for the row-level model, and
	// §8.1.1 scales it to 950 B for unreliable endpoints. The generator sends the
	// reference size unless told otherwise so the numbers line up with the table.
	referencePayloadBytes = 700
	// maxSamplesPerPhase bounds the retained latencies. 20M samples is 160 MB,
	// past which the run is measuring the generator's RAM rather than the
	// instance; the harness says so rather than silently swapping.
	maxSamplesPerPhase = 20_000_000
)

type config struct {
	BaseURL        string
	Key            string
	EventType      string
	ConsumerRef    string
	Rate           float64
	PayloadBytes   int
	Warmup         time.Duration
	Measure        time.Duration
	Evaluate       time.Duration
	Subs           int
	DB             string
	Label          string
	Out            string
	Concurrency    int
	RequestTimeout time.Duration
	// Settle is how long to wait after the last publish before reconciling. The
	// relay fans out asynchronously, so a report that reads the database the
	// instant the generator stops counts deliveries that have not been written
	// yet: the measured delivery unit would then be a statement about the drain
	// speed of the relay rather than about the instance's fan-out.
	Settle time.Duration
	// Class is the §8.1 machine class the run declares for the machine under test
	// (-class). Empty means the run claims none, which leaves AC-30's third clause
	// ungradeable: see class.go.
	Class string
	// SUTCPUs and SUTMemGiB declare the machine under test (-sut-cpus,
	// -sut-mem-gib): the machine the class is claimed for. §12 puts the generator
	// off the machine under test, so on a gradeable run the machine block describes
	// the *generator's* box and the machine being graded has to be stated. Zero
	// means undeclared, which is only coherent when the generator shares the machine
	// under test - then the harness can see it.
	SUTCPUs   int
	SUTMemGiB float64
	// GeneratorOnSUT is the §12 protocol fact - whether the generator shared the
	// machine under test - as auto, yes or no. auto resolves it from -base.
	GeneratorOnSUT string
	// Claim is the class claim classPreflight built, carried into the report.
	Claim *classClaim
	// SUT is the machine under test classPreflight resolved, carried into the
	// report as machine_under_test. It is what `grade` checks the class against, and
	// it is the only source for the machine block of the machine actually under
	// test when the generator ran elsewhere.
	SUT *machine
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "run":
		err = cmdRun(os.Args[2:])
	case "max":
		err = cmdMax(os.Args[2:])
	case "compare":
		err = cmdCompare(os.Args[2:])
	case "grade":
		err = cmdGrade(os.Args[2:])
	case "seed":
		err = cmdSeed(os.Args[2:])
	case "sink":
		err = cmdSink(os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `loadgen - Pigeon load generator (§16 load protocol)

  loadgen run     -base URL -key KEY -event-type NAME -rate N [-subs N] [-db DSN] [-out FILE] [-class NAME]
  loadgen max     -base URL -key KEY -event-type NAME [-db DSN] [-out FILE]
  loadgen grade   -label FILE [-out FILE] [-class NAME]
  loadgen compare -label FILE -label FILE [-label FILE ...]

run flags:
  -base            instance base URL (default http://127.0.0.1:8080)
  -key             producer API key (or PIGEON_LOADGEN_KEY)
  -event-type      event type to publish
  -consumer-ref    optional consumer_ref to route to (default: all subscribers)
  -rate            target events/s
  -payload-bytes   payload size (default 700, §8.1)
  -warmup          excluded warm-up (default 5m)
  -measure         measured phase (default 30m)
  -evaluate        pass-criteria window at the end of the measurement (default 10m)
  -subs            subscriptions per event, for the delivery unit (default 1; -class grades at 10, §8.1)
  -db              Postgres DSN for reconciliation; omit to skip it
  -label           name for this run, e.g. "2vCPU-4GB-2replicas"
  -out             write the JSON report here
  -concurrency     in-flight requests (default 64)
  -class           declare the §8.1 machine class this run claims for the machine under test
                   (2vCPU-2GB, 2vCPU-4GB, 4vCPU-8GB, 8vCPU-16GB); required for AC-30's third
                   clause, which "grade" decides
  -sut-cpus        the machine under test's vCPU count, when the generator runs off it (§12)
  -sut-mem-gib     the machine under test's RAM in GiB; -sut-cpus and -sut-mem-gib are given
                   together and are recorded as machine_under_test, which is the machine the
                   declared class is checked against
  -generator-on-sut  auto|yes|no - did the generator share the machine under test? (§12; auto
                   resolves it from -base)

grade flags:
  -label           the JSON report "run -class" wrote; the exit status is clause 3's verdict
  -out             write the verdict JSON here
  -class           assert the artifact declares this class

"compare" is AC-43's replica-scaling comparison; "grade" is AC-30 clause 3's.
"max" ramps the offered rate on this machine until service pressure or a generator
limit is reached; it writes a ramp summary JSON artifact.
`)
}

func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	var c config
	fs.StringVar(&c.BaseURL, "base", "http://127.0.0.1:8080", "instance base URL")
	fs.StringVar(&c.Key, "key", os.Getenv("PIGEON_LOADGEN_KEY"), "producer API key")
	fs.StringVar(&c.EventType, "event-type", "", "event type to publish")
	fs.StringVar(&c.ConsumerRef, "consumer-ref", "", "consumer_ref to route to")
	fs.Float64Var(&c.Rate, "rate", 0, "target events/s")
	fs.IntVar(&c.PayloadBytes, "payload-bytes", referencePayloadBytes, "payload size in bytes")
	fs.DurationVar(&c.Warmup, "warmup", 5*time.Minute, "excluded warm-up")
	fs.DurationVar(&c.Measure, "measure", 30*time.Minute, "measured phase")
	fs.DurationVar(&c.Evaluate, "evaluate", 10*time.Minute, "pass-criteria window")
	fs.IntVar(&c.Subs, "subs", 1, "subscriptions per event")
	fs.StringVar(&c.DB, "db", os.Getenv("PIGEON_LOADGEN_DB"), "Postgres DSN for reconciliation")
	fs.StringVar(&c.Label, "label", "", "name for this run")
	fs.StringVar(&c.Out, "out", "", "write the JSON report here")
	fs.IntVar(&c.Concurrency, "concurrency", 64, "in-flight requests")
	fs.DurationVar(&c.RequestTimeout, "timeout", 10*time.Second, "per-request timeout")
	fs.DurationVar(&c.Settle, "settle", 60*time.Second, "wait after the last publish before reconciling (relay fan-out is asynchronous)")
	fs.StringVar(&c.Class, "class", "", "§8.1 machine class this run claims (AC-30 clause 3)")
	fs.IntVar(&c.SUTCPUs, "sut-cpus", 0, "vCPU count of the machine under test (with -sut-mem-gib)")
	fs.Float64Var(&c.SUTMemGiB, "sut-mem-gib", 0, "RAM in GiB of the machine under test (with -sut-cpus)")
	fs.StringVar(&c.GeneratorOnSUT, "generator-on-sut", "auto", "auto|yes|no: did the generator share the machine under test? (§12)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if c.Key == "" || c.EventType == "" {
		return errors.New("-key and -event-type are required")
	}
	if c.Rate <= 0 {
		return errors.New("-rate must be positive")
	}
	if c.Measure > 0 && c.Evaluate > c.Measure {
		return errors.New("-evaluate cannot be longer than -measure")
	}
	if c.Concurrency <= 0 {
		return errors.New("-concurrency must be positive")
	}
	if c.Class != "" {
		// A class declaration is a claim that this run grades AC-30's third clause,
		// so the harness checks what makes that possible before spending the 35
		// minutes: §8.1's basis, the database the graded units are counted in, an
		// identifiable machine under test, and that machine carrying the class it
		// claims.
		claim, sut, err := classPreflight(c)
		if err != nil {
			return err
		}
		c.Claim, c.SUT = claim, &sut
	} else if sut, ok, err := c.declaredSUT(); err != nil {
		// A declared machine under test is recorded even without a class, so the §16
		// machine block names the box the instance ran on rather than only the
		// generator's.
		return err
	} else if ok {
		c.SUT = &sut
	}
	rep, err := runLoad(c)
	if err != nil {
		return err
	}
	printReport(rep)
	if c.Out != "" {
		b, _ := json.MarshalIndent(rep, "", "  ")
		if err := os.WriteFile(c.Out, append(b, '\n'), 0o644); err != nil {
			return fmt.Errorf("write report: %w", err)
		}
		fmt.Printf("\njson report: %s\n", c.Out)
	}
	if !allPassed(rep) {
		return errors.New("one or more pass criteria failed; see the verdicts above")
	}
	return nil
}

// ---------------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------------

type phase struct {
	Name       string  `json:"name"`
	Seconds    float64 `json:"seconds"`
	Attempted  int64   `json:"attempted"`
	Accepted   int64   `json:"accepted"` // 202
	Shed       int64   `json:"shed"`     // 429
	Errors     int64   `json:"errors"`   // anything else, including transport errors
	AcceptRate float64 `json:"accepted_per_s"`
	ShedRate   float64 `json:"shed_per_s"`
	ErrorRate  float64 `json:"errors_per_s"`
	// PacerLag counts events whose dispatch had to wait for a free client slot,
	// i.e. the generator could not keep up with its own target. It is reported so
	// a shortfall is attributed rather than guessed at.
	PacerLag int64 `json:"pacer_lag"`

	P50MS float64 `json:"p50_ms"`
	P95MS float64 `json:"p95_ms"`
	P99MS float64 `json:"p99_ms"`
	MaxMS float64 `json:"max_ms"`
}

type reconcile struct {
	Ran          bool    `json:"ran"`
	Events       int64   `json:"db_events"`
	Outbox       int64   `json:"db_outbox"`
	Deliveries   int64   `json:"db_deliveries"`
	Accepted     int64   `json:"generator_accepted"`
	DeliveriesPS float64 `json:"deliveries_per_s"`
	EventsPS     float64 `json:"events_per_s"`
	WindowS      float64 `json:"window_s"`
	XactPS       float64 `json:"xact_commit_per_s"`
	RowsPerXact  float64 `json:"rows_per_xact"`
	EventsMatch  bool    `json:"events_match"`
	OutboxMatch  bool    `json:"outbox_match"`
	Note         string  `json:"note,omitempty"`
}

// window is one reconciliation window: the interval, the count the generator saw
// accepted inside it at dispatch time, and (once counted) its length. §16 counts
// accepted events in the database rather than from the generator's counter, and
// AC-30's third clause is stated over the graded window alone, so the run window and
// the graded window are both counted.
type window struct {
	From     time.Time
	To       time.Time
	Accepted int64
}

type verdict struct {
	AC     string `json:"ac"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail"`
}

type machine struct {
	Hostname string `json:"hostname"`
	// CPUs is the machine's effective vCPU count: the CPU affinity the process may
	// actually use, capped by any cgroup quota. Both inputs are recorded, because a
	// container that has been given two cores of a 32-core host is a 2 vCPU machine
	// and must not be able to claim a bigger class than it carries.
	CPUs        int    `json:"cpus"`
	CPUAffinity int    `json:"cpu_affinity,omitempty"`
	CPUQuota    string `json:"cpu_quota,omitempty"`
	// MemMiB is the effective memory: /proc/meminfo, capped by any cgroup limit.
	MemMiB       int64 `json:"mem_mib"`
	MemHostMiB   int64 `json:"mem_host_mib,omitempty"`
	MemCgroupMiB int64 `json:"mem_cgroup_mib,omitempty"`
	// The disk the harness runs on. A delivery ceiling is a write-path claim, so the
	// machine block §16 asks for is not complete without it.
	DiskGiB     float64 `json:"disk_gib,omitempty"`
	DiskFreeGiB float64 `json:"disk_free_gib,omitempty"`
	Kernel      string  `json:"kernel"`
	Go          string  `json:"go_version"`
	// Revision is the VCS revision the binary was built from, read out of the
	// binary's own build info rather than taken on trust from a flag.
	Revision string `json:"revision,omitempty"`
	// ObservedClass is the §8.1 row this machine itself matches, when it matches
	// one. Empty means it carries none of them.
	ObservedClass string `json:"observed_class,omitempty"`
	// Source says how a machine under test was established: "observed" when the
	// machine this process ran on *is* the machine under test (the generator shared
	// it), "declared" when the generator ran elsewhere (§12) and the operator stated
	// the machine under test with -sut-cpus/-sut-mem-gib. Empty for the machine
	// block proper, which is always the generator's box.
	Source string `json:"source,omitempty"`
	DB     string `json:"database,omitempty"`
}

// runConfig is the generator-side configuration a report carries. It is a named
// type because the verdicts read it back (windowBlockers, `grade`).
type runConfig struct {
	BaseURL     string  `json:"base_url"`
	EventType   string  `json:"event_type"`
	TargetRate  float64 `json:"target_rate_events_per_s"`
	PayloadByte int     `json:"payload_bytes"`
	Subs        int     `json:"subscriptions_per_event"`
	WarmupS     float64 `json:"warmup_s"`
	MeasureS    float64 `json:"measure_s"`
	EvaluateS   float64 `json:"evaluate_s"`
	Concurrency int     `json:"concurrency"`
}

type report struct {
	Label     string    `json:"label"`
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at"`
	Machine   machine   `json:"machine"`
	// MachineUnderTest is the machine the declared class is claimed for, recorded
	// from the generator's own machine when it shared it and from
	// -sut-cpus/-sut-mem-gib when §12 put the generator elsewhere. `grade` checks the
	// class against *this* and not against Machine, because on every gradeable run
	// Machine is the generator's box: a class graded against the generator's machine
	// would be a graded number on an unrated box, which is the failure mode AC-30's
	// third clause exists to prevent.
	MachineUnderTest *machine `json:"machine_under_test,omitempty"`
	// Class is the §8.1 machine class this run declared with -class, absent when it
	// declared none. AC-30's third clause is not gradeable without it: `grade` says
	// so rather than comparing a number against a row nobody claimed.
	Class     *classClaim `json:"class,omitempty"`
	Config    runConfig   `json:"config"`
	Warmup    phase       `json:"warmup"`
	Measured  phase       `json:"measured"`
	Evaluated phase       `json:"evaluated"`
	FirstEval phase       `json:"first_evaluate_window"`
	Reconcile reconcile   `json:"reconcile"`
	// Graded is the same reconciliation over the graded window alone - the last
	// -evaluate of the measured phase - which is the window AC-30's third clause
	// compares against §8.1.
	Graded   reconcile `json:"graded_reconcile"`
	Verdicts []verdict `json:"verdicts"`
	OK       bool      `json:"ok"`
}

// ---------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------

type sample struct {
	at      time.Time
	status  int
	latency time.Duration
	err     error
}

// phaseWindows is §12's load protocol laid on a wall clock: the warm-up (excluded
// from every reported number), the measured phase, the graded window at its end,
// and the first window of equal length at the same offered rate - which is what
// "no silent latency growth" is measured against.
type phaseWindows struct {
	measureStart   time.Time
	measureEnd     time.Time
	gradedStart    time.Time
	firstWindowEnd time.Time
}

func newPhaseWindows(now time.Time, warmup, measure, evaluate time.Duration) phaseWindows {
	start := now.Add(warmup)
	end := start.Add(measure)
	return phaseWindows{
		measureStart:   start,
		measureEnd:     end,
		gradedStart:    end.Add(-evaluate),
		firstWindowEnd: start.Add(evaluate),
	}
}

func runLoad(c config) (*report, error) {
	rep := &report{Label: c.Label, StartedAt: time.Now().UTC()}
	rep.Config.BaseURL = c.BaseURL
	rep.Config.EventType = c.EventType
	rep.Config.TargetRate = c.Rate
	rep.Config.PayloadByte = c.PayloadBytes
	rep.Config.Subs = c.Subs
	rep.Config.WarmupS = c.Warmup.Seconds()
	rep.Config.MeasureS = c.Measure.Seconds()
	rep.Config.EvaluateS = c.Evaluate.Seconds()
	rep.Config.Concurrency = c.Concurrency
	rep.Machine = describeMachine()
	rep.MachineUnderTest = c.SUT
	rep.Class = c.Claim

	client := &http.Client{
		Timeout: c.RequestTimeout,
		Transport: &http.Transport{
			MaxIdleConns:        c.Concurrency * 2,
			MaxIdleConnsPerHost: c.Concurrency * 2,
			MaxConnsPerHost:     c.Concurrency * 2,
			IdleConnTimeout:     30 * time.Second,
			DisableCompression:  true,
		},
	}

	// The generator must not compete with the instance under test (§16): one
	// process per side, and the pacer's sleep is the only thing it does when it is
	// on schedule.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	body := buildPayload(c.PayloadBytes)
	// §12's windows, in one place and computed from the phase lengths: 5 minutes of
	// warm-up excluded, the measured phase, and the graded window at its end.
	win := newPhaseWindows(time.Now(), c.Warmup, c.Measure, c.Evaluate)
	measureStart, measureEnd := win.measureStart, win.measureEnd
	evalStart, firstEvalEnd := win.gradedStart, win.firstWindowEnd

	fmt.Printf("loadgen: %s -> %s %s at %.0f events/s (payload %d B, %d sub(s)/event)\n",
		orUnset(c.Label), c.BaseURL, c.EventType, c.Rate, c.PayloadBytes, c.Subs)
	fmt.Printf("  warm-up %s (excluded), measure %s, criteria over the last %s\n",
		c.Warmup, c.Measure, c.Evaluate)
	if c.Claim != nil {
		fmt.Printf("  class: %s (%d vCPU / %d GiB) declared for the machine under test, class evidence %s\n",
			c.Claim.Name, c.Claim.CPUs, c.Claim.MemGiB, c.Claim.Evidence)
		if c.SUT != nil {
			fmt.Printf("  machine under test (%s): %s\n", c.SUT.Source, c.SUT.describe())
		}
		if c.Claim.Note != "" {
			fmt.Printf("  note: %s\n", c.Claim.Note)
		}
		fmt.Printf("  AC-30 clause 3 (the ceiling against §8.1's row) is graded from the artifact: loadgen grade -label %s\n",
			orUnset(c.Out))
	}
	if c.DB == "" {
		fmt.Println("  reconciliation: SKIPPED (no -db); the accepted count will not be checked against the database")
	}

	var (
		warmupS, measuredS, evalS, firstEvalS sampleSet
		dispatched                            atomic.Int64
		pacerLag                              atomic.Int64
		sem                                   = make(chan struct{}, c.Concurrency)
		wg                                    sync.WaitGroup
	)
	interval := time.Duration(float64(time.Second) / c.Rate)
	if interval <= 0 {
		interval = time.Nanosecond
	}

	// §12 wants FR-815's batching sampled from pg_stat_database.xact_commit, and the
	// counter has to bracket the run: the baseline is taken here, before the first
	// publish. Taking it only around the reconciliation query - which runs after the
	// load has stopped - measures the milliseconds between two reads and the
	// instance's background sweeper. Found on card `t_0bf3effa`: a run reported 0.0
	// xact/s at 0.00 rows/transaction while the counter had advanced by thousands.
	xactBase, xactErr := xactCommits(ctx, c.DB)
	if xactErr != nil && c.DB != "" {
		fmt.Printf("  commit counter baseline unavailable (%s); FR-815's rows/transaction will not be measured\n",
			redactErr(xactErr))
	}

	start := time.Now()
	next := start
	var seq atomic.Int64

loop:
	for {
		now := time.Now()
		if now.After(measureEnd) {
			break
		}
		if d := next.Sub(now); d > 0 {
			select {
			case <-ctx.Done():
				break loop
			case <-time.After(d):
			}
		} else if now.Sub(next) > interval {
			// The schedule has fallen more than one slot behind wall clock. Resync
			// it: a pacer that lags cannot make the instance faster, and if the
			// schedule is allowed to drift, the phase windows computed below - which
			// are wall-clock - stop intersecting the samples, which were being
			// attributed to the schedule. Measured consequence of not resyncing: a
			// 30-minute run whose last ten minutes contained zero samples, so the
			// criteria graded an empty window and AC-30 "passed" on no evidence.
			next = now
		}
		select {
		case <-ctx.Done():
			break loop
		case sem <- struct{}{}:
		default:
			// The client is saturated. Waiting here is the honest behaviour: the
			// generator's achieved rate is reported, and the lag is counted so a
			// shortfall is attributed to the generator rather than to the instance.
			pacerLag.Add(1)
			select {
			case <-ctx.Done():
				break loop
			case sem <- struct{}{}:
			}
		}
		// `at` is when the request was actually sent, not when the schedule said it
		// should be. With a lagging pacer the two differ by seconds: the phase
		// windows are wall-clock, and the measured latency has to be the instance's
		// answer time rather than the time the generator spent waiting for a slot.
		at := time.Now()
		next = next.Add(interval)
		id := seq.Add(1)
		dispatched.Add(1)
		wg.Add(1)
		go func(at time.Time, id int64) {
			defer wg.Done()
			defer func() { <-sem }()
			s := publish(ctx, client, c, body, id, at)
			switch {
			case at.Before(measureStart):
				warmupS.add(s)
			case at.After(measureEnd):
				// dispatched but observed after the window closed; it belongs to
				// neither phase's throughput and is dropped, not interpolated.
			case at.Before(firstEvalEnd):
				firstEvalS.add(s)
				measuredS.add(s)
			case !at.Before(evalStart):
				evalS.add(s)
				measuredS.add(s)
			default:
				measuredS.add(s)
			}
		}(at, id)
	}
	wg.Wait()
	// Let in-flight requests land, and let the relay drain the outbox, before the
	// reconciliation query, so the database count includes everything the generator
	// was told was accepted and the delivery unit includes the fan-out.
	if c.Settle > 0 {
		fmt.Printf("  settling for %s while the relay fans out...\n", c.Settle)
		time.Sleep(c.Settle)
	}

	rep.EndedAt = time.Now().UTC()
	rep.Warmup = warmupS.phase("warmup")
	rep.Warmup.rate(c.Warmup)
	rep.Measured = measuredS.phase("measure")
	rep.Measured.rate(c.Measure)
	rep.Evaluated = evalS.phase("evaluate")
	rep.Evaluated.rate(c.Evaluate)
	rep.FirstEval = firstEvalS.phase("first-evaluate-window")
	rep.FirstEval.rate(c.Evaluate)
	rep.Measured.PacerLag = pacerLag.Load()

	// The reconciliation window covers everything the generator sent - the warm-up
	// included - rather than the measured phase alone. It has to: an event
	// dispatched a millisecond before the measured phase begins commits inside it,
	// and one dispatched a millisecond before it ends commits after, so a window
	// drawn on the measured phase alone is off by the in-flight set at both edges
	// (45 rows on a 10,000-event run, measured). The cost of widening it is that
	// the count is no longer "the measured phase in the database"; the benefit is
	// that the two sides of the comparison describe the same set of requests, so a
	// difference means something (a lost response, a rolled-back accept, or a
	// dropped row) instead of being boundary noise.
	//
	// The graded window is reconciled separately, and the same edge argument is why
	// its two sides are reported rather than equated: AC-30's third clause compares
	// the database's counts over the last 10 minutes against §8.1's row, and the
	// generator's own tally over the same window is carried as a cross-check.
	rep.Reconcile, rep.Graded = reconcileDB(ctx, c,
		window{From: start, To: time.Now(), Accepted: warmupS.acceptedCount() + measuredS.acceptedCount()},
		window{From: evalStart, To: measureEnd, Accepted: evalS.acceptedCount()},
		xactBase, xactErr == nil)
	rep.Verdicts = judge(rep, c)
	rep.OK = allPassed(rep)
	return rep, nil
}

func publish(ctx context.Context, cl *http.Client, c config, payload []byte, id int64, at time.Time) sample {
	req := map[string]any{
		"event_type": c.EventType,
		"payload":    json.RawMessage(payload),
	}
	if c.ConsumerRef != "" {
		req["consumer_ref"] = c.ConsumerRef
	}
	b, _ := json.Marshal(req)
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(c.BaseURL, "/")+"/v1/events", bytes.NewReader(b))
	if err != nil {
		return sample{at: at, err: err}
	}
	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Authorization", "Bearer "+c.Key)
	// A unique key per event: the point of the run is throughput, and a repeated
	// key would exercise the idempotent-replay path instead, counting 202s for
	// events that were never accepted.
	hreq.Header.Set("Idempotency-Key", fmt.Sprintf("loadgen-%d-%d", at.UnixNano(), id))

	t0 := time.Now()
	resp, err := cl.Do(hreq)
	lat := time.Since(t0)
	if err != nil {
		return sample{at: at, latency: lat, err: err}
	}
	drain(resp)
	return sample{at: at, status: resp.StatusCode, latency: lat}
}

func drain(resp *http.Response) {
	// The body is drained and closed even though nothing reads it: an undrained
	// body makes the connection unusable for the next request, and a generator
	// that opens a socket per event measures the kernel's connect path instead of
	// the instance. The bodies are ~300 B, so the copy costs nothing.
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
}

func buildPayload(n int) []byte {
	// A fixed, valid object of the requested size. Padding is a string field, so
	// the payload is legal against any permissive schema and its size is exact
	// rather than approximate.
	const head, tail = `{"data":"`, `"}`
	pad := n - len(head) - len(tail)
	if pad < 0 {
		pad = 0
	}
	return []byte(head + strings.Repeat("x", pad) + tail)
}

// ---------------------------------------------------------------------------
// Sample collection
// ---------------------------------------------------------------------------

type sampleSet struct {
	name      string
	mu        sync.Mutex
	attempted int64
	accepted  int64
	shed      int64
	errors    int64
	lat       []float64 // milliseconds
}

func (s *sampleSet) add(x sample) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempted++
	switch {
	case x.err != nil:
		s.errors++
	case x.status == http.StatusAccepted:
		s.accepted++
	case x.status == http.StatusTooManyRequests:
		s.shed++
	default:
		s.errors++
	}
	if len(s.lat) < maxSamplesPerPhase {
		s.lat = append(s.lat, float64(x.latency.Microseconds())/1000.0)
	}
}

// acceptedCount is how many samples this set saw a 202 for. It reads the same lock
// the recorder uses, so it is safe to call after the workers have been joined, and
// it is compared against `events` rows: a request that errored must contribute no
// row, so counted-requested would hide exactly the case the check exists for.
func (s *sampleSet) acceptedCount() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.accepted
}

func (s *sampleSet) phase(name string) phase {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := phase{
		Name:      name,
		Attempted: s.attempted,
		Accepted:  s.accepted,
		Shed:      s.shed,
		Errors:    s.errors,
	}
	p.P50MS, p.P95MS, p.P99MS, p.MaxMS = percentiles(s.lat)
	return p
}

// rate fills in the per-second figures from the phase's configured length. The
// length comes from the schedule rather than from the span of the samples: a phase
// whose requests all landed in its first second would otherwise report a rate
// several times the truth.
func (p *phase) rate(d time.Duration) {
	sec := d.Seconds()
	if sec <= 0 {
		return
	}
	p.Seconds = sec
	p.AcceptRate = float64(p.Accepted) / sec
	p.ShedRate = float64(p.Shed) / sec
	p.ErrorRate = float64(p.Errors) / sec
}

func percentiles(ms []float64) (p50, p95, p99, max float64) {
	if len(ms) == 0 {
		return 0, 0, 0, 0
	}
	sorted := make([]float64, len(ms))
	copy(sorted, ms)
	sort.Float64s(sorted)
	at := func(q float64) float64 {
		i := int(q * float64(len(sorted)-1))
		return sorted[i]
	}
	return at(0.50), at(0.95), at(0.99), sorted[len(sorted)-1]
}
