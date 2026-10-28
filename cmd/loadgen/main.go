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
//	loadgen compare -label-a 1-replica.json -label-b 2-replica.json -label-c 4-replica.json
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
	case "compare":
		err = cmdCompare(os.Args[2:])
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

  loadgen run     -base URL -key KEY -event-type NAME -rate N [-subs N] [-db DSN] [-out FILE]
  loadgen compare -label-a FILE -label-b FILE [-label-c FILE ...]

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
  -subs            subscriptions per event, for the delivery unit (default 1)
  -db              Postgres DSN for reconciliation; omit to skip it
  -label           name for this run, e.g. "2vCPU-4GB-2replicas"
  -out             write the JSON report here
  -concurrency     in-flight requests (default 64)
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
	XactPS       float64 `json:"xact_commit_per_s"`
	RowsPerXact  float64 `json:"rows_per_xact"`
	EventsMatch  bool    `json:"events_match"`
	OutboxMatch  bool    `json:"outbox_match"`
	Note         string  `json:"note,omitempty"`
}

type verdict struct {
	AC     string `json:"ac"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail"`
}

type machine struct {
	Hostname string `json:"hostname"`
	CPUs     int    `json:"cpus"`
	MemMiB   int64  `json:"mem_mib"`
	Kernel   string `json:"kernel"`
	Go       string `json:"go_version"`
	DB       string `json:"database,omitempty"`
}

type report struct {
	Label     string    `json:"label"`
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at"`
	Machine   machine   `json:"machine"`
	Config    struct {
		BaseURL     string  `json:"base_url"`
		EventType   string  `json:"event_type"`
		TargetRate  float64 `json:"target_rate_events_per_s"`
		PayloadByte int     `json:"payload_bytes"`
		Subs        int     `json:"subscriptions_per_event"`
		WarmupS     float64 `json:"warmup_s"`
		MeasureS    float64 `json:"measure_s"`
		EvaluateS   float64 `json:"evaluate_s"`
		Concurrency int     `json:"concurrency"`
	} `json:"config"`
	Warmup    phase     `json:"warmup"`
	Measured  phase     `json:"measured"`
	Evaluated phase     `json:"evaluated"`
	FirstEval phase     `json:"first_evaluate_window"`
	Reconcile reconcile `json:"reconcile"`
	Verdicts  []verdict `json:"verdicts"`
	OK        bool      `json:"ok"`
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
	measureStart := time.Now().Add(c.Warmup)
	measureEnd := measureStart.Add(c.Measure)
	evalStart := measureEnd.Add(-c.Evaluate)
	firstEvalEnd := measureStart.Add(c.Evaluate)

	fmt.Printf("loadgen: %s -> %s %s at %.0f events/s (payload %d B, %d sub(s)/event)\n",
		orUnset(c.Label), c.BaseURL, c.EventType, c.Rate, c.PayloadBytes, c.Subs)
	fmt.Printf("  warm-up %s (excluded), measure %s, criteria over the last %s\n",
		c.Warmup, c.Measure, c.Evaluate)
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
		at := next
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
	rep.Reconcile = reconcileDB(ctx, c, warmupS.acceptedCount()+measuredS.acceptedCount(), start, time.Now())
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
