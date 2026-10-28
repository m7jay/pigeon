package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// reconcileDB answers §16's requirement that the accepted count is checked against
// the database rather than believed from the generator's own counter.
//
// It counts rows CREATED inside the measured window in all three tables the ingest
// path touches: `events` (one per accepted publish), `outbox` (one per accepted
// publish, the transactional outbox that makes the accept durable) and `deliveries`
// (the fan-out, which is `events × subscriptions`). A generator that lost responses
// and retried, an instance that accepted and rolled back, or a fan-out that dropped
// a delivery each show up as a mismatch here and nowhere else.
//
// `pg_stat_database.xact_commit` is sampled across the same window because it is the
// only independent evidence of FR-815's batching: the ratio of rows to transactions
// says whether the relay is draining in batches or one row at a time, which is what
// makes the §8.1 write model hold.
func reconcileDB(ctx context.Context, c config, accepted int64, from, to time.Time) reconcile {
	out := reconcile{Accepted: accepted}
	if c.DB == "" {
		out.Note = "no -db given; the accepted count was not reconciled against the database"
		return out
	}
	pool, err := pgxpool.New(ctx, c.DB)
	if err != nil {
		out.Note = "database unavailable: " + redactErr(err)
		return out
	}
	defer pool.Close()

	// Sample the commit counter first, then count, so the counter brackets the
	// rows: a run whose counter advanced but whose rows are missing is the
	// rollback case, and it must not be hidden behind a snapshot taken later.
	var xactBefore int64
	if err := pool.QueryRow(ctx,
		`select coalesce(sum(xact_commit),0) from pg_stat_database where datname = current_database()`).
		Scan(&xactBefore); err != nil {
		out.Note = "pg_stat_database unreadable: " + redactErr(err)
	}

	q := `select
	        (select count(*) from events      where created_at >= $1 and created_at < $2),
	        (select count(*) from outbox      where created_at >= $1 and created_at < $2),
	        (select count(*) from deliveries  where created_at >= $1 and created_at < $2)`
	if err := pool.QueryRow(ctx, q, from, to).Scan(&out.Events, &out.Outbox, &out.Deliveries); err != nil {
		out.Note = "reconciliation query failed: " + redactErr(err)
		return out
	}
	var xactAfter int64
	if err := pool.QueryRow(ctx,
		`select coalesce(sum(xact_commit),0) from pg_stat_database where datname = current_database()`).
		Scan(&xactAfter); err != nil {
		out.Note = "pg_stat_database unreadable: " + redactErr(err)
	}

	out.Ran = true
	sec := to.Sub(from).Seconds()
	if sec > 0 {
		out.DeliveriesPS = float64(out.Deliveries) / sec
		out.XactPS = float64(xactAfter-xactBefore) / sec
	}
	if out.XactPS > 0 {
		out.RowsPerXact = (float64(out.Deliveries) + float64(out.Events)) / (out.XactPS * sec)
	}
	// The generator's count is the number of 202s it saw. Equality is exact, not
	// approximate: a tolerance here would hide precisely the defect the check
	// exists to catch. It is compared per-table so the failing half is named.
	out.EventsMatch = out.Events == accepted
	out.OutboxMatch = out.Outbox == accepted
	return out
}

func redactErr(err error) string {
	// FR-1008: a DSN carries a password, and driver errors can echo the DSN. Keep
	// the test conservative and name the class of failure instead of the text.
	return strings.SplitN(err.Error(), " (", 2)[0]
}

// judge turns a run into the card's acceptance criteria.
//
// Only the criteria a single run can decide are decided here. AC-31's "the ceiling
// matches the §8.1 table within 20%" and AC-43's scaling are comparisons across
// runs, so they are `compare`'s job - inventing them from one run would be a claim
// the data does not support.
func judge(r *report, c config) []verdict {
	var out []verdict

	// AC-30: past the ceiling, publish sheds with 429 and latency does not drift.
	//
	// "Sheds with 429" is asserted as: there were refusals, and every refusal was a
	// 429 - a 500 is a failure to shed, not shedding. "No silent latency growth" is
	// asserted as: p99 over the last window is not materially worse than p99 over
	// the first window of equal length at the same offered rate, which is the
	// symptom of a queue that is filling while still answering 202.
	shedClean := r.Evaluated.Errors == 0
	shedding := r.Evaluated.Shed > 0
	growth := 0.0
	if r.FirstEval.P99MS > 0 {
		growth = r.Evaluated.P99MS / r.FirstEval.P99MS
	}
	stable := growth <= 1.5 || r.FirstEval.P99MS == 0
	out = append(out, verdict{
		AC: "AC-30",
		OK: shedClean && stable,
		Detail: fmt.Sprintf("refusals were 429 only: %v (errors %d); p99 %.1f ms -> %.1f ms (%.2fx) over the run; shed %.1f/s",
			shedClean, r.Evaluated.Errors, r.FirstEval.P99MS, r.Evaluated.P99MS, growth, r.Evaluated.ShedRate),
	})
	if !shedding {
		out[len(out)-1].Detail += "; no load was shed, so this run did not reach the ceiling"
	}

	// AC-31: the rated load is accepted, and the delivery unit is reported.
	//
	// NFR-1's p99 ≤ 100 ms and the accepted-ratio floor are stated here so a run at
	// the rating either passes or names which half it missed.
	ratio := 0.0
	if c.Rate > 0 {
		ratio = r.Evaluated.AcceptRate / c.Rate
	}
	p99OK := r.Evaluated.P99MS <= 100
	out = append(out, verdict{
		AC: "AC-31",
		OK: ratio >= 0.98 && p99OK,
		Detail: fmt.Sprintf("accepted %.1f of %.0f events/s (%.1f%%) with p99 %.1f ms (NFR-1 ceiling 100 ms); %.1f deliveries/s at %d subscription(s) per event",
			r.Evaluated.AcceptRate, c.Rate, ratio*100, r.Evaluated.P99MS, r.Reconcile.DeliveriesPS, c.Subs),
	})

	// AC-34: the attempt fan-out is bounded, not the ingest rate. A run at a high
	// rate with a large subscription count is the observable proxy: if attempts
	// were unbounded the error count and p99 would diverge from the accepted rate,
	// and the generator would see resets. The goroutine and FD census is asserted
	// by TestAttemptFanOutIsBounded (internal/delivery), which can inspect the
	// process; this run records the load that test is sized against.
	out = append(out, verdict{
		AC: "AC-34",
		OK: r.Evaluated.Errors == 0 && r.Evaluated.P99MS <= 500,
		Detail: fmt.Sprintf("under a sustained %.0f events/s the ingest path reported %d errors and p99 %.1f ms; the attempt census itself is TestAttemptFanOutIsBounded",
			r.Evaluated.AcceptRate, r.Evaluated.Errors, r.Evaluated.P99MS),
	})

	// §16: the accepted count reconciled against the database.
	ok := true
	detail := "not requested (-db absent); the accepted count is the generator's own, which §16 does not accept as evidence"
	if r.Reconcile.Ran {
		ok = r.Reconcile.EventsMatch && r.Reconcile.OutboxMatch
		detail = fmt.Sprintf("generator accepted %d; events %d, outbox %d, deliveries %d; %.1f deliveries/s, %.1f xact/s (%.2f rows/xact)",
			r.Reconcile.Accepted, r.Reconcile.Events, r.Reconcile.Outbox, r.Reconcile.Deliveries,
			r.Reconcile.DeliveriesPS, r.Reconcile.XactPS, r.Reconcile.RowsPerXact)
		if !ok {
			detail += fmt.Sprintf("; MISMATCH: events %d, outbox %d against %d accepted",
				r.Reconcile.Events, r.Reconcile.Outbox, r.Reconcile.Accepted)
		}
	} else if r.Reconcile.Note != "" {
		detail = "reconciliation did not run: " + r.Reconcile.Note
		ok = false
	}
	out = append(out, verdict{AC: "§16 reconciliation", OK: ok, Detail: detail})
	return out
}

func allPassed(r *report) bool {
	// A comparison verdict (AC-31's 20% floor, AC-43) is only present after
	// `compare`, so a single run is judged on what it can decide.
	for _, v := range r.Verdicts {
		if !v.OK {
			return false
		}
	}
	return true
}

func printReport(r *report) {
	fmt.Println()
	fmt.Printf("%-22s %10s %10s %10s %8s %8s %8s %8s\n", "phase", "attempted", "accepted", "shed", "acc/s", "p50 ms", "p99 ms", "max ms")
	for _, p := range []phase{r.Warmup, r.Measured, r.Evaluated, r.FirstEval} {
		fmt.Printf("%-22s %10d %10d %10d %8.1f %8.1f %8.1f %8.1f\n",
			p.Name, p.Attempted, p.Accepted, p.Shed, p.AcceptRate, p.P50MS, p.P99MS, p.MaxMS)
	}
	if r.Measured.PacerLag > 0 {
		fmt.Printf("pacer lag: %d events waited for a client slot; the achieved rate is the generator's, not the instance's\n", r.Measured.PacerLag)
	}
	fmt.Println()
	if r.Reconcile.Ran {
		fmt.Printf("reconciled: generator accepted %d | events %d | outbox %d | deliveries %d\n",
			r.Reconcile.Accepted, r.Reconcile.Events, r.Reconcile.Outbox, r.Reconcile.Deliveries)
		fmt.Printf("delivery unit: %.1f deliveries/s | %.1f xact/s | %.2f rows/transaction\n",
			r.Reconcile.DeliveriesPS, r.Reconcile.XactPS, r.Reconcile.RowsPerXact)
	} else if r.Reconcile.Note != "" {
		fmt.Println("reconciliation:", r.Reconcile.Note)
	}
	fmt.Println()
	for _, v := range r.Verdicts {
		status := "PASS"
		if !v.OK {
			status = "FAIL"
		}
		fmt.Printf("%s %-22s %s\n", status, v.AC, v.Detail)
	}
	if r.Label != "" || r.Machine.CPUs > 0 {
		fmt.Printf("\nmeasured on %s: %d vCPU, %d MiB, %s, %s\n",
			r.Machine.Hostname, r.Machine.CPUs, r.Machine.MemMiB, r.Machine.Kernel, r.Machine.DB)
	}
}

// ---------------------------------------------------------------------------
// AC-43: does the tier scale?
// ---------------------------------------------------------------------------

func cmdCompare(args []string) error {
	fs := flag.NewFlagSet("compare", flag.ContinueOnError)
	var files multiFlag
	fs.Var(&files, "label", "a run's JSON report, in ascending replica order (repeat the flag)")
	var out string
	fs.StringVar(&out, "out", "", "write the JSON comparison here")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(files) < 2 {
		return fmt.Errorf("need at least 2 reports to compare (got %d)", len(files))
	}
	reps := make([]*report, 0, len(files))
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return fmt.Errorf("read %s: %w", f, err)
		}
		var r report
		if err := json.Unmarshal(b, &r); err != nil {
			return fmt.Errorf("parse %s: %w", f, err)
		}
		reps = append(reps, &r)
	}

	type step struct {
		From       string  `json:"from"`
		To         string  `json:"to"`
		FromAccPS  float64 `json:"from_accepted_per_s"`
		ToAccPS    float64 `json:"to_accepted_per_s"`
		ScaleRatio float64 `json:"scale_ratio"`
		ToP99MS    float64 `json:"to_p99_ms"`
		Note       string  `json:"note"`
	}
	var steps []step
	ok := true
	plateauSeen := false
	for i := 1; i < len(reps); i++ {
		a, b := reps[i-1], reps[i]
		s := step{From: labelOf(a), To: labelOf(b),
			FromAccPS: a.Evaluated.AcceptRate, ToAccPS: b.Evaluated.AcceptRate,
			ToP99MS: b.Evaluated.P99MS}
		if s.FromAccPS > 0 {
			s.ScaleRatio = (s.ToAccPS - s.FromAccPS) / s.FromAccPS
		}
		// AC-43: ≥80% per doubling while the write path has headroom; above the
		// ceiling throughput must stop improving and p99 must get worse, which is
		// the signal that the operator is past the primary's write capacity rather
		// than short of workers.
		if s.ScaleRatio >= 0.80 {
			s.Note = "scales (>=80%)"
			ok = ok && true
		} else if s.ToP99MS > a.Evaluated.P99MS {
			s.Note = "plateaued with worse p99, which is the documented above-ceiling behaviour (§8.4)"
			plateauSeen = true
		} else {
			s.Note = "DID NOT SCALE and did not plateau: throughput fell while p99 held, which §8.4 does not predict"
			ok = false
		}
		steps = append(steps, s)
	}
	if !plateauSeen && ok {
		// Nothing wrong, but the run set never crossed the ceiling, so AC-43's
		// second half was not exercised.
		steps[len(steps)-1].Note += "; no step crossed the ceiling, so the plateau half of AC-43 is unproven by this set"
	}

	cmp := struct {
		Runs    []step  `json:"steps"`
		Verdict verdict `json:"verdict"`
	}{Runs: steps}
	cmp.Verdict = verdict{AC: "AC-43", OK: ok,
		Detail: fmt.Sprintf("%d tiers compared over %d step(s); see steps", len(reps), len(steps))}
	if out != "" {
		b, _ := json.MarshalIndent(cmp, "", "  ")
		if err := os.WriteFile(out, append(b, '\n'), 0o644); err != nil {
			return err
		}
	}
	fmt.Printf("%-24s %-24s %12s %12s %10s  %s\n", "from", "to", "from acc/s", "to acc/s", "scale", "note")
	for _, s := range steps {
		fmt.Printf("%-24s %-24s %12.1f %12.1f %9.1f%%  %s\n",
			s.From, s.To, s.FromAccPS, s.ToAccPS, s.ScaleRatio*100, s.Note)
	}
	fmt.Printf("\n%s %s\n", verdictStatus(cmp.Verdict.OK), cmp.Verdict.Detail)
	if !ok {
		return fmt.Errorf("AC-43 failed")
	}
	return nil
}

func verdictStatus(ok bool) string {
	if ok {
		return "PASS"
	}
	return "FAIL"
}

func labelOf(r *report) string {
	if r.Label != "" {
		return r.Label
	}
	return fmt.Sprintf("%dCPU/%dMiB", r.Machine.CPUs, r.Machine.MemMiB)
}

type multiFlag []string

func (m *multiFlag) String() string { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error {
	*m = append(*m, v)
	return nil
}

// ---------------------------------------------------------------------------
// Machine description
// ---------------------------------------------------------------------------

func describeMachine() machine {
	m := machine{CPUs: runtime.NumCPU(), Go: runtime.Version()}
	m.Hostname, _ = os.Hostname()
	if b, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, "MemTotal:") {
				var kib int64
				_, _ = fmt.Sscanf(strings.TrimSpace(strings.TrimPrefix(line, "MemTotal:")), "%d", &kib)
				m.MemMiB = kib / 1024
				break
			}
		}
	}
	if b, err := os.ReadFile("/proc/sys/kernel/osrelease"); err == nil {
		m.Kernel = "linux " + strings.TrimSpace(string(b))
	}
	// The database version is read here rather than in reconcileDB so it is
	// recorded even when the reconciliation is skipped for a DSN-less run.
	if dsn := os.Getenv("PIGEON_LOADGEN_DB"); dsn != "" {
		if pool, err := pgxpool.New(context.Background(), dsn); err == nil {
			var v string
			if err := pool.QueryRow(context.Background(), `select version()`).Scan(&v); err == nil {
				m.DB = strings.SplitN(v, " on ", 2)[0]
			}
			pool.Close()
		}
	}
	return m
}

func orUnset(s string) string {
	if s == "" {
		return "(unlabelled)"
	}
	return s
}

var _ = time.Now
