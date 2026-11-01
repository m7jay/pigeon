package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// xactCommits reads pg_stat_database.xact_commit for the database this process is
// connected to - the counter whose delta over a run is §12's independent evidence of
// FR-815's batching. The harness needs its value before the run starts, which is
// before the reconciliation pool exists, so it opens (and closes) its own.
func xactCommits(ctx context.Context, dsn string) (int64, error) {
	if dsn == "" {
		return 0, fmt.Errorf("no -db given")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return 0, err
	}
	defer pool.Close()
	var n int64
	if err := pool.QueryRow(ctx,
		`select coalesce(sum(xact_commit),0) from pg_stat_database where datname = current_database()`).
		Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

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
// Both windows are counted from one pool, with the commit counter read once at the
// end of the pair (§16): the second window is the graded one, and AC-30's third
// clause compares its units against §8.1's row.
//
// xactBase is the counter's value *before the first publish* and haveXactBase says
// whether the caller could take it. It has to bracket the run: a delta sampled only
// around these two count queries, which run after the load has stopped, measures the
// milliseconds between two reads and the instance's background sweeper instead of the
// run's commits. That defect was found on card `t_0bf3effa` - a run reporting 0.0
// xact/s at 0.00 rows/transaction while the database's counter had advanced by
// thousands - and is why the figures are now either real or reported as unmeasured.
func reconcileDB(ctx context.Context, c config, run, graded window, xactBase int64, haveXactBase bool) (reconcile, reconcile) {
	runOut := reconcile{Accepted: run.Accepted}
	gradedOut := reconcile{Accepted: graded.Accepted}
	if c.DB == "" {
		runOut.Note = "no -db given; the accepted count was not reconciled against the database"
		gradedOut.Note = runOut.Note
		return runOut, gradedOut
	}
	pool, err := pgxpool.New(ctx, c.DB)
	if err != nil {
		runOut.Note = "database unavailable: " + redactErr(err)
		gradedOut.Note = runOut.Note
		return runOut, gradedOut
	}
	defer pool.Close()

	q := `select
	        (select count(*) from events      where created_at >= $1 and created_at < $2),
	        (select count(*) from outbox      where created_at >= $1 and created_at < $2),
	        (select count(*) from deliveries  where created_at >= $1 and created_at < $2)`
	if err := pool.QueryRow(ctx, q, run.From, run.To).Scan(&runOut.Events, &runOut.Outbox, &runOut.Deliveries); err != nil {
		runOut.Note = "reconciliation query failed: " + redactErr(err)
		return runOut, gradedOut
	}
	if err := pool.QueryRow(ctx, q, graded.From, graded.To).Scan(&gradedOut.Events, &gradedOut.Outbox, &gradedOut.Deliveries); err != nil {
		gradedOut.Note = "graded-window reconciliation query failed: " + redactErr(err)
		return runOut, gradedOut
	}
	// The commit counter's baseline was taken before the run's first publish; read it
	// again now, after both count queries, so the bracket covers the run and the rows
	// it is being compared with. A counter that advanced while the rows are missing is
	// the rollback case, and it must not be hidden behind a snapshot taken earlier.
	var xactDelta int64
	if haveXactBase {
		var xactAfter int64
		if err := pool.QueryRow(ctx,
			`select coalesce(sum(xact_commit),0) from pg_stat_database where datname = current_database()`).
			Scan(&xactAfter); err != nil {
			runOut.Note = "pg_stat_database unreadable: " + redactErr(err)
			gradedOut.Note = runOut.Note
			haveXactBase = false
		} else {
			xactDelta = xactAfter - xactBase
		}
	}
	if !haveXactBase {
		// Say it rather than guess: an unpinned baseline would make the ratio a
		// measurement of whatever else committed while the harness was counting.
		runOut.Note = "FR-815's rows/transaction was not measured: the commit counter was not bracketed across the run"
		gradedOut.Note = runOut.Note
	}

	fillRate(&runOut, run, xactDelta)
	// The graded window is a slice of the same run, so a commit rate over it would be
	// a statement about the whole run's sampling interval, not about the window: it
	// is left zero and FR-815's batching is read from the run window's figures.
	fillRate(&gradedOut, graded, 0)
	gradedOut.XactPS, gradedOut.RowsPerXact = 0, 0
	// Say why the graded window's match flags are false rather than leaving a reader
	// to read them as a mismatch: it is counted the same way as the run window, and
	// the two halves are deliberately not equated.
	gradedNote := "the graded window's edges are the in-flight set at both ends, so its generator tally is carried as a cross-check rather than equated (events_match and outbox_match are false by design) and its commit rate is not a figure about the window; AC-30's third clause grades the database's counts (§16)"
	if gradedOut.Note != "" {
		gradedOut.Note += "; " + gradedNote
	} else {
		gradedOut.Note = gradedNote
	}
	// The generator's count is the number of 202s it saw. Equality is exact, not
	// approximate: a tolerance here would hide precisely the defect the check
	// exists to catch. It is compared per-table so the failing half is named.
	//
	// The graded window is deliberately not equated: its edges are the in-flight set
	// at both ends - an event dispatched just before it opens commits inside it, one
	// dispatched just before it closes commits after - so its tally is carried as a
	// cross-check while AC-30's third clause grades the database's own count.
	runOut.EventsMatch = runOut.Events == run.Accepted
	runOut.OutboxMatch = runOut.Outbox == run.Accepted
	return runOut, gradedOut
}

// fillRate turns a window's counts into per-second figures. The length comes from
// the window rather than from the span of whatever rows landed in it: a rate
// computed from observed samples silently improves whenever the collector is
// starved, which is exactly when the system is in trouble.
func fillRate(out *reconcile, w window, xactDelta int64) {
	out.Ran = true
	out.Accepted = w.Accepted
	out.WindowS = w.To.Sub(w.From).Seconds()
	if out.WindowS > 0 {
		out.DeliveriesPS = float64(out.Deliveries) / out.WindowS
		out.EventsPS = float64(out.Events) / out.WindowS
		out.XactPS = float64(xactDelta) / out.WindowS
	}
	if out.XactPS > 0 {
		out.RowsPerXact = (float64(out.Deliveries) + float64(out.Events)) / (out.XactPS * out.WindowS)
	}
}

func redactErr(err error) string {
	// FR-1008: a DSN carries a password, and driver errors can echo the DSN. Keep
	// the test conservative and name the class of failure instead of the text.
	return strings.SplitN(err.Error(), " (", 2)[0]
}

// emptyIf appends the "not observable" note when the criteria window held no
// samples, so an ungraded criterion never reads as a passed one.
func emptyIf(empty bool, note string) string {
	if empty {
		return note
	}
	return ""
}

// judge turns a run into the card's acceptance criteria.
//
// Only the criteria a single run can decide are decided here. AC-43's scaling is a
// comparison across runs, so it is `compare`'s job; AC-30's third clause - the
// ceiling against §8.1's row - is a comparison against the requirement's table and
// its own protocol preconditions, so it is `grade`'s job (class.go). AC-30's first
// two clauses are decided here, from the run's own numbers, and `grade` re-decides
// them from the artifact rather than reading this verdict back.
func judge(r *report, c config) []verdict {
	var out []verdict

	// An empty criteria window is not a pass. Before this guard the verdicts were
	// computed from a window that could contain no samples at all (a pacer that
	// fell behind wall clock filed every late sample under an earlier phase), and
	// the arithmetic then read "0 errors, 0 shed, p99 0.0 ms" as a clean run and
	// reported AC-30 PASS on no evidence. A criterion with nothing to measure is
	// stated as such.
	empty := r.Evaluated.Attempted == 0
	emptyNote := "; the criteria window holds no samples, so this criterion was not observable in this run"

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
		// shedding is part of the criterion, not a footnote: "past the ceiling"
		// cannot be graded by a run that never reached it.
		OK: !empty && shedClean && stable && shedding,
		Detail: fmt.Sprintf("refusals were 429 only: %v (errors %d); p99 %.1f ms -> %.1f ms (%.2fx) over the run; shed %.1f/s",
			shedClean, r.Evaluated.Errors, r.FirstEval.P99MS, r.Evaluated.P99MS, growth, r.Evaluated.ShedRate),
	})
	if !shedding {
		out[len(out)-1].Detail += "; no load was shed, so this run did not reach the ceiling"
	}
	if empty {
		out[len(out)-1].Detail += emptyNote
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
		OK: !empty && ratio >= 0.98 && p99OK,
		Detail: fmt.Sprintf("accepted %.1f of %.0f events/s (%.1f%%) with p99 %.1f ms (NFR-1 ceiling 100 ms); %.1f deliveries/s at %d subscription(s) per event",
			r.Evaluated.AcceptRate, c.Rate, ratio*100, r.Evaluated.P99MS, r.Reconcile.DeliveriesPS, c.Subs) + emptyIf(empty, emptyNote),
	})

	// AC-34: the attempt fan-out is bounded, not the ingest rate. A run at a high
	// rate with a large subscription count is the observable proxy: if attempts
	// were unbounded the error count and p99 would diverge from the accepted rate,
	// and the generator would see resets. The goroutine and FD census is asserted
	// by TestAttemptFanOutIsBounded (internal/delivery), which can inspect the
	// process; this run records the load that test is sized against.
	out = append(out, verdict{
		AC: "AC-34",
		OK: !empty && r.Evaluated.Errors == 0 && r.Evaluated.P99MS <= 500,
		Detail: fmt.Sprintf("under a sustained %.0f events/s the ingest path reported %d errors and p99 %.1f ms; the attempt census itself is TestAttemptFanOutIsBounded",
			r.Evaluated.AcceptRate, r.Evaluated.Errors, r.Evaluated.P99MS) + emptyIf(empty, emptyNote),
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
		if r.Graded.WindowS > 0 {
			// The graded window AC-30's third clause compares, from the database
			// rather than from the generator's tally (§16).
			fmt.Printf("graded window (last %s of the measurement, %.0f s): %.1f events/s and %.1f deliveries/s in the database; the generator's tally was %d accepted\n",
				r.Evaluated.Name, r.Graded.WindowS, r.Graded.EventsPS, r.Graded.DeliveriesPS, r.Graded.Accepted)
			if r.Graded.Note != "" {
				fmt.Printf("  graded window note: %s\n", r.Graded.Note)
			}
		}
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
		fmt.Printf("\nmeasured on %s: %s, %s\n", r.Machine.describe(), r.Machine.Kernel, r.Machine.DB)
		if r.Machine.ObservedClass != "" {
			fmt.Printf("§8.1 row the machine itself matches: %s\n", r.Machine.ObservedClass)
		} else {
			fmt.Printf("§8.1 row the machine itself matches: none (its class must be attested, not observed)\n")
		}
		if r.MachineUnderTest != nil {
			fmt.Printf("machine under test: %s; AC-30's third clause checks the declared class against it\n",
				r.MachineUnderTest.describe())
		} else if r.Class != nil {
			fmt.Println("machine under test: not recorded, so AC-30's third clause will refuse this artifact " +
				"(declare it with -sut-cpus/-sut-mem-gib when the generator runs off that machine)")
		}
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

// describeMachine records the machine the harness runs on: what §16's machine block
// asks for, and what AC-30's third clause checks a declared class against.
//
// The CPU and memory figures are *effective*: CPU affinity (what the process may
// actually run on) capped by any cgroup quota, and /proc/meminfo capped by any
// cgroup limit. A container given two cores and 4 GiB of a 32-core, 128 GiB host is
// a 2 vCPU / 4 GiB machine, and reading only the host's totals would let it claim a
// class it does not carry - which is the failure mode the class gate exists to
// prevent. Both inputs are recorded, so the arithmetic is auditable.
func describeMachine() machine {
	m := machine{Go: runtime.Version()}
	m.Hostname, _ = os.Hostname()
	m.CPUAffinity = runtime.NumCPU()
	m.CPUs = m.CPUAffinity
	if quota, cores := cgroupCPUQuota(); quota != "" {
		m.CPUQuota = quota
		if cores > 0 && cores < m.CPUs {
			m.CPUs = cores
		}
	}
	if b, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, "MemTotal:") {
				var kib int64
				_, _ = fmt.Sscanf(strings.TrimSpace(strings.TrimPrefix(line, "MemTotal:")), "%d", &kib)
				m.MemHostMiB = kib / 1024
				break
			}
		}
	}
	if m.MemHostMiB == 0 {
		// macOS and other non-Linux hosts do not expose /proc/meminfo. Read the
		// host's physical-memory value where sysctl is available so the artifact
		// still describes the resources this manual run actually had.
		for _, key := range []string{"hw.memsize", "hw.physmem"} {
			if b, err := exec.Command("sysctl", "-n", key).Output(); err == nil {
				if bytes, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64); err == nil && bytes > 0 {
					m.MemHostMiB = bytes / (1024 * 1024)
					break
				}
			}
		}
	}
	m.MemMiB = m.MemHostMiB
	if cg := cgroupMemoryMiB(); cg > 0 {
		m.MemCgroupMiB = cg
		if m.MemMiB == 0 || cg < m.MemMiB {
			m.MemMiB = cg
		}
	}
	if b, err := os.ReadFile("/proc/sys/kernel/osrelease"); err == nil {
		m.Kernel = "linux " + strings.TrimSpace(string(b))
	} else if b, err := exec.Command("uname", "-sr").Output(); err == nil {
		m.Kernel = strings.TrimSpace(string(b))
	}
	if dir, err := os.Getwd(); err == nil {
		m.DiskGiB, m.DiskFreeGiB = diskUsage(dir)
	}
	m.Revision = buildRevision()
	m.ObservedClass = observedClass(m)
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

// cgroupCPUQuota reads the CPU quota the process is held to, whichever cgroup
// layout is in use, and returns it with the whole cores it amounts to. An empty
// string means no quota was found, which is not the same as "no limit": a nested
// cgroup path the process cannot see reads the same way, which is why the raw value
// is recorded next to the count it produced.
func cgroupCPUQuota() (string, int) {
	// cgroup v2: "max 100000" (no limit) or "400000 100000" (4 cores).
	if b, err := os.ReadFile("/sys/fs/cgroup/cpu.max"); err == nil {
		f := strings.Fields(strings.TrimSpace(string(b)))
		if len(f) == 2 {
			if f[0] == "max" {
				return strings.TrimSpace(string(b)), 0
			}
			q, err1 := strconv.ParseInt(f[0], 10, 64)
			p, err2 := strconv.ParseInt(f[1], 10, 64)
			if err1 == nil && err2 == nil {
				return strings.TrimSpace(string(b)), quotaCores(q, p)
			}
		}
	}
	// cgroup v1: two files, -1 meaning no limit.
	qb, err1 := os.ReadFile("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
	pb, err2 := os.ReadFile("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
	if err1 != nil || err2 != nil {
		return "", 0
	}
	q, err1 := strconv.ParseInt(strings.TrimSpace(string(qb)), 10, 64)
	p, err2 := strconv.ParseInt(strings.TrimSpace(string(pb)), 10, 64)
	if err1 != nil || err2 != nil {
		return "", 0
	}
	return fmt.Sprintf("%d %d", q, p), quotaCores(q, p)
}

// quotaCores is a quota in whole cores, floored: a quota below one core is reported
// as one rather than as zero, and either way it matches no §8.1 row, which is the
// direction to fail in.
func quotaCores(quota, period int64) int {
	if quota <= 0 || period <= 0 {
		return 0
	}
	if cores := int(quota / period); cores >= 1 {
		return cores
	}
	return 1
}

// cgroupMemoryMiB is the memory limit the process is held to, or 0 when there is
// none to read ("max" in cgroup v2, a huge sentinel in v1).
func cgroupMemoryMiB() int64 {
	read := func(path string) (int64, bool) {
		b, err := os.ReadFile(path)
		if err != nil {
			return 0, false
		}
		v := strings.TrimSpace(string(b))
		if v == "max" {
			return 0, false
		}
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			return 0, false
		}
		return n >> 20, true // bytes -> MiB
	}
	if mib, ok := read("/sys/fs/cgroup/memory.max"); ok {
		return mib
	}
	if mib, ok := read("/sys/fs/cgroup/memory/memory.limit_in_bytes"); ok {
		return mib
	}
	return 0
}

// buildRevision reads the VCS revision out of the binary's own build info rather
// than accepting a flag: the artifact should say which tree produced it even when
// nobody remembered to pass one.
func buildRevision() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	var rev, modified string
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			if s.Value == "true" {
				modified = "-dirty"
			}
		}
	}
	if rev == "" {
		return ""
	}
	if len(rev) > 12 {
		rev = rev[:12]
	}
	return rev + modified
}

func orUnset(s string) string {
	if s == "" {
		return "(unlabelled)"
	}
	return s
}

var _ = time.Now
