package main

// AC-30's third clause: "the ceiling matches the §8.1 table within 20%".
//
// This is not AC-43. `compare` is the replica-scaling comparison and stays that;
// clause 3 is one run of one instance declared against one row of §8.1's
// machine-class table, compared in both of the units the table is stated in
// (events/s, and deliveries/s at the declared subscription count). `run -class
// NAME` records the claim and the machine it was made on; `grade` applies the
// clause to the artifact and its exit status is the verdict, as `compare`'s is.
//
// The failure mode the clause exists to prevent is a graded number taken on a
// machine that does not carry the class, so the harness records the observed
// machine and refuses when the two disagree.

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"strings"
)

// ---------------------------------------------------------------------------
// §8.1's machine-class table
// ---------------------------------------------------------------------------

// machineClass is one row of §8.1's table (REQUIREMENTS.md §8.1, "Sustained
// deliveries/s | Events/s at 10 subscriptions").
type machineClass struct {
	Name       string  `json:"name"`
	CPUs       int     `json:"cpus"`
	MemGiB     int     `json:"mem_gib"`
	Deliveries float64 `json:"ceiling_deliveries_per_s"`
	Events     float64 `json:"ceiling_events_per_s"`
}

// classTable is §8.1's table, and the harness's only copy of its numbers.
//
// AC-30's third clause compares against "the §8.1 table", so the comparison reads
// those figures here rather than a second copy that could drift from the
// requirement: TestClassTableIsSection81 pins every row and both columns against
// §8.1's text, and an amendment to §8.1 changes this table (and that test) with it.
// The values are the requirement's rounded ones ("~1,200"), which is why the
// comparison is a band rather than an equality. The delivery column is the event
// column × 10 subscriptions for every row - §8.1 rates one workload, stated twice.
var classTable = []machineClass{
	{Name: "2vCPU-2GB", CPUs: 2, MemGiB: 2, Deliveries: 1200, Events: 120},
	{Name: "2vCPU-4GB", CPUs: 2, MemGiB: 4, Deliveries: 2500, Events: 250},
	{Name: "4vCPU-8GB", CPUs: 4, MemGiB: 8, Deliveries: 5000, Events: 500},
	{Name: "8vCPU-16GB", CPUs: 8, MemGiB: 16, Deliveries: 10000, Events: 1000},
}

const (
	// classSubs is the subscription count §8.1's event column is stated at - "Events/s
	// at 10 subscriptions". A graded run is one run of §8.1's own workload, so the
	// harness refuses another subscription count rather than scaling a number the
	// requirement did not state at that basis.
	classSubs = 10
	// classTolerance is AC-30's "within 20%", and it applies in both directions.
	// Below it the class did not reach its documented ceiling; above it §8.1's row is
	// the thing that is wrong. §12's rule is pass, or a defect to fix, or an
	// amendment to §8.1 recorded as one - never a tolerance - so neither direction is
	// a pass, and the verdict says which one it is.
	classTolerance = 0.20
	// classMemBand is how far the observed RAM may sit from the class's before the
	// machine is a different row. Equality would reject every real machine: a "16 GB"
	// host reports 15.3 GiB from /proc/meminfo, and less when a cgroup caps it. ±25%
	// accepts a 16 GB host for the 16 GB row and still rejects the 2 vCPU / 7.9 GiB
	// box this harness was developed on for either 2 vCPU row (+98%), which is the
	// direction that matters.
	classMemBand = 0.25
	// classWarmupS and classMeasureS are §12's protocol floors for a graded claim:
	// 5 minutes of warm-up excluded, 30 minutes measured. A shorter run is not the
	// protocol the numbers are defined against.
	classWarmupS  = 300
	classMeasureS = 1800
	// classEvaluateS is §12's graded window - "pass criteria evaluated over the last
	// 10 minutes" - and it is an exact match rather than a floor: the criterion names
	// that window.
	classEvaluateS = 600

	// classVerdictAC is the verdict label for clause 3, and clause12AC for the
	// artifact's own clauses 1-2, recomputed from the artifact rather than trusted
	// from the verdicts array it carries.
	classVerdictAC  = "AC-30 clause 3"
	clause12Verdict = "AC-30 clauses 1-2"
)

// normalizeClass folds a class name so the CLI accepts the way §8.1 writes the row
// ("8 vCPU / 16 GB") as well as this harness's label form ("8vCPU-16GB").
func normalizeClass(name string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(name) {
		switch r {
		case ' ', '\t', '/', '\\', '-', '_':
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// lookupClass resolves a declared name to its row.
func lookupClass(name string) (machineClass, bool) {
	want := normalizeClass(name)
	for _, c := range classTable {
		if normalizeClass(c.Name) == want {
			return c, true
		}
	}
	return machineClass{}, false
}

// classNames lists the table's rows, for a refusal message.
func classNames() string {
	names := make([]string, 0, len(classTable))
	for _, c := range classTable {
		names = append(names, c.Name)
	}
	return strings.Join(names, ", ")
}

// classMatch returns "" when the machine carries the class, and the reason it does
// not otherwise.
//
// Cores are exact: §8.1's rows differ by vCPU and the delivery ceiling is the write
// path's figure, so a 4 vCPU box is not the 8 vCPU row at any tolerance. Memory is
// a band (classMemBand). Both figures are the machine's *effective* ones - CPU
// affinity and any cgroup quota, RAM as the cgroup caps it - so a container is
// measured as the container and not as its host.
func classMatch(m machine, c machineClass) string {
	if m.CPUs != c.CPUs {
		return fmt.Sprintf("the machine has %d vCPU, the class has %d", m.CPUs, c.CPUs)
	}
	if c.MemGiB > 0 {
		observed := float64(m.MemMiB) / 1024
		if dev := (observed - float64(c.MemGiB)) / float64(c.MemGiB); math.Abs(dev) > classMemBand {
			return fmt.Sprintf("the machine has %.1f GiB of RAM, the class has %d GiB (±%.0f%%)",
				observed, c.MemGiB, classMemBand*100)
		}
	}
	return ""
}

// observedClass names the §8.1 row the machine itself matches, or "" when it
// carries none. The box this harness was developed on (2 vCPU / 7.9 GiB) matches
// no row, because §8.1 rates 2 vCPU at 2 or 4 GiB and it is neither.
func observedClass(m machine) string {
	for _, c := range classTable {
		if classMatch(m, c) == "" {
			return c.Name
		}
	}
	return ""
}

// describe is the machine's one-line form for a report and a refusal message.
func (m machine) describe() string {
	d := fmt.Sprintf("%d vCPU / %.1f GiB", m.CPUs, float64(m.MemMiB)/1024)
	if m.Hostname != "" {
		d += " on " + m.Hostname
	}
	if m.MemHostMiB > 0 && m.MemCgroupMiB > 0 {
		d += fmt.Sprintf(" (host %d MiB, cgroup %d MiB)", m.MemHostMiB, m.MemCgroupMiB)
	}
	return d
}

// ---------------------------------------------------------------------------
// The declared class, as a run records it
// ---------------------------------------------------------------------------

// classClaim is the machine class a run declares with -class: §8.1's row for it,
// how the class was evidenced, and the §12 protocol facts a verdict needs.
type classClaim struct {
	Name       string  `json:"name"`
	CPUs       int     `json:"cpus"`
	MemGiB     int     `json:"mem_gib"`
	Deliveries float64 `json:"ceiling_deliveries_per_s"`
	Events     float64 `json:"ceiling_events_per_s"`
	SubsBasis  int     `json:"subscriptions_basis"`
	Tolerance  float64 `json:"tolerance"`

	// Evidence is "observed" when the generator ran on the machine under test, so the
	// class was checked against the machine this process describes, and "attested"
	// when it ran off that machine - §12's arrangement, "the generator outside the
	// machine under test or in its own container" - so the class is the operator's
	// statement about a box this process cannot see. It is derived from
	// GeneratorOnSUT rather than passed in, so an artifact cannot claim a class was
	// observed while recording that the generator ran elsewhere.
	Evidence string `json:"class_evidence"`
	// GeneratorOnSUT records the §12 protocol fact: true when the generator shared
	// the machine under test, which makes every number in the report a lower bound
	// rather than a graded claim.
	GeneratorOnSUT bool `json:"generator_on_machine_under_test"`
	// Source says whether that was deduced from -base ("auto") or stated with
	// -generator-on-sut ("flag").
	Source string `json:"generator_host_source"`
	// Note carries what a run recorded about the claim but did not refuse.
	Note string `json:"note,omitempty"`
}

func (c machineClass) claim(onSUT bool, source string) *classClaim {
	return &classClaim{
		Name: c.Name, CPUs: c.CPUs, MemGiB: c.MemGiB,
		Deliveries: c.Deliveries, Events: c.Events,
		SubsBasis: classSubs, Tolerance: classTolerance,
		Evidence: evidenceOf(onSUT), GeneratorOnSUT: onSUT, Source: source,
	}
}

// generatorIsOnSUT decides whether the generator shared the machine under test.
//
// auto (the default) resolves -base: a loopback, unspecified or own-host address
// means the instance is on this machine, so the generator shares it unless the
// operator says otherwise. It reads addresses only - it cannot see whether a
// container gave the instance its own CPU allowance, which is the arrangement §12
// allows ("or in its own container so it does not compete for the 2 vCPU") and
// exactly why -generator-on-sut=no exists as an explicit statement. Which one was
// used is recorded next to the claim.
func generatorIsOnSUT(mode, baseURL string) (bool, string, error) {
	switch mode {
	case "yes":
		return true, "flag", nil
	case "no":
		return false, "flag", nil
	case "", "auto":
		local, err := baseIsLocal(baseURL)
		if err != nil {
			return false, "auto", err
		}
		return local, "auto", nil
	default:
		return false, "", fmt.Errorf("-generator-on-sut must be auto, yes or no (got %q)", mode)
	}
}

// baseIsLocal reports whether -base points at this machine, which is what makes the
// generator a competitor for the instance's CPU. A name that resolves to a loopback
// or local address counts; anything else does not. An unresolvable name is an error
// rather than a silent "off the machine": the run could not reach it either.
func baseIsLocal(baseURL string) (bool, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return false, fmt.Errorf("parse -base %q: %w", baseURL, err)
	}
	host := u.Hostname()
	if host == "" {
		return false, fmt.Errorf("-base %q has no host", baseURL)
	}
	if strings.EqualFold(host, "localhost") {
		return true, nil
	}
	if self, _ := os.Hostname(); self != "" && strings.EqualFold(host, self) {
		return true, nil
	}
	var resolved []string
	if ip := net.ParseIP(host); ip != nil {
		resolved = []string{host}
	} else {
		if resolved, err = net.LookupHost(host); err != nil {
			return false, fmt.Errorf("resolve -base host %q: %w", host, err)
		}
	}
	local := map[string]bool{}
	if addrs, err := net.InterfaceAddrs(); err == nil {
		for _, a := range addrs {
			if ipn, ok := a.(*net.IPNet); ok {
				local[ipn.IP.String()] = true
			}
		}
	}
	for _, s := range resolved {
		ip := net.ParseIP(s)
		if ip == nil {
			continue
		}
		if ip.IsLoopback() || ip.IsUnspecified() || local[ip.String()] {
			return true, nil
		}
	}
	return false, nil
}

// classPreflight validates a run that declares a class, before it spends 35 minutes
// on a claim that cannot be graded. It refuses a class §8.1 does not rate, a
// subscription count other than §8.1's own basis, a run with no -db (the graded
// units are database counts, not the generator's), and - when the generator runs on
// the machine under test, so that the class is observable - a machine that does not
// carry the declared class. The same rules are applied to the artifact by
// compareClass, so a run cannot pass what a verdict refuses.
func classPreflight(c config) (*classClaim, error) {
	row, ok := lookupClass(c.Class)
	if !ok {
		return nil, fmt.Errorf("-class %q is not a §8.1 row: %s", c.Class, classNames())
	}
	if c.Subs != classSubs {
		return nil, fmt.Errorf("§8.1's event column is stated at %d subscriptions per event and -class grades that workload; -subs is %d", classSubs, c.Subs)
	}
	if c.DB == "" {
		return nil, errors.New("§12 counts accepted events in events and outbox rather than from the generator's own counter; -class requires -db")
	}
	onSUT, source, err := generatorIsOnSUT(c.GeneratorOnSUT, c.BaseURL)
	if err != nil {
		return nil, err
	}
	m := describeMachine()
	claim := row.claim(onSUT, source)
	switch {
	case !onSUT:
		// §12's arrangement. The machine this process describes is the generator's,
		// not the class's, so the class is recorded as the operator's statement about
		// the box under test rather than as something this harness verified.
		claim.Note = fmt.Sprintf("the class is attested, not observed: the generator ran off the machine under test, so the machine block in this report (%s) describes the generator", m.describe())
	default:
		if reason := classMatch(m, row); reason != "" {
			return nil, fmt.Errorf("this machine does not carry -class %s: %s. §8.1 rates %s. A graded number on a machine that does not carry the class is the failure mode AC-30's third clause exists to prevent, so this run is refused rather than graded",
				row.Name, reason, classNames())
		}
	}
	return claim, nil
}

func evidenceOf(onSUT bool) string {
	if onSUT {
		return "observed"
	}
	return "attested"
}

// ---------------------------------------------------------------------------
// AC-30 clause 3: the graded window against the declared row
// ---------------------------------------------------------------------------

// classComparison is clause 3's verdict with every number it compared, so the
// verdict artifact carries the comparison rather than only its outcome.
type classComparison struct {
	Verdict  verdict      `json:"verdict"`
	Class    classClaim   `json:"class"`
	Row      machineClass `json:"section_8_1_row"`
	Evidence string       `json:"class_evidence"`
	Blockers []string     `json:"not_gradeable,omitempty"`

	Subscriptions        int     `json:"subscriptions_per_event"`
	GradedWindowS        float64 `json:"graded_window_s"`
	OfferedEventsPS      float64 `json:"offered_events_per_s"`
	ExpectedEventsPS     float64 `json:"expected_events_per_s"`
	ExpectedDeliveriesPS float64 `json:"expected_deliveries_per_s"`
	MeasuredEventsPS     float64 `json:"measured_events_per_s"`
	MeasuredDeliveriesPS float64 `json:"measured_deliveries_per_s"`
	GeneratorEventsPS    float64 `json:"generator_tally_events_per_s,omitempty"`
	EventsDeviation      float64 `json:"events_deviation"`
	DeliveriesDeviation  float64 `json:"deliveries_deviation"`
	Tolerance            float64 `json:"tolerance"`
}

// compareClass applies AC-30's third clause to a run's artifact: both units of the
// graded window - events/s, and deliveries/s at the declared subscription count -
// against the declared §8.1 row, within 20%.
//
// The row is re-read from classTable by the artifact's class NAME, so an artifact
// cannot move its own target by carrying different numbers beside the name. The
// measured units are the database's, not the generator's: §16 counts accepted events
// in events/outbox, and the graded window is reconciled the same way as the run.
func compareClass(r *report) classComparison {
	out := classComparison{
		Subscriptions:   r.Config.Subs,
		OfferedEventsPS: r.Config.TargetRate,
		Tolerance:       classTolerance,
	}
	fail := func(format string, args ...any) classComparison {
		out.Verdict = verdict{AC: classVerdictAC, OK: false, Detail: fmt.Sprintf(format, args...)}
		return out
	}
	if r.Class == nil || r.Class.Name == "" {
		return fail("the report declares no machine class, so there is no §8.1 row to compare against: re-run with -class <one of %s>, which is what makes the clause gradeable",
			classNames())
	}
	row, ok := lookupClass(r.Class.Name)
	if !ok {
		return fail("the report declares class %q, which is not a row of §8.1's table (%s)", r.Class.Name, classNames())
	}
	out.Class, out.Row, out.Evidence = *r.Class, row, r.Class.Evidence

	out.GradedWindowS = r.Graded.WindowS
	out.MeasuredEventsPS, out.MeasuredDeliveriesPS = r.Graded.EventsPS, r.Graded.DeliveriesPS
	out.GeneratorEventsPS = rate(r.Graded.Accepted, r.Graded.WindowS)
	out.ExpectedEventsPS, out.ExpectedDeliveriesPS = row.Events, row.Deliveries
	if out.ExpectedEventsPS > 0 {
		out.EventsDeviation = (out.MeasuredEventsPS - out.ExpectedEventsPS) / out.ExpectedEventsPS
	}
	if out.ExpectedDeliveriesPS > 0 {
		out.DeliveriesDeviation = (out.MeasuredDeliveriesPS - out.ExpectedDeliveriesPS) / out.ExpectedDeliveriesPS
	}

	// Every way this run cannot support the claim, named rather than folded into a
	// number: a deviation computed from a run that never reached the ceiling, was
	// graded on a different subscription count, or was never reconciled would be a
	// measurement of something else wearing the clause's name.
	var blockers []string
	// The class gate: the machine the harness describes must carry the class it is
	// grading. It applies whenever the machine under test *is* that machine - the
	// evidence is "observed" then, and an artifact that records no evidence at all is
	// checked rather than trusted - and is skipped only for an explicitly attested
	// class, where the block below is the generator's and a mismatch would say
	// nothing about the box being graded.
	if out.Evidence != "attested" {
		if reason := classMatch(r.Machine, row); reason != "" {
			blockers = append(blockers, fmt.Sprintf("the run claims %s but ran on %s: %s", row.Name, r.Machine.describe(), reason))
		}
	}
	if r.Config.Subs != classSubs {
		blockers = append(blockers, fmt.Sprintf("§8.1's event column is stated at %d subscriptions per event; this run ran %d", classSubs, r.Config.Subs))
	}
	blockers = append(blockers, windowBlockers(r.Config)...)
	if r.Config.TargetRate <= row.Events {
		blockers = append(blockers, fmt.Sprintf("the run offered %.0f events/s, at or below the declared row's %.0f: it did not go past the ceiling, so the ceiling is not what this run measured",
			r.Config.TargetRate, row.Events))
	}
	if r.Class.GeneratorOnSUT {
		blockers = append(blockers, "§12 wants the generator outside the machine under test or in its own container so it does not compete for it; this run's generator shared it, so every number here is a lower bound rather than a graded claim")
	}
	if !r.Graded.Ran {
		blockers = append(blockers, "the graded window was not reconciled against the database (-db absent or unusable), and §16 counts accepted events in events/outbox rather than from the generator's own counter")
	}
	out.Blockers = blockers
	if len(blockers) > 0 {
		return fail("clause 3 is not gradeable from this run: %s", strings.Join(blockers, "; "))
	}

	within := math.Abs(out.EventsDeviation) <= classTolerance && math.Abs(out.DeliveriesDeviation) <= classTolerance
	detail := fmt.Sprintf("class %s (%d vCPU / %d GiB, %s), graded window %.0f s at %d subscription(s)/event: events/s %.1f against §8.1's %.0f (%+.1f%%), deliveries/s %.1f against §8.1's %.0f (%+.1f%%), both within ±%.0f%%: %v",
		row.Name, row.CPUs, row.MemGiB, evidenceText(out.Evidence), out.GradedWindowS, r.Config.Subs,
		out.MeasuredEventsPS, row.Events, out.EventsDeviation*100,
		out.MeasuredDeliveriesPS, row.Deliveries, out.DeliveriesDeviation*100,
		classTolerance*100, within)
	if !within {
		detail += "; " + deviationAdvice(out)
	}
	if r.Graded.Accepted > 0 {
		detail += fmt.Sprintf("; the generator's own tally over the same window was %.1f events/s, reported as a cross-check and not graded (§16)",
			out.GeneratorEventsPS)
	}
	if r.Measured.PacerLag > 0 {
		detail += fmt.Sprintf("; the generator's pacer lagged by %d events, so the offered rate was not achieved and part of any shortfall may be the generator's", r.Measured.PacerLag)
	}
	out.Verdict = verdict{AC: classVerdictAC, OK: within, Detail: detail}
	return out
}

func evidenceText(evidence string) string {
	switch evidence {
	case "observed":
		return "observed on the machine under test"
	case "attested":
		return "class attested by the operator on the machine under test"
	default:
		return "class evidence unrecorded"
	}
}

// deviationAdvice names which side of §12's rule a miss falls on. "A tolerance is
// not an outcome": a miss is a defect or an amendment, and which one is decided by
// the sign.
func deviationAdvice(out classComparison) string {
	below := out.EventsDeviation < -classTolerance || out.DeliveriesDeviation < -classTolerance
	above := out.EventsDeviation > classTolerance || out.DeliveriesDeviation > classTolerance
	switch {
	case below && above:
		return "one unit is below the row and the other above it, so the miss is not a single correction: §12's verdict is a defect to fix (named with its owning card) or an amendment to §8.1 recorded as one, and the owner decides which"
	case below:
		return "below the declared row by more than 20%: §12 makes that a defect to fix, named with its owning card, not a tolerance added to the test"
	default:
		return "above the declared row by more than 20%: §12 makes that an amendment to §8.1 recorded as one in the gap list, with this measurement attached"
	}
}

// windowBlockers checks the artifact's windows against §12's protocol: 5 minutes of
// warm-up excluded, 30 minutes measured, pass criteria over the last 10.
func windowBlockers(cfg runConfig) []string {
	var out []string
	if cfg.WarmupS < classWarmupS {
		out = append(out, fmt.Sprintf("§12 excludes 5 minutes of warm-up; this run excluded %.0f s", cfg.WarmupS))
	}
	if cfg.MeasureS < classMeasureS {
		out = append(out, fmt.Sprintf("§12 measures for 30 minutes; this run measured %.0f s", cfg.MeasureS))
	}
	if cfg.EvaluateS != classEvaluateS {
		out = append(out, fmt.Sprintf("§12 grades the last 10 minutes; this run's graded window is %.0f s", cfg.EvaluateS))
	}
	return out
}

func rate(n int64, sec float64) float64 {
	if sec <= 0 {
		return 0
	}
	return float64(n) / sec
}

// ---------------------------------------------------------------------------
// `grade`: clause 3's verdict, and its exit status
// ---------------------------------------------------------------------------

// gradeResult is `grade`'s JSON artifact: the clause-3 comparison with the numbers
// it compared, and the artifact's own clause 1-2 verdict recomputed from the run's
// recorded phase data (not read back from the verdicts array the artifact carries,
// which a doctored artifact could have written).
type gradeResult struct {
	Report     string          `json:"report"`
	Label      string          `json:"label"`
	Revision   string          `json:"revision,omitempty"`
	Machine    machine         `json:"machine"`
	Comparison classComparison `json:"comparison"`
	Verdicts   []verdict       `json:"verdicts"`
	OK         bool            `json:"ok"`
}

func cmdGrade(args []string) error {
	fs := flag.NewFlagSet("grade", flag.ContinueOnError)
	var label, out, wantClass string
	fs.StringVar(&label, "label", "", "the run's JSON report to grade")
	fs.StringVar(&out, "out", "", "write the verdict JSON here")
	fs.StringVar(&wantClass, "class", "", "assert the artifact declares this class")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if label == "" {
		return errors.New("-label is required (the JSON report `run` wrote)")
	}
	b, err := os.ReadFile(label)
	if err != nil {
		return fmt.Errorf("read %s: %w", label, err)
	}
	var r report
	if err := json.Unmarshal(b, &r); err != nil {
		return fmt.Errorf("parse %s: %w", label, err)
	}
	if wantClass != "" {
		if r.Class == nil || r.Class.Name == "" {
			return fmt.Errorf("-class %s was asserted but %s declares no class", wantClass, label)
		}
		if a, b := normalizeClass(wantClass), normalizeClass(r.Class.Name); a != b {
			return fmt.Errorf("-class %s was asserted but %s declares %s", wantClass, label, r.Class.Name)
		}
	}

	res := gradeResult{
		Report:   label,
		Label:    r.Label,
		Revision: r.Machine.Revision,
		Machine:  r.Machine,
	}
	res.Comparison = compareClass(&r)
	// Clauses 1 and 2 are decided by the run's own numbers, recomputed here from the
	// artifact so the verdicts array is not the evidence.
	c := config{Rate: r.Config.TargetRate, Subs: r.Config.Subs}
	for _, v := range judge(&r, c) {
		if v.AC == "AC-30" {
			v.AC = clause12Verdict
			res.Verdicts = append(res.Verdicts, v)
		}
	}
	res.Verdicts = append(res.Verdicts, res.Comparison.Verdict)
	res.OK = true
	for _, v := range res.Verdicts {
		res.OK = res.OK && v.OK
	}

	printGrade(&res)
	if out != "" {
		enc, _ := json.MarshalIndent(res, "", "  ")
		if err := os.WriteFile(out, append(enc, '\n'), 0o644); err != nil {
			return fmt.Errorf("write verdict: %w", err)
		}
		fmt.Printf("\njson verdict: %s\n", out)
	}
	if !res.OK {
		return errors.New("AC-30 is not green on this artifact; see the verdicts above")
	}
	return nil
}

func printGrade(res *gradeResult) {
	row := res.Comparison.Row
	fmt.Println()
	if row.Name != "" {
		fmt.Printf("§8.1 row %s: %d vCPU / %d GiB, ceiling ~%.0f deliveries/s, ~%.0f events/s at %d subscriptions\n",
			row.Name, row.CPUs, row.MemGiB, row.Deliveries, row.Events, classSubs)
		fmt.Printf("  graded window %.0f s, offered %.0f events/s, class evidence: %s\n",
			res.Comparison.GradedWindowS, res.Comparison.OfferedEventsPS, evidenceText(res.Comparison.Evidence))
		fmt.Printf("  %-14s %12s %12s %11s %10s\n", "unit", "§8.1 row", "measured", "deviation", "tolerance")
		fmt.Printf("  %-14s %12.1f %12.1f %10.1f%% %9.0f%%\n", "events/s",
			res.Comparison.ExpectedEventsPS, res.Comparison.MeasuredEventsPS, res.Comparison.EventsDeviation*100, res.Comparison.Tolerance*100)
		fmt.Printf("  %-14s %12.1f %12.1f %10.1f%% %9.0f%%\n", "deliveries/s",
			res.Comparison.ExpectedDeliveriesPS, res.Comparison.MeasuredDeliveriesPS, res.Comparison.DeliveriesDeviation*100, res.Comparison.Tolerance*100)
		if res.Comparison.GeneratorEventsPS > 0 {
			fmt.Printf("  (the generator's own tally over the same window: %.1f events/s - reported as a cross-check, not graded)\n", res.Comparison.GeneratorEventsPS)
		}
	}
	fmt.Println()
	for _, v := range res.Verdicts {
		fmt.Printf("%s %-22s %s\n", verdictStatus(v.OK), v.AC, v.Detail)
	}
	if res.Machine.CPUs > 0 {
		fmt.Printf("\nmeasured on %s\n", res.Machine.describe())
	}
}
