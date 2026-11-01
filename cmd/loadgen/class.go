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
// machine that does not carry the class, so the harness records the machine under
// test and refuses when the two disagree.
//
// "The machine under test" is not the machine this process runs on. §12 wants the
// generator outside the machine under test or in its own container, and this process
// *is* the generator: on every gradeable run the machine block describes the
// generator's box. So a graded run declares the machine under test with
// -sut-cpus/-sut-mem-gib, the artifact carries it as machine_under_test, and the
// class is checked against *that* - in `run` before the 35 minutes are spent, and in
// `grade` from the artifact. An artifact with no machine under test cannot support
// the claim and is refused rather than graded.

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

const (
	// machineObserved and machineDeclared say how a machine under test was
	// established, and they are recorded in the artifact beside it. "observed" is
	// the machine this process ran on, which is the machine under test only when the
	// generator shared it; "declared" is -sut-cpus/-sut-mem-gib, the operator's
	// statement about a machine the generator cannot see (§12's arrangement).
	machineObserved = "observed"
	machineDeclared = "declared"
)

// sameMachine returns "" when two machine descriptions are the same machine, and what
// differs otherwise. It is the class match with the class taken out: cores are exact
// and memory is the classMemBand, so a declaration cannot be tuned to agree with the
// observed machine by a hair.
func sameMachine(a, b machine) string {
	if a.CPUs != b.CPUs {
		return fmt.Sprintf("%d vCPU against %d", a.CPUs, b.CPUs)
	}
	am, bm := float64(a.MemMiB)/1024, float64(b.MemMiB)/1024
	if am > 0 && bm > 0 {
		if dev := (am - bm) / bm; math.Abs(dev) > classMemBand {
			return fmt.Sprintf("%.1f GiB against %.1f GiB (±%.0f%% allowed)", am, bm, classMemBand*100)
		}
	}
	return ""
}

// declaredSUT is the machine under test as the operator stated it (-sut-cpus,
// -sut-mem-gib). Both figures are required together: a machine named by its cores
// alone or its RAM alone is not a machine, and filling the other half in is how an
// unrated box gets graded.
func (c config) declaredSUT() (machine, bool, error) {
	switch {
	case c.SUTCPUs == 0 && c.SUTMemGiB == 0:
		return machine{}, false, nil
	case c.SUTCPUs <= 0 || c.SUTMemGiB <= 0:
		return machine{}, false, errors.New("-sut-cpus and -sut-mem-gib describe the machine under test (-class checks the class against it) and must be given together, both positive")
	}
	m := machine{
		CPUs:   c.SUTCPUs,
		MemMiB: int64(math.Round(c.SUTMemGiB * 1024)),
		Source: machineDeclared,
	}
	m.ObservedClass = observedClass(m)
	return m, true, nil
}

// machineUnderTest resolves the machine the declared class is claimed for.
//
// When the generator shared the machine under test (§12's lower-bound arrangement)
// the machine under test is the one this process describes - the only machine the
// harness can see - and a -sut-cpus/-sut-mem-gib declaration that disagrees with it
// is refused rather than picked over: otherwise a run could describe a 2 vCPU box in
// the machine block and claim the 8 vCPU class in a flag.
//
// When the generator ran elsewhere - the arrangement §12 asks for, and the only one
// clause 3 can be graded from - the harness cannot see the machine under test at all,
// so the declaration is required. Without it the class would be checked against
// nothing, which is exactly the graded-number-on-an-unrated-box failure the clause
// exists to prevent.
func machineUnderTest(c config, observed machine, onSUT bool) (machine, error) {
	declared, ok, err := c.declaredSUT()
	if err != nil {
		return machine{}, err
	}
	if onSUT {
		if ok {
			if reason := sameMachine(observed, declared); reason != "" {
				return machine{}, fmt.Errorf("the generator shared the machine under test, so the machine under test is %s, but -sut-cpus %d -sut-mem-gib %g declares %s: %s",
					observed.describe(), c.SUTCPUs, c.SUTMemGiB, declared.describe(), reason)
			}
		}
		observed.Source = machineObserved
		return observed, nil
	}
	if !ok {
		return machine{}, errors.New("§12 puts the generator off the machine under test, so this process cannot see the machine the class is claimed for: declare it with -sut-cpus N -sut-mem-gib N (the instance's vCPU and RAM in GiB). Without it the class is checked against nothing, which is the failure mode AC-30's third clause exists to prevent")
	}
	return declared, nil
}

// describe is the machine's one-line form for a report and a refusal message.
func (m machine) describe() string {
	d := fmt.Sprintf("%d vCPU / %.1f GiB", m.CPUs, float64(m.MemMiB)/1024)
	if m.Source != "" {
		d += " (" + m.Source + ")"
	}
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
// units are database counts, not the generator's), a machine under test it cannot
// identify (-sut-cpus/-sut-mem-gib when the generator runs off the machine), and a
// machine under test that does not carry the declared class. The same rules are
// applied to the artifact by compareClass, so a run cannot pass what a verdict
// refuses.
//
// It returns the claim and the machine under test the claim is checked against; the
// run records both.
func classPreflight(c config) (*classClaim, machine, error) {
	row, ok := lookupClass(c.Class)
	if !ok {
		return nil, machine{}, fmt.Errorf("-class %q is not a §8.1 row: %s", c.Class, classNames())
	}
	if c.Subs != classSubs {
		return nil, machine{}, fmt.Errorf("§8.1's event column is stated at %d subscriptions per event and -class grades that workload; -subs is %d", classSubs, c.Subs)
	}
	if c.BaseURL == "" {
		c.BaseURL = "http://127.0.0.1:8080"
	}
	onSUT, source, err := generatorIsOnSUT(c.GeneratorOnSUT, c.BaseURL)
	if err != nil {
		return nil, machine{}, err
	}
	m := describeMachine()
	sut, err := machineUnderTest(c, m, onSUT)
	if err != nil {
		return nil, machine{}, err
	}
	claim := row.claim(onSUT, source)
	if !onSUT {
		// §12's arrangement. The machine this process describes is the generator's,
		// not the class's, so the class is recorded as the operator's statement about
		// the box under test rather than as something this harness verified.
		claim.Note = fmt.Sprintf("the class is attested, not observed: the generator ran off the machine under test, so the machine block in this report (%s) describes the generator and the class is checked against the declared machine under test (%s)",
			m.describe(), sut.describe())
	} else {
		claim.Note = fmt.Sprintf("the class is observed on this machine (%s), and this run cannot be graded: §12 wants the generator outside the machine under test or in its own container, and this generator shared it, so every number here is a lower bound. A graded run puts the generator on another host (or in its own container, stated with -generator-on-sut no) and declares the machine under test with -sut-cpus/-sut-mem-gib",
			sut.describe())
	}
	if reason := classMatch(sut, row); reason != "" {
		return nil, machine{}, fmt.Errorf("the machine under test does not carry -class %s: %s (§8.1 rates %s). A graded number on a machine that does not carry the class is the failure mode AC-30's third clause exists to prevent, so this run is refused rather than graded",
			row.Name, reason, classNames())
	}
	if c.DB == "" {
		return nil, machine{}, errors.New("§12 counts accepted events in events and outbox rather than from the generator's own counter; -class requires -db")
	}
	return claim, sut, nil
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
	// MachineUnderTest is the machine the class was checked against, as the artifact
	// recorded it.
	MachineUnderTest *machine `json:"machine_under_test,omitempty"`
	Blockers         []string `json:"not_gradeable,omitempty"`

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
	out.MachineUnderTest = r.MachineUnderTest

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
	// The class gate: the machine the class is claimed for must carry it.
	//
	// It reads machine_under_test, not the machine block. §12 puts the generator off
	// the machine under test and this harness *is* the generator, so on every
	// gradeable run the block describes the generator's box: checking the class
	// against it would check the class against the wrong machine, and would never
	// fire on the runs the clause is graded from. The machine under test is the
	// machine-block's figures when the generator shared it and the operator's
	// -sut-cpus/-sut-mem-gib declaration when it did not. An artifact that records
	// neither cannot support the claim.
	if r.MachineUnderTest == nil || r.MachineUnderTest.CPUs <= 0 {
		blockers = append(blockers, "the report records no machine under test, so the declared class is checked against nothing: a graded run declares it with -sut-cpus N -sut-mem-gib N (the machine the instance ran on, §12 puts the generator off it)")
	} else {
		if reason := classMatch(*r.MachineUnderTest, row); reason != "" {
			blockers = append(blockers, fmt.Sprintf("the run claims %s but the machine under test is %s: %s", row.Name, r.MachineUnderTest.describe(), reason))
		}
		if out.Evidence == evidenceOf(true) {
			// The class is recorded as observed, so the machine under test is by
			// definition the machine this process described. An artifact whose two
			// blocks disagree is not describing one machine.
			if reason := sameMachine(r.Machine, *r.MachineUnderTest); reason != "" {
				blockers = append(blockers, fmt.Sprintf("the class is recorded as observed, so the machine under test is the machine the generator ran on, but the two blocks disagree: %s", reason))
			}
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
	detail := fmt.Sprintf("class %s (%d vCPU / %d GiB, %s), machine under test %s, graded window %.0f s at %d subscription(s)/event: events/s %.1f against §8.1's %.0f (%+.1f%%), deliveries/s %.1f against §8.1's %.0f (%+.1f%%), both within ±%.0f%%: %v",
		row.Name, row.CPUs, row.MemGiB, evidenceText(out.Evidence), sutText(r.MachineUnderTest), out.GradedWindowS, r.Config.Subs,
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

// sutText names the machine under test in a verdict, and says so plainly when the
// artifact records none - which is a refusal, not a footnote.
func sutText(m *machine) string {
	if m == nil || m.CPUs <= 0 {
		return "unrecorded"
	}
	return m.describe()
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
	Report string `json:"report"`
	Label  string `json:"label"`
	// Revision is the tree the artifact was produced from, read from the generator's
	// machine block (the block describes the generator on every §12 run).
	Revision string `json:"revision,omitempty"`
	// Machine is the generator's machine, as the artifact's block recorded it. The
	// machine the class is checked against is MachineUnderTest, not this: see
	// classComparison.MachineUnderTest.
	Machine machine `json:"machine"`
	// MachineUnderTest is the machine the clause was decided against - the same one as
	// comparison.machine_under_test, lifted here so a verdict can be read without
	// digging into the comparison.
	MachineUnderTest *machine        `json:"machine_under_test,omitempty"`
	Comparison       classComparison `json:"comparison"`
	Verdicts         []verdict       `json:"verdicts"`
	OK               bool            `json:"ok"`
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
	res.MachineUnderTest = res.Comparison.MachineUnderTest
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
		fmt.Printf("  machine under test: %s\n", sutText(res.Comparison.MachineUnderTest))
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
		fmt.Printf("\nmeasured on %s (the generator's machine; the class is claimed for the machine under test above)\n", res.Machine.describe())
	}
}
