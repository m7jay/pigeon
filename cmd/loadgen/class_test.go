package main

// Deterministic tests for AC-30's third clause. They need no rated host, no
// database and no network: the clause's inputs are the §8.1 table, the artifact's
// window lengths and two database counts, so the verdict is a pure function of an
// artifact and the table. The one end-to-end test drives a fake instance over
// loopback; the one process-level test builds this package and reads its exit
// status, because the exit status is the verdict.

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// §8.1's table, and reading a class name
// ---------------------------------------------------------------------------

// TestClassTableIsSection81 pins the harness's table against the requirement's own
// text (REQUIREMENTS.md §8.1, the "Machine | Sustained deliveries/s | Events/s at 10
// subscriptions" table). AC-30's third clause compares against *that* table, so a
// copy that has drifted from it would grade the clause against numbers nobody
// agreed to. If §8.1 is amended, both move together.
func TestClassTableIsSection81(t *testing.T) {
	want := []machineClass{
		{Name: "2vCPU-2GB", CPUs: 2, MemGiB: 2, Deliveries: 1200, Events: 120},
		{Name: "2vCPU-4GB", CPUs: 2, MemGiB: 4, Deliveries: 2500, Events: 250},
		{Name: "4vCPU-8GB", CPUs: 4, MemGiB: 8, Deliveries: 5000, Events: 500},
		{Name: "8vCPU-16GB", CPUs: 8, MemGiB: 16, Deliveries: 10000, Events: 1000},
	}
	if len(classTable) != len(want) {
		t.Fatalf("classTable has %d rows, §8.1 has %d", len(classTable), len(want))
	}
	for i, w := range want {
		got := classTable[i]
		if got != w {
			t.Errorf("classTable[%d] = %+v, §8.1's row is %+v", i, got, w)
		}
		if want := got.Events * classSubs; got.Deliveries != want {
			t.Errorf("%s: §8.1 states one workload twice, so %.0f deliveries/s must be %.0f events/s × %d subscriptions",
				got.Name, got.Deliveries, got.Events, classSubs)
		}
	}
	if classSubs != 10 {
		t.Errorf("classSubs = %d; §8.1's event column is headed \"at 10 subscriptions\"", classSubs)
	}
	if classTolerance != 0.20 {
		t.Errorf("classTolerance = %v; AC-30 says \"within 20%%\"", classTolerance)
	}
}

// TestClassNamesAcceptSection81Spellings: the operator should be able to declare the
// class the way §8.1 writes it.
func TestClassNamesAcceptSection81Spellings(t *testing.T) {
	for _, name := range []string{"8vCPU-16GB", "8 vCPU / 16 GB", "  8VCPU_16GB ", "8 vcpu/16 gb"} {
		got, ok := lookupClass(name)
		if !ok {
			t.Fatalf("lookupClass(%q) did not find §8.1's 8 vCPU / 16 GB row", name)
		}
		if got.Name != "8vCPU-16GB" {
			t.Errorf("lookupClass(%q) = %s, want the 8vCPU-16GB row", name, got.Name)
		}
	}
	if _, ok := lookupClass("3vCPU-6GB"); ok {
		t.Error("a class §8.1 does not rate was accepted")
	}
	if _, ok := lookupClass(""); ok {
		t.Error("an empty class name was accepted")
	}
	for _, name := range []string{"2vCPU-2GB", "2vCPU-4GB", "4vCPU-8GB", "8vCPU-16GB"} {
		if !strings.Contains(classNames(), name) {
			t.Errorf("classNames() omits %s, so a refusal would not say what is rateable", name)
		}
	}
}

// TestClassMatchIsCoresExactAndMemoryABand: §8.1's rows differ by vCPU and by RAM.
// Cores decide, because the delivery ceiling is the write path's figure and scales
// with them; memory is a band, because no real machine reports its class's RAM
// exactly.
func TestClassMatchIsCoresExactAndMemoryABand(t *testing.T) {
	row := func(name string) machineClass {
		c, ok := lookupClass(name)
		if !ok {
			t.Fatalf("no such class %q", name)
		}
		return c
	}
	cases := []struct {
		name      string
		machine   machine
		class     string
		wantMatch bool
	}{
		{"the class itself", machine{CPUs: 2, MemMiB: 4 * 1024}, "2vCPU-4GB", true},
		{"a 16 GB host reporting 15 GiB", machine{CPUs: 8, MemMiB: 15 * 1024}, "8vCPU-16GB", true},
		{"a 4 GiB container", machine{CPUs: 2, MemMiB: 4096}, "2vCPU-4GB", true},
		{"the 2 GiB row's machine", machine{CPUs: 2, MemMiB: 2048}, "2vCPU-2GB", true},
		// This repository's box: 2 vCPU, 7.75 GiB. §8.1 rates 2 vCPU at 2 or 4 GiB, so
		// it carries neither row, and the refusal must be about the RAM.
		{"this repo's 2 vCPU / 7.75 GiB box against 2vCPU-4GB", machine{CPUs: 2, MemMiB: 7936}, "2vCPU-4GB", false},
		{"this repo's 2 vCPU / 7.75 GiB box against 2vCPU-2GB", machine{CPUs: 2, MemMiB: 7936}, "2vCPU-2GB", false},
		{"2 GiB claimed on a 4 GiB box", machine{CPUs: 2, MemMiB: 4096}, "2vCPU-2GB", false},
		{"4 vCPU is not the 8 vCPU row", machine{CPUs: 4, MemMiB: 16 * 1024}, "8vCPU-16GB", false},
	}
	for _, tc := range cases {
		reason := classMatch(tc.machine, row(tc.class))
		if tc.wantMatch && reason != "" {
			t.Errorf("%s: %s was refused (%s)", tc.name, tc.class, reason)
		}
		if !tc.wantMatch && reason == "" {
			t.Errorf("%s: %s was accepted on %s", tc.name, tc.class, tc.machine.describe())
		}
		if !tc.wantMatch && reason != "" && !strings.Contains(reason, "RAM") && !strings.Contains(reason, "vCPU") {
			t.Errorf("%s: the refusal does not say what differs: %s", tc.name, reason)
		}
	}
}

// TestObservedClassNamesTheRowTheMachineCarries: the artifact records the row its own
// machine matches, which is what makes an unrated box visible in the report rather
// than only in a verdict.
func TestObservedClassNamesTheRowTheMachineCarries(t *testing.T) {
	cases := []struct {
		machine machine
		want    string
	}{
		{machine{CPUs: 2, MemMiB: 7936}, ""},                // this repo's box: no row
		{machine{CPUs: 2, MemMiB: 4096}, "2vCPU-4GB"},       // a 2 vCPU / 4 GiB container
		{machine{CPUs: 8, MemMiB: 15 * 1024}, "8vCPU-16GB"}, // a 16 GB host
		{machine{CPUs: 16, MemMiB: 64 * 1024}, ""},          // bigger than any row
	}
	for _, tc := range cases {
		if got := observedClass(tc.machine); got != tc.want {
			t.Errorf("observedClass(%s) = %q, want %q", tc.machine.describe(), got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// The preflight: what a run refuses before spending 35 minutes
// ---------------------------------------------------------------------------

// TestClassPreflightRefusesAMachineThatDoesNotCarryTheClass is the card's own
// requirement: a graded number on an unrated box is the failure mode clause 3 exists
// to prevent, so the harness refuses the claim rather than measuring and explaining
// afterwards.
func TestClassPreflightRefusesAMachineThatDoesNotCarryTheClass(t *testing.T) {
	m := describeMachine()
	unrated := ""
	for _, row := range classTable {
		if classMatch(m, row) != "" {
			unrated = row.Name
			break
		}
	}
	if unrated == "" {
		t.Fatalf("every §8.1 row matches %s, so this test cannot build an unrated claim", m.describe())
	}
	_, err := classPreflight(config{
		Class: unrated, Subs: classSubs, DB: "postgres://user@localhost/db",
		BaseURL: "http://127.0.0.1:8080",
	})
	if err == nil {
		t.Fatalf("-class %s was accepted on %s, which does not carry it", unrated, m.describe())
	}
	for _, want := range []string{unrated, "does not carry"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not mention %q: %v", want, err)
		}
	}
	// The mirror: a class the machine *does* carry is accepted, with the class
	// observed rather than attested (the generator is on the machine under test here).
	if carried := observedClass(m); carried != "" {
		claim, err := classPreflight(config{
			Class: carried, Subs: classSubs, DB: "postgres://user@localhost/db",
			BaseURL: "http://127.0.0.1:8080",
		})
		if err != nil {
			t.Fatalf("-class %s was refused on the machine that carries it: %v", carried, err)
		}
		if claim.Evidence != "observed" || !claim.GeneratorOnSUT {
			t.Errorf("claim on %s = evidence %q, generator_on_sut %v; want observed/true for a loopback run",
				carried, claim.Evidence, claim.GeneratorOnSUT)
		}
	}
}

// TestClassPreflightRequiresSection81BasisAndTheDatabase: §8.1's event column is
// stated at 10 subscriptions, and §16 counts accepted events in the database rather
// than from the generator's counter, so a claim that cannot be graded that way is
// refused up front.
func TestClassPreflightRequiresSection81BasisAndTheDatabase(t *testing.T) {
	base := func() config {
		return config{Class: "2vCPU-4GB", Subs: classSubs, DB: "postgres://user@localhost/db", BaseURL: "http://127.0.0.1:8080"}
	}
	c := base()
	c.Subs = 1
	if _, err := classPreflight(c); err == nil || !strings.Contains(err.Error(), "10 subscriptions") {
		t.Errorf("-subs 1 with -class: err = %v, want a refusal naming §8.1's 10-subscription basis", err)
	}
	c = base()
	c.DB = ""
	if _, err := classPreflight(c); err == nil || !strings.Contains(err.Error(), "-db") {
		t.Errorf("-class without -db: err = %v, want a refusal naming the database", err)
	}
	c = base()
	c.Class = "3vCPU-6GB"
	if _, err := classPreflight(c); err == nil || !strings.Contains(err.Error(), "3vCPU-6GB") {
		t.Errorf("-class 3vCPU-6GB: err = %v, want a refusal naming the table", err)
	}
}

// TestGeneratorOnSUTResolvesFromBaseAndFlag: §12 allows the generator off the machine
// under test or in its own container. auto resolves it from -base - the only thing
// this process can see - and the flag overrides it, because a container's CPU
// allowance is not visible from the generator's address.
func TestGeneratorOnSUTResolvesFromBaseAndFlag(t *testing.T) {
	cases := []struct {
		mode, base string
		wantOnSUT  bool
		wantSource string
		wantErr    bool
	}{
		{"auto", "http://127.0.0.1:8080", true, "auto", false},
		{"", "http://localhost:8080", true, "auto", false},
		{"auto", "http://192.0.2.10:8080", false, "auto", false},
		{"yes", "http://192.0.2.10:8080", true, "flag", false},
		{"no", "http://127.0.0.1:8080", false, "flag", false},
		{"maybe", "http://127.0.0.1:8080", false, "", true},
	}
	for _, tc := range cases {
		on, source, err := generatorIsOnSUT(tc.mode, tc.base)
		if tc.wantErr {
			if err == nil {
				t.Errorf("generatorIsOnSUT(%q) accepted an unknown mode", tc.mode)
			}
			continue
		}
		if err != nil {
			t.Errorf("generatorIsOnSUT(%q, %q): %v", tc.mode, tc.base, err)
			continue
		}
		if on != tc.wantOnSUT || source != tc.wantSource {
			t.Errorf("generatorIsOnSUT(%q, %q) = %v/%s, want %v/%s", tc.mode, tc.base, on, source, tc.wantOnSUT, tc.wantSource)
		}
	}
}

// ---------------------------------------------------------------------------
// Clause 3: the comparison itself
// ---------------------------------------------------------------------------

// classArtifact builds the artifact `run -class` would write: §12's windows, a
// declared class, and a graded window reconciled against the database. The two units
// are passed as *counts* over the 600 s window, so the expected rates are exact and
// the tolerance's boundary can be tested without floating-point drift.
//
// onSUT is §12's generator fact. When it is true the generator shared the machine
// under test, which makes the class observable - and the run a lower bound.
func classArtifact(t *testing.T, className string, events, deliveries int64, onSUT bool) *report {
	t.Helper()
	row, ok := lookupClass(className)
	if !ok {
		t.Fatalf("no such class %q", className)
	}
	start := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	r := &report{Label: "fixture", StartedAt: start, EndedAt: start.Add(35 * time.Minute)}
	r.Class = row.claim(onSUT, "flag")
	r.Config = runConfig{
		BaseURL: "http://127.0.0.1:8080", EventType: "load.created",
		TargetRate: row.Events * 1.2, PayloadByte: referencePayloadBytes,
		Subs: classSubs, WarmupS: classWarmupS, MeasureS: classMeasureS,
		EvaluateS: classEvaluateS, Concurrency: 512,
	}
	// The machine block the harness records. For an observed class it is the graded
	// machine; for an attested one the generator's box happens to be the same class.
	r.Machine = machine{
		Hostname: "sut", CPUs: row.CPUs, MemMiB: int64(row.MemGiB) * 1024,
		MemHostMiB: int64(row.MemGiB) * 1024, ObservedClass: row.Name,
		Kernel: "linux fixture", Go: runtime.Version(),
	}
	// The graded window: the last 10 minutes, reconciled against the database (§16).
	r.Graded = reconcile{Events: events, Outbox: events, Deliveries: deliveries}
	fillRate(&r.Graded, window{
		From: start.Add(25 * time.Minute), To: start.Add(35 * time.Minute), Accepted: events,
	}, 0)
	// The run window, and clauses 1-2's phase data (shedding, p99 flat).
	r.Reconcile = reconcile{Events: events * 5, Outbox: events * 5, Deliveries: deliveries * 5}
	fillRate(&r.Reconcile, window{From: start, To: start.Add(35 * time.Minute), Accepted: events * 5}, 0)
	r.Evaluated = phase{Name: "evaluate", Seconds: classEvaluateS, Attempted: events,
		Accepted: events, Shed: 12, P50MS: 12, P95MS: 30, P99MS: 40, MaxMS: 60}
	r.FirstEval = phase{Name: "first-evaluate-window", Seconds: classEvaluateS,
		Attempted: events, Accepted: events, Shed: 12, P99MS: 38}
	r.Measured = phase{Name: "measure", Seconds: classMeasureS, Attempted: events * 3,
		Accepted: events * 3, Shed: 36, P99MS: 40}
	return r
}

// TestGradeComparesBothUnitsOfTheGradedWindow is the clause: events/s *and*
// deliveries/s at the declared subscription count, both within 20% of the row, with
// the numbers printed and carried in the verdict.
func TestGradeComparesBothUnitsOfTheGradedWindow(t *testing.T) {
	// 8 vCPU / 16 GB: §8.1's row is ~1,000 events/s and ~10,000 deliveries/s, so the
	// window's counts are 600,000 events and 6,000,000 deliveries for an exact match.
	const class = "8vCPU-16GB"
	cases := []struct {
		name               string
		events, deliveries int64
		wantOK             bool
		wantDetail         string
	}{
		{"exactly on the row", 600000, 6000000, true, ""},
		{"4% under on both units", 576000, 5760000, true, ""},
		{"the 20% boundary, inclusive", 720000, 7200000, true, ""},
		{"just past the boundary", 720600, 7206000, false, "amendment to §8.1"},
		{"one unit far below the row", 420000, 6000000, false, "below the declared row"},
		{"just below the boundary", 479400, 6000000, false, "below the declared row"},
		{"the delivery unit far above the row", 600000, 8400000, false, "amendment to §8.1"},
	}
	for _, tc := range cases {
		r := classArtifact(t, class, tc.events, tc.deliveries, false)
		cmp := compareClass(r)
		if cmp.Verdict.OK != tc.wantOK {
			t.Errorf("%s: clause 3 = %v, want %v (%s)", tc.name, cmp.Verdict.OK, tc.wantOK, cmp.Verdict.Detail)
		}
		if tc.wantDetail != "" && !strings.Contains(cmp.Verdict.Detail, tc.wantDetail) {
			t.Errorf("%s: the verdict does not say which side of §12's rule the miss falls on: %s", tc.name, cmp.Verdict.Detail)
		}
		if len(cmp.Blockers) != 0 {
			t.Errorf("%s: an artifact that meets the protocol was blocked: %v", tc.name, cmp.Blockers)
		}
		if cmp.ExpectedEventsPS != 1000 || cmp.ExpectedDeliveriesPS != 10000 {
			t.Errorf("%s: compared against %v/%v, want §8.1's 1000/10000", tc.name, cmp.ExpectedEventsPS, cmp.ExpectedDeliveriesPS)
		}
		// The numbers compared are in the verdict and in the artifact.
		for _, want := range []string{fmt.Sprintf("%.0f", cmp.ExpectedEventsPS), fmt.Sprintf("%.1f", cmp.MeasuredEventsPS)} {
			if !strings.Contains(cmp.Verdict.Detail, want) {
				t.Errorf("%s: the verdict does not print %s: %s", tc.name, want, cmp.Verdict.Detail)
			}
		}
	}
}

// TestGradeReadsTheTargetFromTheTableNotTheArtifact: an artifact carries the row it
// was graded against, and it must not be able to move its own target by carrying
// friendlier numbers beside the name.
func TestGradeReadsTheTargetFromTheTableNotTheArtifact(t *testing.T) {
	r := classArtifact(t, "2vCPU-4GB", 150000, 1500000, false) // exactly on the 2 vCPU / 4 GB row
	r.Class.Events, r.Class.Deliveries = 1, 1                  // doctored: a target of one event/s
	cmp := compareClass(r)
	if !cmp.Verdict.OK {
		t.Fatalf("the artifact's doctored target overrode §8.1's row: %s", cmp.Verdict.Detail)
	}
	if cmp.ExpectedEventsPS != 250 || cmp.ExpectedDeliveriesPS != 2500 {
		t.Errorf("compared against %v/%v, want §8.1's 250/2500 for 2vCPU-4GB", cmp.ExpectedEventsPS, cmp.ExpectedDeliveriesPS)
	}
}

// TestGradeFailsWhenTheMachineDoesNotCarryTheClass is the clause's own failure mode,
// asserted at the verdict as well as at the preflight: the harness records the
// machine and refuses a class that machine does not carry.
func TestGradeFailsWhenTheMachineDoesNotCarryTheClass(t *testing.T) {
	cases := []struct {
		name    string
		machine machine
		want    []string
	}{
		{"a 2 vCPU box claiming the 8 vCPU row", machine{Hostname: "shared-box", CPUs: 2, MemMiB: 7936},
			[]string{"8vCPU-16GB", "2 vCPU"}},
		{"enough cores, the wrong RAM", machine{Hostname: "shared-box", CPUs: 8, MemMiB: 40 * 1024},
			[]string{"8vCPU-16GB", "RAM"}},
	}
	for _, tc := range cases {
		r := classArtifact(t, "8vCPU-16GB", 600000, 6000000, true)
		r.Machine = tc.machine
		cmp := compareClass(r)
		if cmp.Verdict.OK {
			t.Fatalf("%s: a number on a machine that does not carry the class was graded green: %s", tc.name, cmp.Verdict.Detail)
		}
		if len(cmp.Blockers) == 0 {
			t.Fatalf("%s: the verdict reports no blocker", tc.name)
		}
		joined := strings.Join(cmp.Blockers, "; ")
		for _, want := range tc.want {
			if !strings.Contains(joined, want) {
				t.Errorf("%s: the blocker does not mention %q: %s", tc.name, want, joined)
			}
		}
	}
}

// TestGradeChecksTheMachineWhenTheArtifactRecordsNoClassEvidence: an artifact from a
// build that did not record how the class was evidenced is checked rather than
// trusted, because the failure it could be hiding is the one the clause exists for.
func TestGradeChecksTheMachineWhenTheArtifactRecordsNoClassEvidence(t *testing.T) {
	r := classArtifact(t, "8vCPU-16GB", 600000, 6000000, false)
	r.Class.Evidence = ""
	r.Machine = machine{Hostname: "unrated", CPUs: 2, MemMiB: 7936}
	cmp := compareClass(r)
	if cmp.Verdict.OK {
		t.Fatalf("an artifact with no class evidence was graded green on %s", r.Machine.describe())
	}
	if !strings.Contains(strings.Join(cmp.Blockers, "; "), "8vCPU-16GB") {
		t.Errorf("the blockers do not name the claimed class: %v", cmp.Blockers)
	}
}

// TestGradeRefusesASharedGeneratorMachine: §12 wants the generator outside the
// machine under test or in its own container so it does not compete for it. A run
// whose generator shared the machine is a lower bound, and the card that owns clause
// 3 forbids claiming AC-30 green from one.
func TestGradeRefusesASharedGeneratorMachine(t *testing.T) {
	r := classArtifact(t, "8vCPU-16GB", 600000, 6000000, true) // on the row, but shared
	cmp := compareClass(r)
	if cmp.Verdict.OK {
		t.Fatalf("a run whose generator shared the machine under test was graded green: %s", cmp.Verdict.Detail)
	}
	joined := strings.Join(cmp.Blockers, "; ")
	for _, want := range []string{"§12", "shared"} {
		if !strings.Contains(joined, want) {
			t.Errorf("the blocker does not mention %q: %s", want, joined)
		}
	}
	if cmp.Evidence != "observed" {
		t.Errorf("class evidence = %q for a generator on the machine under test, want observed", cmp.Evidence)
	}
}

// TestGradeRefusesRunsThatCannotSupportTheClaim: each precondition is named, and none
// of them is folded into a number - a deviation computed from a run that never
// reached the ceiling, was graded on another subscription count, was not the
// protocol, or was never reconciled would be a measurement of something else.
func TestGradeRefusesRunsThatCannotSupportTheClaim(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*report)
		want   string
	}{
		{"offered below the row", func(r *report) { r.Config.TargetRate = 900 }, "did not go past the ceiling"},
		{"no machine class declared", func(r *report) { r.Class = nil }, "declares no machine class"},
		{"a class §8.1 does not rate", func(r *report) { r.Class.Name = "12vCPU-32GB" }, "not a row of §8.1's table"},
		{"graded at another subscription count", func(r *report) { r.Config.Subs = 3 }, "10 subscriptions per event"},
		{"a warm-up shorter than §12's", func(r *report) { r.Config.WarmupS = 60 }, "5 minutes of warm-up"},
		{"a measurement shorter than §12's", func(r *report) { r.Config.MeasureS = 600 }, "30 minutes"},
		{"a graded window that is not the last 10 minutes", func(r *report) { r.Config.EvaluateS = 120 }, "last 10 minutes"},
		{"the graded window was never reconciled", func(r *report) {
			r.Graded = reconcile{Note: "no -db given; the accepted count was not reconciled against the database"}
		}, "not reconciled against the database"},
	}
	for _, tc := range cases {
		r := classArtifact(t, "8vCPU-16GB", 600000, 6000000, false)
		tc.mutate(r)
		cmp := compareClass(r)
		if cmp.Verdict.OK {
			t.Errorf("%s: clause 3 passed a run that cannot support the claim: %s", tc.name, cmp.Verdict.Detail)
		}
		if !strings.Contains(cmp.Verdict.Detail, tc.want) {
			t.Errorf("%s: the refusal does not say why (%q missing): %s", tc.name, tc.want, cmp.Verdict.Detail)
		}
	}
}

// TestGradeReportsTheGeneratorTallyWithoutGradingIt: the graded window's edges are
// the in-flight set at both ends, so the database's count is what clause 3 grades and
// the generator's own tally is a cross-check (§16).
func TestGradeReportsTheGeneratorTallyWithoutGradingIt(t *testing.T) {
	r := classArtifact(t, "8vCPU-16GB", 600000, 6000000, false)
	cmp := compareClass(r)
	if !cmp.Verdict.OK {
		t.Fatalf("fixture is not green: %s", cmp.Verdict.Detail)
	}
	if math.Abs(cmp.GeneratorEventsPS-1000) > 0.001 {
		t.Errorf("generator tally reported as %.1f events/s, want the fixture's 1000", cmp.GeneratorEventsPS)
	}
	if !strings.Contains(cmp.Verdict.Detail, "generator's own tally") {
		t.Errorf("the verdict does not label the tally as a cross-check: %s", cmp.Verdict.Detail)
	}
}

// ---------------------------------------------------------------------------
// `grade` as a command: the verdict is not read back from the artifact
// ---------------------------------------------------------------------------

// TestGradeRecomputesClausesOneAndTwoFromTheArtifact: `grade` decides all three
// clauses of AC-30 from the artifact's recorded numbers. The verdicts array an
// artifact carries is not the evidence - a run that shed nothing fails clause 1 even
// if the artifact says otherwise.
func TestGradeRecomputesClausesOneAndTwoFromTheArtifact(t *testing.T) {
	r := classArtifact(t, "8vCPU-16GB", 600000, 6000000, false)
	r.Evaluated.Shed = 0 // the run never reached the ceiling
	r.Verdicts = []verdict{{AC: "AC-30", OK: true, Detail: "the artifact says it passed"}}

	got := gradeVerdicts(t, r)
	var clause12 *verdict
	for i := range got {
		if got[i].AC == clause12Verdict {
			clause12 = &got[i]
		}
	}
	if clause12 == nil {
		t.Fatalf("`grade` decided no clauses 1-2 verdict: %+v", got)
	}
	if clause12.OK {
		t.Errorf("clauses 1-2 passed on a run that shed nothing, because the artifact's own verdict was believed: %s", clause12.Detail)
	}
	if !strings.Contains(clause12.Detail, "no load was shed") {
		t.Errorf("the recomputed verdict does not name the missing shed: %s", clause12.Detail)
	}
}

// gradeVerdicts runs the recomputation `grade` does, without printing.
func gradeVerdicts(t *testing.T, r *report) []verdict {
	t.Helper()
	var out []verdict
	c := config{Rate: r.Config.TargetRate, Subs: r.Config.Subs}
	for _, v := range judge(r, c) {
		if v.AC == "AC-30" {
			v.AC = clause12Verdict
			out = append(out, v)
		}
	}
	out = append(out, compareClass(r).Verdict)
	return out
}

// TestGradeExitStatusIsTheVerdict builds the harness and reads its exit status, the
// way an operator's runbook does: zero when AC-30 is green on the artifact, non-zero
// when clause 3 misses.
func TestGradeExitStatusIsTheVerdict(t *testing.T) {
	bin := buildLoadgen(t)
	dir := t.TempDir()
	green := filepath.Join(dir, "green.json")
	writeArtifact(t, green, classArtifact(t, "2vCPU-4GB", 150000, 1500000, false))

	out, err := exec.Command(bin, "grade", "-label", green, "-class", "2 vCPU / 4 GB").CombinedOutput()
	if err != nil {
		t.Fatalf("grade on a run on its row exited %v\n%s", err, out)
	}
	for _, want := range []string{"PASS", "AC-30 clause 3", "AC-30 clauses 1-2", "deliveries/s"} {
		if !strings.Contains(string(out), want) {
			t.Errorf("the verdict output does not contain %q:\n%s", want, out)
		}
	}

	miss := filepath.Join(dir, "miss.json")
	writeArtifact(t, miss, classArtifact(t, "2vCPU-4GB", 90000, 900000, false))
	out, err = exec.Command(bin, "grade", "-label", miss).CombinedOutput()
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() == 0 {
		t.Fatalf("grade on a run 40%% under the row exited %v, want a non-zero status\n%s", err, out)
	}
	if !strings.Contains(string(out), "FAIL AC-30 clause 3") {
		t.Errorf("a failing clause 3 was not reported as one:\n%s", out)
	}

	// An artifact that declares no class, and a class asserted that the artifact does
	// not declare, are both refused rather than graded.
	writeArtifact(t, filepath.Join(dir, "noclass.json"), func() *report {
		r := classArtifact(t, "2vCPU-4GB", 150000, 1500000, false)
		r.Class = nil
		return r
	}())
	writeArtifact(t, filepath.Join(dir, "otherclass.json"), classArtifact(t, "2vCPU-4GB", 150000, 1500000, false))
	for _, tc := range []struct{ file, class, want string }{
		{"noclass.json", "", "declares no machine class"},
		{"otherclass.json", "8vCPU-16GB", "declares 2vCPU-4GB"},
	} {
		args := []string{"grade", "-label", filepath.Join(dir, tc.file)}
		if tc.class != "" {
			args = append(args, "-class", tc.class)
		}
		out, err := exec.Command(bin, args...).CombinedOutput()
		if err == nil {
			t.Errorf("grade %v exited zero, want a refusal\n%s", args, out)
		}
		if !strings.Contains(string(out), tc.want) {
			t.Errorf("grade %v does not say %q:\n%s", args, tc.want, out)
		}
	}
}

func writeArtifact(t *testing.T, path string, r *report) {
	t.Helper()
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		t.Fatalf("marshal artifact: %v", err)
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
		t.Fatalf("write artifact: %v", err)
	}
}

// buildLoadgen builds this package and returns the binary, so the tests can read a
// real exit status. The toolchain is taken from the running binary's own GOROOT (a
// `go test` run always has one) with PATH as the fallback, so this does not depend on
// the harness being installed.
func buildLoadgen(t *testing.T) string {
	t.Helper()
	goBin := filepath.Join(runtime.GOROOT(), "bin", "go")
	if _, err := os.Stat(goBin); err != nil {
		found, lookErr := exec.LookPath("go")
		if lookErr != nil {
			t.Fatalf("no Go toolchain to build the harness with (GOROOT %s, PATH): %v", runtime.GOROOT(), lookErr)
		}
		goBin = found
	}
	bin := filepath.Join(t.TempDir(), "loadgen")
	cmd := exec.Command(goBin, "build", "-o", bin, ".")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build the harness: %v\n%s", err, out)
	}
	return bin
}

// ---------------------------------------------------------------------------
// §12's windows, and the run's artifact
// ---------------------------------------------------------------------------

// TestPhaseWindowsFollowSection12 pins the protocol's arithmetic without needing a
// 35-minute run: 5 minutes of warm-up excluded, 30 minutes measured, the graded
// window the last 10 minutes of it, and the comparison window the first 10.
func TestPhaseWindowsFollowSection12(t *testing.T) {
	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	w := newPhaseWindows(now, 5*time.Minute, 30*time.Minute, 10*time.Minute)
	if got, want := w.measureStart, now.Add(5*time.Minute); !got.Equal(want) {
		t.Errorf("measureStart = %s, want %s (the warm-up is excluded)", got, want)
	}
	if got, want := w.measureEnd, now.Add(35*time.Minute); !got.Equal(want) {
		t.Errorf("measureEnd = %s, want %s (30 minutes measured)", got, want)
	}
	if got, want := w.gradedStart, now.Add(25*time.Minute); !got.Equal(want) {
		t.Errorf("gradedStart = %s, want %s (the last 10 minutes of the measurement)", got, want)
	}
	if got, want := w.firstWindowEnd, now.Add(15*time.Minute); !got.Equal(want) {
		t.Errorf("firstWindowEnd = %s, want %s (the first equal window)", got, want)
	}
	if w.gradedStart.Before(w.firstWindowEnd) {
		t.Error("the graded window overlaps the first comparison window, so the two are not disjoint")
	}
}

// TestRunRecordsTheClassAndRefusesToGradeWithoutTheDatabase drives the harness end to
// end against a fake instance - the pacer, the phase attribution, the class claim and
// the artifact's graded window - and then feeds the artifact through the verdict. It
// says nothing about §8.1's numbers (a fake instance has no ceiling); what it pins is
// that a run with a class claim produces an artifact whose verdict is about the
// missing database, not about a number.
func TestRunRecordsTheClassAndRefusesToGradeWithoutTheDatabase(t *testing.T) {
	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := requests.Add(1)
		if n%3 == 0 {
			w.WriteHeader(http.StatusTooManyRequests) // the ceiling the fake instance has
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	row, _ := lookupClass("2vCPU-4GB")
	c := config{
		BaseURL: srv.URL, Key: "fixture-key", EventType: "load.created",
		Rate: 60, Subs: classSubs, PayloadBytes: referencePayloadBytes,
		Warmup: 200 * time.Millisecond, Measure: 700 * time.Millisecond,
		Evaluate: 300 * time.Millisecond, Concurrency: 8,
		RequestTimeout: 2 * time.Second, Settle: 0, Label: "fixture",
		Claim: row.claim(false, "flag"),
	}
	rep, err := runLoad(c)
	if err != nil {
		t.Fatalf("runLoad: %v", err)
	}
	if rep.Class == nil || rep.Class.Name != row.Name {
		t.Fatalf("the artifact's class claim = %+v, want %s", rep.Class, row.Name)
	}
	if rep.Class.Evidence != "attested" || rep.Class.GeneratorOnSUT {
		t.Errorf("claim evidence = %q / generator_on_sut %v, want attested/false", rep.Class.Evidence, rep.Class.GeneratorOnSUT)
	}
	if rep.Config.WarmupS != 0.2 || rep.Config.MeasureS != 0.7 || rep.Config.EvaluateS != 0.3 {
		t.Errorf("the artifact's windows = %v/%v/%v, want the flags' 0.2/0.7/0.3",
			rep.Config.WarmupS, rep.Config.MeasureS, rep.Config.EvaluateS)
	}
	if rep.Evaluated.Attempted == 0 || rep.Evaluated.Shed == 0 {
		t.Errorf("the evaluated window saw %d attempts and %d sheds; the fake instance sheds every third request",
			rep.Evaluated.Attempted, rep.Evaluated.Shed)
	}
	if rep.Graded.Ran || !strings.Contains(rep.Graded.Note, "-db") {
		t.Errorf("graded reconcile without -db = ran:%v note:%q, want it to say the database was needed", rep.Graded.Ran, rep.Graded.Note)
	}

	// The artifact as `grade` sees it: through JSON, not in memory.
	var round report
	b, err := json.Marshal(rep)
	if err != nil {
		t.Fatalf("marshal the artifact: %v", err)
	}
	if err := json.Unmarshal(b, &round); err != nil {
		t.Fatalf("unmarshal the artifact: %v", err)
	}
	cmp := compareClass(&round)
	if cmp.Verdict.OK {
		t.Fatalf("a run whose graded window was never reconciled was graded green: %s", cmp.Verdict.Detail)
	}
	if !strings.Contains(cmp.Verdict.Detail, "not reconciled") {
		t.Errorf("the refusal does not name the reconciliation: %s", cmp.Verdict.Detail)
	}
	if round.Machine.CPUs == 0 || round.Machine.Kernel == "" || round.Machine.Go == "" {
		t.Errorf("the artifact's machine block is incomplete: %+v", round.Machine)
	}
}
