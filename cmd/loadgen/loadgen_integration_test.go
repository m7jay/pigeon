//go:build loadgen_integration

package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// This runtime-level test intentionally drives the load loop. It is opt-in so
// ordinary unit tests and builds never generate requests, even to a fake server.
func TestRunRecordsTheClassAndRefusesToGradeWithoutTheDatabase(t *testing.T) {
	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := requests.Add(1)
		if n%3 == 0 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	row, _ := lookupClass("2vCPU-4GB")
	sut := machine{CPUs: 2, MemMiB: 4 * 1024, Source: machineDeclared}
	c := config{
		BaseURL: srv.URL, Key: "fixture-key", EventType: "load.created",
		Rate: 60, Subs: classSubs, PayloadBytes: referencePayloadBytes,
		Warmup: 200 * time.Millisecond, Measure: 700 * time.Millisecond,
		Evaluate: 300 * time.Millisecond, Concurrency: 8,
		RequestTimeout: 2 * time.Second, Settle: 0, Label: "fixture",
		Claim: row.claim(false, "flag"), SUT: &sut,
	}
	rep, err := runLoad(c)
	if err != nil {
		t.Fatalf("runLoad: %v", err)
	}
	if rep.Class == nil || rep.Class.Name != row.Name {
		t.Fatalf("the artifact's class claim = %+v, want %s", rep.Class, row.Name)
	}
	if rep.MachineUnderTest == nil || rep.MachineUnderTest.CPUs != 2 || rep.MachineUnderTest.Source != machineDeclared {
		t.Fatalf("the artifact's machine under test = %+v, want the declared 2 vCPU / 4 GiB machine", rep.MachineUnderTest)
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
