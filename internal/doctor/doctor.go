// Package doctor implements `pigeon doctor` (OPS-4, OPS-10, OPS-11): a read-only
// diagnostic that answers "why is this box not working" with the arithmetic and
// the fix, not just a red cross.
//
// Three properties are structural rather than conventional, because they are what
// makes the command safe to run on a production box at 3 a.m.:
//
//   - Read-only. Every probe is a SELECT, a PING, a CONFIG GET, a statfs or a
//     getrlimit. Nothing here writes to Postgres or Redis, and nothing repairs:
//     OPS-11 forbids automatic repair, so the fix is printed as text for the
//     operator to run.
//   - Bounded. Each check runs under its own 5 s context and the whole run under
//     20 s (OPS-11), so doctor cannot hang behind the very fault it is diagnosing.
//   - Secret-free. Observed values are rendered through Redact, so a URL carrying
//     a password appears as scheme://user@host/db (FR-1008).
//
// Every check reports under a stable id, so a runbook can quote one and an
// operator can grep for it across boots.
package doctor

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

// Status is OPS-11's four-value vocabulary. They are the literal strings the
// output line starts with, so an operator's grep and the JSON agree.
type Status string

const (
	// Pass: the check observed what it requires.
	Pass Status = "pass"
	// Warn: it observed something the box tolerates, with a documented follow-up.
	Warn Status = "warn"
	// Fail: it observed something that will break the deployment.
	Fail Status = "fail"
	// Skip: it could not observe anything - the dependency is absent, disabled or
	// unreachable. A skip is never evidence that the check passed.
	Skip Status = "skip"
)

// Check is one line of OPS-11's output, in either rendering.
type Check struct {
	ID       string `json:"id"`
	Status   Status `json:"status"`
	Observed string `json:"observed"`
	Required string `json:"required"`
	Fix      string `json:"fix"`
}

// Report is the whole run: healthy only when nothing failed and nothing warned.
type Report struct {
	OK     bool    `json:"ok"`
	Checks []Check `json:"checks"`
}

// Exit codes, exactly as OPS-11 fixes them. A warning is not a failure - it is a
// follow-up - and the distinct code is what lets a monitoring system tell the two
// apart without parsing text.
const (
	// ExitOK: every check passed or skipped.
	ExitOK = 0
	// ExitFail: at least one check failed.
	ExitFail = 1
	// ExitWarn: nothing failed, at least one check warned.
	ExitWarn = 2
	// ExitCannotRun: doctor could not observe anything at all - every check
	// skipped. This is the "wrong box" answer: no Postgres, no Redis, no disk
	// budget, nothing to say.
	ExitCannotRun = 3
)

// Add appends a check and recomputes the verdict. Nothing else in the package
// appends, so the ordering of the printed lines is the order the checks ran.
//
// OK is derived rather than accumulated so that a Report is correct whether it was
// built by Add or decoded from JSON: a zero-value Report with a failing check in it
// must never read as healthy.
func (r *Report) Add(c Check) {
	if c.Status == "" {
		c.Status = Skip
	}
	// OPS-11 fixes the output as ONE line per check. Driver errors arrive with
	// newlines and tabs in them (a pgx multi-host dial error is the common one),
	// and an embedded newline would silently turn one check into several lines -
	// breaking the grep the format exists for. Normalise at the boundary, so no
	// individual check has to remember.
	c.Observed = oneLine(c.Observed)
	c.Required = oneLine(c.Required)
	c.Fix = oneLine(c.Fix)
	r.Checks = append(r.Checks, c)
	r.OK = true
	for _, x := range r.Checks {
		if x.Status == Fail || x.Status == Warn {
			r.OK = false
			break
		}
	}
}

// ExitCode is OPS-11's mapping from the checks to a process exit status.
//
// "Doctor could not run at all" is defined as: not one dependency check reached a
// verdict. config.load is excluded from that count because a refusal there is
// already the answer - the operator has a variable to fix, and exit 1 says so.
// What exit 3 reports is the other situation entirely: doctor reached the box,
// asked, and nothing was there to answer.
func (r Report) ExitCode() int {
	failed, warned, observed := false, false, false
	for _, c := range r.Checks {
		switch c.Status {
		case Fail:
			failed = true
		case Warn:
			warned = true
		case Pass:
			if c.ID != CheckConfig {
				observed = true
			}
		}
	}
	switch {
	case failed:
		return ExitFail
	case warned:
		return ExitWarn
	case !observed:
		return ExitCannotRun
	default:
		return ExitOK
	}
}

// Lines renders OPS-11's `<status> <check-id> <observed> <required> <fix>` one line
// per check. Fields are joined with two spaces so a value that is a sentence stays
// readable, and a check with nothing to say about a field leaves it a dash rather
// than shifting the columns.
func (r Report) Lines() []string {
	out := make([]string, 0, len(r.Checks))
	for _, c := range r.Checks {
		out = append(out, strings.Join([]string{
			string(c.Status),
			c.ID,
			dash(c.Observed),
			dash(c.Required),
			dash(c.Fix),
		}, "  "))
	}
	return out
}

// Print writes the human rendering. It is deliberately not io.WriterTo: that
// interface's signature carries a byte count nobody wants here, and reporting a
// wrong one would be worse than not implementing it.
func (r Report) Print(w io.Writer) error {
	for _, line := range r.Lines() {
		if _, err := fmt.Fprintln(w, line); err != nil {
			return err
		}
	}
	return nil
}

// JSON renders the OPS-11 object. Checks is never null: an empty array is the
// honest encoding of "nothing ran", and a consumer that ranges over it should not
// have to handle a null.
func (r Report) JSON() ([]byte, error) {
	if r.Checks == nil {
		r.Checks = []Check{}
	}
	return json.Marshal(r)
}

// oneLine collapses every run of whitespace (including newlines and tabs) to a
// single space and trims the ends.
func oneLine(s string) string { return strings.Join(strings.Fields(s), " ") }

func dash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "-"
	}
	return s
}

// DefaultCheckTimeout bounds one check and DefaultTotalTimeout the whole run
// (OPS-11: 5 s and 20 s). Both are named here so the bound is visible in the code
// that has to honour it rather than buried in each probe.
const (
	DefaultCheckTimeout = 5 * time.Second
	DefaultTotalTimeout = 20 * time.Second
)
