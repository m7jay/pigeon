package config

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// TestEnvExampleDocumentsEveryKnob is OPS-3 with teeth: "Config MUST be
// environment-variable only, with a documented .env.example covering every knob in
// this document".
//
// .env.example's own header goes further - it is "the complete list of settings.
// If a setting is not here, it is not configurable" - so every environment
// variable this package reads has to have an entry there. Two choices make the
// check the one the failure mode actually needs:
//
//   - The left side is the call sites of the env helper family, not every
//     PIGEON_* token in config.go: a name mentioned in a comment here is not a
//     knob the process reads, and this test must not demand documentation for
//     prose.
//   - The right side is an assignment (`NAME=...`), not a mention: the drift this
//     test exists to catch is a setting that is documented only in passing - the
//     delivery block was absent from .env.example entirely while
//     PIGEON_RETENTION_MAX_DISK_PCT was named once in a comment there, and a
//     token scan cannot tell those apart.
//
// Only one direction is asserted. .env.example also carries deployment secrets
// that compose consumes and the consumer portal's settings
// (PIGEON_APP_PASSWORD, PORTAL_SESSION_SECRET, ...), which this package rightly
// never reads; requiring an entry here for each of those would be the same bug
// pointing the other way.
func TestEnvExampleDocumentsEveryKnob(t *testing.T) {
	src, err := os.ReadFile("config.go")
	if err != nil {
		t.Fatalf("read config.go: %v", err)
	}
	example, err := os.ReadFile(filepath.Join("..", "..", ".env.example"))
	if err != nil {
		t.Fatalf("read .env.example: %v", err)
	}

	// Every knob Load reads: `env(...)`, `envInt(...)`, `envDuration(...)`, and
	// friends, whose first argument is always the variable name.
	reads := regexp.MustCompile(`env[A-Za-z0-9]*\(\s*"(PIGEON_[A-Z0-9_]+)"`)
	// A setting is documented when .env.example assigns it. The leading anchor
	// keeps a name out of the list when it is only written inside a comment.
	assigns := regexp.MustCompile(`(?m)^\s*(PIGEON_[A-Z0-9_]+)=`)

	documented := map[string]bool{}
	for _, m := range assigns.FindAllStringSubmatch(string(example), -1) {
		documented[m[1]] = true
	}

	knobs := map[string]bool{}
	for _, m := range reads.FindAllStringSubmatch(string(src), -1) {
		knobs[m[1]] = true
	}
	// A canary for the scan itself, so a refactor of the helper call shape fails
	// here rather than passing vacuously with nothing to check.
	if len(knobs) < 20 {
		t.Fatalf("config.go: found only %d knobs by scanning the env helper call sites, "+
			"which cannot be right - the scan is broken, not .env.example", len(knobs))
	}

	var missing []string
	for name := range knobs {
		if !documented[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		t.Errorf(".env.example does not document %d setting(s) this package reads: %s\n"+
			"OPS-3 requires .env.example to cover every knob, and its header claims it does",
			len(missing), strings.Join(missing, ", "))
	}
}
