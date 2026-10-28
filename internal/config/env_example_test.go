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
// environment-variable only, with a documented .env.example covering every knob".
//
// .env.example's own header goes further - "this file is the complete list of
// knobs. If a setting is not here, it is not configurable, and that is
// deliberate" - so every environment variable this package reads must appear
// there. The check is deliberately the same one the requirement is audited with
// (a plain token scan of config.go, comments included), because a knob named in a
// comment and missing from the file is exactly the drift that happened: 21
// delivery, sweeper, attempt-buffer and retention knobs were read here and
// documented nowhere.
//
// Only one direction is asserted. .env.example also carries deployment secrets
// that compose consumes and the consumer portal's settings (PIGEON_APP_PASSWORD,
// PORTAL_SESSION_SECRET, ...), which this package rightly never reads.
func TestEnvExampleDocumentsEveryKnob(t *testing.T) {
	src, err := os.ReadFile("config.go")
	if err != nil {
		t.Fatalf("read config.go: %v", err)
	}
	example, err := os.ReadFile(filepath.Join("..", "..", ".env.example"))
	if err != nil {
		t.Fatalf("read .env.example: %v", err)
	}

	token := regexp.MustCompile(`PIGEON_[A-Z_]+`)
	documented := map[string]bool{}
	for _, name := range token.FindAllString(string(example), -1) {
		documented[name] = true
	}

	var missing []string
	for _, name := range token.FindAllString(string(src), -1) {
		if !documented[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		missing = dedupe(missing)
		t.Errorf(".env.example does not document %d knob(s) this package reads: %s\n"+
			"OPS-3 requires .env.example to cover every knob, and its header claims it does",
			len(missing), strings.Join(missing, ", "))
	}
}

func dedupe(sorted []string) []string {
	out := sorted[:0]
	for i, s := range sorted {
		if i == 0 || s != sorted[i-1] {
			out = append(out, s)
		}
	}
	return out
}
