package config

import (
	"fmt"
	"io/fs"
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
// variable this repository's non-test code reads has to have an entry there. Four
// choices make the check the one the failure mode actually needs:
//
//   - The left side is the call sites that read the environment, not every
//     PIGEON_* token in a source file: a name mentioned in a comment is not a knob
//     the process reads, and this test must not demand documentation for prose.
//   - The left side is the whole repository, not this package's config.go. A
//     deployable reads knobs of its own - the portals' PIGEON_PRODUCER_PORTAL_*
//     and PIGEON_OIDC_* blocks and both portals' PIGEON_CORE_API_URL live in cmd/,
//     the brand in internal/portalui, the URLs doctor prints in internal/doctor,
//     the load harness's defaults in cmd/loadgen - and a scan of one file cannot
//     see any of them. The scan walks every non-test .go file, so a knob read in a
//     new package is covered without editing this test.
//   - The reference deployment's own inputs are included: the knobs compose.yaml
//     interpolates out of its environment (${NAME}) are settings an operator must
//     supply, which is the same claim one step out from the binary.
//   - The right side is an assignment (`NAME=...`), not a mention: the drift this
//     test exists to catch is a setting that is documented only in passing - the
//     delivery block was absent from .env.example entirely while
//     PIGEON_RETENTION_MAX_DISK_PCT was named once in a comment there, and a
//     token scan cannot tell those apart.
//
// Only one direction is asserted. .env.example may also carry a setting nothing in
// this repository reads - a knob consumed by a wrapper, by an operator's own
// tooling, or one documented ahead of its reader - and requiring a reader for each
// of those would be the same bug pointing the other way.
func TestEnvExampleDocumentsEveryKnob(t *testing.T) {
	root := filepath.Join("..", "..")
	example, err := os.ReadFile(filepath.Join(root, ".env.example"))
	if err != nil {
		t.Fatalf("read .env.example: %v", err)
	}

	// Every environment read this scan can name, by its argument: os.Getenv and
	// os.LookupEnv, plus the helpers this repository puts in front of them (this
	// package's env(...) / envInt(...) family, the portals' envOr("NAME",
	// default)), whose names all contain "env". The captured literal must have the
	// shape of an environment-variable name, so a helper taking some other string
	// cannot be mistaken for a knob.
	envReads := regexp.MustCompile(`\b\w*[Ee]nv\w*\s*\(\s*"([A-Z][A-Z0-9_]*)"`)
	// compose.yaml's interpolation form: ${NAME}, ${NAME:-default}, ${NAME:?msg}.
	composeKnobs := regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)`)
	// Any os.Getenv / os.LookupEnv call, whatever its argument: a file that reads
	// the environment this way and names nothing the scan can see is a file whose
	// knobs are invisible to the check. (A name computed and handed to one of the
	// helper functions is beyond a scan of literals; this catches the raw shape,
	// which is what a knob read through a variable or a table looks like.)
	rawReads := regexp.MustCompile(`\bos\.(?:Getenv|LookupEnv)\s*\(`)
	// A setting is documented when .env.example assigns it. The leading anchor
	// keeps a name out of the list when it is only written inside a comment.
	assigns := regexp.MustCompile(`(?m)^\s*([A-Z][A-Z0-9_]*)=`)

	documented := map[string]bool{}
	for _, m := range assigns.FindAllStringSubmatch(string(example), -1) {
		documented[m[1]] = true
	}

	// name -> the file that reads it, so a failure names both.
	knobs := map[string]string{}
	record := func(name, where string) {
		if prev, ok := knobs[name]; !ok || where < prev {
			knobs[name] = where
		}
	}

	var scanned int
	var blindSpots []string
	walkedTop := map[string]bool{}
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if rel != "." && (strings.HasPrefix(d.Name(), ".") || d.Name() == "vendor" ||
				d.Name() == "node_modules" || d.Name() == "testdata") {
				return fs.SkipDir
			}
			// A nested checkout (a worktree under .worktrees, a vendored clone)
			// holds another branch's code, which must not decide whether this
			// branch's .env.example is complete.
			if rel != "." {
				if _, statErr := os.Stat(filepath.Join(path, ".git")); statErr == nil {
					return fs.SkipDir
				}
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		scanned++
		if top, _, ok := strings.Cut(rel, "/"); ok {
			walkedTop[top] = true
		}
		// Whole-line comments are dropped first: prose that names a variable inside
		// something call-shaped is not a call site, and this test must not demand
		// documentation for a comment.
		code := codeLines(string(src))
		matches := envReads.FindAllStringSubmatch(code, -1)
		if len(matches) == 0 && rawReads.MatchString(code) {
			// The file reads the environment but names no knob: the read is through
			// a computed name, and nothing here can say what it reads.
			blindSpots = append(blindSpots, rel)
		}
		for _, m := range matches {
			record(m[1], rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan the repository from %s: %v", root, err)
	}

	// compose.yaml is read by fixed name rather than found by the walk, so a
	// renamed or deleted reference deployment fails here instead of quietly
	// narrowing the scan back to the Go code.
	compose, err := os.ReadFile(filepath.Join(root, "compose.yaml"))
	if err != nil {
		t.Fatalf("read compose.yaml: %v", err)
	}
	for _, m := range composeKnobs.FindAllStringSubmatch(string(compose), -1) {
		record(m[1], "compose.yaml")
	}

	// Canaries for the scan itself, so a refactor of the walk, or of the shape of
	// an environment read, fails here rather than passing vacuously with nothing to
	// check.
	//
	// The first is the defect this widening fixes: the scan used to read exactly one
	// file. Every top-level directory holding non-test Go code has to have
	// contributed to the walk, so a scan that has narrowed back to this package's
	// tree - the deployables unexamined, their knobs invisible - fails here.
	top, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read %s: %v", root, err)
	}
	for _, e := range top {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") || e.Name() == "vendor" {
			continue
		}
		if !holdsGoSource(filepath.Join(root, e.Name())) {
			continue
		}
		if !walkedTop[e.Name()] {
			t.Fatalf("the scan visited no Go file under %s/, which holds Go code: the walk "+
				"is not covering the repository, and a scan that misses a tree cannot see the "+
				"knobs read there", e.Name())
		}
	}
	if scanned < 20 {
		t.Fatalf("walked only %d non-test .go files from %s, which cannot be right - "+
			"the walk is broken, not .env.example", scanned, root)
	}
	if len(knobs) < 20 {
		t.Fatalf("found only %d knobs by scanning environment reads, which cannot be "+
			"right - the scan is broken, not .env.example", len(knobs))
	}
	// A file that reads the environment without naming a knob is the same blind
	// spot one layer down: whatever it reads is invisible here. A read through a
	// computed name cannot be documented by inspection, so it is reported.
	if len(blindSpots) > 0 {
		sort.Strings(blindSpots)
		t.Errorf("%d file(s) read the environment without naming a knob this scan can see: %s\n"+
			"nothing the check can name means nothing the check can require - pass the "+
			"literal to the helper (envOr(\"NAME\", def)) or teach this scan the new shape",
			len(blindSpots), strings.Join(blindSpots, ", "))
	}

	var missing []string
	for name := range knobs {
		if !documented[name] {
			missing = append(missing, fmt.Sprintf("%s (read by %s)", name, knobs[name]))
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		t.Errorf(".env.example does not document %d setting(s) this repository reads: %s\n"+
			"OPS-3 requires .env.example to cover every knob, and its header claims it does",
			len(missing), strings.Join(missing, "; "))
	}
}

// codeLines returns src without its whole-line `//` comments, so prose that happens
// to look like a call cannot be read as one. A trailing comment on a code line is
// left in place: cutting at the first `//` on a line would also cut the line at the
// `//` inside a URL literal, and a read that vanishes is worse than a comment that
// has to be reworded.
func codeLines(src string) string {
	var b strings.Builder
	b.Grow(len(src))
	for _, line := range strings.Split(src, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "//") {
			continue
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return b.String()
}

// holdsGoSource reports whether dir holds a non-test .go file anywhere below it.
// It is what keeps the walk's coverage canary honest: "this tree holds Go code" is
// a fact about the checkout, not something the scan under test reports about
// itself.
func holdsGoSource(dir string) bool {
	found := false
	_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || found {
			return fs.SkipAll
		}
		if d.IsDir() {
			if path != dir && (strings.HasPrefix(d.Name(), ".") || d.Name() == "vendor" ||
				d.Name() == "testdata") {
				return fs.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
			found = true
			return fs.SkipAll
		}
		return nil
	})
	return found
}
