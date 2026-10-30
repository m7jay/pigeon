package config

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The shapes this check knows. An environment read is matched by its literal
// argument: os.Getenv and os.LookupEnv, plus the helpers this repository puts in
// front of them (this package's env(...) / envInt(...) family, the portals'
// envOr("NAME", default)), whose names all contain "env". The captured literal
// must have the shape of an environment-variable name, so a helper taking some
// other string cannot be mistaken for a knob.
var (
	envReads = regexp.MustCompile(`\b\w*[Ee]nv\w*\s*\(\s*"([A-Z][A-Z0-9_]*)"`)
	// compose.yaml's interpolation form: ${NAME}, ${NAME:-default}, ${NAME:?msg}.
	composeKnobs = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)`)
	// Any os.Getenv / os.LookupEnv call, whatever its argument: a file that reads
	// the environment this way and names nothing the scan can see is a file whose
	// knobs are invisible to the check.
	rawReads = regexp.MustCompile(`\bos\.(?:Getenv|LookupEnv)\s*\(`)
	// A setting is documented when .env.example assigns it. The leading anchor
	// keeps a name out of the list when it is only written inside a comment.
	envAssigns = regexp.MustCompile(`(?m)^\s*([A-Z][A-Z0-9_]*)=`)
)

// TestEnvExampleDocumentsEveryKnob is OPS-3 with teeth: "Config MUST be
// environment-variable only, with a documented .env.example covering every knob in
// this document".
//
// .env.example's own header goes further - it is "the complete list of settings.
// If a setting is not here, it is not configurable" - so every environment
// variable this module's non-test code reads has to have an entry there. Four
// choices make the check the one the failure mode actually needs:
//
//   - The left side is the call sites that read the environment, not every
//     PIGEON_* token in a source file: a name mentioned in a comment is not a knob
//     the process reads, and this test must not demand documentation for prose.
//   - The left side is this module's own code, not this package's config.go. A
//     deployable reads knobs of its own - the portals' PIGEON_PRODUCER_PORTAL_*
//     and PIGEON_OIDC_* blocks and both portals' PIGEON_CORE_API_URL live in cmd/,
//     the brand in internal/portalui, the URLs doctor prints in internal/doctor,
//     the load harness's defaults in cmd/loadgen - and a scan of one file cannot
//     see any of them. The scan walks every non-test .go file, so a knob read in a
//     new package is covered without editing this test.
//   - "This module's own code" is the root module, by skipDir's single rule: the
//     walk does not descend into a nested module or module cache (any non-root
//     directory holding its own go.mod - gopath/pkg/mod/<module>@<version>/ in a
//     checkout that carries one), a nested checkout (.git), vendor/, node_modules/,
//     testdata/ or a hidden tree. Those hold somebody else's code - the GOROOT,
//     APPDATA or DATABASE_URL reads inside a module cache are not Pigeon settings -
//     and it must not decide whether this branch's .env.example is complete. A
//     nested module is not part of this module either: `go build ./...` does not
//     compile it.
//   - The reference deployment's own inputs are included: the knobs compose.yaml
//     interpolates out of its environment (${NAME}) are settings an operator must
//     supply, which is the same claim one step out from the binary.
//   - The right side is an assignment (`NAME=...`), not a mention: the drift this
//     test exists to catch is a setting that is documented only in passing - the
//     delivery block was absent from .env.example entirely while
//     PIGEON_RETENTION_MAX_DISK_PCT was named once in a comment there, and a
//     token scan cannot tell those apart.
//
// What the left side cannot see, stated rather than implied: a name computed at
// runtime (`os.Getenv(name)`, a lookup in a table) or handed to a helper as a
// variable (`envOr(name, def)`) is beyond a scan of literals, and this test does
// not claim it. What it does report is a file that reads the environment and names
// no knob at all - the shape a table-driven read has - because in that file the
// whole left side is invisible. It does not report a computed read in a file that
// also names another knob by literal: the forwarding definitions in
// internal/config/config.go and both portals' mains have exactly that shape and are
// legitimate, so flagging the raw shape per occurrence would red on the shipped
// tree.
//
// Every literal read is treated as a setting, deliberately without an exclusion
// list for ambient names: an exclusion list is a hole a real knob can hide in
// later, and an entry for HOME would be prose nobody can act on. If code starts
// reading an ambient variable (HOME, PATH, TZ), the failure is the prompt to decide
// there whether it is a setting to document here or a read to change - not
// something this test decides silently.
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

	scan, err := scanGoKnobs(root)
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
		record(scan.knobs, m[1], "compose.yaml")
	}

	// Canaries for the scan itself, so a refactor of the walk, or of the shape of
	// an environment read, fails here rather than passing vacuously with nothing to
	// check.
	//
	// The first is the defect this widening fixes: the scan used to read exactly one
	// file. Every top-level directory holding non-test Go code has to have
	// contributed to the walk, so a scan that has narrowed back to this package's
	// tree - the deployables unexamined, their knobs invisible - fails here. It asks
	// holdsGoSource, which answers with the walk's own skipDir rule, so the canary
	// cannot demand a tree the scan is right to skip.
	top, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read %s: %v", root, err)
	}
	for _, e := range top {
		if !e.IsDir() || !holdsGoSource(root, filepath.Join(root, e.Name())) {
			continue
		}
		if !scan.walkedTop[e.Name()] {
			t.Fatalf("the scan visited no Go file under %s/, which holds Go code the scan is "+
				"supposed to see: the walk is not covering the repository, and a scan that "+
				"misses a tree cannot see the knobs read there", e.Name())
		}
	}
	if scan.scanned < 20 {
		t.Fatalf("walked only %d non-test .go files from %s, which cannot be right - "+
			"the walk is broken, not .env.example", scan.scanned, root)
	}
	if len(scan.knobs) < 20 {
		t.Fatalf("found only %d knobs by scanning environment reads, which cannot be "+
			"right - the scan is broken, not .env.example", len(scan.knobs))
	}
	// A file that reads the environment without naming a knob is the same blind
	// spot one layer down: whatever it reads is invisible here. A read through a
	// computed name cannot be documented by inspection, so it is reported.
	if len(scan.blindSpots) > 0 {
		t.Errorf("%d file(s) read the environment without naming a knob this scan can see: %s\n"+
			"nothing the check can name means nothing the check can require - pass the "+
			"literal to the helper (envOr(\"NAME\", def)) or teach this scan the new shape",
			len(scan.blindSpots), strings.Join(scan.blindSpots, ", "))
	}

	if missing := undocumented(scan.knobs, string(example)); len(missing) > 0 {
		t.Errorf(".env.example does not document %d setting(s) this repository reads: %s\n"+
			"OPS-3 requires .env.example to cover every knob, and its header claims it does",
			len(missing), strings.Join(missing, "; "))
	}
}

// TestEnvKnobScanIgnoresOtherTrees pins the scan's scope rule on a tree built to
// contain every shape the rule excludes, and asks for the verdict - not just the
// walk - because the failure the rule prevents is a demand in .env.example for
// somebody else's variable. The scan runs against a fixture root rather than this
// repository, so the rule is exercised without a module cache or a second checkout
// having to exist here; without it, a checkout that carries one (the shared
// development clone keeps a gopath/ module cache) turns the check red.
func TestEnvKnobScanIgnoresOtherTrees(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{
		// This module's own code: the only reads that count.
		"go.mod": "module example.com/undertest\n\ngo 1.22\n",
		"cmd/tool/main.go": "package main\n\nimport \"os\"\n\nfunc main() {\n" +
			"\t_ = os.Getenv(\"PIGEON_FIXTURE_KNOB\")\n}\n",
		"internal/lib/lib.go": "package lib\n\nimport \"os\"\n\n" +
			"var addr = os.Getenv(\"PIGEON_FIXTURE_SECOND_KNOB\")\n",
		// A nested module inside this module's own tree: its own go.mod means
		// `go build ./...` does not compile it. Its files must not be read, and the
		// computed name beside them must not be reported as a blind spot.
		"services/thing/main.go": "package main\n\nimport \"os\"\n\n" +
			"func main() { _ = os.Getenv(\"PIGEON_FIXTURE_SERVICE_KNOB\") }\n",
		"services/thing/sub/go.mod": "module example.com/sub\n\ngo 1.22\n",
		"services/thing/sub/sub.go": "package sub\n\nimport \"os\"\n\n" +
			"func knob(table map[string]string) string { return os.Getenv(table[\"db\"]) }\n",
		// A module cache inside the checkout, in the shape that made the check red
		// on a real clone: gopath/pkg/mod/<module>@<version>/.
		"gopath/pkg/mod/example.com/dep@v1.2.3/go.mod": "module example.com/dep\n\ngo 1.22\n",
		"gopath/pkg/mod/example.com/dep@v1.2.3/dep.go": "package dep\n\nimport \"os\"\n\n" +
			"var dsn = os.Getenv(\"FIXTURE_DEP_KNOB\")\n",
		"gopath/pkg/mod/example.com/dep@v1.2.3/bag.go": "package dep\n\nimport \"os\"\n\n" +
			"func load(names []string) string { return os.Getenv(names[0]) }\n",
		// A nested checkout: another branch's worktree, whose .git is a file.
		"third_party/otherbranch/.git": "gitdir: ../../.git/worktrees/otherbranch\n",
		"third_party/otherbranch/reader.go": "package other\n\nimport \"os\"\n\n" +
			"var k = os.Getenv(\"FIXTURE_OTHER_BRANCH_KNOB\")\n",
		// And the exclusions that predate the rule.
		"vendor/example.com/v/v.go":  "package v\n\nimport \"os\"\n\nvar k = os.Getenv(\"FIXTURE_VENDOR_KNOB\")\n",
		"node_modules/left/index.go": "package left\n\nimport \"os\"\n\nvar k = os.Getenv(\"FIXTURE_NODE_KNOB\")\n",
		"testdata/golden/gen.go":     "package golden\n\nimport \"os\"\n\nvar k = os.Getenv(\"FIXTURE_TESTDATA_KNOB\")\n",
		".hidden/worker.go":          "package hidden\n\nimport \"os\"\n\nvar k = os.Getenv(\"FIXTURE_HIDDEN_KNOB\")\n",
	}
	for rel, body := range files {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatalf("fixture %s: %v", rel, err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatalf("fixture %s: %v", rel, err)
		}
	}

	scan, err := scanGoKnobs(root)
	if err != nil {
		t.Fatalf("scan the fixture tree: %v", err)
	}
	want := map[string]string{
		"PIGEON_FIXTURE_KNOB":         "cmd/tool/main.go",
		"PIGEON_FIXTURE_SECOND_KNOB":  "internal/lib/lib.go",
		"PIGEON_FIXTURE_SERVICE_KNOB": "services/thing/main.go",
	}
	if !reflect.DeepEqual(scan.knobs, want) {
		t.Errorf("scan of the fixture tree found %v, want exactly %v: a read in a tree the rule "+
			"excludes is not a setting of this module", scan.knobs, want)
	}
	if len(scan.blindSpots) != 0 {
		t.Errorf("scan of the fixture tree reported blind spots %v, want none: a computed read in "+
			"an excluded tree is not a blind spot in this module", scan.blindSpots)
	}
	if scan.scanned != 3 {
		t.Errorf("scan of the fixture tree walked %d non-test .go files, want 3: the other 8 are "+
			"in trees the rule excludes", scan.scanned)
	}
	// The verdict, not just the walk: the three covered knobs are the only entries
	// .env.example needs, so this tree is green even though excluded code reads 5
	// names more.
	example := "PIGEON_FIXTURE_KNOB=\nPIGEON_FIXTURE_SECOND_KNOB=\nPIGEON_FIXTURE_SERVICE_KNOB=\n"
	if missing := undocumented(scan.knobs, example); len(missing) > 0 {
		t.Errorf("the fixture tree's verdict is RED on %v, want GREEN: excluded trees must not "+
			"put demands into .env.example", missing)
	}
	// The canary beside the walk must answer with the same rule, or it fires on
	// exactly the trees the rule exists for ("the scan visited no Go file under
	// third_party/, which holds Go code") and the check cannot be satisfied.
	for name := range map[string]bool{
		"cmd": true, "internal": true, "services": true, "gopath": false,
		"third_party": false, "vendor": false, "node_modules": false,
		"testdata": false, ".hidden": false,
	} {
		if got := holdsGoSource(root, filepath.Join(root, name)); got != scan.walkedTop[name] {
			t.Errorf("holdsGoSource(%s)=%v but the walk %v it: the canary and the scan disagree "+
				"about whether this tree is this module's code", name, got,
				map[bool]string{true: "covered", false: "skipped"}[scan.walkedTop[name]])
		}
	}
	// The rule skips the nested module's subtree, not its parent's.
	if !holdsGoSource(root, filepath.Join(root, "services")) {
		t.Errorf("holdsGoSource(services) is false: skipping a nested module must not hide the " +
			"package that holds it")
	}
}

// knobScan is what one pass over a tree found.
type knobScan struct {
	// knobs maps a knob name to the repository-relative file that reads it (the
	// lexicographically first reader, so the answer does not depend on walk order).
	knobs map[string]string
	// blindSpots are files that read the environment and name no knob at all.
	blindSpots []string
	// scanned counts the non-test .go files the walk read.
	scanned int
	// walkedTop records the top-level directories that contributed a file.
	walkedTop map[string]bool
}

// scanGoKnobs walks root and returns every environment read it can name, by its
// literal argument, in this module's own non-test Go files.
func scanGoKnobs(root string) (knobScan, error) {
	scan := knobScan{knobs: map[string]string{}, walkedTop: map[string]bool{}}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if skipDir(root, path, d) {
				return fs.SkipDir
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
		scan.scanned++
		if top, _, ok := strings.Cut(rel, "/"); ok {
			scan.walkedTop[top] = true
		}
		// Whole-line comments are dropped first: prose that names a variable inside
		// something call-shaped is not a call site, and this test must not demand
		// documentation for a comment.
		code := codeLines(string(src))
		matches := envReads.FindAllStringSubmatch(code, -1)
		if len(matches) == 0 && rawReads.MatchString(code) {
			// The file reads the environment but names no knob: the read is through
			// a computed name, and nothing here can say what it reads.
			scan.blindSpots = append(scan.blindSpots, rel)
		}
		for _, m := range matches {
			record(scan.knobs, m[1], rel)
		}
		return nil
	})
	return scan, err
}

// undocumented returns the knobs a scan found that example does not assign, each
// with the file that reads it, sorted so the failure is stable.
func undocumented(knobs map[string]string, example string) []string {
	documented := map[string]bool{}
	for _, m := range envAssigns.FindAllStringSubmatch(example, -1) {
		documented[m[1]] = true
	}
	var missing []string
	for name := range knobs {
		if !documented[name] {
			missing = append(missing, fmt.Sprintf("%s (read by %s)", name, knobs[name]))
		}
	}
	sort.Strings(missing)
	return missing
}

// record keeps the lexicographically first reader of a knob, so a failure names a
// file that does not depend on walk order.
func record(knobs map[string]string, name, where string) {
	if prev, ok := knobs[name]; !ok || where < prev {
		knobs[name] = where
	}
}

// skipDir is the single rule about which directories hold this module's own code.
// The scan and holdsGoSource both ask it, so "the walk reads this tree" and "this
// tree holds code the walk covers" cannot drift apart. root is the module root,
// which is never skipped; every other directory is skipped when it holds something
// other than this module's code.
func skipDir(root, path string, d fs.DirEntry) bool {
	if path == root {
		return false
	}
	name := d.Name()
	if strings.HasPrefix(name, ".") || name == "vendor" || name == "node_modules" ||
		name == "testdata" {
		return true
	}
	// A nested checkout (a worktree under .worktrees, a vendored clone) holds
	// another branch's code, which must not decide whether this branch's
	// .env.example is complete.
	if _, err := os.Stat(filepath.Join(path, ".git")); err == nil {
		return true
	}
	// A nested module - a module cache such as gopath/pkg/mod/<module>@<version>/,
	// a dependency checked out inside the tree - holds third-party code, whose
	// go.mod is the boundary. Only the root's go.mod is this module's, so the rule
	// cannot hide a package of this module by accident, and a nested module is not
	// part of this module: `go build ./...` does not compile it.
	if _, err := os.Stat(filepath.Join(path, "go.mod")); err == nil {
		return true
	}
	return false
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

// holdsGoSource reports whether dir holds a non-test .go file this module's scan
// would read. It answers with skipDir, the walk's own rule, because "this tree
// holds code the scan covers" has to mean the same thing as "the scan covers this
// tree": a canary that hand-copies the rule fires on a nested checkout or a module
// cache the scan is right to skip, on exactly the trees the rule exists for.
func holdsGoSource(root, dir string) bool {
	found := false
	_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || found {
			return fs.SkipAll
		}
		if d.IsDir() {
			if skipDir(root, path, d) {
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
