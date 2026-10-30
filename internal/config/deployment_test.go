package config

import (
	"go/parser"
	"go/token"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestMigratorRoleMustDifferFromTheRuntimeRole is OPS-7 applied to the database
// role split. PostgreSQL exempts a table's owner from row-level security, so a
// deployment that migrates and serves as the same role has no tenant isolation at
// all — the policies run for nobody. That is not a warning: it is a refusal.
func TestMigratorRoleMustDifferFromTheRuntimeRole(t *testing.T) {
	safe := defaults()
	// S6's default is buffered evidence (D-1), and Validate refuses buffered mode
	// without a Redis URL, so the reference split-role deployment must carry one.
	safe.RedisURL = "redis://redis:6379/0"
	safe.DatabaseURL = "postgres://pigeon_app:secret@db:5432/pigeon?sslmode=disable"
	safe.MigrateDatabaseURL = "postgres://pigeon_migrator:secret@db:5432/pigeon?sslmode=disable"
	safe.InstanceAdminPassword = "long-enough-admin-password"
	// FR-1006's at-rest key is required like the database URL.
	safe.HeadersKey = testHeadersKey

	if err := safe.Validate(); err != nil {
		t.Fatalf("a split-role configuration was refused: %v", err)
	}

	// Not setting it at all keeps single-role local development possible; the
	// deployment checks below are what stop that shape from shipping.
	single := safe
	single.MigrateDatabaseURL = ""
	if err := single.Validate(); err != nil {
		t.Fatalf("an unset migrate URL (defaults to the runtime URL) was refused: %v", err)
	}

	sameRole := safe
	sameRole.MigrateDatabaseURL = safe.DatabaseURL
	err := sameRole.Validate()
	if err == nil {
		t.Fatal("the migrator role and the runtime role are the same; every RLS policy is bypassed for that role")
	}
	if !strings.Contains(err.Error(), "PMIGRATE") && !strings.Contains(strings.ToLower(err.Error()), "role") {
		t.Fatalf("the refusal does not explain the role split: %v", err)
	}
}

// TestComposeSplitsTheMigratorFromTheRuntimeRole asserts the shipped deployment,
// because that is the thing the review found broken: compose ran migrations *and*
// the binary as the table owner, so the RLS policies the suite proves were inert
// in production. A test over the artifact is the only way this cannot silently
// regress with a one-line compose edit.
func TestComposeSplitsTheMigratorFromTheRuntimeRole(t *testing.T) {
	compose := readSibling(t, "../../compose.yaml")

	runtime := dsUser(t, compose, "PIGEON_DATABASE_URL")
	migrator := dsUser(t, compose, "PIGEON_MIGRATE_DATABASE_URL")

	if runtime == "" {
		t.Fatal("compose.yaml does not set PIGEON_DATABASE_URL")
	}
	if migrator == "" {
		t.Fatal("compose.yaml does not set PIGEON_MIGRATE_DATABASE_URL: the core must migrate as the owner and serve as the runtime role")
	}
	if runtime == migrator {
		t.Fatalf("compose.yaml runs migrations and the API as %q, which owns every table and therefore bypasses row-level security", runtime)
	}
	if runtime != "pigeon_app" {
		t.Fatalf("compose.yaml serves as %q, want the unprivileged pigeon_app role", runtime)
	}

	// The runtime role is created by the Postgres image's init hook, without
	// superuser or BYPASSRLS, and with default privileges so tables the migrator
	// creates later are usable by it.
	roles := readSibling(t, "../../deploy/postgres/01-roles.sql")
	for _, want := range []string{
		"create role pigeon_app",
		"nosuperuser",
		"nobypassrls",
		"alter default privileges",
		"grant",
	} {
		if !strings.Contains(roles, want) {
			t.Errorf("deploy/postgres/01-roles.sql does not mention %q", want)
		}
	}

	// The documented topology has to say the same thing, or the next operator
	// copies the old single-role URL out of the docs.
	topology := readSibling(t, "../../docs/topology.md")
	for _, want := range []string{"pigeon_migrator", "pigeon_app", "PIGEON_MIGRATE_DATABASE_URL"} {
		if !strings.Contains(topology, want) {
			t.Errorf("docs/topology.md does not mention %q; the role split is undocumented", want)
		}
	}
}

// readSibling reads a file relative to this package. The deployment artifacts are
// at the repository root, two levels up.
func readSibling(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(raw)
}

// dsUser extracts the username from the URL assigned to key in a compose file.
// It is a deliberately small parser: the assertion is about which role appears,
// not about YAML.
func dsUser(t *testing.T, compose, key string) string {
	t.Helper()
	for _, line := range strings.Split(compose, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, key+":") {
			continue
		}
		value := strings.TrimSpace(strings.TrimPrefix(trimmed, key+":"))
		if !strings.HasPrefix(value, "postgres://") {
			t.Fatalf("%s is not a postgres URL: %q", key, value)
		}
		rest := strings.TrimPrefix(value, "postgres://")
		user, _, ok := strings.Cut(rest, ":")
		if !ok {
			t.Fatalf("%s has no credentials in %q", key, value)
		}
		return user
	}
	return ""
}

// TestCoreDoesNotDependOnThePortal is AC-40: the consumer portal is a separate
// deployable that can be omitted entirely, and the core "keeps working exactly as
// before" - including the consumer self-service API the portal happens to use.
// That is a property of two artifacts, so it is asserted on both of them:
//
//   - the import graph: walk every package cmd/pigeon reaches and refuse an edge
//     into internal/portal or internal/portalui. Nothing else notices a careless
//     import - the compiler is happy and the rest of the suite runs in-process -
//     yet it would silently re-couple the core to a deployable that is meant to be
//     removable.
//   - the shipped compose: the core service must depend on Postgres and Redis and
//     on nothing else, so deleting the consumer-portal service leaves it intact,
//     and the portal must depend on the core (the coupling runs one way).
func TestCoreDoesNotDependOnThePortal(t *testing.T) {
	const root = "../.."
	module := modulePath(t, root)
	portals := []string{module + "/internal/portal", module + "/internal/portalui"}

	for _, dep := range localImportsOf(t, root, module, module+"/cmd/pigeon") {
		for _, p := range portals {
			if dep == p || strings.HasPrefix(dep, p+"/") {
				t.Errorf("cmd/pigeon depends on %s; the core must not import the portal (AC-40)", dep)
			}
		}
	}

	compose := readSibling(t, "../../compose.yaml")
	core := serviceBlock(t, compose, "core")
	if core == "" {
		t.Fatal("compose.yaml has no core service")
	}
	if !strings.Contains(core, "postgres:") || !strings.Contains(core, "redis:") {
		t.Errorf("the core service does not depend on postgres and redis:\n%s", core)
	}
	if strings.Contains(core, "consumer-portal") {
		t.Errorf("the core service names the consumer portal, so the portal is not removable (AC-40):\n%s", core)
	}
	portal := serviceBlock(t, compose, "consumer-portal")
	if portal == "" {
		t.Fatal("compose.yaml does not ship the consumer portal as a deployable")
	}
	if !strings.Contains(portal, "core:") {
		t.Errorf("the consumer-portal service does not depend on the core:\n%s", portal)
	}
}

// ---- The shipped deployment's own knobs (OPS-1, OPS-3) ----------------------

// composeDoc is the slice of compose.yaml this check reads. It is parsed as YAML
// rather than scanned as text: a service's environment is a mapping three levels
// down, and a healthcheck's `test:`/`interval:` keys read exactly like settings
// to a line reader.
type composeDoc struct {
	Services map[string]composeService `yaml:"services"`
}

// composeService is one service as far as this check needs it: whether this
// repository builds it, which of the image's binaries it runs, and the names that
// binary is handed.
type composeService struct {
	Build       composeBuild `yaml:"build"`
	Entrypoint  yaml.Node    `yaml:"entrypoint"`
	EnvFile     yaml.Node    `yaml:"env_file"`
	Environment yaml.Node    `yaml:"environment"`
}

type composeBuild struct {
	Context    string `yaml:"context"`
	Dockerfile string `yaml:"dockerfile"`
}

// composeInterp matches a setting whose whole value is compose's own interpolation
// of one variable: ${NAME}, ${NAME:-fallback}, ${NAME:?message}.
var composeInterp = regexp.MustCompile(`^\$\{([A-Za-z_][A-Za-z0-9_]*)(?::([-?])(.*))?\}$`)

// TestComposeRunsEachDeployableWithItsOwnKnobs is OPS-1 and OPS-3 on the shipped
// deployment, in the direction TestEnvExampleDocumentsEveryKnob does not cover:
// that check requires a knob this module reads to be documented, and this one
// requires a knob a deployable reads to be *forwarded to that deployable*.
//
// The core is handed .env wholesale (`env_file: .env`), so every name in it
// reaches the process and there is nothing to check. A portal is handed an
// explicit environment list instead, and compose only ever gives a container the
// variables that list names: a knob the portal's code reads and the list omits
// never reaches the process, which is what made PORTAL_SESSION_COOKIE_NAME,
// PIGEON_PORTAL_ADDR, PIGEON_PRODUCER_PORTAL_ADDR and
// PIGEON_PRODUCER_PORTAL_COOKIE_NAME inert under this deployment while
// .env.example documented all four as the operator's interface.
//
// Three decisions make the check decidable without a table to maintain:
//
//   - Only services this repository builds are checked (they have a `build:`
//     block). postgres and redis are published images; they run no binary of this
//     module, so they carry no knob of ours.
//   - Which binary a service runs is read from its own `entrypoint`, and a
//     service that names none is refused rather than assumed: the image's own
//     ENTRYPOINT is the core (Dockerfile), so a portal service that says nothing
//     is a core container - which is the second defect this test found, and the
//     reason the forwarding check could not be anchored on the service name.
//   - The knobs of that binary are those read by every package it links
//     (localImportsOf), not just its cmd/ directory: a knob a deployable reads in
//     a shared package - both portals' PORTAL_BRAND_*, from internal/portalui - is
//     still a knob the deployable reads.
//
// It also reads the list the other way, for the fallback compose supplies when
// .env is silent, because "forward it at the code's default" is what the ADDR
// knobs need: an empty fallback means the code's own default applies (envOr and
// this package's helpers both read empty as unset), and a non-empty one has to BE
// the code's default - otherwise the deployment hands the process a value the
// operator never chose and a run outside compose never gets.
//
// What it deliberately does not claim: a setting .env.example documents that only
// a wrapper outside compose reads (the load harness, `make run-portal`) needs no
// forwarding - an operator's own tooling is not a defect - and a knob read through
// a computed name is invisible here exactly as it is in the sibling check.
func TestComposeRunsEachDeployableWithItsOwnKnobs(t *testing.T) {
	const root = "../.."
	var doc composeDoc
	if err := yaml.Unmarshal([]byte(readSibling(t, filepath.Join(root, "compose.yaml"))), &doc); err != nil {
		t.Fatalf("parse compose.yaml as YAML: %v", err)
	}
	if len(doc.Services) == 0 {
		t.Fatal("compose.yaml declares no services")
	}

	scan, err := scanGoKnobs(root)
	if err != nil {
		t.Fatalf("scan %s: %v", root, err)
	}
	// The canary for the scan, as in the sibling check: an empty or tiny left side
	// would make every service green for the wrong reason.
	if len(scan.readers) < 60 {
		t.Fatalf("the scan named only %d knobs, which cannot be right - the walk is broken, not compose.yaml", len(scan.readers))
	}
	module := modulePath(t, root)
	documented := documentedNames(readSibling(t, filepath.Join(root, ".env.example")))

	names := make([]string, 0, len(doc.Services))
	for name := range doc.Services {
		names = append(names, name)
	}
	sort.Strings(names)

	built := 0
	for _, name := range names {
		svc := doc.Services[name]
		if svc.Build.Context == "" && svc.Build.Dockerfile == "" {
			continue
		}
		built++

		entrypoint := scalarsOf(t, "compose.yaml service "+name+" entrypoint", svc.Entrypoint)
		if len(entrypoint) == 0 {
			t.Errorf("compose.yaml's %s service overrides no entrypoint, so the container runs the image's own - /pigeon, "+
				"the core - whatever the service is called. Name the binary it runs (`entrypoint: [\"/pigeon\"]`, "+
				"`[\"/producer-portal\"]`, `[\"/consumer-portal\"]`): this check reads that line to decide whose knobs the "+
				"service has to carry, and a portal container running the core carries none of the portal's", name)
			continue
		}
		cmdDir := path.Join("cmd", filepath.Base(entrypoint[0]))
		if info, err := os.Stat(filepath.Join(root, cmdDir)); err != nil || !info.IsDir() {
			t.Errorf("compose.yaml's %s service runs %s, which is not a package this module builds (no %s/ directory)",
				name, entrypoint[0], cmdDir)
			continue
		}

		dirs := map[string]bool{}
		for _, pkg := range localImportsOf(t, root, module, module+"/"+cmdDir) {
			dirs[strings.TrimPrefix(strings.TrimPrefix(pkg, module), "/")] = true
		}
		type wanted struct{ knob, reader string }
		var want []wanted
		for knob, readers := range scan.readers {
			var inBinary []string
			for reader := range readers {
				if dirs[path.Dir(reader)] {
					inBinary = append(inBinary, reader)
				}
			}
			if len(inBinary) > 0 {
				sort.Strings(inBinary)
				want = append(want, wanted{knob, inBinary[0]})
			}
		}
		sort.Slice(want, func(i, j int) bool { return want[i].knob < want[j].knob })
		if len(want) == 0 {
			t.Errorf("compose.yaml's %s service runs %s, and no package it links reads a knob this scan can name: either "+
				"those packages read the environment through computed names (the blind spot the sibling check reports) or "+
				"the import walk did not reach them", name, entrypoint[0])
			continue
		}

		// What the container is handed. A service given an env_file is handed
		// whatever that file names: the file is the operator's and is not in the
		// repository, so the names it can carry are the ones .env.example assigns -
		// the file its own header tells the operator to copy to .env.
		env := envOf(t, "compose.yaml service "+name+" environment", svc.Environment)
		carried := map[string]bool{}
		for knob := range env {
			carried[knob] = true
		}
		if len(scalarsOf(t, "compose.yaml service "+name+" env_file", svc.EnvFile)) > 0 {
			for knob := range documented {
				carried[knob] = true
			}
		}

		for _, w := range want {
			if !carried[w.knob] {
				t.Errorf("compose.yaml's %s service does not forward %s, which %s reads: only this list reaches a portal "+
					"container (.env does not), so the knob is documented in .env.example and inert under this deployment "+
					"(OPS-1, OPS-3)", name, w.knob, w.reader)
			}
		}

		knobs := make([]string, 0, len(env))
		for knob := range env {
			knobs = append(knobs, knob)
		}
		sort.Strings(knobs)
		for _, knob := range knobs {
			m := composeInterp.FindStringSubmatch(env[knob])
			if m == nil || m[2] != "-" || m[3] == "" {
				// Not an interpolation, a required one (:?), or an empty fallback -
				// and empty means "the code's own default applies", not "compose
				// chose a value".
				continue
			}
			if scan.defaults[knob][m[3]] {
				continue
			}
			if len(scan.defaults[knob]) == 0 {
				t.Errorf("compose.yaml's %s service falls back to %s=%s, and no code in this module defaults that knob "+
					"to a literal this scan can see: either give the process the same default, or forward ${%s:-} and let "+
					"the code decide", name, knob, m[3], knob)
				continue
			}
			t.Errorf("compose.yaml's %s service falls back to %s=%s, but the code's own default is %s: the deployment "+
				"would hand the process a value the operator never chose and a run outside compose does not get",
				name, knob, m[3], strings.Join(sortedOf(scan.defaults[knob]), " or "))
		}
	}
	if built == 0 {
		t.Error("compose.yaml builds no service from this repository, so this check verified nothing")
	}
}

// scalarsOf returns the strings a compose field holds, whether it is written as
// one scalar (`env_file: .env`) or as a list (`entrypoint: ["/pigeon"]`). An
// absent field is an empty list, not an error.
func scalarsOf(t *testing.T, what string, n yaml.Node) []string {
	t.Helper()
	switch n.Kind {
	case 0:
		return nil
	case yaml.ScalarNode:
		return []string{n.Value}
	case yaml.SequenceNode:
		var out []string
		for _, item := range n.Content {
			if item.Kind != yaml.ScalarNode {
				t.Fatalf("%s: expected strings, found a nested structure", what)
			}
			out = append(out, item.Value)
		}
		return out
	default:
		t.Fatalf("%s: expected a string or a list of strings", what)
		return nil
	}
}

// envOf returns a service's environment as name -> value, in either of compose's
// two shapes: a mapping (`NAME: value`) or a list of `NAME=value` entries.
func envOf(t *testing.T, what string, n yaml.Node) map[string]string {
	t.Helper()
	out := map[string]string{}
	switch n.Kind {
	case 0:
		return out
	case yaml.MappingNode:
		for i := 0; i+1 < len(n.Content); i += 2 {
			out[n.Content[i].Value] = n.Content[i+1].Value
		}
		return out
	case yaml.SequenceNode:
		for _, item := range n.Content {
			knob, value, ok := strings.Cut(item.Value, "=")
			if !ok {
				t.Fatalf("%s: %q is neither NAME=value nor a mapping entry", what, item.Value)
			}
			out[knob] = value
		}
		return out
	default:
		t.Fatalf("%s: expected a mapping or a list of NAME=value strings", what)
		return out
	}
}

// sortedOf returns the keys of a set in a stable order.
func sortedOf(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// modulePath reads the module path from go.mod.
func modulePath(t *testing.T, root string) string {
	t.Helper()
	for _, line := range strings.Split(readSibling(t, filepath.Join(root, "go.mod")), "\n") {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), "module "); ok {
			return strings.TrimSpace(rest)
		}
	}
	t.Fatal("go.mod has no module line")
	return ""
}

// localImportsOf walks the import graph rooted at pkgPath and returns every
// in-module import it reaches, transitively, including pkgPath itself. Only
// non-test files are read: a _test.go import does not ship in the binary.
func localImportsOf(t *testing.T, root, module, pkgPath string) []string {
	t.Helper()
	seen := map[string]bool{}
	queue := []string{pkgPath}
	var out []string
	for len(queue) > 0 {
		pkg := queue[0]
		queue = queue[1:]
		if seen[pkg] {
			continue
		}
		seen[pkg] = true
		out = append(out, pkg)

		dir := filepath.Join(root, strings.TrimPrefix(strings.TrimPrefix(pkg, module), "/"))
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read %s: %v", dir, err)
		}
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, name), nil, parser.ImportsOnly)
			if err != nil {
				t.Fatalf("parse %s: %v", name, err)
			}
			for _, imp := range file.Imports {
				path, err := strconv.Unquote(imp.Path.Value)
				if err != nil {
					continue
				}
				if path == module || strings.HasPrefix(path, module+"/") {
					queue = append(queue, path)
				}
			}
		}
	}
	return out
}

// serviceBlock returns one top-level compose service's block. Content inside a
// service is indented at least four spaces, so the block ends at the first
// non-blank line indented less than that (the next service, or a top-level key
// such as `volumes:`).
func serviceBlock(t *testing.T, compose, name string) string {
	t.Helper()
	lines := strings.Split(compose, "\n")
	start := -1
	for i, line := range lines {
		if start < 0 {
			if line == "  "+name+":" {
				start = i
			}
			continue
		}
		if strings.TrimSpace(line) == "" {
			continue
		}
		if len(line)-len(strings.TrimLeft(line, " ")) < 4 {
			return strings.Join(lines[start:i], "\n")
		}
	}
	if start >= 0 {
		return strings.Join(lines[start:], "\n")
	}
	return ""
}
