package config

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
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
