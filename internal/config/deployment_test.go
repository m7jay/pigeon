package config

import (
	"os"
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
