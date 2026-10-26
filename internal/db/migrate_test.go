package db

import (
	"regexp"
	"testing"
)

// LoadMigrations is the one part of the migrator that can be tested without a
// database, and it catches the mistakes that actually happen: a bad filename, a
// duplicate version, or a missing checksum.
func TestLoadMigrationsIsOrderedAndWellFormed(t *testing.T) {
	ms, err := LoadMigrations()
	if err != nil {
		t.Fatalf("LoadMigrations: %v", err)
	}
	if len(ms) == 0 {
		t.Fatal("no embedded migrations")
	}

	versionRe := regexp.MustCompile(`^\d{4}$`)
	prev := ""
	for _, m := range ms {
		if !versionRe.MatchString(m.Version) {
			t.Errorf("migration version %q is not four digits", m.Version)
		}
		if prev != "" && m.Version <= prev {
			t.Errorf("migrations out of order: %s after %s", m.Version, prev)
		}
		prev = m.Version

		if len(m.Checksum) != 64 {
			t.Errorf("%s_%s checksum %q is not a sha256 hex digest", m.Version, m.Name, m.Checksum)
		}
		if m.SQL == "" {
			t.Errorf("%s_%s is empty", m.Version, m.Name)
		}
	}
}

func TestFirstMigrationCreatesTheTenancyTables(t *testing.T) {
	ms, err := LoadMigrations()
	if err != nil {
		t.Fatalf("LoadMigrations: %v", err)
	}
	first := ms[0].SQL
	for _, table := range []string{"producers", "users", "producer_members", "consumers", "api_keys"} {
		if !regexp.MustCompile(`create table ` + table + `\b`).MatchString(first) {
			t.Errorf("first migration does not create %s", table)
		}
	}
	if !regexp.MustCompile(`enable row level security`).MatchString(first) {
		t.Error("first migration does not enable RLS (MT-1)")
	}
}

func TestAuthLookupFunctionIsSecurityDefiner(t *testing.T) {
	ms, err := LoadMigrations()
	if err != nil {
		t.Fatalf("LoadMigrations: %v", err)
	}
	var found bool
	for _, m := range ms {
		if regexp.MustCompile(`security definer`).MatchString(m.SQL) &&
			regexp.MustCompile(`pigeon_auth_lookup`).MatchString(m.SQL) {
			found = true
		}
	}
	if !found {
		t.Fatal("no migration defines pigeon_auth_lookup as SECURITY DEFINER")
	}
}
