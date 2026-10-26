package config

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func valid() Config {
	return Config{
		HTTPAddr:              ":8080",
		DatabaseURL:           "postgres://pigeon@localhost/pigeon",
		DBMaxConns:            10,
		InstanceAdminPassword: "correct-horse-battery",
		SessionTTL:            time.Hour,
		ShutdownGrace:         20 * time.Second,
		LogLevel:              0,
	}
}

func TestValidateAcceptsAReferenceDeployment(t *testing.T) {
	if err := valid().Validate(); err != nil {
		t.Fatalf("Validate rejected a valid config: %v", err)
	}
}

func TestValidateRefusesUnsafeConfigurations(t *testing.T) {
	cases := map[string]func(*Config){
		"no database":     func(c *Config) { c.DatabaseURL = "" },
		"no admin secret": func(c *Config) { c.InstanceAdminPassword = "" },
		"weak admin":      func(c *Config) { c.InstanceAdminPassword = "password" },
		"short admin":     func(c *Config) { c.InstanceAdminPassword = "short" },
		"tiny pool":       func(c *Config) { c.DBMaxConns = 1 },
		"zero session":    func(c *Config) { c.SessionTTL = 0 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			c := valid()
			mutate(&c)
			err := c.Validate()
			if err == nil {
				t.Fatal("want a refusal, got nil")
			}
			if !errors.Is(err, ErrUnsafeConfig) {
				t.Fatalf("error %v is not ErrUnsafeConfig", err)
			}
		})
	}
}

func TestValidateReportsEveryProblemAtOnce(t *testing.T) {
	c := Config{} // everything missing
	err := c.Validate()
	if err == nil {
		t.Fatal("want a refusal")
	}
	// An operator fixing one field at a time and restarting is a bad loop; the
	// message must name every problem in one pass.
	for _, want := range []string{"PIGEON_DATABASE_URL", "PIGEON_INSTANCE_ADMIN_PASSWORD"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

func TestConnectionBudget(t *testing.T) {
	// OPS-8: replicas × pool_max + reserved ≤ max_connections.
	if got := ConnectionBudget(2, 10, 5); got != 25 {
		t.Fatalf("ConnectionBudget(2,10,5) = %d, want 25", got)
	}
}

func TestParseLevel(t *testing.T) {
	for _, in := range []string{"debug", "INFO", " warn ", "error"} {
		if _, err := parseLevel(in); err != nil {
			t.Errorf("parseLevel(%q) = %v", in, err)
		}
	}
	if _, err := parseLevel("verbose"); err == nil {
		t.Fatal("parseLevel accepted an unknown level")
	}
}

func TestLoadFromEnvironment(t *testing.T) {
	t.Setenv("PIGEON_DATABASE_URL", "postgres://pigeon@localhost/pigeon")
	t.Setenv("PIGEON_INSTANCE_ADMIN_PASSWORD", "correct-horse-battery")
	t.Setenv("PIGEON_LOG_LEVEL", "debug")
	t.Setenv("PIGEON_DB_MAX_CONNS", "20")
	t.Setenv("PIGEON_SESSION_TTL", "2h")

	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.DBMaxConns != 20 || c.SessionTTL != 2*time.Hour {
		t.Fatalf("unexpected values: %+v", c)
	}
}

func TestLoadRejectsGarbage(t *testing.T) {
	t.Setenv("PIGEON_DATABASE_URL", "postgres://x/y")
	t.Setenv("PIGEON_INSTANCE_ADMIN_PASSWORD", "correct-horse-battery")
	t.Setenv("PIGEON_DB_MAX_CONNS", "lots")
	if _, err := Load(); err == nil {
		t.Fatal("Load accepted a non-numeric PIGEON_DB_MAX_CONNS")
	}
}
