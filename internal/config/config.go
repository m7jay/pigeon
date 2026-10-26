// Package config loads every runtime knob from the environment.
//
// OPS-3: configuration is environment-variable only, and .env.example documents
// every knob. OPS-7: the process must refuse to start on an unsafe configuration
// rather than warn about it.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the whole runtime configuration. There are no flags and no config
// files: environment only, per OPS-3.
type Config struct {
	// HTTP
	HTTPAddr        string        // PIGEON_HTTP_ADDR
	ShutdownGrace   time.Duration // PIGEON_SHUTDOWN_GRACE (D-3: drain on SIGTERM)
	ReadHeaderLimit time.Duration

	// Storage
	DatabaseURL   string // PIGEON_DATABASE_URL
	DBMaxConns    int32  // PIGEON_DB_MAX_CONNS
	RedisURL      string // PIGEON_REDIS_URL
	MigrationsOn  bool   // PIGEON_MIGRATIONS_ON_BOOT (OPS-9: exactly one migrator)
	MigrateWaitDL time.Duration

	// Identity
	InstanceAdminPassword string // PIGEON_INSTANCE_ADMIN_PASSWORD
	SessionTTL            time.Duration

	// Observability
	LogLevel slog.Level // PIGEON_LOG_LEVEL
	LogJSON  bool       // PIGEON_LOG_JSON
}

// Defaults are the values the reference deployment runs with (§2.1).
func defaults() Config {
	return Config{
		HTTPAddr:              ":8080",
		ShutdownGrace:         20 * time.Second,
		ReadHeaderLimit:       10 * time.Second,
		DBMaxConns:            10,
		MigrationsOn:          true,
		MigrateWaitDL:         30 * time.Second,
		SessionTTL:            12 * time.Hour,
		LogLevel:              slog.LevelInfo,
		LogJSON:               true,
		InstanceAdminPassword: "",
	}
}

// Load reads the environment and validates it. A returned Config is always safe
// to start; an error means the process must exit non-zero.
func Load() (Config, error) {
	c := defaults()
	c.HTTPAddr = env("PIGEON_HTTP_ADDR", c.HTTPAddr)
	c.DatabaseURL = env("PIGEON_DATABASE_URL", "")
	c.RedisURL = env("PIGEON_REDIS_URL", "")
	c.InstanceAdminPassword = env("PIGEON_INSTANCE_ADMIN_PASSWORD", "")

	var err error
	if c.DBMaxConns, err = envInt32("PIGEON_DB_MAX_CONNS", c.DBMaxConns); err != nil {
		return Config{}, err
	}
	if c.MigrationsOn, err = envBool("PIGEON_MIGRATIONS_ON_BOOT", c.MigrationsOn); err != nil {
		return Config{}, err
	}
	if c.LogJSON, err = envBool("PIGEON_LOG_JSON", c.LogJSON); err != nil {
		return Config{}, err
	}
	if c.SessionTTL, err = envDuration("PIGEON_SESSION_TTL", c.SessionTTL); err != nil {
		return Config{}, err
	}
	if c.ShutdownGrace, err = envDuration("PIGEON_SHUTDOWN_GRACE", c.ShutdownGrace); err != nil {
		return Config{}, err
	}
	if c.LogLevel, err = parseLevel(env("PIGEON_LOG_LEVEL", "info")); err != nil {
		return Config{}, err
	}
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

// ErrUnsafeConfig wraps every OPS-7 refusal so callers can distinguish "operator
// misconfigured the box" from "the binary is broken".
var ErrUnsafeConfig = errors.New("unsafe configuration")

// Validate enforces OPS-7. It is exported so tests can assert each refusal
// without going through the environment.
func (c Config) Validate() error {
	var problems []string
	if c.DatabaseURL == "" {
		problems = append(problems, "PIGEON_DATABASE_URL is required")
	}
	if c.InstanceAdminPassword == "" {
		problems = append(problems, "PIGEON_INSTANCE_ADMIN_PASSWORD is required: first run has no other credential (FR-101)")
	} else if isWeakPassword(c.InstanceAdminPassword) {
		problems = append(problems, "PIGEON_INSTANCE_ADMIN_PASSWORD is a known-weak default; choose another (OPS-7)")
	}
	if c.DBMaxConns < 2 {
		problems = append(problems, "PIGEON_DB_MAX_CONNS must be >= 2")
	}
	if c.SessionTTL <= 0 {
		problems = append(problems, "PIGEON_SESSION_TTL must be positive")
	}
	if len(problems) > 0 {
		return fmt.Errorf("%w: %s", ErrUnsafeConfig, strings.Join(problems, "; "))
	}
	return nil
}

// ConnectionBudget is OPS-8/OPS-10 arithmetic, exported so `pigeon doctor` and
// the startup check share one implementation instead of two.
func ConnectionBudget(replicas, poolMax, reserved int32) int32 {
	return replicas*poolMax + reserved
}

func isWeakPassword(p string) bool {
	switch strings.ToLower(p) {
	case "password", "changeme", "admin", "pigeon", "secret", "test":
		return true
	}
	return len(p) < 8
}

func parseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	}
	return 0, fmt.Errorf("%w: PIGEON_LOG_LEVEL=%q (want debug|info|warn|error)", ErrUnsafeConfig, s)
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envInt32(key string, def int32) (int32, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return def, fmt.Errorf("%w: %s=%q is not an integer", ErrUnsafeConfig, key, raw)
	}
	return int32(n), nil
}

func envBool(key string, def bool) (bool, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return def, nil
	}
	b, err := strconv.ParseBool(raw)
	if err != nil {
		return def, fmt.Errorf("%w: %s=%q is not a boolean", ErrUnsafeConfig, key, raw)
	}
	return b, nil
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return def, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return def, fmt.Errorf("%w: %s=%q is not a duration (e.g. 20s, 12h)", ErrUnsafeConfig, key, raw)
	}
	return d, nil
}
