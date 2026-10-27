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
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// maxSweeperInterval is D-6's ceiling on the sweep cadence: a lease that has
// lapsed must be re-queued within 10 s. internal/delivery enforces the same
// ceiling in NewSweeper (delivery.MaxSweepInterval); it is repeated here rather
// than imported so the config package stays a leaf.
const maxSweeperInterval = 10 * time.Second

// Config is the whole runtime configuration. There are no flags and no config
// files: environment only, per OPS-3.
type Config struct {
	// HTTP
	HTTPAddr        string        // PIGEON_HTTP_ADDR
	ShutdownGrace   time.Duration // PIGEON_SHUTDOWN_GRACE (D-3: drain on SIGTERM)
	ReadHeaderLimit time.Duration

	// Storage
	//
	// DatabaseURL is the runtime role: the one that serves requests, which must
	// not own the tables or PostgreSQL would exempt it from row-level security.
	// MigrateDatabaseURL is the owning role, used only for migrations; it defaults
	// to DatabaseURL for single-role development, and Validate refuses an explicit
	// configuration where the two are the same role.
	DatabaseURL        string // PIGEON_DATABASE_URL
	MigrateDatabaseURL string // PIGEON_MIGRATE_DATABASE_URL
	DBMaxConns         int32  // PIGEON_DB_MAX_CONNS
	RedisURL           string // PIGEON_REDIS_URL
	MigrationsOn       bool   // PIGEON_MIGRATIONS_ON_BOOT (OPS-9: exactly one migrator)
	MigrateWaitDL      time.Duration

	// Identity
	InstanceAdminPassword string // PIGEON_INSTANCE_ADMIN_PASSWORD
	SessionTTL            time.Duration
	SelfRegistration      bool // PIGEON_SELF_REGISTRATION (FR-102: default off)

	// Rate limits (FR-207). In-process per replica; see SPEC.md's S1 notes.
	RateLimitPerKeyRPS        float64 // PIGEON_RATE_LIMIT_PER_KEY_RPS
	RateLimitPerKeyBurst      int     // PIGEON_RATE_LIMIT_PER_KEY_BURST
	RateLimitPerProducerRPS   float64 // PIGEON_RATE_LIMIT_PER_PRODUCER_RPS
	RateLimitPerProducerBurst int     // PIGEON_RATE_LIMIT_PER_PRODUCER_BURST

	// OPS-8 connection budget
	CoreReplicas    int32 // PIGEON_CORE_REPLICAS
	DBReservedConns int32 // PIGEON_DB_RESERVED_CONNS

	// Delivery worker (S4) and sweeper. The timeout is capped at the FR-802 hard
	// ceiling of 5 s in Load.
	DeliveryWorkers         int           // PIGEON_DELIVERY_WORKERS (0 disables the worker)
	DeliveryLease           time.Duration // PIGEON_DELIVERY_LEASE
	DeliveryTimeout         time.Duration // PIGEON_DELIVERY_TIMEOUT (<= 5 s, FR-802)
	DeliveryMaxBodyBytes    int           // PIGEON_DELIVERY_MAX_BODY_BYTES (FR-804)
	DeliveryBackoffBase     time.Duration // PIGEON_DELIVERY_BACKOFF_BASE
	DeliveryBackoffMax      time.Duration // PIGEON_DELIVERY_BACKOFF_MAX
	DeliveryMaxAttempts     int           // PIGEON_DELIVERY_MAX_ATTEMPTS
	DeliveryMaxConnsPerHost int           // PIGEON_DELIVERY_MAX_CONNS_PER_HOST (FR-817)
	DeliveryPollInterval    time.Duration // PIGEON_DELIVERY_POLL_INTERVAL
	DeliveryUserAgent       string        // PIGEON_DELIVERY_USER_AGENT (FR-810)
	SweeperInterval         time.Duration // PIGEON_SWEEPER_INTERVAL (<= 10 s, D-6; clamped in Load)
	SweeperLockURL          string        // PIGEON_SWEEPER_LOCK_URL (defaults to DatabaseURL)
	SweeperLockWait         time.Duration // PIGEON_SWEEPER_LOCK_WAIT

	// Observability
	LogLevel slog.Level // PIGEON_LOG_LEVEL
	LogJSON  bool       // PIGEON_LOG_JSON
}

// Defaults are the values the reference deployment runs with (§2.1).
func defaults() Config {
	return Config{
		HTTPAddr:                  ":8080",
		ShutdownGrace:             20 * time.Second,
		ReadHeaderLimit:           10 * time.Second,
		DBMaxConns:                10,
		MigrationsOn:              true,
		MigrateWaitDL:             30 * time.Second,
		SessionTTL:                12 * time.Hour,
		LogLevel:                  slog.LevelInfo,
		LogJSON:                   true,
		InstanceAdminPassword:     "",
		SelfRegistration:          false,
		RateLimitPerKeyRPS:        50,
		RateLimitPerKeyBurst:      100,
		RateLimitPerProducerRPS:   200,
		RateLimitPerProducerBurst: 400,
		CoreReplicas:              1,
		DBReservedConns:           5,
		DeliveryWorkers:           1,
		DeliveryLease:             40 * time.Second,
		DeliveryTimeout:           5 * time.Second,
		DeliveryMaxBodyBytes:      2048,
		DeliveryBackoffBase:       30 * time.Second,
		DeliveryBackoffMax:        time.Hour,
		DeliveryMaxAttempts:       15,
		DeliveryMaxConnsPerHost:   1,
		DeliveryPollInterval:      time.Second,
		SweeperInterval:           5 * time.Second,
		SweeperLockWait:           60 * time.Second,
	}
}

// Load reads the environment and validates it. A returned Config is always safe
// to start; an error means the process must exit non-zero.
func Load() (Config, error) {
	c := defaults()
	c.HTTPAddr = env("PIGEON_HTTP_ADDR", c.HTTPAddr)
	c.DatabaseURL = env("PIGEON_DATABASE_URL", "")
	c.MigrateDatabaseURL = env("PIGEON_MIGRATE_DATABASE_URL", "")
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
	if c.SelfRegistration, err = envBool("PIGEON_SELF_REGISTRATION", c.SelfRegistration); err != nil {
		return Config{}, err
	}
	if c.RateLimitPerKeyRPS, err = envFloat("PIGEON_RATE_LIMIT_PER_KEY_RPS", c.RateLimitPerKeyRPS); err != nil {
		return Config{}, err
	}
	if c.RateLimitPerKeyBurst, err = envInt("PIGEON_RATE_LIMIT_PER_KEY_BURST", c.RateLimitPerKeyBurst); err != nil {
		return Config{}, err
	}
	if c.RateLimitPerProducerRPS, err = envFloat("PIGEON_RATE_LIMIT_PER_PRODUCER_RPS", c.RateLimitPerProducerRPS); err != nil {
		return Config{}, err
	}
	if c.RateLimitPerProducerBurst, err = envInt("PIGEON_RATE_LIMIT_PER_PRODUCER_BURST", c.RateLimitPerProducerBurst); err != nil {
		return Config{}, err
	}
	if c.CoreReplicas, err = envInt32("PIGEON_CORE_REPLICAS", c.CoreReplicas); err != nil {
		return Config{}, err
	}
	if c.DBReservedConns, err = envInt32("PIGEON_DB_RESERVED_CONNS", c.DBReservedConns); err != nil {
		return Config{}, err
	}
	if c.DeliveryWorkers, err = envInt("PIGEON_DELIVERY_WORKERS", c.DeliveryWorkers); err != nil {
		return Config{}, err
	}
	if c.DeliveryLease, err = envDuration("PIGEON_DELIVERY_LEASE", c.DeliveryLease); err != nil {
		return Config{}, err
	}
	if c.DeliveryTimeout, err = envDuration("PIGEON_DELIVERY_TIMEOUT", c.DeliveryTimeout); err != nil {
		return Config{}, err
	}
	if c.DeliveryMaxBodyBytes, err = envInt("PIGEON_DELIVERY_MAX_BODY_BYTES", c.DeliveryMaxBodyBytes); err != nil {
		return Config{}, err
	}
	if c.DeliveryBackoffBase, err = envDuration("PIGEON_DELIVERY_BACKOFF_BASE", c.DeliveryBackoffBase); err != nil {
		return Config{}, err
	}
	if c.DeliveryBackoffMax, err = envDuration("PIGEON_DELIVERY_BACKOFF_MAX", c.DeliveryBackoffMax); err != nil {
		return Config{}, err
	}
	if c.DeliveryMaxAttempts, err = envInt("PIGEON_DELIVERY_MAX_ATTEMPTS", c.DeliveryMaxAttempts); err != nil {
		return Config{}, err
	}
	if c.DeliveryMaxConnsPerHost, err = envInt("PIGEON_DELIVERY_MAX_CONNS_PER_HOST", c.DeliveryMaxConnsPerHost); err != nil {
		return Config{}, err
	}
	if c.DeliveryPollInterval, err = envDuration("PIGEON_DELIVERY_POLL_INTERVAL", c.DeliveryPollInterval); err != nil {
		return Config{}, err
	}
	c.DeliveryUserAgent = env("PIGEON_DELIVERY_USER_AGENT", c.DeliveryUserAgent)
	if c.SweeperInterval, err = envDuration("PIGEON_SWEEPER_INTERVAL", c.SweeperInterval); err != nil {
		return Config{}, err
	}
	c.SweeperLockURL = env("PIGEON_SWEEPER_LOCK_URL", c.SweeperLockURL)
	if c.SweeperLockWait, err = envDuration("PIGEON_SWEEPER_LOCK_WAIT", c.SweeperLockWait); err != nil {
		return Config{}, err
	}
	// FR-802: 5 s is a hard ceiling on the per-attempt timeout, not a default; a
	// receiver that cannot acknowledge inside it is treated as unavailable.
	if c.DeliveryTimeout > 5*time.Second {
		c.DeliveryTimeout = 5 * time.Second
	}
	// D-6: the sweeper MUST re-queue any lease that has lapsed within 10 s, so its
	// cadence is a ceiling in the same sense — a slower tick silently widens how
	// long a crashed worker's delivery stays unclaimed. It is clamped rather than
	// rejected so one careless env var cannot void a P0 requirement.
	if c.SweeperInterval > maxSweeperInterval {
		c.SweeperInterval = maxSweeperInterval
	}
	// The sweeper's leadership-lock connection defaults to the runtime DSN.
	if c.SweeperLockURL == "" {
		c.SweeperLockURL = c.DatabaseURL
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
	// The migrator owns the tables and PostgreSQL exempts a table owner from
	// row-level security, so "migrate and serve as the same role" is a deployment
	// with no tenant isolation at all. Refusing it is the same class of check as
	// the connection budget: cheap at boot, unfindable in production.
	if c.MigrateDatabaseURL != "" && c.DatabaseURL != "" {
		runtimeRole, migratorRole := roleOf(c.DatabaseURL), roleOf(c.MigrateDatabaseURL)
		if runtimeRole != "" && runtimeRole == migratorRole {
			problems = append(problems, fmt.Sprintf(
				"PIGEON_MIGRATE_DATABASE_URL uses the same role as PIGEON_DATABASE_URL (%q): the migrator owns the tenant tables, "+
					"and a table owner bypasses row-level security, so the migrator role and the runtime role must differ", runtimeRole))
		}
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
	if c.CoreReplicas < 1 {
		problems = append(problems, "PIGEON_CORE_REPLICAS must be >= 1 (OPS-8 counts every replica's pool)")
	}
	if c.DBReservedConns < 0 {
		problems = append(problems, "PIGEON_DB_RESERVED_CONNS must be >= 0")
	}
	if c.DeliveryWorkers < 0 {
		problems = append(problems, "PIGEON_DELIVERY_WORKERS must be >= 0 (0 disables the worker)")
	}
	if c.DeliveryMaxAttempts < 1 {
		problems = append(problems, "PIGEON_DELIVERY_MAX_ATTEMPTS must be >= 1")
	}
	if c.DeliveryMaxBodyBytes < 0 {
		problems = append(problems, "PIGEON_DELIVERY_MAX_BODY_BYTES must be >= 0 (0 disables capture, SB-4)")
	}
	if c.DeliveryMaxConnsPerHost < 1 {
		problems = append(problems, "PIGEON_DELIVERY_MAX_CONNS_PER_HOST must be >= 1 (FR-817)")
	}
	if c.RateLimitPerKeyRPS < 0 || c.RateLimitPerProducerRPS < 0 {
		problems = append(problems, "rate limits must be >= 0 (0 disables the limit)")
	}
	if c.RateLimitPerKeyBurst < 0 || c.RateLimitPerProducerBurst < 0 {
		problems = append(problems, "rate-limit bursts must be >= 0")
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

// ValidateBudget is OPS-8's arithmetic, checked at startup against the server's
// actual max_connections rather than a documented number. The point of the
// requirement is that per-replica pools exhaust connections long before CPU
// becomes the limit, so the failure has to be loud at boot: a replica that starts
// and then steals another replica's connections is an outage with no single
// culprit.
func (c Config) ValidateBudget(serverMaxConnections int32) error {
	budget := ConnectionBudget(c.CoreReplicas, c.DBMaxConns, c.DBReservedConns)
	if budget > serverMaxConnections {
		return fmt.Errorf("%w: OPS-8 connection budget: %d replicas x %d pool + %d reserved = %d > max_connections %d. "+
			"Lower PIGEON_DB_MAX_CONNS, lower PIGEON_CORE_REPLICAS, or put pgbouncer in front of Postgres",
			ErrUnsafeConfig, c.CoreReplicas, c.DBMaxConns, c.DBReservedConns, budget, serverMaxConnections)
	}
	return nil
}

// roleOf returns the username part of a Postgres URL, or "" when the URL does not
// name one (a malformed URL is Connect's problem to report, not Validate's).
func roleOf(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.User == nil {
		return ""
	}
	return u.User.Username()
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

func envInt(key string, def int) (int, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return def, fmt.Errorf("%w: %s=%q is not an integer", ErrUnsafeConfig, key, raw)
	}
	return n, nil
}

func envFloat(key string, def float64) (float64, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return def, nil
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return def, fmt.Errorf("%w: %s=%q is not a number", ErrUnsafeConfig, key, raw)
	}
	return f, nil
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
