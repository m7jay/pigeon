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

// maxFlushInterval is §6's evidence window seen from the flusher's side: the
// attempt tail may be up to 1 s behind, so the flush cadence is a ceiling in the
// same sense as FR-802's timeout and D-6's sweep interval.
const maxFlushInterval = time.Second

// MinFlushBatch is FR-815's floor: at least 2,000 rows per transaction at the
// reference load, so the commit rate stays in the tens per second rather than
// the thousands. It is exported because the flusher and its tests assert the
// same number; a configured value below it is raised rather than refused, so one
// careless env var cannot turn the evidence path back into single-row commits.
const MinFlushBatch = 2000

// The two D-2 attempt-log modes. `buffered` is the design D-1 mandates (the
// buffer lives in Redis, the flusher drains it); `sync` writes every attempt
// inline, for operators who value evidence over throughput.
const (
	AttemptLogBuffered = "buffered"
	AttemptLogSync     = "sync"
)

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

	// Retention (SB-3, SB-5, SB-6, SB-7). The disk budget is a percentage of the
	// volume that holds the data directory; the age windows are configured as
	// durations with the rest of the retention settings (see
	// RetentionDeliveries/Attempts/Events below), and docs/retention.md documents
	// the per-row sizing they are chosen from. Both are configuration rather than
	// migrations (FR-1104), because they depend on the operator's volume.
	RetentionMaxDiskPct int // PIGEON_RETENTION_MAX_DISK_PCT (default 80)
	// DiskDir is the directory whose filesystem is measured against the budget
	// (usually the Postgres data directory, visible when core and postgres share
	// a volume). Empty disables the filesystem reading, leaving the
	// database-size fallback below.
	DiskDir string // PIGEON_DISK_DIR
	// DiskBytes, when set, is the volume size to compare against instead of the
	// filesystem's own total. It is the fallback for a deployment where core
	// cannot statfs the data directory: usage is then pigeon's own database size
	// against this number, reported as source "database" in /v1/meta/usage.
	DiskBytes uint64 // PIGEON_DISK_BYTES

	// Delivery worker (S4), the S5 limits, and sweeper. The timeout is capped at
	// the FR-802 hard ceiling of 5 s in Load.
	DeliveryWorkers          int           // PIGEON_DELIVERY_WORKERS (0 disables the worker)
	DeliveryLease            time.Duration // PIGEON_DELIVERY_LEASE
	DeliveryTimeout          time.Duration // PIGEON_DELIVERY_TIMEOUT (<= 5 s, FR-802)
	DeliveryMaxBodyBytes     int           // PIGEON_DELIVERY_MAX_BODY_BYTES (FR-804)
	DeliveryBackoffBase      time.Duration // PIGEON_DELIVERY_BACKOFF_BASE
	DeliveryBackoffMax       time.Duration // PIGEON_DELIVERY_BACKOFF_MAX (<= 12 h, §9)
	DeliveryMaxRetries       int           // PIGEON_DELIVERY_MAX_RETRIES (§9: 15 retries, not attempts)
	DeliveryRetryWindow      time.Duration // PIGEON_DELIVERY_RETRY_WINDOW (1 h - 7 d, §9; default 72 h)
	DeliveryJitter           float64       // PIGEON_DELIVERY_JITTER (§9: 0.2 = ±20%)
	DeliveryConcurrency      int           // PIGEON_DELIVERY_CONCURRENCY (FR-816 per-process cap)
	DeliveryRetryConcurrency int           // PIGEON_DELIVERY_RETRY_CONCURRENCY (FR-1303 retry lane; partitioned out of DeliveryConcurrency)
	DeliverySlotTTL          time.Duration // PIGEON_DELIVERY_SLOT_TTL (FR-1302; 0 = 3x timeout)
	DeliveryMaxConnsPerHost  int           // PIGEON_DELIVERY_MAX_CONNS_PER_HOST (FR-817, FR-1302's default cap: 10)
	DeliveryPollInterval     time.Duration // PIGEON_DELIVERY_POLL_INTERVAL
	DeliveryUserAgent        string        // PIGEON_DELIVERY_USER_AGENT (FR-810)
	SweeperInterval          time.Duration // PIGEON_SWEEPER_INTERVAL (<= 10 s, D-6; clamped in Load)
	SweeperLockURL           string        // PIGEON_SWEEPER_LOCK_URL (defaults to DatabaseURL)
	SweeperLockWait          time.Duration // PIGEON_SWEEPER_LOCK_WAIT (how long to compete before logging "standing by"; the replica keeps competing)

	// Attempt evidence buffering (S6). D-1: attempts are buffered in Redis and
	// never in worker process memory; D-2: the mode is configurable; FR-812/815:
	// the flusher writes them in batches of at least 2,000 rows per transaction
	// under synchronous_commit=off.
	AttemptLogMode       string        // PIGEON_ATTEMPT_LOG_MODE (buffered | sync)
	AttemptFlushBatch    int           // PIGEON_ATTEMPT_FLUSH_BATCH (>= 2000, FR-815; clamped up)
	AttemptFlushInterval time.Duration // PIGEON_ATTEMPT_FLUSH_INTERVAL (<= 1 s, §6 evidence window)
	AttemptFlushStale    time.Duration // PIGEON_ATTEMPT_FLUSH_STALE (when a peer flusher's batch is reclaimed)
	SuccessLogRPS        int           // PIGEON_SUCCESS_LOG_RPS (LOG-2's bound; counters carry the rest)

	// Retention windows (SB-5): configuration, never migration constants, so an
	// operator sizes history for their own disk. The partition dropper (S8) is
	// the consumer; the defaults are the conservative ones SB-5 names. Sustained
	// throughput shortens the effective window automatically (§8.1.1), which the
	// docs state rather than leaving to be discovered.
	RetentionDeliveries time.Duration // PIGEON_RETENTION_DELIVERIES (14 d)
	RetentionAttempts   time.Duration // PIGEON_RETENTION_ATTEMPTS (7 d)
	RetentionEvents     time.Duration // PIGEON_RETENTION_EVENTS (14 d)
	// RetentionDedupe is FR-905's archive window (30 d). The archive is what
	// answers the dedupe question between the deliveries window and this one, so
	// a value below RetentionDeliveries leaves the archive nothing to hold and a
	// value below 30 d shortens FR-903's promise.
	RetentionDedupe time.Duration // PIGEON_RETENTION_DEDUPE (30 d)

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
		RetentionMaxDiskPct:       80,
		DeliveryWorkers:           1,
		DeliveryLease:             40 * time.Second,
		DeliveryTimeout:           5 * time.Second,
		DeliveryMaxBodyBytes:      2048,
		DeliveryBackoffBase:       30 * time.Second,
		DeliveryBackoffMax:        12 * time.Hour,
		DeliveryMaxRetries:        15,
		DeliveryRetryWindow:       72 * time.Hour,
		DeliveryJitter:            0.2,
		DeliveryConcurrency:       64,
		DeliveryRetryConcurrency:  16,
		DeliveryMaxConnsPerHost:   10,
		DeliveryPollInterval:      time.Second,
		SweeperInterval:           5 * time.Second,
		SweeperLockWait:           60 * time.Second,
		// S6's defaults: buffered evidence (D-1), 2,000-row transactions
		// (FR-815), a 1 s flush cadence (the §6 window), and the SB-5 retention
		// windows. SuccessLogRPS is LOG-2's bound.
		AttemptLogMode:       AttemptLogBuffered,
		AttemptFlushBatch:    MinFlushBatch,
		AttemptFlushInterval: time.Second,
		AttemptFlushStale:    30 * time.Second,
		SuccessLogRPS:        100,
		RetentionDeliveries:  14 * 24 * time.Hour,
		RetentionAttempts:    7 * 24 * time.Hour,
		RetentionEvents:      14 * 24 * time.Hour,
		RetentionDedupe:      30 * 24 * time.Hour,
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
	if c.RetentionMaxDiskPct, err = envInt("PIGEON_RETENTION_MAX_DISK_PCT", c.RetentionMaxDiskPct); err != nil {
		return Config{}, err
	}
	c.DiskDir = env("PIGEON_DISK_DIR", c.DiskDir)
	if c.DiskBytes, err = envBytes("PIGEON_DISK_BYTES", c.DiskBytes); err != nil {
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
	if c.DeliveryMaxRetries, err = envInt("PIGEON_DELIVERY_MAX_RETRIES", c.DeliveryMaxRetries); err != nil {
		return Config{}, err
	}
	if c.DeliveryRetryWindow, err = envDuration("PIGEON_DELIVERY_RETRY_WINDOW", c.DeliveryRetryWindow); err != nil {
		return Config{}, err
	}
	if c.DeliveryJitter, err = envFloat("PIGEON_DELIVERY_JITTER", c.DeliveryJitter); err != nil {
		return Config{}, err
	}
	if c.DeliveryConcurrency, err = envInt("PIGEON_DELIVERY_CONCURRENCY", c.DeliveryConcurrency); err != nil {
		return Config{}, err
	}
	if c.DeliveryRetryConcurrency, err = envInt("PIGEON_DELIVERY_RETRY_CONCURRENCY", c.DeliveryRetryConcurrency); err != nil {
		return Config{}, err
	}
	if c.DeliverySlotTTL, err = envDuration("PIGEON_DELIVERY_SLOT_TTL", c.DeliverySlotTTL); err != nil {
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
	c.AttemptLogMode = strings.ToLower(strings.TrimSpace(env("PIGEON_ATTEMPT_LOG_MODE", c.AttemptLogMode)))
	if c.AttemptFlushBatch, err = envInt("PIGEON_ATTEMPT_FLUSH_BATCH", c.AttemptFlushBatch); err != nil {
		return Config{}, err
	}
	if c.AttemptFlushInterval, err = envDuration("PIGEON_ATTEMPT_FLUSH_INTERVAL", c.AttemptFlushInterval); err != nil {
		return Config{}, err
	}
	if c.AttemptFlushStale, err = envDuration("PIGEON_ATTEMPT_FLUSH_STALE", c.AttemptFlushStale); err != nil {
		return Config{}, err
	}
	if c.SuccessLogRPS, err = envInt("PIGEON_SUCCESS_LOG_RPS", c.SuccessLogRPS); err != nil {
		return Config{}, err
	}
	if c.RetentionDeliveries, err = envDuration("PIGEON_RETENTION_DELIVERIES", c.RetentionDeliveries); err != nil {
		return Config{}, err
	}
	if c.RetentionAttempts, err = envDuration("PIGEON_RETENTION_ATTEMPTS", c.RetentionAttempts); err != nil {
		return Config{}, err
	}
	if c.RetentionEvents, err = envDuration("PIGEON_RETENTION_EVENTS", c.RetentionEvents); err != nil {
		return Config{}, err
	}
	if c.RetentionDedupe, err = envDuration("PIGEON_RETENTION_DEDUPE", c.RetentionDedupe); err != nil {
		return Config{}, err
	}
	// FR-815 is a floor, not a target: a batch below 2,000 rows is raised rather
	// than refused, the same way FR-802's timeout and D-6's cadence are clamped.
	// One careless env var must not turn the evidence path back into single-row
	// commits, because that is a throughput property of the whole delivery path.
	if c.AttemptFlushBatch < MinFlushBatch {
		c.AttemptFlushBatch = MinFlushBatch
	}
	// §6 bounds the evidence tail at 1 s. A slower cadence would widen the window
	// the durability contract already states, so it is clamped like the others.
	if c.AttemptFlushInterval > maxFlushInterval {
		c.AttemptFlushInterval = maxFlushInterval
	}
	// LOG-2 bounds per-success lines above 100/s. The floor is one line per
	// second, not zero: an operator still needs a liveness signal, and the
	// counters carry the volume either way. It is also LOG-2's guard for the
	// environment: this runs before Validate, so no environment value turns the
	// gate off (A-8). PIGEON_SUCCESS_LOG_RPS=0 and a negative value both arrive at
	// Validate as 1, and main passes the floored value on.
	if c.SuccessLogRPS < 1 {
		c.SuccessLogRPS = 1
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
	if c.DeliveryMaxRetries < 1 {
		problems = append(problems, "PIGEON_DELIVERY_MAX_RETRIES must be >= 1 (§9: retries, so attempts = retries + 1)")
	}
	// The window bound is §9's per-endpoint range (1 h - 7 d). The deployment
	// default is the same 72 h the endpoint default carries, and a global value
	// outside the per-endpoint range would make the override meaningless: an
	// endpoint could not express the policy the platform claims to support.
	if c.DeliveryRetryWindow < time.Hour || c.DeliveryRetryWindow > 7*24*time.Hour {
		problems = append(problems, "PIGEON_DELIVERY_RETRY_WINDOW must be between 1h and 168h (FR-1201, §9)")
	}
	// Jitter is bounded at 100%: beyond that the delay could be negative, and a
	// negative backoff is not a backoff.
	if c.DeliveryJitter < 0 || c.DeliveryJitter > 1 {
		problems = append(problems, "PIGEON_DELIVERY_JITTER must be between 0 and 1 (§9: 0.2 = ±20%)")
	}
	if c.DeliveryConcurrency < 1 {
		problems = append(problems, "PIGEON_DELIVERY_CONCURRENCY must be >= 1 (FR-816)")
	}
	// FR-1303: the retry lane's allowance is validated exactly as C is, so a
	// configured 0 is refused rather than accepted as a mode an operator can pick
	// - a zero retry allowance reads as "stop retrying", which FR-1201 forbids.
	// The partition itself (R = clamp(configured, 0, C-1), so the first-attempt
	// lane keeps at least one slot) is the worker's: it needs the effective C.
	if c.DeliveryRetryConcurrency < 1 {
		problems = append(problems, "PIGEON_DELIVERY_RETRY_CONCURRENCY must be >= 1 (FR-1303: the retry lane's allowance; 0 would stop retrying, which FR-1201 forbids)")
	}
	if c.DeliverySlotTTL < 0 {
		problems = append(problems, "PIGEON_DELIVERY_SLOT_TTL must be >= 0 (0 = three times the attempt timeout)")
	}
	if c.DeliveryMaxBodyBytes < 0 {
		problems = append(problems, "PIGEON_DELIVERY_MAX_BODY_BYTES must be >= 0 (0 disables capture, SB-4)")
	}
	if c.DeliveryMaxConnsPerHost < 1 {
		problems = append(problems, "PIGEON_DELIVERY_MAX_CONNS_PER_HOST must be >= 1 (FR-817)")
	}
	// D-2: the two attempt-log modes, and D-1's Redis requirement for the one
	// that buffers. Buffered evidence with no Redis would mean the worker holding
	// attempts in its own memory, which is the failure mode D-1 exists to
	// prevent - so it is a refusal at boot, not a runtime surprise.
	switch c.AttemptLogMode {
	case AttemptLogBuffered:
		if c.RedisURL == "" {
			problems = append(problems, "PIGEON_ATTEMPT_LOG_MODE=buffered requires PIGEON_REDIS_URL: D-1 keeps the attempt buffer in Redis, never in worker process memory (set PIGEON_ATTEMPT_LOG_MODE=sync to write evidence inline instead)")
		}
	case AttemptLogSync:
	default:
		problems = append(problems, fmt.Sprintf("PIGEON_ATTEMPT_LOG_MODE=%q must be %s or %s (D-2)",
			c.AttemptLogMode, AttemptLogBuffered, AttemptLogSync))
	}
	if c.AttemptFlushBatch < 0 {
		problems = append(problems, fmt.Sprintf("PIGEON_ATTEMPT_FLUSH_BATCH must be >= 0 (a value under %d is raised to it, FR-815)", MinFlushBatch))
	}
	if c.AttemptFlushInterval < 0 {
		problems = append(problems, "PIGEON_ATTEMPT_FLUSH_INTERVAL must be >= 0")
	}
	if c.AttemptFlushStale < 0 {
		problems = append(problems, "PIGEON_ATTEMPT_FLUSH_STALE must be >= 0")
	}
	// Unreachable through Load: the LOG-2 floor in Load raises anything below 1 to
	// 1 before Validate runs, so no environment value disables the gate (A-8).
	// Kept as the guard for a Config built and validated without Load.
	if c.SuccessLogRPS < 0 {
		problems = append(problems, "PIGEON_SUCCESS_LOG_RPS must be >= 0 (LOG-2)")
	}
	// SB-5: retention is configuration. A zero window is not "keep everything",
	// it is "drop everything", so it is refused rather than defaulted silently.
	if c.RetentionDeliveries <= 0 || c.RetentionAttempts <= 0 || c.RetentionEvents <= 0 || c.RetentionDedupe <= 0 {
		problems = append(problems, fmt.Sprintf(
			"retention windows must be positive (SB-5/FR-905 defaults: deliveries %s, attempts %s, events %s, dedupe archive %s)",
			14*24*time.Hour, 7*24*time.Hour, 14*24*time.Hour, 30*24*time.Hour))
	}
	if c.RateLimitPerKeyRPS < 0 || c.RateLimitPerProducerRPS < 0 {
		problems = append(problems, "rate limits must be >= 0 (0 disables the limit)")
	}
	if c.RateLimitPerKeyBurst < 0 || c.RateLimitPerProducerBurst < 0 {
		problems = append(problems, "rate-limit bursts must be >= 0")
	}
	// SB-3's budget. A 0% budget would drop history on an empty disk and a >100%
	// one would never drop at all, so both are refusals rather than clamps: the
	// operator meant something, and a silent clamp would hide it until the disk
	// filled. SB-5's windows are validated as durations with the other S6
	// settings, above.
	if c.RetentionMaxDiskPct < 1 || c.RetentionMaxDiskPct > 100 {
		problems = append(problems, "PIGEON_RETENTION_MAX_DISK_PCT must be between 1 and 100 (SB-3)")
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

// envBytes parses a size like 40GiB, 512MiB or a plain byte count, so an
// operator can write the volume size in the units they think in. Suffixes are
// binary (GiB = 1024^3) because that is what `du` and the filesystem report.
func envBytes(key string, def uint64) (uint64, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return def, nil
	}
	s := strings.TrimSpace(raw)
	mult := uint64(1)
	for _, suf := range []struct {
		suffix string
		mult   uint64
	}{
		{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}, {"TiB", 1 << 40},
		{"K", 1 << 10}, {"M", 1 << 20}, {"G", 1 << 30}, {"T", 1 << 40},
	} {
		trimmed := strings.TrimSuffix(s, suf.suffix)
		if trimmed != s {
			s, mult = trimmed, suf.mult
			break
		}
	}
	n, err := strconv.ParseUint(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return def, fmt.Errorf("%w: %s=%q is not a size (e.g. 40GiB, 512MiB, 10737418240)", ErrUnsafeConfig, key, raw)
	}
	return n * mult, nil
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
