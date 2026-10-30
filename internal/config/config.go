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
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/m7jay/pigeon/internal/atrest"
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

// S12's two bounds on the write budget (FR-818, SPEC.md A-11).
const (
	// minWriteQueueDepth is the smallest named PIGEON_WRITE_QUEUE_DEPTH. One slot
	// serialises every write in the process behind whoever holds it and zero would
	// admit nothing, so an explicit value at or below 1 is clamped up to this
	// rather than refused - it is a budget, not a mode, and the default (follow
	// the pool) is the value operators are expected to leave alone.
	minWriteQueueDepth = 2
	// maxIngestAdmitTimeout is OPS-7's ceiling on PIGEON_INGEST_ADMIT_TIMEOUT.
	// FR-818's admission deadline exists so a publisher is told "come back" rather
	// than made to wait, and NFR-1 rates publish at a 100 ms p99: a deadline past
	// 1 s would keep the request inside the handler long past the latency the
	// platform promises, which is the parking A-11 removed, one order of
	// magnitude smaller. Above it is a refusal, not a clamp, because a value that
	// large means the operator meant to disable the shed and OPS-7 wants that
	// said out loud.
	maxIngestAdmitTimeout = time.Second
	// DefaultWriteQueueWorkers and DefaultIngestAdmitTimeout are the shipped
	// values of PIGEON_WRITE_QUEUE_WORKERS and PIGEON_INGEST_ADMIT_TIMEOUT. They
	// are exported so main and the tests name the same numbers the defaults do,
	// and a test asserts they match writeq's own defaults (they are the same
	// resource described in two packages).
	DefaultWriteQueueWorkers  = 2
	DefaultIngestAdmitTimeout = 100 * time.Millisecond
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
	// DBMaxConns is the pool ceiling, and since S12 it is also the default width
	// of FR-818's write budget (PIGEON_WRITE_QUEUE_DEPTH=0 follows it), because
	// the slot that holds a connection and the budget that admits one are the
	// same resource. The default moved 10 -> 40 with S12: 10 was measured to
	// serialize a single publish for 10.2 s against 40 concurrent publishers on
	// the 2 vCPU reference box, and 40 measured 86 ms (t_b5272d2d, docs/load-test.md).
	// It stays OPS-8-safe at 1 and 2 replicas against a default Postgres
	// (1 x 40 + 5 = 45 <= 100; 2 x 40 + 5 = 85 <= 100) and does NOT at 4, which
	// docs/topology.md states with the arithmetic.
	DBMaxConns    int32  // PIGEON_DB_MAX_CONNS (default 40)
	RedisURL      string // PIGEON_REDIS_URL
	MigrationsOn  bool   // PIGEON_MIGRATIONS_ON_BOOT (OPS-9: exactly one migrator)
	MigrateWaitDL time.Duration

	// Identity
	InstanceAdminPassword string // PIGEON_INSTANCE_ADMIN_PASSWORD
	SessionTTL            time.Duration
	SelfRegistration      bool // PIGEON_SELF_REGISTRATION (FR-102: default off)

	// FR-111's password reset (S9a). The window is short and the transport is the
	// deployment's: v1.0 ships no mailer, so the token goes to the core's notifier
	// seam, which logs it only when the operator opts in - a reset link in a
	// shared log is a credential in a shared log.
	PasswordResetTTL       time.Duration // PIGEON_PASSWORD_RESET_TTL (default 1h)
	PasswordResetLogTokens bool          // PIGEON_PASSWORD_RESET_LOG_TOKENS (default off)

	// FR-106's optional OIDC login (S9a). The API server needs only the issuer and
	// the client id, because it VERIFIES an id_token the portal obtained; the
	// client secret and the redirect URI belong to the portal (the relying party)
	// and are read there, so the core process never holds the value that lets a
	// caller speak for the portal at the issuer. Both empty means the feature is
	// off, which is the default: an instance that configures no IdP has no OIDC
	// control on either portal.
	OIDCIssuer   string // PIGEON_OIDC_ISSUER
	OIDCClientID string // PIGEON_OIDC_CLIENT_ID

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

	// Deployment identity (FR-1007). EgressIPs is the allowlist GET
	// /v1/meta/egress-ips publishes: the addresses this instance's deliveries
	// come from, as the operator declared them. Empty means "read the host's own
	// interfaces", which is correct only on a flat network - behind NAT, a load
	// balancer or a container network the receiver sees a different address, and
	// only the operator knows it. Each entry is an address or a CIDR block.
	EgressIPs []string // PIGEON_EGRESS_IPS (comma-separated)

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

	// Write budget (S12, FR-818 amended by SPEC.md A-11). One bounded write
	// budget per process, shared by ingest and every control-state writer: ingest
	// is admitted within IngestAdmitTimeout or shed with 429, and the writer pool
	// is fixed at boot rather than growing a goroutine per event (FR-816).
	WriteQueueDepth    int           // PIGEON_WRITE_QUEUE_DEPTH (0 = follow PIGEON_DB_MAX_CONNS; clamped to 2..DBMaxConns)
	WriteQueueWorkers  int           // PIGEON_WRITE_QUEUE_WORKERS (the fixed batch-writer pool, >= 1)
	IngestAdmitTimeout time.Duration // PIGEON_INGEST_ADMIT_TIMEOUT (NFR-1's rated publish p99; > 0 and <= 1 s)

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

	// HeadersKey is the at-rest encryption key for endpoint custom header values
	// (FR-1006): 32 bytes, base64, under PIGEON_HEADERS_KEY. It is required, and
	// Validate refuses a missing or mis-sized one, because the alternative is a
	// platform that accepts a header value and then has nowhere safe to put it -
	// and because a key that is only noticed at the first registration is a
	// misconfiguration discovered by a customer.
	HeadersKey string // PIGEON_HEADERS_KEY
}

// Defaults are the values the reference deployment runs with (§2.1).
func defaults() Config {
	return Config{
		HTTPAddr:                  ":8080",
		ShutdownGrace:             20 * time.Second,
		ReadHeaderLimit:           10 * time.Second,
		DBMaxConns:                40,
		MigrationsOn:              true,
		MigrateWaitDL:             30 * time.Second,
		SessionTTL:                12 * time.Hour,
		PasswordResetTTL:          time.Hour,
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
		// S12's write budget (FR-818, SPEC.md A-11). The depth follows the pool
		// (0 means "use PIGEON_DB_MAX_CONNS"), the writer pool is the fixed two
		// FR-816 asks for, and admission waits NFR-1's rated publish p99 - the
		// arithmetic for all three is in .env.example and docs/topology.md.
		WriteQueueDepth:    0,
		WriteQueueWorkers:  DefaultWriteQueueWorkers,
		IngestAdmitTimeout: DefaultIngestAdmitTimeout,
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
	if c.PasswordResetTTL, err = envDuration("PIGEON_PASSWORD_RESET_TTL", c.PasswordResetTTL); err != nil {
		return Config{}, err
	}
	if c.PasswordResetLogTokens, err = envBool("PIGEON_PASSWORD_RESET_LOG_TOKENS", c.PasswordResetLogTokens); err != nil {
		return Config{}, err
	}
	c.OIDCIssuer = env("PIGEON_OIDC_ISSUER", "")
	c.OIDCClientID = env("PIGEON_OIDC_CLIENT_ID", "")
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
	// FR-1007: a comma-separated allowlist, empty entries dropped so a trailing
	// comma is not a boot failure.
	c.EgressIPs = splitList(env("PIGEON_EGRESS_IPS", ""))
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
	c.HeadersKey = strings.TrimSpace(env("PIGEON_HEADERS_KEY", c.HeadersKey))
	if c.SweeperInterval, err = envDuration("PIGEON_SWEEPER_INTERVAL", c.SweeperInterval); err != nil {
		return Config{}, err
	}
	c.SweeperLockURL = env("PIGEON_SWEEPER_LOCK_URL", c.SweeperLockURL)
	if c.SweeperLockWait, err = envDuration("PIGEON_SWEEPER_LOCK_WAIT", c.SweeperLockWait); err != nil {
		return Config{}, err
	}
	// S12's write budget (FR-818, SPEC.md A-11).
	if c.WriteQueueDepth, err = envInt("PIGEON_WRITE_QUEUE_DEPTH", c.WriteQueueDepth); err != nil {
		return Config{}, err
	}
	if c.WriteQueueWorkers, err = envInt("PIGEON_WRITE_QUEUE_WORKERS", c.WriteQueueWorkers); err != nil {
		return Config{}, err
	}
	if c.IngestAdmitTimeout, err = envDuration("PIGEON_INGEST_ADMIT_TIMEOUT", c.IngestAdmitTimeout); err != nil {
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
	// S12's write budget: an unnamed depth follows the pool (0), and a named one
	// is clamped into [2, PIGEON_DB_MAX_CONNS]. Both ends are deliberate rather
	// than refusals - see minWriteQueueDepth - and the pool end matters because a
	// budget wider than the pool it admits onto is a longer queue in front of the
	// same connections, which is the parking A-11 removed.
	if c.WriteQueueDepth != 0 {
		if c.WriteQueueDepth < minWriteQueueDepth {
			c.WriteQueueDepth = minWriteQueueDepth
		}
		if c.WriteQueueDepth > int(c.DBMaxConns) {
			c.WriteQueueDepth = int(c.DBMaxConns)
		}
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
	// FR-1006: the at-rest key for endpoint custom header values. It is required
	// rather than optional, and the shape is checked here rather than at the first
	// registration, so a deployment with a wrong-sized key (or none) is a boot
	// refusal with one actionable line instead of a 500 discovered by a customer.
	// The parse is the same code the store and the delivery path use, so the
	// refusal and the runtime cannot disagree about what a key is.
	if _, err := atrest.ParseKey(c.HeadersKey); err != nil {
		problems = append(problems, fmt.Sprintf(
			"%s is required and must be %d bytes of base64 (FR-1006 encrypts endpoint custom header values with it): %v; "+
				"generate one with `openssl rand -base64 32`",
			atrest.KeyEnv, atrest.KeyBytes, err))
	}
	if c.DBMaxConns < 2 {
		problems = append(problems, "PIGEON_DB_MAX_CONNS must be >= 2")
	}
	// FR-818 (SPEC.md A-11): the write budget's refusals. The depth is clamped by
	// Load (see minWriteQueueDepth) and so is not checked here; these two decide
	// whether the budget can do its job at all, so they are refusals.
	if c.WriteQueueWorkers < 1 {
		problems = append(problems, "PIGEON_WRITE_QUEUE_WORKERS must be >= 1 (FR-816's batch-writer pool: 0 leaves the write budget with no writer, so every control-state write would wait forever)")
	}
	if c.IngestAdmitTimeout <= 0 || c.IngestAdmitTimeout > maxIngestAdmitTimeout {
		problems = append(problems, fmt.Sprintf(
			"PIGEON_INGEST_ADMIT_TIMEOUT must be > 0 and <= %s (FR-818: ingest is admitted within NFR-1's rated publish p99 or shed with 429, so the deadline cannot be disabled)", maxIngestAdmitTimeout))
	}
	if c.SessionTTL <= 0 {
		problems = append(problems, "PIGEON_SESSION_TTL must be positive")
	}
	if c.PasswordResetTTL <= 0 {
		problems = append(problems, "PIGEON_PASSWORD_RESET_TTL must be positive (a reset window of zero or less would mint tokens that are already dead)")
	}
	// A half-configured OIDC login would silently disable the feature the operator
	// thinks they enabled: verification needs the issuer to find the keys and the
	// client id to check the audience, so one without the other is a refusal.
	if (c.OIDCIssuer == "") != (c.OIDCClientID == "") {
		problems = append(problems, "PIGEON_OIDC_ISSUER and PIGEON_OIDC_CLIENT_ID must be set together (FR-106: the API verifies an id_token against the issuer's keys and its own client id; one without the other cannot verify anything)")
	}
	if c.CoreReplicas < 1 {
		problems = append(problems, "PIGEON_CORE_REPLICAS must be >= 1 (OPS-8 counts every replica's pool)")
	}
	if c.DBReservedConns < 0 {
		problems = append(problems, "PIGEON_DB_RESERVED_CONNS must be >= 0")
	}
	// FR-1007 publishes this list to receivers as an allowlist. A typo here is a
	// delivery outage on the consumer's side, discovered days later, so it is
	// refused at boot rather than served.
	for _, entry := range c.EgressIPs {
		if net.ParseIP(entry) != nil {
			continue
		}
		if _, _, err := net.ParseCIDR(entry); err == nil {
			continue
		}
		problems = append(problems, fmt.Sprintf(
			"PIGEON_EGRESS_IPS entry %q is neither an IP address nor a CIDR block: FR-1007 publishes this list as the allowlist receivers configure", entry))
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

// splitList parses OPS-3's comma-separated list form. Empty entries are dropped
// so a trailing comma, which is what a hand-written .env file usually has, is not
// a boot failure. Nothing is deduplicated: a repeated entry is harmless, and
// silently rewriting what the operator declared would hide a copy-paste mistake.
func splitList(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
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
