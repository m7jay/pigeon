package config

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func valid() Config {
	return Config{
		HTTPAddr:                  ":8080",
		DatabaseURL:               "postgres://pigeon@localhost/pigeon",
		DBMaxConns:                10,
		DBReservedConns:           5,
		CoreReplicas:              1,
		InstanceAdminPassword:     "correct-horse-battery",
		SessionTTL:                time.Hour,
		ShutdownGrace:             20 * time.Second,
		RateLimitPerKeyRPS:        50,
		RateLimitPerKeyBurst:      100,
		RateLimitPerProducerRPS:   200,
		RateLimitPerProducerBurst: 400,
		LogLevel:                  0,
		DeliveryWorkers:           1,
		DeliveryMaxRetries:        15,
		DeliveryRetryWindow:       72 * time.Hour,
		DeliveryJitter:            0.2,
		DeliveryConcurrency:       64,
		DeliveryMaxConnsPerHost:   10,
		// The reference deployment buffers attempt evidence in Redis (D-1) and
		// keeps SB-5's default retention windows. The buffered mode is only valid
		// with a Redis URL, so a "valid config" must carry both; S8's storage
		// budget must be a real percentage (SB-3), so it is set here too.
		RedisURL:                 "redis://localhost:6379/0",
		DeliveryRetryConcurrency: 16,
		AttemptLogMode:           AttemptLogBuffered,
		AttemptFlushBatch:        MinFlushBatch,
		AttemptFlushInterval:     time.Second,
		AttemptFlushStale:        30 * time.Second,
		SuccessLogRPS:            100,
		RetentionMaxDiskPct:      80,
		RetentionDeliveries:      14 * 24 * time.Hour,
		RetentionAttempts:        7 * 24 * time.Hour,
		RetentionEvents:          14 * 24 * time.Hour,
		// FR-905's archive window. It answers the dedupe question between the
		// deliveries window and this one, so it is the longest of the four and has
		// to be here: a hand-built reference config that leaves it zero is refused
		// by the same SB-5 rule that refuses a zero deliveries window.
		RetentionDedupe: 30 * 24 * time.Hour,
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
		// S5: the §9/FR-1302 bounds are refused where they are set, so a typo
		// cannot ship a policy the requirements do not describe.
		"window under an hour":   func(c *Config) { c.DeliveryRetryWindow = time.Minute },
		"window over seven days": func(c *Config) { c.DeliveryRetryWindow = 8 * 24 * time.Hour },
		"jitter over 100%":       func(c *Config) { c.DeliveryJitter = 1.5 },
		"zero concurrency":       func(c *Config) { c.DeliveryConcurrency = 0 },
		// FR-1303: a zero retry allowance reads as "stop retrying" (FR-1201), so it
		// is refused at load rather than accepted as a mode an operator can pick.
		"zero retry concurrency":     func(c *Config) { c.DeliveryRetryConcurrency = 0 },
		"negative retry concurrency": func(c *Config) { c.DeliveryRetryConcurrency = -1 },
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

// TestValidateBudgetIsTheOPS8Arithmetic pins the refusal message to the numbers,
// because the requirement is that an operator can see which part of the product
// is too big rather than only that something is.
func TestValidateBudgetIsTheOPS8Arithmetic(t *testing.T) {
	c := valid()
	c.CoreReplicas = 4
	c.DBMaxConns = 10
	c.DBReservedConns = 5

	if err := c.ValidateBudget(45); err != nil {
		t.Fatalf("4 x 10 + 5 = 45 fit in max_connections 45 but was refused: %v", err)
	}
	err := c.ValidateBudget(44)
	if err == nil {
		t.Fatal("4 x 10 + 5 = 45 was accepted against max_connections 44")
	}
	if !errors.Is(err, ErrUnsafeConfig) {
		t.Fatalf("error %v is not ErrUnsafeConfig", err)
	}
	for _, want := range []string{"4 replicas", "10 pool", "5 reserved", "45", "pgbouncer"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not name %q", err, want)
		}
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
	t.Setenv("PIGEON_REDIS_URL", "redis://localhost:6379/0") // S6's default mode is buffered (D-1)
	t.Setenv("PIGEON_INSTANCE_ADMIN_PASSWORD", "correct-horse-battery")
	t.Setenv("PIGEON_LOG_LEVEL", "debug")
	t.Setenv("PIGEON_DB_MAX_CONNS", "20")
	t.Setenv("PIGEON_SESSION_TTL", "2h")
	// S5: §9's retry policy and the FR-816 per-process cap are all configurable.
	t.Setenv("PIGEON_DELIVERY_MAX_RETRIES", "7")
	t.Setenv("PIGEON_DELIVERY_RETRY_WINDOW", "24h")
	t.Setenv("PIGEON_DELIVERY_JITTER", "0.1")
	t.Setenv("PIGEON_DELIVERY_CONCURRENCY", "16")
	// FR-1303: the retry lane's allowance is its own knob, read like the others.
	t.Setenv("PIGEON_DELIVERY_RETRY_CONCURRENCY", "8")

	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.DBMaxConns != 20 || c.SessionTTL != 2*time.Hour {
		t.Fatalf("unexpected values: %+v", c)
	}
	if c.DeliveryMaxRetries != 7 || c.DeliveryRetryWindow != 24*time.Hour ||
		c.DeliveryJitter != 0.1 || c.DeliveryConcurrency != 16 ||
		c.DeliveryRetryConcurrency != 8 {
		t.Fatalf("S5 delivery settings not read from the environment: %+v", c)
	}
}

// TestDeliveryRetryConcurrencyHasAShippedDefault pins FR-1303's default: the retry
// lane takes 16 of the process's 64 attempt slots, and the first-attempt lane keeps
// the rest. A default of zero would read as "stop retrying" (FR-1201) and one at or
// above C would empty the first-attempt lane, so both numbers are load-bearing.
func TestDeliveryRetryConcurrencyHasAShippedDefault(t *testing.T) {
	t.Setenv("PIGEON_DATABASE_URL", "postgres://pigeon@localhost/pigeon")
	t.Setenv("PIGEON_REDIS_URL", "redis://localhost:6379/0")
	t.Setenv("PIGEON_INSTANCE_ADMIN_PASSWORD", "correct-horse-battery")

	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.DeliveryRetryConcurrency != 16 {
		t.Errorf("PIGEON_DELIVERY_RETRY_CONCURRENCY defaults to %d, want 16 (FR-1303)", c.DeliveryRetryConcurrency)
	}
	if c.DeliveryConcurrency-c.DeliveryRetryConcurrency < 1 {
		t.Errorf("the defaults leave the first-attempt lane %d slots: C=%d, R=%d (FR-1303)",
			c.DeliveryConcurrency-c.DeliveryRetryConcurrency, c.DeliveryConcurrency, c.DeliveryRetryConcurrency)
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

// TestLoadClampsTheSweeperInterval is D-6: a lapsed lease must be re-queued within
// 10 s, so the sweep cadence is a ceiling. Without the clamp one env var silently
// voids the recovery guarantee, which is the class of configuration OPS-7 refuses
// to start on (FR-802's timeout is clamped the same way).
func TestLoadClampsTheSweeperInterval(t *testing.T) {
	t.Setenv("PIGEON_DATABASE_URL", "postgres://pigeon@localhost/pigeon")
	t.Setenv("PIGEON_REDIS_URL", "redis://localhost:6379/0") // S6's default mode is buffered (D-1)
	t.Setenv("PIGEON_INSTANCE_ADMIN_PASSWORD", "correct-horse-battery")
	t.Setenv("PIGEON_SWEEPER_INTERVAL", "2m")
	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.SweeperInterval != maxSweeperInterval {
		t.Fatalf("SweeperInterval = %s, want it clamped to %s (D-6)", c.SweeperInterval, maxSweeperInterval)
	}
	// A faster cadence is a choice, not a violation: it is left alone.
	t.Setenv("PIGEON_SWEEPER_INTERVAL", "3s")
	c, err = Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.SweeperInterval != 3*time.Second {
		t.Fatalf("SweeperInterval = %s, want the configured 3s", c.SweeperInterval)
	}
}

// TestValidateRefusesANegativeBodyCaptureCap: 0 disables response-body capture
// (SB-4), so a negative value from the environment is a configuration error rather
// than a silent "use the default".
func TestValidateRefusesANegativeBodyCaptureCap(t *testing.T) {
	c := valid()
	c.DeliveryMaxBodyBytes = -1
	if err := c.Validate(); err == nil {
		t.Fatal("Validate accepted a negative PIGEON_DELIVERY_MAX_BODY_BYTES")
	}
	c.DeliveryMaxBodyBytes = 0
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate rejected 0, which SB-4 requires to mean \"capture nothing\": %v", err)
	}
}

// TestLoadClampsTheAttemptFlushInterval is §6's evidence window: the attempt tail
// may be up to 1 s behind, so the flush cadence is a ceiling like D-6's sweep
// interval. Without the clamp one env var silently widens the durability window
// the contract already states.
func TestLoadClampsTheAttemptFlushInterval(t *testing.T) {
	t.Setenv("PIGEON_DATABASE_URL", "postgres://pigeon@localhost/pigeon")
	t.Setenv("PIGEON_REDIS_URL", "redis://localhost:6379/0")
	t.Setenv("PIGEON_INSTANCE_ADMIN_PASSWORD", "correct-horse-battery")
	t.Setenv("PIGEON_ATTEMPT_FLUSH_INTERVAL", "30s")
	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.AttemptFlushInterval != maxFlushInterval {
		t.Fatalf("AttemptFlushInterval = %s, want it clamped to %s (§6)", c.AttemptFlushInterval, maxFlushInterval)
	}
	// Faster than the window is a choice, not a violation.
	t.Setenv("PIGEON_ATTEMPT_FLUSH_INTERVAL", "100ms")
	if c, err = Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.AttemptFlushInterval != 100*time.Millisecond {
		t.Fatalf("AttemptFlushInterval = %s, want the configured 100ms", c.AttemptFlushInterval)
	}
}

// TestLoadRaisesTheFlushBatchToTheFloor is FR-815: at least 2,000 rows per
// transaction at the reference load. A configured value below the floor is raised
// rather than refused, so one careless env var cannot turn the evidence path back
// into single-row commits.
func TestLoadRaisesTheFlushBatchToTheFloor(t *testing.T) {
	t.Setenv("PIGEON_DATABASE_URL", "postgres://pigeon@localhost/pigeon")
	t.Setenv("PIGEON_REDIS_URL", "redis://localhost:6379/0")
	t.Setenv("PIGEON_INSTANCE_ADMIN_PASSWORD", "correct-horse-battery")
	t.Setenv("PIGEON_ATTEMPT_FLUSH_BATCH", "10")
	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.AttemptFlushBatch != MinFlushBatch {
		t.Fatalf("AttemptFlushBatch = %d, want it raised to FR-815's floor %d", c.AttemptFlushBatch, MinFlushBatch)
	}
	// Above the floor is a choice: a bigger batch is a bigger transaction, not a
	// smaller one, so it is left alone.
	t.Setenv("PIGEON_ATTEMPT_FLUSH_BATCH", "5000")
	if c, err = Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.AttemptFlushBatch != 5000 {
		t.Fatalf("AttemptFlushBatch = %d, want the configured 5000", c.AttemptFlushBatch)
	}
}

// TestValidateRefusesBufferedEvidenceWithoutRedis is D-1: the buffer lives in
// Redis, never in the worker's own memory. A buffered configuration with no Redis
// is a refusal at boot, not a runtime surprise where attempts pile up in a
// process that is about to be restarted.
func TestValidateRefusesBufferedEvidenceWithoutRedis(t *testing.T) {
	c := valid()
	c.RedisURL = ""
	if err := c.Validate(); err == nil {
		t.Fatal("Validate accepted attempt_log.mode=buffered with no PIGEON_REDIS_URL")
	}
	// The explicit inline mode is the escape hatch, and it needs no Redis.
	c.AttemptLogMode = AttemptLogSync
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate rejected sync mode without Redis: %v", err)
	}
	// An unknown mode is a refusal too, not a silent buffered.
	c.AttemptLogMode = "sometimes"
	if err := c.Validate(); err == nil {
		t.Fatal("Validate accepted an unknown PIGEON_ATTEMPT_LOG_MODE")
	}
}

// TestValidateRefusesNonPositiveRetention is SB-5: retention is configuration,
// and a zero window is not "keep everything", it is "drop everything". Refusing
// it is the difference between "operator sized history" and "operator erased it".
func TestValidateRefusesNonPositiveRetention(t *testing.T) {
	for _, zero := range []func(*Config){
		func(c *Config) { c.RetentionDeliveries = 0 },
		func(c *Config) { c.RetentionAttempts = 0 },
		func(c *Config) { c.RetentionEvents = 0 },
		// FR-905's archive window is the same kind of promise about how long an
		// answer survives, so zero is refused here too rather than meaning "no
		// archive", which would silently break FR-903's 30 days.
		func(c *Config) { c.RetentionDedupe = 0 },
	} {
		c := valid()
		zero(&c)
		if err := c.Validate(); err == nil {
			t.Fatalf("Validate accepted a non-positive retention window: %+v", c)
		}
	}
	// The defaults() bootstrap sets SB-5's windows, so they are configuration and
	// not a migration constant.
	d := defaults()
	if d.RetentionDeliveries != 14*24*time.Hour || d.RetentionAttempts != 7*24*time.Hour || d.RetentionEvents != 14*24*time.Hour {
		t.Fatalf("SB-5 defaults drifted: %s / %s / %s", d.RetentionDeliveries, d.RetentionAttempts, d.RetentionEvents)
	}
	if d.RetentionDedupe != 30*24*time.Hour {
		t.Fatalf("FR-905 archive default drifted: %s, want 30 d", d.RetentionDedupe)
	}
}
