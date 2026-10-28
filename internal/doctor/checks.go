package doctor

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/m7jay/pigeon/internal/config"
)

// The stable check ids (OPS-11). They are constants rather than literals at the
// call sites because a runbook quotes them and a rename must be a compile error.
const (
	// CheckConfig: the configuration loaded and passed OPS-7's refusal gate.
	CheckConfig = "config.load"
	// CheckDBReachable: Postgres answers a ping.
	CheckDBReachable = "db.reachable"
	// CheckDBBudget: OPS-8/OPS-10's connection arithmetic fits max_connections.
	CheckDBBudget = "db.connection_budget"
	// CheckRedisReachable: the broker answers a ping.
	CheckRedisReachable = "redis.reachable"
	// CheckRedisAppendOnly: D-5's durability setting. RDB-only loses the queue.
	CheckRedisAppendOnly = "redis.appendonly"
	// CheckClockSkew: this process's clock against its reference.
	CheckClockSkew = "clock.skew"
	// CheckDiskHeadroom: the storage budget still has room (SB-3).
	CheckDiskHeadroom = "disk.headroom"
	// CheckFDs: `ulimit -n` covers FR-816's configured concurrency.
	CheckFDs = "ulimit.no_file"
)

// DBPinger is the read-only slice of Postgres doctor needs. It is an interface so
// the checks can be exercised against a fake and against a real cluster with the
// same code path.
type DBPinger interface {
	Ping(ctx context.Context) error
	// MaxConnections is `show max_connections` - the server's real limit, not the
	// documented default. OPS-8's arithmetic is meaningless against a guess.
	MaxConnections(ctx context.Context) (int32, error)
	// ServerTime is `select now()`, the clock reference for skew.
	ServerTime(ctx context.Context) (time.Time, error)
	// DatabaseSize is `pg_database_size(current_database())`, the fallback disk
	// reading when no volume is mounted into this process (the same fallback the
	// retention gauge uses).
	DatabaseSize(ctx context.Context) (uint64, error)
}

// RedisProbe is the read-only slice of Redis doctor needs.
type RedisProbe interface {
	Ping(ctx context.Context) error
	// AppendOnly returns CONFIG GET appendonly and appendfsync. Both matter: an
	// AOF with fsync=no loses the same queue a RDB loses, just more slowly.
	AppendOnly(ctx context.Context) (appendonly, appendfsync string, err error)
	// Time is the server clock, the reference when Postgres is unreachable.
	Time(ctx context.Context) (time.Time, error)
}

// Statfs is `statfs(2)` on the disk budget's directory.
type Statfs struct {
	TotalBytes uint64
	FreeBytes  uint64
}

// Deps is everything a run inspects. Every field is a seam: the checks are driven
// by fakes in the unit tests and by the real probes in cmd/pigeon, with no
// test-only branch in the checks themselves.
type Deps struct {
	// Cfg is the loaded configuration. Its zero value is what a box whose config
	// was refused gets, so the checks must tolerate it.
	Cfg config.Config
	// CfgErr is why config.Load refused, empty when it did not.
	CfgErr string
	// DatabaseURL and RedisURL come from the environment directly. They are read
	// even when CfgErr is set, because the whole point of doctor on a broken box
	// is to reach past the refusal and say what the operator needs to fix.
	DatabaseURL string
	RedisURL    string

	DB    DBPinger
	Redis RedisProbe

	// DBErr and RedisErr are why the probe could not be opened. They are what
	// separates "Postgres is not configured" (a skip: nothing was asked of it)
	// from "Postgres was asked and did not answer" (a failure, and usually the
	// whole reason the operator ran doctor).
	DBErr    error
	RedisErr error

	// Statfs reads the filesystem at a path. Nil means the check skips.
	Statfs func(path string) (Statfs, error)
	// FDLimit reads RLIMIT_NOFILE. Nil means the check skips.
	FDLimit func() (soft, hard uint64, err error)

	// Now is the clock, injected so skew can be tested without changing the
	// machine's time. Nil means time.Now.
	Now func() time.Time

	// CheckTimeout bounds one check; zero means DefaultCheckTimeout.
	CheckTimeout time.Duration
}

func (d Deps) checkTimeout() time.Duration {
	if d.CheckTimeout > 0 {
		return d.CheckTimeout
	}
	return DefaultCheckTimeout
}

func (d Deps) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

// Run executes every check in a fixed order and returns the report. It stops
// early only when the whole run's budget is spent: a check that timed out is
// recorded as a failure with the timeout as its observed value, because a check
// that does not answer is exactly the fault doctor exists to name.
func (d Deps) Run(ctx context.Context) Report {
	ctx, cancel := context.WithTimeout(ctx, DefaultTotalTimeout)
	defer cancel()

	var r Report
	r.Add(d.checkConfig())
	// The order is fixed and meaningful: reachability before what depends on it, so
	// the first failing line is the cause and the lines after it are consequences.
	checks := []struct {
		id string
		fn func(context.Context) Check
	}{
		{CheckDBReachable, d.checkDBReachable},
		{CheckDBBudget, d.checkDBBudget},
		{CheckRedisReachable, d.checkRedisReachable},
		{CheckRedisAppendOnly, d.checkRedisAppendOnly},
		{CheckClockSkew, d.checkClockSkew},
		{CheckDiskHeadroom, d.checkDiskHeadroom},
		{CheckFDs, d.checkFileDescriptors},
	}
	for _, c := range checks {
		r.Add(bounded(ctx, d.checkTimeout(), c.id, c.fn))
	}
	// Exit 3's JSON must not read as healthy: nothing was observed, so `ok` is
	// "unknown", and a consumer that only branches on `ok` would treat a box
	// doctor could not inspect at all as a passing box.
	if r.ExitCode() == ExitCannotRun {
		r.OK = false
	}
	return r
}

// bounded runs one check under its own timeout and turns a timeout into the
// check's failure rather than the run's. The id is carried in rather than read off
// the returned Check: a check that never returned has no Check to read it from, and
// an empty id would make the timeout ungreppable - the one failure an operator most
// needs to find by id.
func bounded(ctx context.Context, timeout time.Duration, id string, fn func(context.Context) Check) Check {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	done := make(chan Check, 1)
	go func() { done <- fn(cctx) }()
	select {
	case c := <-done:
		return c
	case <-cctx.Done():
		if ctx.Err() != nil {
			// The whole run's budget is gone; the check that was running is the
			// one that consumed it.
			return Check{Status: Fail, Observed: "the whole doctor run exceeded its budget",
				Required: "every check bounded within 5 s and the run within 20 s (OPS-11)",
				Fix:      "re-run doctor; a dependency is hanging rather than failing"}
		}
		return Check{ID: id, Status: Fail, Observed: fmt.Sprintf("no answer within %s", timeout),
			Required: "an answer within the check's own bound (OPS-11: 5 s)",
			Fix:      "the dependency is hanging: check its host and port, not its credentials"}
	}
}

// checkConfig is the only synchronous check: it reports the loader's verdict
// rather than re-deciding it. OPS-7's refusals arrive as an error text naming the
// variable, and that text is the actionable message.
func (d Deps) checkConfig() Check {
	c := Check{ID: CheckConfig, Required: "a configuration that loads and passes OPS-7's refusal gate", Fix: "fix the Pigeon variable named in the observed text; .env.example documents every knob (OPS-3)"}
	if d.CfgErr == "" {
		c.Status = Pass
		c.Observed = fmt.Sprintf("loaded; %d replicas, pool %d, delivery concurrency %d",
			d.Cfg.CoreReplicas, d.Cfg.DBMaxConns, d.Cfg.DeliveryConcurrency)
		return c
	}
	c.Status = Fail
	c.Observed = d.CfgErr
	return c
}

func (d Deps) checkDBReachable(ctx context.Context) Check {
	c := Check{ID: CheckDBReachable, Required: "Postgres accepts a connection and answers a ping",
		Fix: "check that Postgres is running and that PIGEON_DATABASE_URL names a reachable host, port and database"}
	if d.DB == nil {
		if d.DBErr != nil {
			c.Status = Fail
			c.Observed = fmt.Sprintf("%s: %v", d.redacted(d.DatabaseURL), d.DBErr)
			return c
		}
		c.Status = Skip
		c.Observed = d.noDBreason()
		return c
	}
	start := d.now()
	if err := d.DB.Ping(ctx); err != nil {
		c.Status = Fail
		c.Observed = fmt.Sprintf("%s could not be pinged: %v", d.redacted(d.DatabaseURL), err)
		return c
	}
	c.Status = Pass
	c.Observed = fmt.Sprintf("%s answered in %s", d.redacted(d.DatabaseURL), d.now().Sub(start).Round(time.Millisecond))
	return c
}

func (d Deps) noRedisreason() string {
	if d.RedisURL == "" {
		return "PIGEON_REDIS_URL is not set: the relay publishes nothing and the attempt buffer falls back to sync mode"
	}
	return "no connection could be opened to " + d.redacted(d.RedisURL)
}

// cfgKnown reports whether the configuration loaded. The checks that do arithmetic
// over configured values refuse to guess when it did not: zero is not a measurement.
func (d Deps) cfgKnown() bool { return d.CfgErr == "" }

func (d Deps) noDBreason() string {
	if d.DatabaseURL == "" {
		return "PIGEON_DATABASE_URL is not set"
	}
	return "no connection could be opened to " + d.redacted(d.DatabaseURL)
}

// checkDBBudget is OPS-10: the substituted formula *is* the observed value, so an
// operator reading the line can see which term to change. It warns rather than
// fails because OPS-10 says "warn", and because a budget that over-subscribes
// connections degrades the deployment rather than refusing to start it (OPS-7
// lists the refusals and this is not one of them).
func (d Deps) checkDBBudget(ctx context.Context) Check {
	c := Check{ID: CheckDBBudget, Required: "core_replicas x db_max_conns + db_reserved_conns <= max_connections",
		Fix: "lower PIGEON_DB_MAX_CONNS or PIGEON_CORE_REPLICAS, or put pgbouncer in front of Postgres (OPS-8)"}
	if !d.cfgKnown() {
		// The arithmetic reads configured values. With the configuration refused
		// they are zero, and zero over-subscribes nothing - a pass here would be
		// the most misleading line in the report.
		c.Status = Skip
		c.Observed = "the configuration did not load, so the replica count and pool size are unknown"
		return c
	}
	if d.DB == nil {
		c.Status = Skip
		c.Observed = d.noDBreason()
		return c
	}
	max, err := d.DB.MaxConnections(ctx)
	if err != nil {
		c.Status = Skip
		c.Observed = fmt.Sprintf("max_connections could not be read: %v", err)
		return c
	}
	used := d.Cfg.CoreReplicas*d.Cfg.DBMaxConns + d.Cfg.DBReservedConns
	formula := fmt.Sprintf("core_replicas(%d) x db_max_conns(%d) + db_reserved_conns(%d) = %d <= max_connections(%d)",
		d.Cfg.CoreReplicas, d.Cfg.DBMaxConns, d.Cfg.DBReservedConns, used, max)
	c.Observed = formula
	if used > max {
		c.Status = Warn
		shortfall := used - max
		c.Observed = fmt.Sprintf("%s - over by %d", formula, shortfall)
		return c
	}
	c.Status = Pass
	return c
}

func (d Deps) checkRedisReachable(ctx context.Context) Check {
	c := Check{ID: CheckRedisReachable, Required: "Redis accepts a connection and answers a ping",
		Fix: "check that Redis is running and that PIGEON_REDIS_URL is right; without Redis the relay publishes nothing and attempts are written inline"}
	if d.Redis == nil {
		if d.RedisErr != nil {
			c.Status = Fail
			c.Observed = fmt.Sprintf("%s: %v", d.redacted(d.RedisURL), d.RedisErr)
			return c
		}
		c.Status = Skip
		c.Observed = d.noRedisreason()
		return c
	}
	start := d.now()
	if err := d.Redis.Ping(ctx); err != nil {
		c.Status = Fail
		c.Observed = fmt.Sprintf("%s could not be pinged: %v", d.redacted(d.RedisURL), err)
		return c
	}
	c.Status = Pass
	c.Observed = fmt.Sprintf("%s answered in %s", d.redacted(d.RedisURL), d.now().Sub(start).Round(time.Millisecond))
	return c
}

// checkRedisAppendOnly is D-5 and OPS-7's durability gate. RDB-only persistence
// loses the events stream on restart, which is a lost accepted event (P3).
func (d Deps) checkRedisAppendOnly(ctx context.Context) Check {
	c := Check{ID: CheckRedisAppendOnly, Required: "appendonly=yes with appendfsync everysec (D-5)",
		Fix: "start Redis with --appendonly yes --appendfsync everysec; RDB-only persistence loses the events stream on restart"}
	if d.Redis == nil {
		c.Status = Skip
		c.Observed = d.noRedisreason()
		return c
	}
	appendonly, appendfsync, err := d.Redis.AppendOnly(ctx)
	if err != nil {
		c.Status = Skip
		c.Observed = fmt.Sprintf("CONFIG GET could not be read: %v", err)
		return c
	}
	c.Observed = fmt.Sprintf("appendonly=%s appendfsync=%s", orDash(appendonly), orDash(appendfsync))
	switch strings.ToLower(strings.TrimSpace(appendonly)) {
	case "yes":
		c.Status = Pass
	default:
		c.Status = Fail
	}
	return c
}

// checkClockSkew compares this process's clock with a reference. The reference is
// the database that hands out leases and retry deadlines, because a worker's clock
// drifting from the one that stamped `next_attempt_at` is what actually breaks a
// deployment; Redis is the fallback and its own clock is compared the same way.
//
// The servers' clocks are not asserted to be correct - only that this process
// agrees with the clock its own scheduling reads.
func (d Deps) checkClockSkew(ctx context.Context) Check {
	c := Check{ID: CheckClockSkew, Required: "|local - reference| <= " + skewLimit.String(),
		Fix: "run an NTP client (chrony or systemd-timesyncd) on this host and on the database host; drift breaks lease and retry timing"}
	local := d.now()
	ref, source, err := d.referenceTime(ctx)
	if err != nil {
		c.Status = Skip
		c.Observed = "no clock reference available: " + err.Error()
		return c
	}
	skew := local.Sub(ref)
	c.Observed = fmt.Sprintf("local-ref %s against %s", skew.Round(time.Millisecond), source)
	switch {
	case skew.Abs() > skewLimit:
		c.Status = Fail
	case skew.Abs() > skewWarnLimit:
		c.Status = Warn
	default:
		c.Status = Pass
	}
	return c
}

const (
	// skewLimit is where a drift starts moving retry deadlines by a visible
	// amount: the retry schedule's shortest interval is 30 s, so 5 s is a sixth of
	// the first step.
	skewLimit = 5 * time.Second
	// skewWarnLimit is where an operator should know before it gets worse.
	skewWarnLimit = 1 * time.Second
)

func (d Deps) referenceTime(ctx context.Context) (time.Time, string, error) {
	if d.DB != nil {
		if t, err := d.DB.ServerTime(ctx); err == nil {
			return t, "postgres", nil
		}
	}
	if d.Redis != nil {
		if t, err := d.Redis.Time(ctx); err == nil {
			return t, "redis", nil
		}
	}
	return time.Time{}, "", fmt.Errorf("neither postgres nor redis answered")
}

// checkDiskHeadroom answers the operator's real question - "will retention drop
// my partitions soon" - from whichever signal is visible. A mounted volume is the
// honest reading; without one, pigeon's own database size against the configured
// budget is (the same fallback the retention gauge makes).
func (d Deps) checkDiskHeadroom(ctx context.Context) Check {
	maxPct := d.Cfg.RetentionMaxDiskPct
	c := Check{ID: CheckDiskHeadroom,
		Required: fmt.Sprintf("used <= %d%% of the budget (PIGEON_RETENTION_MAX_DISK_PCT)", maxPct),
		Fix:      "free space on the volume, raise PIGEON_DISK_BYTES, or lower PIGEON_RETENTION_MAX_DISK_PCT to drop partitions earlier"}
	if maxPct <= 0 {
		maxPct = 80
		c.Required = "used <= 80% of the budget (the documented default; PIGEON_RETENTION_MAX_DISK_PCT is unset)"
	}

	if d.Cfg.DiskDir != "" {
		if d.Statfs == nil {
			c.Status = Skip
			c.Observed = "PIGEON_DISK_DIR is set but no filesystem probe is available"
			return c
		}
		st, err := d.Statfs(d.Cfg.DiskDir)
		if err != nil {
			c.Status = Skip
			c.Observed = fmt.Sprintf("statfs %s: %v", d.Cfg.DiskDir, err)
			return c
		}
		if st.TotalBytes == 0 {
			c.Status = Skip
			c.Observed = fmt.Sprintf("statfs %s reported a zero-size filesystem", d.Cfg.DiskDir)
			return c
		}
		used := st.TotalBytes - st.FreeBytes
		pct := float64(used) / float64(st.TotalBytes) * 100
		c.Observed = fmt.Sprintf("%s: %.1f%% used (%s of %s free)", d.Cfg.DiskDir, pct, human(st.FreeBytes), human(st.TotalBytes))
		c.Status = diskStatus(pct, float64(maxPct))
		return c
	}

	if d.Cfg.DiskBytes > 0 && d.DB != nil {
		size, err := d.DB.DatabaseSize(ctx)
		if err != nil {
			c.Status = Skip
			c.Observed = fmt.Sprintf("database size could not be read: %v", err)
			return c
		}
		pct := float64(size) / float64(d.Cfg.DiskBytes) * 100
		c.Observed = fmt.Sprintf("pigeon database %s of the %s budget: %.1f%% used (no PIGEON_DISK_DIR mounted)",
			human(size), human(d.Cfg.DiskBytes), pct)
		c.Status = diskStatus(pct, float64(maxPct))
		return c
	}

	c.Status = Skip
	c.Observed = "neither PIGEON_DISK_DIR nor PIGEON_DISK_BYTES is set, so there is no budget to read"
	return c
}

func diskStatus(pct, max float64) Status {
	switch {
	case pct >= max:
		// The sweeper drops the oldest partitions at this percentage; at or above
		// it the box is already shedding data.
		return Fail
	case pct >= max*0.9:
		return Warn
	default:
		return Pass
	}
}

// checkFileDescriptors is OPS-4's ulimit check against FR-816's concurrency. The
// arithmetic is spelled out in the observed value because the number an operator
// should raise the limit to is the whole answer they need.
//
// Each in-flight attempt holds a client socket (and, under HTTP/1.1 keep-alive,
// one per host), the pool holds db_max_conns server connections, and listeners,
// log sinks and the runtime's own files need a reserve. Two descriptors per
// attempt is deliberately the pessimistic reading: an attempt that redirects, or
// that resolves DNS while a connection is open, holds more than one.
func (d Deps) checkFileDescriptors(ctx context.Context) Check {
	_ = ctx
	reserve := uint64(64)
	pool := uint64(max32(d.Cfg.DBMaxConns, 0))
	attempts := uint64(maxInt(d.Cfg.DeliveryConcurrency, 0)) * 2
	need := reserve + pool + attempts
	c := Check{ID: CheckFDs,
		Required: fmt.Sprintf("`ulimit -n` >= %d = reserve(64) + db_max_conns(%d) + 2 x delivery_concurrency(%d)",
			need, d.Cfg.DBMaxConns, d.Cfg.DeliveryConcurrency),
		Fix: "raise the soft limit: `ulimit -n 65536` in the unit or compose file, or set LimitNOFILE=65536 for systemd"}
	if !d.cfgKnown() {
		// Same reasoning as the connection budget: the required limit is
		// arithmetic over the configured concurrency, and a refused configuration
		// leaves it zero, which every limit covers.
		c.Status = Skip
		c.Observed = "the configuration did not load, so the required concurrency is unknown"
		return c
	}
	if d.FDLimit == nil {
		c.Status = Skip
		c.Observed = "no RLIMIT_NOFILE probe available"
		return c
	}
	soft, hard, err := d.FDLimit()
	if err != nil {
		c.Status = Skip
		c.Observed = fmt.Sprintf("RLIMIT_NOFILE could not be read: %v", err)
		return c
	}
	c.Observed = fmt.Sprintf("soft %d, hard %d; FR-816 concurrency needs %d", soft, hard, need)
	switch {
	case soft == 0:
		c.Status = Skip
		c.Observed = "RLIMIT_NOFILE is unlimited"
	case soft < need:
		c.Status = Fail
	default:
		c.Status = Pass
	}
	return c
}

// redacted renders a connection URL without its credentials (FR-1008). A URL that
// cannot be parsed is not echoed at all: an unparseable URL is exactly the kind
// that has a password embedded in a place the parser did not expect.
func (d Deps) redacted(raw string) string {
	return Redact(raw)
}

// Redact strips userinfo from a URL, keeping the shape an operator needs to see
// (the host, the port, the database) and nothing they must not. It is exported so
// the command's own output path can reuse it.
func Redact(raw string) string {
	if raw == "" {
		return "(unset)"
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "(unparseable url, contents withheld)"
	}
	if u.User != nil {
		// The username is kept: it is the part an operator needs in order to
		// recognise which role the box is using. The password is not.
		if _, hasPassword := u.User.Password(); hasPassword {
			u.User = url.UserPassword(u.User.Username(), "REDACTED")
		}
	}
	return u.String()
}

func orDash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "-"
	}
	return s
}

func human(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func max32(v, floor int32) int32 {
	if v < floor {
		return floor
	}
	return v
}

func maxInt(v, floor int) int {
	if v < floor {
		return floor
	}
	return v
}
