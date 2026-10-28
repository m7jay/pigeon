package doctor

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/m7jay/pigeon/internal/config"
)

// fakeDB and fakeRedis are the probes' test doubles. Every check runs through the
// same Deps path the command uses, so nothing here re-implements a check.

type fakeDB struct {
	pingErr   error
	maxConns  int32
	maxErr    error
	serverNow time.Time
	timeErr   error
	size      uint64
	sizeErr   error
}

func (f fakeDB) Ping(context.Context) error { return f.pingErr }
func (f fakeDB) MaxConnections(context.Context) (int32, error) {
	return f.maxConns, f.maxErr
}
func (f fakeDB) ServerTime(context.Context) (time.Time, error) { return f.serverNow, f.timeErr }
func (f fakeDB) DatabaseSize(context.Context) (uint64, error)  { return f.size, f.sizeErr }

type fakeRedis struct {
	pingErr error
	aof     string
	fsync   string
	aofErr  error
	now     time.Time
	timeErr error
}

func (f fakeRedis) Ping(context.Context) error { return f.pingErr }
func (f fakeRedis) AppendOnly(context.Context) (string, string, error) {
	return f.aof, f.fsync, f.aofErr
}
func (f fakeRedis) Time(context.Context) (time.Time, error) { return f.now, f.timeErr }

// healthy builds the Deps of a box that passes every check, so each test can
// change exactly one thing and attribute the difference.
func healthy() Deps {
	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	return Deps{
		Cfg: config.Config{
			DatabaseURL:         "postgres://pigeon:pw@db:5432/pigeon",
			RedisURL:            "redis:6379",
			DBMaxConns:          10,
			CoreReplicas:        1,
			DBReservedConns:     15,
			DeliveryConcurrency: 50,
			DiskDir:             "/var/lib/pigeon",
			RetentionMaxDiskPct: 80,
		},
		DatabaseURL: "postgres://pigeon:pw@db:5432/pigeon",
		RedisURL:    "redis:6379",
		DB:          fakeDB{maxConns: 100, serverNow: now},
		Redis:       fakeRedis{aof: "yes", fsync: "everysec", now: now},
		Statfs:      func(string) (Statfs, error) { return Statfs{TotalBytes: 100e9, FreeBytes: 60e9}, nil },
		FDLimit:     func() (uint64, uint64, error) { return 65536, 65536, nil },
		Now:         func() time.Time { return now },
	}
}

func find(t *testing.T, r Report, id string) Check {
	t.Helper()
	for _, c := range r.Checks {
		if c.ID == id {
			return c
		}
	}
	t.Fatalf("no check with id %q in %v", id, ids(r))
	return Check{}
}

func ids(r Report) []string {
	out := make([]string, 0, len(r.Checks))
	for _, c := range r.Checks {
		out = append(out, c.ID)
	}
	return out
}

// TestHealthyBoxPassesAndExitsZero is the baseline: without it, every failure test
// below could be passing for the wrong reason.
func TestHealthyBoxPassesAndExitsZero(t *testing.T) {
	r := healthy().Run(context.Background())
	if code := r.ExitCode(); code != ExitOK {
		t.Fatalf("exit = %d, want 0; report:\n%s", code, strings.Join(r.Lines(), "\n"))
	}
	if !r.OK {
		t.Fatalf("report is not OK: %+v", r.Checks)
	}
	for _, want := range []string{CheckConfig, CheckDBReachable, CheckDBBudget, CheckRedisReachable,
		CheckRedisAppendOnly, CheckClockSkew, CheckDiskHeadroom, CheckFDs} {
		if c := find(t, r, want); c.Status != Pass {
			t.Errorf("%s = %s, want pass (observed %q)", want, c.Status, c.Observed)
		}
	}
}

// TestRedisWithoutAOFIsAFailure is D-5: a Redis without the AOF loses the events
// stream on restart, which is an accepted event lost (P3). The check must also
// name the fix, because "appendonly=no" alone does not tell an operator what to do.
func TestRedisWithoutAOFIsAFailure(t *testing.T) {
	d := healthy()
	d.Redis = fakeRedis{aof: "no", fsync: "everysec"}
	r := d.Run(context.Background())
	c := find(t, r, CheckRedisAppendOnly)
	if c.Status != Fail {
		t.Fatalf("redis.appendonly = %s, want fail (observed %q)", c.Status, c.Observed)
	}
	if !strings.Contains(c.Fix, "appendonly yes") {
		t.Errorf("fix does not name the setting to change: %q", c.Fix)
	}
	if r.ExitCode() != ExitFail {
		t.Errorf("exit = %d, want 1", r.ExitCode())
	}
}

// TestConnectionBudgetNamesTheArithmetic is OPS-10 to the letter: the id is
// `db.connection_budget`, the observed value IS the substituted formula, and it
// warns rather than fails (OPS-10 says warn; a budget that over-subscribes is
// degradation, not one of OPS-7's refusals).
func TestConnectionBudgetNamesTheArithmetic(t *testing.T) {
	d := healthy()
	d.Cfg.CoreReplicas = 10
	d.Cfg.DBMaxConns = 10
	d.Cfg.DBReservedConns = 15
	db := healthy().DB.(fakeDB)
	db.maxConns = 100
	d.DB = db
	r := d.Run(context.Background())
	c := find(t, r, CheckDBBudget)
	if c.Status != Warn {
		t.Fatalf("db.connection_budget = %s, want warn (observed %q)", c.Status, c.Observed)
	}
	for _, want := range []string{"core_replicas(10)", "db_max_conns(10)", "db_reserved_conns(15)", "= 115", "max_connections(100)", "over by 15"} {
		if !strings.Contains(c.Observed, want) {
			t.Errorf("observed %q does not carry %q", c.Observed, want)
		}
	}
	if r.ExitCode() != ExitWarn {
		t.Errorf("exit = %d, want 2 (warnings only)", r.ExitCode())
	}
}

// TestUlimitNamesTheConcurrencyItMustCover is OPS-4's ulimit check: the number the
// limit must reach is the answer, so it is in the required field.
func TestUlimitNamesTheConcurrencyItMustCover(t *testing.T) {
	d := healthy()
	d.FDLimit = func() (uint64, uint64, error) { return 100, 1048576, nil }
	r := d.Run(context.Background())
	c := find(t, r, CheckFDs)
	if c.Status != Fail {
		t.Fatalf("ulimit.no_file = %s, want fail (observed %q)", c.Status, c.Observed)
	}
	// 64 reserve + 10 pool + 2x50 attempts = 174.
	if !strings.Contains(c.Required, "174") || !strings.Contains(c.Required, "delivery_concurrency(50)") {
		t.Errorf("required %q does not show the arithmetic that fixes the number", c.Required)
	}
	if !strings.Contains(c.Observed, "soft 100") {
		t.Errorf("observed %q does not state the current limit", c.Observed)
	}
	if !strings.Contains(c.Fix, "ulimit -n") {
		t.Errorf("fix %q is not a runnable instruction", c.Fix)
	}
}

// TestConfigRefusalIsTheActionableMessage is AC-22: doctor on a box that will not
// boot reports the loader's refusal as a failing check with the fix, rather than
// crashing with the same error a plain start would have printed.
func TestConfigRefusalIsTheActionableMessage(t *testing.T) {
	d := healthy()
	d.CfgErr = "PIGEON_DATABASE_URL is required"
	r := d.Run(context.Background())
	c := find(t, r, CheckConfig)
	if c.Status != Fail {
		t.Fatalf("config.load = %s, want fail", c.Status)
	}
	if !strings.Contains(c.Observed, "PIGEON_DATABASE_URL") {
		t.Errorf("observed %q does not name the variable", c.Observed)
	}
	if c.Fix == "" {
		t.Error("a failing check carries no fix (AC-22 wants an actionable message)")
	}
	if r.ExitCode() != ExitFail {
		t.Errorf("exit = %d, want 1", r.ExitCode())
	}
}

// TestClockSkewFailsPastTheLimit covers OPS-4's reference comparison: this process
// against the clock that stamps its leases.
func TestClockSkewFailsPastTheLimit(t *testing.T) {
	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name   string
		drift  time.Duration
		status Status
	}{
		{"in agreement", 200 * time.Millisecond, Pass},
		{"drifting", 2 * time.Second, Warn},
		{"badly wrong", 30 * time.Second, Fail},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := healthy()
			db := healthy().DB.(fakeDB)
			db.serverNow = now.Add(-tc.drift)
			d.DB = db
			d.Now = func() time.Time { return now }
			r := d.Run(context.Background())
			if c := find(t, r, CheckClockSkew); c.Status != tc.status {
				t.Fatalf("clock.skew = %s, want %s (observed %q)", c.Status, tc.status, c.Observed)
			}
		})
	}
}

// TestDiskHeadroomReadsTheVolumeOrTheDatabase covers both readings: a mounted
// volume is the honest one, pigeon's own size against PIGEON_DISK_BYTES is the
// fallback for a split-container deployment, and neither being available is a skip
// rather than a calm zero.
func TestDiskHeadroomReadsTheVolumeOrTheDatabase(t *testing.T) {
	d := healthy()
	d.Statfs = func(string) (Statfs, error) { return Statfs{TotalBytes: 100e9, FreeBytes: 5e9}, nil }
	if c := find(t, d.Run(context.Background()), CheckDiskHeadroom); c.Status != Fail {
		t.Fatalf("95%% used = %s, want fail (observed %q)", c.Status, c.Observed)
	}

	d = healthy()
	d.Cfg.DiskDir = ""
	d.Cfg.DiskBytes = 30e9
	db := healthy().DB.(fakeDB)
	db.size = 3e9
	d.DB = db
	c := find(t, d.Run(context.Background()), CheckDiskHeadroom)
	if c.Status != Pass {
		t.Fatalf("database at 10%% of budget = %s, want pass (observed %q)", c.Status, c.Observed)
	}
	if !strings.Contains(c.Observed, "no PIGEON_DISK_DIR") {
		t.Errorf("observed %q does not say which reading it used", c.Observed)
	}

	d = healthy()
	d.Cfg.DiskDir = ""
	d.Cfg.DiskBytes = 0
	if c := find(t, d.Run(context.Background()), CheckDiskHeadroom); c.Status != Skip {
		t.Fatalf("no budget configured = %s, want skip (observed %q)", c.Status, c.Observed)
	}
}

// TestArithmeticChecksSkipWhenTheConfigurationDidNotLoad guards the most
// misleading line doctor could print: with the configuration refused, every value
// the budget and ulimit arithmetic reads is zero, so both would pass - "your
// connection budget is fine" on a box that never read its budget at all.
func TestArithmeticChecksSkipWhenTheConfigurationDidNotLoad(t *testing.T) {
	d := healthy()
	d.CfgErr = "PIGEON_DATABASE_URL is required"
	d.Cfg.DBMaxConns = 0
	d.Cfg.DeliveryConcurrency = 0
	d.Cfg.CoreReplicas = 0
	r := d.Run(context.Background())
	for _, id := range []string{CheckDBBudget, CheckFDs} {
		c := find(t, r, id)
		if c.Status != Skip {
			t.Errorf("%s = %s, want skip when the configuration is unknown (observed %q)", id, c.Status, c.Observed)
		}
	}
}

// TestAConfiguredDependencyThatWillNotAnswerIsAFailure separates the two silences
// doctor can produce. A dependency that was never configured is a skip - nothing
// was asked of it. A dependency that WAS configured and did not answer is the whole
// reason an operator runs doctor, and reporting it as a skip would bury the finding
// among lines that say "not set".
func TestAConfiguredDependencyThatWillNotAnswerIsAFailure(t *testing.T) {
	d := healthy()
	d.DB = nil
	d.DBErr = errors.New("postgres unreachable: dial tcp 10.0.0.5:5432: connect: connection refused")
	d.Redis = nil
	d.RedisErr = errors.New("broker ping: dial tcp 10.0.0.6:6379: connect: connection refused")
	d.Statfs = nil
	d.FDLimit = nil
	d.Cfg.DiskDir = ""
	d.Cfg.DiskBytes = 0

	r := d.Run(context.Background())
	for _, id := range []string{CheckDBReachable, CheckRedisReachable} {
		c := find(t, r, id)
		if c.Status != Fail {
			t.Errorf("%s = %s, want fail when the dependency is configured but silent (observed %q)", id, c.Status, c.Observed)
		}
		if !strings.Contains(c.Observed, "connection refused") {
			t.Errorf("%s observed %q does not carry the reason the operator needs", id, c.Observed)
		}
	}
	if r.ExitCode() != ExitFail {
		t.Errorf("exit = %d, want 1", r.ExitCode())
	}
}

// TestObservedValuesAreSingleLine is OPS-11's one-line contract. A driver error
// carries newlines (pgx's multi-host dial error is the one that actually happens),
// and an embedded newline would split one check's line into several - which is
// exactly the grep the format exists to support, broken silently.
func TestObservedValuesAreSingleLine(t *testing.T) {
	d := healthy()
	d.DB = nil
	d.DBErr = errors.New("postgres unreachable: failed to connect to `user=postgres database=pigeon`:\n\t127.0.0.1:5432 (localhost): dial error: connect: connection refused\n\t[::1]:5432 (localhost): dial error: connect: connection refused")
	r := d.Run(context.Background())
	for _, line := range r.Lines() {
		if strings.ContainsAny(line, "\n\r\t") {
			t.Fatalf("a rendered line carries a control character: %q", line)
		}
		if fields := strings.SplitN(line, "  ", 5); len(fields) != 5 {
			t.Fatalf("line %q does not have OPS-11's five fields", line)
		}
	}
	if c := find(t, r, CheckDBReachable); !strings.Contains(c.Observed, "127.0.0.1:5432") {
		t.Errorf("the multi-host reason was lost in normalisation: %q", c.Observed)
	}
}

// TestNothingToObserveIsExitThree is OPS-11's "could not run at all": the box is
// reachable, the configuration loads, and not one dependency answered.
func TestNothingToObserveIsExitThree(t *testing.T) {
	d := healthy()
	d.DB = nil
	d.Redis = nil
	d.Statfs = nil
	d.FDLimit = nil
	d.DatabaseURL = ""
	d.RedisURL = ""
	d.Cfg.DiskDir = ""
	d.Cfg.DiskBytes = 0
	r := d.Run(context.Background())
	if code := r.ExitCode(); code != ExitCannotRun {
		t.Fatalf("exit = %d, want 3; report:\n%s", code, strings.Join(r.Lines(), "\n"))
	}
}

// TestNoSecretAppearsInAnyRendering is FR-1008 and OPS-11's no-secrets rule. The
// URL carries a password into an observed value on a failing path, which is the
// path most likely to be pasted into a ticket.
func TestNoSecretAppearsInAnyRendering(t *testing.T) {
	const password = "sup3rs3cretpw"
	d := healthy()
	d.DatabaseURL = "postgres://pigeon:" + password + "@db:5432/pigeon"
	d.DB = fakeDB{pingErr: errors.New("dial tcp: connection refused")}
	d.DB = nil
	d.RedisURL = "redis://default:" + password + "@redis:6379"
	d.Redis = fakeRedis{pingErr: errors.New("connection refused")}

	r := d.Run(context.Background())
	rendered := strings.Join(r.Lines(), "\n")
	if strings.Contains(rendered, password) {
		t.Fatalf("the password appears in the report:\n%s", rendered)
	}
	blob, err := r.JSON()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(blob), password) {
		t.Fatalf("the password appears in the JSON report:\n%s", blob)
	}
	if !strings.Contains(rendered, "REDACTED") {
		t.Errorf("no observed value was redacted, so redaction is untested by this case:\n%s", rendered)
	}

	// The redactor itself, on the shapes a connection string actually takes.
	for _, tc := range []struct{ in, want, never string }{
		{"postgres://u:p@h/db", "postgres://u:REDACTED@h/db", "p"},
		{"redis://h:6379", "redis://h:6379", ""},
		{"not a url ://%%%", "(unparseable url, contents withheld)", ""},
		{"", "(unset)", ""},
	} {
		got := Redact(tc.in)
		if got != tc.want {
			t.Errorf("Redact(%q) = %q, want %q", tc.in, got, tc.want)
		}
		if tc.never != "" && strings.Contains(got, ":"+tc.never+"@") {
			t.Errorf("Redact(%q) = %q leaked the password", tc.in, got)
		}
	}
}

// TestJSONMatchesTheLineOutput pins OPS-11's two renderings to the same facts, so
// a consumer that parses the JSON and an operator who greps the lines cannot
// disagree about what happened.
func TestJSONMatchesTheLineOutput(t *testing.T) {
	r := healthy().Run(context.Background())
	blob, err := r.JSON()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded struct {
		OK     bool    `json:"ok"`
		Checks []Check `json:"checks"`
	}
	if err := json.Unmarshal(blob, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(decoded.Checks) != len(r.Checks) {
		t.Fatalf("JSON carries %d checks, the report has %d", len(decoded.Checks), len(r.Checks))
	}
	if !decoded.OK {
		t.Fatalf("JSON ok = false on a healthy box")
	}
	// Every line begins with the status and the id, which is what makes a grep
	// over a log the same query as a parse of the JSON.
	for i, line := range r.Lines() {
		c := r.Checks[i]
		if !strings.HasPrefix(line, string(c.Status)+"  "+c.ID+"  ") {
			t.Errorf("line %d = %q, want it to start with %q", i, line, string(c.Status)+"  "+c.ID)
		}
		fields := strings.SplitN(line, "  ", 5)
		if len(fields) != 5 {
			t.Errorf("line %d = %q, want five space-separated fields (OPS-11)", i, line)
		}
	}
}

// TestChecksAreBoundedAndOrdered covers OPS-11's 5 s per-check bound: a probe that
// never answers is a failure with the timeout as its observed value, not a hang.
func TestChecksAreBoundedAndOrdered(t *testing.T) {
	d := healthy()
	d.CheckTimeout = 50 * time.Millisecond
	blocked := make(unblockable)
	d.DB = blocked
	start := time.Now()
	r := d.Run(context.Background())
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("the run took %s: the per-check bound is not applied", elapsed)
	}
	c := find(t, r, CheckDBReachable)
	if c.Status != Fail || !strings.Contains(c.Observed, "no answer within") {
		t.Fatalf("a hanging probe = %s %q, want a failure naming the timeout", c.Status, c.Observed)
	}
	// config.load is always first: it is the check that explains every other
	// line's absence.
	if r.Checks[0].ID != CheckConfig {
		t.Fatalf("first check = %q, want %q", r.Checks[0].ID, CheckConfig)
	}
}

// unblockable is a DB probe whose Ping never returns, standing in for a host that
// accepts a connection and then stops answering.
type unblockable chan struct{}

func (u unblockable) Ping(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
func (u unblockable) MaxConnections(context.Context) (int32, error) {
	return 0, errors.New("blocked")
}
func (u unblockable) ServerTime(ctx context.Context) (time.Time, error) {
	<-ctx.Done()
	return time.Time{}, ctx.Err()
}
func (u unblockable) DatabaseSize(context.Context) (uint64, error) {
	return 0, errors.New("blocked")
}
