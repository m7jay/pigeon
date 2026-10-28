// Package retention enforces the storage budget (SB-1…SB-8).
//
// The contract in one paragraph: `deliveries`, `attempts` and the FR-905 dedupe
// archive `delivery_dedupe` are daily range partitions (SB-1); history leaves by
// dropping whole partitions, never by DELETE (SB-2); when the volume holding the
// data directory is above retention.max_disk_pct a partition goes first - the
// archive's oldest before any delivery partition, because the archive is derived
// from it - and the drop is logged at warning level together with the dedupe
// answers it costs (FR-903/SB-3); while the volume is above the budget the ingest
// path sheds load with 429 rather than accepting events it cannot keep (SB-7);
// and GET /v1/meta/usage reports the volume against the budget, the archive's
// measured size and the live partition catalogue, so pressure is visible before
// history disappears (SB-6, FR-905).
//
// It is deliberately one place. The sweeper (which owns the leadership lease,
// D-7) drives RunOnce; the HTTP layer only reads Status/OverBudget. Partition DDL
// itself lives behind two SECURITY DEFINER functions in 0012_partitioning.sql,
// extended by 0014_dedupe_archive.sql, because the runtime role owns nothing and
// must not hold CREATE on the schema.
package retention

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Usage is one reading of the volume that holds the data directory (SB-6).
type Usage struct {
	UsedBytes     uint64  `json:"used_bytes"`
	TotalBytes    uint64  `json:"total_bytes"`
	UsedPct       float64 `json:"used_pct"`
	DatabaseBytes int64   `json:"database_bytes"`
	// Source says where the reading came from: "statfs:<dir>" for a real
	// filesystem, "database" when only the database's own size was available,
	// or "none" when neither could be read. It is in the payload so an operator
	// can tell "80% of a 40 GB volume" from "80% of what we could see".
	Source string `json:"source"`
}

// Partition is one daily leaf partition of a retained table.
type Partition struct {
	Table string    `json:"table"`
	Name  string    `json:"name"`
	Day   time.Time `json:"day"`
	Bytes int64     `json:"bytes"`
	Rows  int64     `json:"rows"`
}

// Status is what GET /v1/meta/usage returns (SB-6) and what RunOnce reports.
type Status struct {
	Disk              Usage                  `json:"disk"`
	MaxDiskPct        int                    `json:"max_disk_pct"`
	OverBudget        bool                   `json:"over_budget"`
	RetentionDays     map[string]int         `json:"retention_days"`
	Partitions        map[string][]Partition `json:"partitions"`
	DroppedPartitions int64                  `json:"dropped_partitions_total"`
	CheckedAt         time.Time              `json:"checked_at"`
	// DedupeArchiveBytes is the FR-905 gauge: the dedupe archive's on-disk size,
	// measured with pg_total_relation_size rather than computed from the pair
	// count, so an operator reads the real figure. DedupeArchiveError says the
	// reading failed, because a hard zero and "we could not measure it" must not
	// look the same on a disk-pressure dashboard.
	DedupeArchiveBytes int64  `json:"dedupe_archive_bytes"`
	DedupeArchiveError string `json:"dedupe_archive_error,omitempty"`
	// ReadError is set when the most recent gauge read failed and Disk carries
	// the last good reading. The payload stays useful, but an operator can see
	// that the number is stale instead of trusting a calm figure that stopped
	// updating.
	ReadError string `json:"read_error,omitempty"`
}

// Gauge reads the volume's usage. Implemented by *FilesystemGauge in production
// and by a fake in tests; the interface is what makes AC-18 deterministic without
// filling a real disk.
type Gauge interface {
	Usage(ctx context.Context) (Usage, error)
}

// Store is the partition catalogue the manager drives. Implemented by
// *store.Store; kept as an interface so the policy can be tested against fakes
// and so this package does not import the database layer.
type Store interface {
	EnsurePartition(ctx context.Context, table string, day time.Time) (string, error)
	ListPartitions(ctx context.Context, table string) ([]Partition, error)
	DropPartition(ctx context.Context, table, name string, reason DropReason) (bool, error)
	DatabaseSize(ctx context.Context) (int64, error)
	// DedupeArchiveSize is the FR-905 gauge: pg_total_relation_size over the
	// archive, measured by the database rather than estimated in Go.
	DedupeArchiveSize(ctx context.Context) (int64, error)
}

// DropReason is why a partition is leaving, and it is not decoration: FR-905
// makes an ordinary (age-driven) drop of a `deliveries` partition copy the day's
// dedupe keys into the archive in the same transaction as the drop, while an SB-3
// disk-pressure drop may skip the copy because the space is what is being
// reclaimed. The primitive (pigeon_drop_partition) validates the value and warns
// with the partition and the day range when the copy is skipped (FR-903).
type DropReason string

const (
	// DropByAge is the ordinary path: the day is past the configured window and
	// nothing may be lost. A `deliveries` partition is copied into
	// `delivery_dedupe` before it is dropped, in one transaction.
	DropByAge DropReason = "age"
	// DropByPressure is SB-3's disk-pressure path, which drops before the age
	// window and may lose dedupe answers. It is logged at warn (FR-903).
	DropByPressure DropReason = "pressure"
)

// Options are the policy knobs. Defaults are SB-5's conservative, volume-aware
// values; config.Load reads them from the environment (FR-1104: retention is
// configuration, not a migration).
type Options struct {
	// MaxDiskPct is SB-3's budget: at or above this percentage of the volume the
	// sweeper drops the oldest partition before dropping on age. Defaults to 80.
	MaxDiskPct int
	// DeliveriesRetention and AttemptsRetention are SB-5's age defaults.
	DeliveriesRetention time.Duration
	AttemptsRetention   time.Duration
	// DedupeRetention is FR-905's archive window (default 30 d). It is what the
	// 30-day dedupe answer rides on: the archive holds the days between the
	// deliveries window and this one, so shortening it shortens FR-903's answer
	// just like shortening DeliveriesRetention does.
	DedupeRetention time.Duration
	// EventsRetention is reported in /v1/meta/usage (SB-5). Events is not
	// partitioned yet, so enforcement is deferred and documented; the default is
	// still surfaced so the operator sees the intended window.
	EventsRetention time.Duration
	// AheadDays is how many future day partitions are kept ready. Two is enough
	// for a sweeper that ticks every few seconds, with margin for a missed tick.
	AheadDays int
	// CacheTTL bounds how often Status/OverBudget re-reads the gauge. The sweeper
	// refreshes uncached on every tick; the request path uses the cache.
	CacheTTL time.Duration
	Now      func() time.Time
	Logger   *slog.Logger
}

// Defaults returns SB-5's conservative defaults.
func Defaults() Options {
	return Options{
		MaxDiskPct:          80,
		DeliveriesRetention: 14 * 24 * time.Hour,
		AttemptsRetention:   7 * 24 * time.Hour,
		DedupeRetention:     30 * 24 * time.Hour,
		EventsRetention:     14 * 24 * time.Hour,
		AheadDays:           2,
		CacheTTL:            5 * time.Second,
		Now:                 time.Now,
	}
}

// Manager applies the retention policy. It is safe for concurrent use: the
// sweeper runs RunOnce while HTTP readers call Status/OverBudget.
type Manager struct {
	store Store
	gauge Gauge
	opts  Options
	log   *slog.Logger

	mu      sync.Mutex
	usage   Usage
	checked time.Time
	over    bool
	readErr error // last gauge read failure, cleared by the next good read
	dropped atomic.Int64
}

// NewManager builds a Manager. A nil gauge means "no disk signal": only age
// retention runs and OverBudget is always false. A nil logger is replaced.
func NewManager(st Store, g Gauge, opts Options) *Manager {
	def := Defaults()
	if opts.MaxDiskPct <= 0 || opts.MaxDiskPct > 100 {
		opts.MaxDiskPct = def.MaxDiskPct
	}
	if opts.DeliveriesRetention <= 0 {
		opts.DeliveriesRetention = def.DeliveriesRetention
	}
	if opts.AttemptsRetention <= 0 {
		opts.AttemptsRetention = def.AttemptsRetention
	}
	if opts.DedupeRetention <= 0 {
		opts.DedupeRetention = def.DedupeRetention
	}
	if opts.EventsRetention <= 0 {
		opts.EventsRetention = def.EventsRetention
	}
	if opts.AheadDays <= 0 {
		opts.AheadDays = def.AheadDays
	}
	if opts.CacheTTL <= 0 {
		opts.CacheTTL = def.CacheTTL
	}
	if opts.Now == nil {
		opts.Now = def.Now
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Manager{store: st, gauge: g, opts: opts, log: opts.Logger}
}

// Options returns the resolved policy, for the usage payload and tests.
func (m *Manager) Options() Options { return m.opts }

// Tables are the partitioned tables this manager owns, in the order they are
// listed and have their days created. `delivery_dedupe` is FR-905's archive: it is
// derived from `deliveries`, which is why the disk-pressure path drops its
// partitions first (see oldestDroppable) and why its age window is the longer one.
func (m *Manager) Tables() []string { return []string{"deliveries", "attempts", "delivery_dedupe"} }

// RetentionFor returns the configured age window per table.
func (m *Manager) RetentionFor(table string) time.Duration {
	switch table {
	case "attempts":
		return m.opts.AttemptsRetention
	case "delivery_dedupe":
		return m.opts.DedupeRetention
	default:
		return m.opts.DeliveriesRetention
	}
}

// refresh reads the gauge and updates the cached budget state. Callers hold m.mu.
func (m *Manager) refreshLocked(ctx context.Context) Usage {
	u := Usage{Source: "none"}
	if m.gauge != nil {
		got, err := m.gauge.Usage(ctx)
		if err == nil {
			u = got
			m.readErr = nil
		} else {
			m.readErr = err
			m.log.Warn("retention: disk usage read failed", "error", err)
			// Keep the last good volume reading rather than reporting "0% of an
			// unknown volume": an operator who has seen 95% must not be shown a
			// calm 0% just because one statfs call failed. Status still surfaces
			// the failure in ReadError.
			if m.usage.TotalBytes > 0 {
				u = m.usage
			}
		}
	}
	if m.store != nil {
		if n, err := m.store.DatabaseSize(ctx); err == nil {
			u.DatabaseBytes = n
		}
	}
	if u.TotalBytes > 0 {
		u.UsedPct = float64(u.UsedBytes) / float64(u.TotalBytes) * 100
	}
	m.usage = u
	m.checked = m.opts.Now()
	m.over = u.TotalBytes > 0 && u.UsedPct >= float64(m.opts.MaxDiskPct)
	return u
}

// Refresh re-reads the gauge when the cache is older than CacheTTL. The sweeper
// calls RunOnce (which forces a read); the request path uses this cheap form.
func (m *Manager) Refresh(ctx context.Context) Usage {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.opts.Now().Sub(m.checked) < m.opts.CacheTTL {
		return m.usage
	}
	return m.refreshLocked(ctx)
}

// OverBudget is SB-7's ingest gate: true while the volume is at or above the
// configured budget, so the publish path sheds with 429. It reads the cached
// reading; IngestGate is the refreshing form the request path uses.
func (m *Manager) OverBudget() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.over
}

// IngestGate is SB-7's publish gate: it refreshes the cached volume reading at
// most once per CacheTTL and reports whether publish should shed load. A 429 on
// the publish path is deliberate load shedding, not an outage; the alternative
// is accepting events the platform cannot keep (SB-7).
func (m *Manager) IngestGate(ctx context.Context) bool {
	m.Refresh(ctx)
	return m.OverBudget()
}

// Status assembles SB-6's payload: the volume against the budget, the configured
// windows and each table's live partitions with their sizes.
func (m *Manager) Status(ctx context.Context) (Status, error) {
	m.mu.Lock()
	u := m.refreshLocked(ctx)
	dropped := m.dropped.Load()
	m.mu.Unlock()

	st := Status{
		Disk:              u,
		MaxDiskPct:        m.opts.MaxDiskPct,
		OverBudget:        u.TotalBytes > 0 && u.UsedPct >= float64(m.opts.MaxDiskPct),
		RetentionDays:     map[string]int{},
		Partitions:        map[string][]Partition{},
		DroppedPartitions: dropped,
		CheckedAt:         m.opts.Now().UTC(),
	}
	if m.readErr != nil {
		st.ReadError = m.readErr.Error()
	}
	st.RetentionDays["deliveries"] = int(m.opts.DeliveriesRetention / (24 * time.Hour))
	st.RetentionDays["attempts"] = int(m.opts.AttemptsRetention / (24 * time.Hour))
	st.RetentionDays["delivery_dedupe"] = int(m.opts.DedupeRetention / (24 * time.Hour))
	st.RetentionDays["events"] = int(m.opts.EventsRetention / (24 * time.Hour))

	st.DedupeArchiveBytes, st.DedupeArchiveError = m.archiveBytes(ctx)

	for _, table := range m.Tables() {
		parts, err := m.store.ListPartitions(ctx, table)
		if err != nil {
			return st, fmt.Errorf("retention: list %s partitions: %w", table, err)
		}
		st.Partitions[table] = parts
	}

	// An error here means "there has never been a usable volume reading", not
	// "the last read failed": refreshLocked keeps the previous reading and
	// records the failure in ReadError, so Status stays useful. Only when there
	// is no reading at all does the handler turn this into a 503, which is why
	// the distinction matters (SB-6: the endpoint must not look calm when it
	// cannot tell).
	if u.TotalBytes == 0 {
		if m.readErr != nil {
			return st, m.readErr
		}
		if m.gauge == nil {
			return st, ErrNoGauge
		}
	}
	return st, nil
}

// Report is the outcome of one sweep tick.
type Report struct {
	Status     Status
	Dropped    []Partition
	OverBudget bool
	// KeptOverBudget is true when the budget is exceeded but nothing may be
	// dropped any more (only today's partition remains). That is the point where
	// the platform is shedding ingest and out of history to give, so the caller
	// logs it at error level rather than warning.
	KeptOverBudget bool
}

// RunOnce performs one retention tick: create the partitions the next days need,
// read the volume, then drop partitions — oldest first under disk pressure, by
// age otherwise. It drops at most one partition per call when the volume is over
// budget so a burst of drops never runs long in one statement; age-based drops
// remove every expired partition in one tick.
//
// The sweeper calls this while it holds the leadership lease (D-7); the SQL drop
// primitive additionally takes its own advisory lock so a bug cannot drop two
// partitions concurrently even within one process.
func (m *Manager) RunOnce(ctx context.Context) (Report, error) {
	now := m.opts.Now().UTC()
	rep := Report{}

	// 1. Make sure the next days have somewhere to land before dropping anything.
	for _, table := range m.Tables() {
		for i := 0; i <= m.opts.AheadDays; i++ {
			day := truncDay(now).AddDate(0, 0, i)
			if _, err := m.store.EnsurePartition(ctx, table, day); err != nil {
				return rep, fmt.Errorf("retention: ensure %s partition for %s: %w", table, day.Format("2006-01-02"), err)
			}
		}
	}

	// 2. Read the volume, uncached: a tick is the moment the decision matters.
	m.mu.Lock()
	u := m.refreshLocked(ctx)
	dropped := m.dropped.Load()
	m.mu.Unlock()

	size, sizeErr := m.archiveBytes(ctx)
	rep.OverBudget = u.TotalBytes > 0 && u.UsedPct >= float64(m.opts.MaxDiskPct)
	rep.Status = Status{
		Disk:               u,
		MaxDiskPct:         m.opts.MaxDiskPct,
		OverBudget:         rep.OverBudget,
		DroppedPartitions:  dropped,
		CheckedAt:          now,
		DedupeArchiveBytes: size,
		DedupeArchiveError: sizeErr,
	}

	// 3. Disk pressure first, as SB-3 requires: a partition goes before anything
	//    is dropped on age.
	if rep.OverBudget {
		oldest, ok, err := m.oldestDroppable(ctx, now)
		if err != nil {
			return rep, err
		}
		if !ok {
			rep.KeptOverBudget = true
			m.log.Error("retention: over disk budget but no partition may be dropped; ingest is shedding",
				"used_pct", u.UsedPct, "max_disk_pct", m.opts.MaxDiskPct, "database_bytes", u.DatabaseBytes)
			return rep, nil
		}
		// SB-3: log the drop at warning level, with the day range it covers and,
		// when the drop can cost a dedupe answer, which of FR-903's two loss paths
		// this is. A support answer that is gone must be known, not discovered.
		attrs := []any{
			"partition", oldest.Name, "table", oldest.Table,
			"day", oldest.Day.Format("2006-01-02"), "day_range", dayRange(oldest),
			"bytes", oldest.Bytes,
			"used_pct", u.UsedPct, "max_disk_pct", m.opts.MaxDiskPct,
		}
		if loss := dedupeLoss(oldest.Table); loss != "" {
			attrs = append(attrs, "dedupe_loss", loss)
		}
		m.log.Warn("retention: dropping oldest partition under disk pressure", attrs...)
		if droppedNow, err := m.drop(ctx, oldest, DropByPressure); err != nil {
			return rep, err
		} else if droppedNow {
			rep.Dropped = append(rep.Dropped, oldest)
		}
		rep.refreshArchiveGauge(ctx, m)
		return rep, nil
	}

	// 4. Under budget: enforce age. Partitions are dropped whole (SB-2), never
	//    row-by-row. A `deliveries` partition leaving here is copied into the
	//    dedupe archive by the drop primitive in the same transaction (FR-905).
	for _, table := range m.Tables() {
		cutoff := now.Add(-m.RetentionFor(table))
		parts, err := m.store.ListPartitions(ctx, table)
		if err != nil {
			return rep, fmt.Errorf("retention: list %s partitions: %w", table, err)
		}
		for _, p := range parts {
			if !p.Day.AddDate(0, 0, 1).After(cutoff) {
				if droppedNow, err := m.drop(ctx, p, DropByAge); err != nil {
					return rep, err
				} else if droppedNow {
					rep.Dropped = append(rep.Dropped, p)
				}
			}
		}
	}
	// FR-905's gauge is re-read after the drops: the tick that copies a day into
	// the archive (or drops archive days) is the tick the operator's number has to
	// move on, not the next one. The reading above is what an operator sees when
	// the tick decides nothing - it is the same measurement, taken earlier.
	rep.refreshArchiveGauge(ctx, m)
	return rep, nil
}

// refreshArchiveGauge re-reads the archive's measured size into the report, so
// whatever the tick dropped is reflected in the number the sweeper exports to
// GET /metrics on the same tick (FR-905).
func (r *Report) refreshArchiveGauge(ctx context.Context, m *Manager) {
	r.Status.DedupeArchiveBytes, r.Status.DedupeArchiveError = m.archiveBytes(ctx)
}

// archiveBytes reads FR-905's archive gauge. A failed read is reported rather than
// hidden: the caller puts the error in the payload, because 0 bytes and "we could
// not measure it" must not read the same on a disk-pressure dashboard.
func (m *Manager) archiveBytes(ctx context.Context) (int64, string) {
	if m.store == nil {
		return 0, ""
	}
	n, err := m.store.DedupeArchiveSize(ctx)
	if err != nil {
		m.log.Warn("retention: reading the dedupe archive size failed", "error", err)
		return 0, err.Error()
	}
	return n, ""
}

// dayRange is the half-open UTC day range a daily partition covers, for the warn
// logs FR-903 requires ("the partition and the day range it covered").
func dayRange(p Partition) string {
	start := p.Day.UTC().Format("2006-01-02")
	end := p.Day.UTC().AddDate(0, 0, 1).Format("2006-01-02")
	return start + "/" + end
}

// dedupeLoss names which of FR-903's two dedupe loss paths a partition drop takes,
// or "" when the drop loses no dedupe answer at all. Only SB-3 disk pressure
// reaches it: an age-driven drop copies before it drops (FR-905) and a pressure
// drop of `attempts` holds no dedupe key.
func dedupeLoss(table string) string {
	switch table {
	case "deliveries":
		return "deliveries partition dropped before its FR-905 copy: every dedupe key it held is no longer answerable (FR-903)"
	case "delivery_dedupe":
		return "archive partition dropped ahead of dedupe_retention: those archived dedupe days are gone whole (FR-903)"
	default:
		return ""
	}
}

// oldestDroppable finds the partition a pressure drop should take: oldest first,
// to a day that has fully elapsed (so today's and tomorrow's partitions - the ones
// receiving writes - are never candidates).
//
// The archive is derived from `deliveries`, so FR-905 orders it first: while any
// archive partition is droppable, none of its source tables is touched. Only when
// the archive has nothing left to give does the drop fall back to the oldest
// partition of the retained tables - which is the second of FR-903's loss paths,
// because that `deliveries` partition leaves without its copy.
func (m *Manager) oldestDroppable(ctx context.Context, now time.Time) (Partition, bool, error) {
	today := truncDay(now)

	// Derived first (FR-905).
	archive, err := m.store.ListPartitions(ctx, "delivery_dedupe")
	if err != nil {
		return Partition{}, false, fmt.Errorf("retention: list delivery_dedupe partitions: %w", err)
	}
	if oldest, ok := oldestBefore(archive, today); ok {
		return oldest, true, nil
	}

	var oldest Partition
	found := false
	for _, table := range m.Tables() {
		if table == "delivery_dedupe" {
			continue
		}
		parts, err := m.store.ListPartitions(ctx, table)
		if err != nil {
			return Partition{}, false, fmt.Errorf("retention: list %s partitions: %w", table, err)
		}
		if p, ok := oldestBefore(parts, today); ok && (!found || p.Day.Before(oldest.Day)) {
			oldest = p
			found = true
		}
	}
	return oldest, found, nil
}

// oldestBefore returns the oldest partition that ended before today.
func oldestBefore(parts []Partition, today time.Time) (Partition, bool) {
	var oldest Partition
	found := false
	for _, p := range parts {
		if !p.Day.Before(today) {
			continue
		}
		if !found || p.Day.Before(oldest.Day) {
			oldest = p
			found = true
		}
	}
	return oldest, found
}

// drop removes one partition, saying why, and counts it. A partition a writer held
// is skipped (the SQL primitive returns false after its lock_timeout) rather than
// retried in a tight loop; the next tick tries again.
func (m *Manager) drop(ctx context.Context, p Partition, reason DropReason) (bool, error) {
	dropped, err := m.store.DropPartition(ctx, p.Table, p.Name, reason)
	if err != nil {
		return false, fmt.Errorf("retention: drop %s: %w", p.Name, err)
	}
	if dropped {
		m.dropped.Add(1)
		attrs := []any{"partition", p.Name, "table", p.Table,
			"day", p.Day.Format("2006-01-02"), "day_range", dayRange(p), "bytes", p.Bytes}
		if reason == DropByPressure {
			attrs = append(attrs, "reason", string(reason))
		}
		m.log.Info("retention: dropped partition", attrs...)
	}
	return dropped, nil
}

// DroppedTotal is retention_dropped_partitions_total (OBS-11) without a scrape.
func (m *Manager) DroppedTotal() int64 { return m.dropped.Load() }

// ErrNoGauge is returned by Status when the manager has no disk signal at all;
// callers treat it as "usage unavailable" rather than a hard failure.
var ErrNoGauge = errors.New("retention: no disk gauge configured")

// truncDay returns midnight UTC of t's day. Every partition boundary in this
// package is a UTC day, matching 0012_partitioning.sql.
func truncDay(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// SortPartitions orders oldest-first; exported so tests and callers that build
// their own lists agree with the sweeper's ordering.
func SortPartitions(parts []Partition) {
	sort.Slice(parts, func(i, j int) bool { return parts[i].Day.Before(parts[j].Day) })
}
