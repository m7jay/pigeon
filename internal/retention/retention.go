// Package retention enforces the storage budget (SB-1…SB-8).
//
// The contract in one paragraph: `deliveries` and `attempts` are daily range
// partitions (SB-1); history leaves by dropping whole partitions, never by
// DELETE (SB-2); when the volume holding the data directory is above
// retention.max_disk_pct the oldest partition goes first and the drop is logged
// at warning level (SB-3); while the volume is above the budget the ingest path
// sheds load with 429 rather than accepting events it cannot keep (SB-7); and
// GET /v1/meta/usage reports the volume against the budget so pressure is
// visible before history disappears (SB-6).
//
// It is deliberately one place. The sweeper (which owns the leadership lease,
// D-7) drives RunOnce; the HTTP layer only reads Status/OverBudget. Partition DDL
// itself lives behind two SECURITY DEFINER functions in 0009_partitioning.sql,
// because the runtime role owns nothing and must not hold CREATE on the schema.
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
	DropPartition(ctx context.Context, table, name string) (bool, error)
	DatabaseSize(ctx context.Context) (int64, error)
}

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

// Tables are the partitioned tables this manager owns.
func (m *Manager) Tables() []string { return []string{"deliveries", "attempts"} }

// RetentionFor returns the configured age window per table.
func (m *Manager) RetentionFor(table string) time.Duration {
	switch table {
	case "attempts":
		return m.opts.AttemptsRetention
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
	st.RetentionDays["events"] = int(m.opts.EventsRetention / (24 * time.Hour))

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

	rep.OverBudget = u.TotalBytes > 0 && u.UsedPct >= float64(m.opts.MaxDiskPct)
	rep.Status = Status{
		Disk:              u,
		MaxDiskPct:        m.opts.MaxDiskPct,
		OverBudget:        rep.OverBudget,
		DroppedPartitions: dropped,
		CheckedAt:         now,
	}

	// 3. Disk pressure first, as SB-3 requires: the oldest partition goes before
	//    anything is dropped on age.
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
		// SB-3: log the drop at warning level.
		m.log.Warn("retention: dropping oldest partition under disk pressure",
			"partition", oldest.Name, "table", oldest.Table,
			"day", oldest.Day.Format("2006-01-02"), "bytes", oldest.Bytes,
			"used_pct", u.UsedPct, "max_disk_pct", m.opts.MaxDiskPct)
		if droppedNow, err := m.drop(ctx, oldest); err != nil {
			return rep, err
		} else if droppedNow {
			rep.Dropped = append(rep.Dropped, oldest)
		}
		return rep, nil
	}

	// 4. Under budget: enforce age. Partitions are dropped whole (SB-2), never
	//    row-by-row.
	for _, table := range m.Tables() {
		cutoff := now.Add(-m.RetentionFor(table))
		parts, err := m.store.ListPartitions(ctx, table)
		if err != nil {
			return rep, fmt.Errorf("retention: list %s partitions: %w", table, err)
		}
		for _, p := range parts {
			if !p.Day.AddDate(0, 0, 1).After(cutoff) {
				if droppedNow, err := m.drop(ctx, p); err != nil {
					return rep, err
				} else if droppedNow {
					rep.Dropped = append(rep.Dropped, p)
				}
			}
		}
	}
	return rep, nil
}

// oldestDroppable finds the oldest partition across both tables that may be
// dropped: its day must have fully elapsed (so today's and tomorrow's partitions
// — the ones receiving writes — are never candidates).
func (m *Manager) oldestDroppable(ctx context.Context, now time.Time) (Partition, bool, error) {
	today := truncDay(now)
	var oldest Partition
	found := false
	for _, table := range m.Tables() {
		parts, err := m.store.ListPartitions(ctx, table)
		if err != nil {
			return Partition{}, false, fmt.Errorf("retention: list %s partitions: %w", table, err)
		}
		for _, p := range parts {
			if !p.Day.Before(today) {
				continue
			}
			if !found || p.Day.Before(oldest.Day) {
				oldest = p
				found = true
			}
		}
	}
	return oldest, found, nil
}

// drop removes one partition and counts it. A partition a writer held is skipped
// (the SQL primitive returns false after its lock_timeout) rather than retried in
// a tight loop; the next tick tries again.
func (m *Manager) drop(ctx context.Context, p Partition) (bool, error) {
	dropped, err := m.store.DropPartition(ctx, p.Table, p.Name)
	if err != nil {
		return false, fmt.Errorf("retention: drop %s: %w", p.Name, err)
	}
	if dropped {
		m.dropped.Add(1)
		m.log.Info("retention: dropped partition", "partition", p.Name, "table", p.Table,
			"day", p.Day.Format("2006-01-02"), "bytes", p.Bytes)
	}
	return dropped, nil
}

// DroppedTotal is retention_dropped_partitions_total (OBS-11) without a scrape.
func (m *Manager) DroppedTotal() int64 { return m.dropped.Load() }

// ErrNoGauge is returned by Status when the manager has no disk signal at all;
// callers treat it as "usage unavailable" rather than a hard failure.
var ErrNoGauge = errors.New("retention: no disk gauge configured")

// truncDay returns midnight UTC of t's day. Every partition boundary in this
// package is a UTC day, matching 0009_partitioning.sql.
func truncDay(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// SortPartitions orders oldest-first; exported so tests and callers that build
// their own lists agree with the sweeper's ordering.
func SortPartitions(parts []Partition) {
	sort.Slice(parts, func(i, j int) bool { return parts[i].Day.Before(parts[j].Day) })
}
