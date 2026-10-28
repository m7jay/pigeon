package retention

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// This file tests the policy in isolation, with no database: the Gauge and Store
// interfaces exist precisely so AC-18 ("fill disk past max_disk_pct → oldest
// partition dropped and logged; ingest sheds with 429") is deterministic without
// filling a real disk. The SQL side is covered by the store integration tests.

type fakeGauge struct {
	usage Usage
	err   error
	calls int
}

func (g *fakeGauge) Usage(context.Context) (Usage, error) {
	g.calls++
	if g.err != nil {
		return Usage{}, g.err
	}
	return g.usage, nil
}

type fakeStore struct {
	parts   map[string][]Partition
	ensured []string
	dropped []string
	// drops records every call with the reason it carried: FR-905's copy rule turns
	// on the reason, so the policy tests read it rather than inferring it.
	drops        []dropCall
	dbSize       int64
	archiveBytes int64
	archiveErr   error
}

// dropCall is one DropPartition call, with the reason it carried.
type dropCall struct {
	table  string
	name   string
	reason DropReason
}

// EnsurePartition records the call but does not mutate the catalogue: the tests
// pin exactly which partitions exist, and a real EnsurePartition is idempotent
// anyway. That keeps ListPartitions deterministic.
func (s *fakeStore) EnsurePartition(_ context.Context, table string, day time.Time) (string, error) {
	name := table + "_" + day.UTC().Format("20060102")
	s.ensured = append(s.ensured, name)
	return name, nil
}

func (s *fakeStore) ListPartitions(_ context.Context, table string) ([]Partition, error) {
	return append([]Partition(nil), s.parts[table]...), nil
}

func (s *fakeStore) DropPartition(_ context.Context, table, name string, reason DropReason) (bool, error) {
	s.dropped = append(s.dropped, name)
	s.drops = append(s.drops, dropCall{table: table, name: name, reason: reason})
	return true, nil
}

func (s *fakeStore) DatabaseSize(context.Context) (int64, error) { return s.dbSize, nil }

func (s *fakeStore) DedupeArchiveSize(context.Context) (int64, error) {
	return s.archiveBytes, s.archiveErr
}

// day is a helper for a UTC midnight partition stamp.
func day(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// pctUsage builds a reading that is pct% full on a 1000-byte volume. The small
// volume matters: the manager recomputes UsedPct from the byte counts, so these
// values are chosen to land on the intended side of the 80% boundary.
func pctUsage(pct float64) Usage {
	total := uint64(1000)
	return Usage{
		UsedBytes:  uint64(pct * 10),
		TotalBytes: total,
		UsedPct:    pct,
		Source:     "test",
	}
}

func testManager(t *testing.T, g Gauge, st Store, now time.Time, opts Options) *Manager {
	t.Helper()
	opts.Now = func() time.Time { return now }
	opts.Logger = slog.New(slog.DiscardHandler)
	return NewManager(st, g, opts)
}

func names(ps []Partition) []string {
	out := make([]string, 0, len(ps))
	for _, p := range ps {
		out = append(out, p.Name)
	}
	return out
}

func contains(hay []string, needle string) bool {
	for _, h := range hay {
		if h == needle {
			return true
		}
	}
	return false
}

// textRecords parses slog's text output into one attribute map per record, so a
// test can require a level, a message and named attributes instead of
// substring-matching a whole log. TextHandler writes `key=value` pairs and
// strconv-quotes any value with a space or punctuation in it, so the parser
// reverses exactly that.
func textRecords(out string) []map[string]string {
	var recs []map[string]string
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		rec := map[string]string{}
		for key, val, rest := nextField(line); key != ""; key, val, rest = nextField(rest) {
			rec[key] = val
		}
		recs = append(recs, rec)
	}
	return recs
}

// nextField reads one `key=value` pair off the front of s and returns the
// remainder. An unparsable tail yields an empty key, which ends textRecords'
// loop. Quoted values are unquoted before they are returned.
func nextField(s string) (key, val, rest string) {
	s = strings.TrimLeft(s, " ")
	eq := strings.IndexByte(s, '=')
	if eq <= 0 {
		return "", "", ""
	}
	key, s = s[:eq], s[eq+1:]
	if strings.HasPrefix(s, `"`) {
		end := 1
		for end < len(s) && s[end] != '"' {
			if s[end] == '\\' {
				end++
			}
			end++
		}
		if end < len(s) {
			end++ // consume the closing quote
		}
		val = s[:end]
		if unq, err := strconv.Unquote(val); err == nil {
			val = unq
		}
		return key, val, s[end:]
	}
	if sp := strings.IndexByte(s, ' '); sp >= 0 {
		return key, s[:sp], s[sp+1:]
	}
	return key, s, ""
}

// logRecord returns the first parsed record whose level and message both match.
func logRecord(recs []map[string]string, level, msg string) map[string]string {
	for _, rec := range recs {
		if rec["level"] == level && rec["msg"] == msg {
			return rec
		}
	}
	return nil
}

// TestRunOnceUnderBudgetDropsExpiredPartitionsByAge is SB-5/SB-2: below the disk
// budget, history leaves by age, whole partitions at a time.
func TestRunOnceUnderBudgetDropsExpiredPartitionsByAge(t *testing.T) {
	now := time.Date(2026, time.June, 15, 12, 0, 0, 0, time.UTC)
	st := &fakeStore{parts: map[string][]Partition{
		"deliveries": {
			{Table: "deliveries", Name: "deliveries_20260531", Day: day(2026, time.May, 31)},
			{Table: "deliveries", Name: "deliveries_20260614", Day: day(2026, time.June, 14)},
		},
		"attempts": {
			{Table: "attempts", Name: "attempts_20260607", Day: day(2026, time.June, 7)},
			{Table: "attempts", Name: "attempts_20260614", Day: day(2026, time.June, 14)},
		},
	}}
	m := testManager(t, &fakeGauge{usage: pctUsage(10)}, st, now, Options{
		MaxDiskPct: 80, DeliveriesRetention: 14 * 24 * time.Hour, AttemptsRetention: 7 * 24 * time.Hour,
	})

	rep, err := m.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if rep.OverBudget {
		t.Fatal("OverBudget = true at 10%, want false")
	}
	got := names(rep.Dropped)
	if len(got) != 2 || !contains(got, "deliveries_20260531") || !contains(got, "attempts_20260607") {
		t.Fatalf("dropped = %v, want exactly the expired 06-01 and 06-07 partitions", got)
	}
	if contains(got, "deliveries_20260614") || contains(got, "attempts_20260614") {
		t.Fatalf("dropped = %v, want the recent partitions kept", got)
	}
}

// TestRunOnceOverBudgetDropsOldestPartitionFirst is SB-3: over budget, the oldest
// partition goes first, ahead of any age-based decision.
func TestRunOnceOverBudgetDropsOldestPartitionFirst(t *testing.T) {
	now := time.Date(2026, time.June, 15, 12, 0, 0, 0, time.UTC)
	st := &fakeStore{parts: map[string][]Partition{
		"deliveries": {
			{Table: "deliveries", Name: "deliveries_20260610", Day: day(2026, time.June, 10)},
			{Table: "deliveries", Name: "deliveries_20260615", Day: day(2026, time.June, 15)},
		},
		"attempts": {
			{Table: "attempts", Name: "attempts_20260605", Day: day(2026, time.June, 5)},
		},
	}}
	m := testManager(t, &fakeGauge{usage: pctUsage(95)}, st, now, Options{
		MaxDiskPct: 80, DeliveriesRetention: 14 * 24 * time.Hour, AttemptsRetention: 7 * 24 * time.Hour,
	})

	rep, err := m.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !rep.OverBudget {
		t.Fatal("OverBudget = false at 95%, want true (SB-3)")
	}
	if got := names(rep.Dropped); len(got) != 1 || got[0] != "attempts_20260605" {
		t.Fatalf("dropped = %v, want exactly the oldest partition attempts_20260605", got)
	}
	if rep.KeptOverBudget {
		t.Fatal("KeptOverBudget = true while a droppable partition existed")
	}
}

// TestRunOnceOverBudgetNeverDropsToday is the safety rail: the partition taking
// writes this day must never be a drop candidate, even under disk pressure.
func TestRunOnceOverBudgetNeverDropsToday(t *testing.T) {
	now := time.Date(2026, time.June, 15, 12, 0, 0, 0, time.UTC)
	st := &fakeStore{parts: map[string][]Partition{
		"deliveries": {{Table: "deliveries", Name: "deliveries_20260615", Day: day(2026, time.June, 15)}},
		"attempts":   {{Table: "attempts", Name: "attempts_20260615", Day: day(2026, time.June, 15)}},
	}}
	m := testManager(t, &fakeGauge{usage: pctUsage(99)}, st, now, Options{MaxDiskPct: 80})

	rep, err := m.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(rep.Dropped) != 0 {
		t.Fatalf("dropped = %v, want nothing: only today's partition exists", names(rep.Dropped))
	}
	if !rep.KeptOverBudget {
		t.Fatal("KeptOverBudget = false, want true: budget exceeded with nothing droppable (SB-7 sheds here)")
	}
}

// TestRunOnceEnsuresTheDaysAhead is SB-1: the tick creates tomorrow's partitions
// before it drops anything, so a write never lands on a day with no partition.
func TestRunOnceEnsuresTheDaysAhead(t *testing.T) {
	now := time.Date(2026, time.June, 15, 12, 0, 0, 0, time.UTC)
	st := &fakeStore{}
	m := testManager(t, &fakeGauge{usage: pctUsage(10)}, st, now, Options{MaxDiskPct: 80, AheadDays: 2})

	if _, err := m.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	for _, want := range []string{
		"deliveries_20260615", "deliveries_20260616", "deliveries_20260617",
		"attempts_20260615", "attempts_20260616", "attempts_20260617",
		// FR-905: the archive is partitioned too, so its days are created the same
		// way. It is empty until a deliveries partition is dropped into it.
		"delivery_dedupe_20260615", "delivery_dedupe_20260616", "delivery_dedupe_20260617",
	} {
		if !contains(st.ensured, want) {
			t.Fatalf("ensured = %v, want %s among them (SB-1)", st.ensured, want)
		}
	}
	if len(st.ensured) != 9 {
		t.Fatalf("ensured %d partitions, want 9 (3 tables × today+2)", len(st.ensured))
	}
}

// TestIngestGateShedsOverBudget is SB-7: the publish gate is true at or above the
// budget and false below it.
func TestIngestGateShedsOverBudget(t *testing.T) {
	now := time.Date(2026, time.June, 15, 12, 0, 0, 0, time.UTC)
	over := testManager(t, &fakeGauge{usage: pctUsage(80)}, &fakeStore{}, now, Options{MaxDiskPct: 80})
	if !over.IngestGate(context.Background()) {
		t.Fatal("IngestGate = false at exactly the budget, want true (SB-3 says at max_disk_pct)")
	}
	under := testManager(t, &fakeGauge{usage: pctUsage(79.9)}, &fakeStore{}, now, Options{MaxDiskPct: 80})
	if under.IngestGate(context.Background()) {
		t.Fatal("IngestGate = true below the budget, want false")
	}
}

// TestStatusReportsReadErrorAfterAGoodReading is SB-6's honesty rule: a failed
// gauge read keeps the last good figure and says so, instead of reporting calm.
func TestStatusReportsReadErrorAfterAGoodReading(t *testing.T) {
	now := time.Date(2026, time.June, 15, 12, 0, 0, 0, time.UTC)
	g := &fakeGauge{usage: pctUsage(95)}
	st := &fakeStore{}
	m := testManager(t, g, st, now, Options{MaxDiskPct: 80})

	if _, err := m.Status(context.Background()); err != nil {
		t.Fatalf("first Status: %v", err)
	}
	g.err = errors.New("statfs: input/output error")
	got, err := m.Status(context.Background())
	if err != nil {
		t.Fatalf("Status after a failed read: %v, want the last good reading", err)
	}
	if got.ReadError == "" {
		t.Fatal("ReadError empty after a failed gauge read, want it surfaced")
	}
	if diff := got.Disk.UsedPct - 95; diff > 0.001 || diff < -0.001 {
		t.Fatalf("Disk.UsedPct = %v, want the last good 95", got.Disk.UsedPct)
	}
	if !got.OverBudget {
		t.Fatal("OverBudget = false, want true from the last good reading")
	}
}

// TestStatusWithoutAnyReadingIsAnError: never having had a reading is different
// from a failed one; the handler turns this into a 503.
func TestStatusWithoutAnyReadingIsAnError(t *testing.T) {
	now := time.Date(2026, time.June, 15, 12, 0, 0, 0, time.UTC)
	m := testManager(t, &fakeGauge{err: errors.New("no statfs")}, &fakeStore{}, now, Options{MaxDiskPct: 80})
	if _, err := m.Status(context.Background()); err == nil {
		t.Fatal("Status = nil error with no usable reading, want a failure")
	}
}

// TestNilGaugeIsNeverOverBudget: with no disk signal, only age retention runs and
// the ingest gate never sheds — the documented degraded mode.
func TestNilGaugeIsNeverOverBudget(t *testing.T) {
	now := time.Date(2026, time.June, 15, 12, 0, 0, 0, time.UTC)
	m := NewManager(&fakeStore{}, nil, Options{MaxDiskPct: 80, Now: func() time.Time { return now }})
	if m.OverBudget() {
		t.Fatal("OverBudget = true with no gauge, want false")
	}
	if _, err := m.Status(context.Background()); !errors.Is(err, ErrNoGauge) {
		t.Fatalf("Status error = %v, want ErrNoGauge", err)
	}
}

// TestNewManagerFillsDefaults: zero options must not mean "0% budget".
func TestNewManagerFillsDefaults(t *testing.T) {
	m := NewManager(&fakeStore{}, &fakeGauge{}, Options{})
	o := m.Options()
	if o.MaxDiskPct != 80 || o.DeliveriesRetention != 14*24*time.Hour || o.AttemptsRetention != 7*24*time.Hour {
		t.Fatalf("defaults not applied: %+v", o)
	}
	if o.DedupeRetention != 30*24*time.Hour {
		t.Fatalf("DedupeRetention = %s, want FR-905's default 30 d", o.DedupeRetention)
	}
	if m.RetentionFor("attempts") != 7*24*time.Hour || m.RetentionFor("deliveries") != 14*24*time.Hour {
		t.Fatal("RetentionFor does not map attempts/deliveries to their windows")
	}
	if m.RetentionFor("delivery_dedupe") != 30*24*time.Hour {
		t.Fatalf("RetentionFor(delivery_dedupe) = %s, want 30 d (FR-905)",
			m.RetentionFor("delivery_dedupe"))
	}
}

func TestSortPartitionsOldestFirst(t *testing.T) {
	ps := []Partition{
		{Name: "c", Day: day(2026, time.June, 3)},
		{Name: "a", Day: day(2026, time.June, 1)},
		{Name: "b", Day: day(2026, time.June, 2)},
	}
	SortPartitions(ps)
	if ps[0].Name != "a" || ps[1].Name != "b" || ps[2].Name != "c" {
		t.Fatalf("SortPartitions = %v, want a,b,c", names(ps))
	}
}

// --- FR-905: the dedupe archive, the drop order and the two loss paths -----

// TestRunOnceOverBudgetPrefersTheArchive is FR-905's drop order. The archive is
// derived from `deliveries`, so while an archive partition is droppable it goes
// first - even when a `deliveries` partition is older. Dropping the source first
// would take those keys with it and shrink the archive for nothing.
func TestRunOnceOverBudgetPrefersTheArchive(t *testing.T) {
	now := time.Date(2026, time.June, 15, 12, 0, 0, 0, time.UTC)
	st := &fakeStore{parts: map[string][]Partition{
		"deliveries": {
			{Table: "deliveries", Name: "deliveries_20260610", Day: day(2026, time.June, 10)},
		},
		"delivery_dedupe": {
			{Table: "delivery_dedupe", Name: "delivery_dedupe_20260612", Day: day(2026, time.June, 12)},
		},
	}}
	m := testManager(t, &fakeGauge{usage: pctUsage(95)}, st, now, Options{MaxDiskPct: 80})

	rep, err := m.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if got := names(rep.Dropped); len(got) != 1 || got[0] != "delivery_dedupe_20260612" {
		t.Fatalf("dropped = %v, want the archive partition delivery_dedupe_20260612 (FR-905: derived first, oldest first)", got)
	}
	if len(st.drops) != 1 || st.drops[0].reason != DropByPressure {
		t.Fatalf("drop calls = %+v, want one under DropByPressure", st.drops)
	}
}

// TestRunOnceOverBudgetFallsBackToTheSourceWhenTheArchiveIsEmpty is the other half
// of the order: with nothing droppable in the archive, the pressure drop takes the
// oldest source partition - and says why, because that drop loses dedupe answers
// (FR-903's first loss path).
func TestRunOnceOverBudgetFallsBackToTheSourceWhenTheArchiveIsEmpty(t *testing.T) {
	now := time.Date(2026, time.June, 15, 12, 0, 0, 0, time.UTC)
	st := &fakeStore{parts: map[string][]Partition{
		"deliveries": {
			{Table: "deliveries", Name: "deliveries_20260610", Day: day(2026, time.June, 10)},
		},
		"attempts": {
			{Table: "attempts", Name: "attempts_20260611", Day: day(2026, time.June, 11)},
		},
		"delivery_dedupe": {
			// Today's archive partition is not droppable whatever the pressure.
			{Table: "delivery_dedupe", Name: "delivery_dedupe_20260615", Day: day(2026, time.June, 15)},
		},
	}}
	m := testManager(t, &fakeGauge{usage: pctUsage(95)}, st, now, Options{MaxDiskPct: 80})

	rep, err := m.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if got := names(rep.Dropped); len(got) != 1 || got[0] != "deliveries_20260610" {
		t.Fatalf("dropped = %v, want the oldest source partition deliveries_20260610", got)
	}
	if st.drops[0].reason != DropByPressure {
		t.Fatalf("reason = %q, want pressure: the copy may only be skipped on the SB-3 path (FR-905)", st.drops[0].reason)
	}
}

// TestRunOnceAgeDropsTheArchiveAtDedupeRetention is FR-905's configurable archive
// retention: an archive day older than `dedupe_retention` leaves by age, like
// everything else, and the days inside the window stay.
func TestRunOnceAgeDropsTheArchiveAtDedupeRetention(t *testing.T) {
	now := time.Date(2026, time.June, 15, 12, 0, 0, 0, time.UTC)
	st := &fakeStore{parts: map[string][]Partition{
		"delivery_dedupe": {
			{Table: "delivery_dedupe", Name: "delivery_dedupe_20260514", Day: day(2026, time.May, 14)},
			{Table: "delivery_dedupe", Name: "delivery_dedupe_20260518", Day: day(2026, time.May, 18)},
			{Table: "delivery_dedupe", Name: "delivery_dedupe_20260614", Day: day(2026, time.June, 14)},
		},
	}}
	m := testManager(t, &fakeGauge{usage: pctUsage(10)}, st, now, Options{
		MaxDiskPct: 80, DedupeRetention: 30 * 24 * time.Hour,
	})

	rep, err := m.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	got := names(rep.Dropped)
	if len(got) != 1 || got[0] != "delivery_dedupe_20260514" {
		t.Fatalf("dropped = %v, want only the day older than the 30 d archive window", got)
	}
	if st.drops[0].reason != DropByAge {
		t.Fatalf("reason = %q, want age: an ordinary drop copies and loses nothing (FR-905)", st.drops[0].reason)
	}
}

// TestRunOnceReportsTheDedupeArchiveGauge is FR-905's "the archive's on-disk size
// MUST be exposed as a gauge read from pg_total_relation_size": the manager reads
// it from the store (which measures it in SQL) and reports it on every tick, which
// is where the sweeper picks it up for GET /metrics.
func TestRunOnceReportsTheDedupeArchiveGauge(t *testing.T) {
	now := time.Date(2026, time.June, 15, 12, 0, 0, 0, time.UTC)
	st := &fakeStore{archiveBytes: 12_345_678}
	m := testManager(t, &fakeGauge{usage: pctUsage(10)}, st, now, Options{MaxDiskPct: 80})

	rep, err := m.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if rep.Status.DedupeArchiveBytes != 12_345_678 {
		t.Fatalf("RunOnce reported the archive as %d bytes, want the measured 12345678",
			rep.Status.DedupeArchiveBytes)
	}
}

// TestStatusReportsTheArchiveGaugeAndItsWindow is SB-6's side of FR-905: the
// operator sees the archive's measured size and its window beside the other
// tables' - and a failed measurement says so instead of reading as an empty
// archive.
func TestStatusReportsTheArchiveGaugeAndItsWindow(t *testing.T) {
	now := time.Date(2026, time.June, 15, 12, 0, 0, 0, time.UTC)
	st := &fakeStore{
		archiveBytes: 4_321_000,
		parts: map[string][]Partition{
			"delivery_dedupe": {{Table: "delivery_dedupe", Name: "delivery_dedupe_20260614", Day: day(2026, time.June, 14)}},
		},
	}
	m := testManager(t, &fakeGauge{usage: pctUsage(10)}, st, now, Options{MaxDiskPct: 80})

	got, err := m.Status(context.Background())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if got.DedupeArchiveBytes != 4_321_000 {
		t.Fatalf("DedupeArchiveBytes = %d, want 4321000", got.DedupeArchiveBytes)
	}
	if got.RetentionDays["delivery_dedupe"] != 30 {
		t.Fatalf("retention_days[delivery_dedupe] = %d, want 30", got.RetentionDays["delivery_dedupe"])
	}
	if len(got.Partitions["delivery_dedupe"]) != 1 {
		t.Fatalf("partitions[delivery_dedupe] = %v, want the archive's day partition",
			got.Partitions["delivery_dedupe"])
	}

	// A failed measurement is reported, not dressed up as a hard zero.
	broken := &fakeStore{archiveErr: errors.New("catalogue read failed")}
	bm := testManager(t, &fakeGauge{usage: pctUsage(10)}, broken, now, Options{MaxDiskPct: 80})
	got, err = bm.Status(context.Background())
	if err != nil {
		t.Fatalf("Status after a failed archive read: %v", err)
	}
	if got.DedupeArchiveError == "" {
		t.Fatal("DedupeArchiveError empty after a failed measurement, want it surfaced beside the zeros")
	}
}

// TestPressureDropLogsTheLossAndTheDayRange is FR-903's logging requirement: each
// of the two SB-3 loss paths is logged at warn with the partition and the day range
// it covered, so a disappearing support answer is known rather than discovered.
// The two paths are distinguishable in the log by the table they name.
func TestPressureDropLogsTheLossAndTheDayRange(t *testing.T) {
	now := time.Date(2026, time.June, 15, 12, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		name  string
		table string
		part  Partition
	}{
		{"deliveries dropped before its copy", "deliveries",
			Partition{Table: "deliveries", Name: "deliveries_20260610", Day: day(2026, time.June, 10)}},
		{"archive dropped ahead of its retention", "delivery_dedupe",
			Partition{Table: "delivery_dedupe", Name: "delivery_dedupe_20260611", Day: day(2026, time.June, 11)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &fakeStore{parts: map[string][]Partition{tc.table: {tc.part}}}
			rec := &recordingHandler{}
			opts := Options{MaxDiskPct: 80, Now: func() time.Time { return now }, Logger: slog.New(rec)}
			m := NewManager(st, &fakeGauge{usage: pctUsage(95)}, opts)

			if _, err := m.RunOnce(context.Background()); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			attrs := rec.warnAttrs(t, tc.part.Name)
			wantRange := tc.part.Day.Format("2006-01-02") + "/" + tc.part.Day.AddDate(0, 0, 1).Format("2006-01-02")
			if attrs["day_range"] != wantRange {
				t.Fatalf("warn day_range = %q, want %q (FR-903: the day range it covered)", attrs["day_range"], wantRange)
			}
			if attrString(attrs, "dedupe_loss") == "" {
				t.Fatalf("warn for %s carries no dedupe_loss: this drop loses support answers (FR-903)", tc.part.Name)
			}
		})
	}

	// An `attempts` partition holds no dedupe key, so its pressure drop must not
	// claim a loss: a warn line that cries wolf is one an operator stops reading.
	st := &fakeStore{parts: map[string][]Partition{
		"attempts": {{Table: "attempts", Name: "attempts_20260610", Day: day(2026, time.June, 10)}},
	}}
	rec := &recordingHandler{}
	m := NewManager(st, &fakeGauge{usage: pctUsage(95)},
		Options{MaxDiskPct: 80, Now: func() time.Time { return now }, Logger: slog.New(rec)})
	if _, err := m.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if attrs := rec.warnAttrs(t, "attempts_20260610"); attrString(attrs, "dedupe_loss") != "" {
		t.Fatalf("an attempts drop claimed a dedupe_loss (%q); it holds no dedupe key", attrString(attrs, "dedupe_loss"))
	}
}

// attrString reads one string attribute, treating a missing key as empty (the
// manager omits dedupe_loss entirely when a drop costs no answer).
func attrString(attrs map[string]any, key string) string {
	if v, ok := attrs[key].(string); ok {
		return v
	}
	return ""
}

// recordingHandler is a slog.Handler that keeps what was logged, so the tests can
// assert on the operator-facing warn line rather than on the manager's bookkeeping.
type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
	attrs   []map[string]any
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := map[string]any{}
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.Any()
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r)
	h.attrs = append(h.attrs, attrs)
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *recordingHandler) WithGroup(string) slog.Handler { return h }

// warnAttrs returns the attributes of the warn record naming the partition, so a
// test cannot pass by matching an info line.
func (h *recordingHandler) warnAttrs(t *testing.T, partition string) map[string]any {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, r := range h.records {
		if r.Level != slog.LevelWarn || h.attrs[i]["partition"] != partition {
			continue
		}
		return h.attrs[i]
	}
	t.Fatalf("no WARN record naming %s; records: %v", partition, h.records)
	return nil
}

// TestRunOnceLogsTheDroppedPartition is AC-18's "and logged" leg: the criterion
// is "oldest partition dropped and logged", and a drop is a destructive action
// an operator has to be able to find afterwards, so the record is part of the
// criterion and not an implementation detail. Every other test in this package
// (and the sweeper's) installs slog.DiscardHandler or a discard logger, so
// without this test the two log statements in retention.go are deletable with
// the whole suite still green. The drop counter (OBS-11,
// pigeon_partition_drops_total) is a different observable and stays asserted
// where it is.
func TestRunOnceLogsTheDroppedPartition(t *testing.T) {
	now := time.Date(2026, time.June, 15, 12, 0, 0, 0, time.UTC)
	st := &fakeStore{parts: map[string][]Partition{
		"deliveries": {
			{Table: "deliveries", Name: "deliveries_20260610", Day: day(2026, time.June, 10)},
			{Table: "deliveries", Name: "deliveries_20260615", Day: day(2026, time.June, 15)},
		},
		"attempts": {
			{Table: "attempts", Name: "attempts_20260605", Day: day(2026, time.June, 5)},
		},
	}}
	var buf bytes.Buffer
	// The age windows are a year long on purpose: nothing here may leave by age,
	// so disk pressure is the only path that can produce a drop.
	m := NewManager(st, &fakeGauge{usage: pctUsage(95)}, Options{
		MaxDiskPct:          80,
		DeliveriesRetention: 365 * 24 * time.Hour,
		AttemptsRetention:   365 * 24 * time.Hour,
		Now:                 func() time.Time { return now },
		Logger:              slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})

	rep, err := m.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if got := names(rep.Dropped); len(got) != 1 || got[0] != "attempts_20260605" {
		t.Fatalf("dropped = %v, want exactly the oldest partition attempts_20260605 under disk pressure", got)
	}

	// The warning announces the decision before the drop; the info line records
	// it once the partition is gone. Both name the partition and its table, which
	// is what makes the record actionable; the exact messages are pinned so a
	// silent rewording is a failure rather than a quiet loss of the record.
	for _, want := range []struct{ level, msg string }{
		{slog.LevelWarn.String(), "retention: dropping oldest partition under disk pressure"},
		{slog.LevelInfo.String(), "retention: dropped partition"},
	} {
		rec := logRecord(textRecords(buf.String()), want.level, want.msg)
		if rec == nil {
			t.Fatalf("no %s %q record; log was:\n%s", want.level, want.msg, buf.String())
		}
		if rec["partition"] != "attempts_20260605" || rec["table"] != "attempts" {
			t.Fatalf("%s %q record names partition=%q table=%q, want attempts_20260605/attempts",
				want.level, want.msg, rec["partition"], rec["table"])
		}
	}
}
