package retention

import (
	"context"
	"fmt"
	"time"
)

// FilesystemGauge measures the volume that holds the database's data directory.
//
// SB-3 says "the data directory", and in the reference deployment (compose.yaml)
// the core container and the postgres container sit on the same host disk, so a
// statfs of a configured directory is the honest reading. The directory is
// configurable because a split deployment may mount the volume somewhere else;
// when it is not visible at all, an operator can set Bytes to the volume size and
// the gauge falls back to the database's own size against that configured budget.
//
// DatabaseBytes comes from Postgres (pg_database_size), so it is correct even
// when the app cannot see the volume: it is how much of the disk pigeon's own data
// is using, which is what "the data directory" means for a pigeon operator.
type FilesystemGauge struct {
	// Dir is the path whose filesystem is measured. Empty disables the filesystem
	// reading and leaves only the database-size fallback.
	Dir string
	// Bytes, when non-zero, is the volume size to compare against. Zero means
	// "use the statfs total".
	Bytes uint64
	// Database reads the database's own size. Nil leaves DatabaseBytes at zero.
	Database func(ctx context.Context) (int64, error)
	// Statfs is injectable for tests; nil uses the platform implementation.
	Statfs func(path string) (used, total uint64, err error)
}

// Usage reads one snapshot. It never returns an error for "no signal": a gauge
// with neither a directory nor a database reader reports Source "none" so the
// caller can tell "nothing to measure" from "measurement failed".
func (g *FilesystemGauge) Usage(ctx context.Context) (Usage, error) {
	u := Usage{Source: "none"}

	if g.Database != nil {
		n, err := g.Database(ctx)
		if err != nil {
			return u, fmt.Errorf("retention: database size: %w", err)
		}
		u.DatabaseBytes = n
	}

	stat := g.Statfs
	if stat == nil {
		stat = diskUsage
	}
	if g.Dir != "" {
		used, total, err := stat(g.Dir)
		if err != nil {
			// The directory is not visible from this process. Fall back to the
			// database's own size against a configured budget, rather than
			// pretending the disk is empty.
			if g.Bytes > 0 {
				u.UsedBytes = uint64(max64(u.DatabaseBytes, 0))
				u.TotalBytes = g.Bytes
				u.Source = "database"
				u.UsedPct = percent(u.UsedBytes, u.TotalBytes)
				return u, nil
			}
			return u, fmt.Errorf("retention: statfs %s: %w", g.Dir, err)
		}
		u.UsedBytes = used
		u.TotalBytes = total
		u.Source = "statfs:" + g.Dir
	}

	if g.Bytes > 0 {
		u.TotalBytes = g.Bytes
		if u.UsedBytes == 0 && u.DatabaseBytes > 0 {
			u.UsedBytes = uint64(u.DatabaseBytes)
			if u.Source == "none" {
				u.Source = "database"
			}
		}
	}
	u.UsedPct = percent(u.UsedBytes, u.TotalBytes)
	return u, nil
}

func percent(used, total uint64) float64 {
	if total == 0 {
		return 0
	}
	return float64(used) / float64(total) * 100
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// FixedGauge is the test double: it returns the same reading every time. AC-18
// needs a deterministic "the disk is at 95%" without filling a real volume.
type FixedGauge struct {
	Reading Usage
	Err     error
}

// Usage implements Gauge.
func (f *FixedGauge) Usage(context.Context) (Usage, error) {
	if f.Err != nil {
		return Usage{}, f.Err
	}
	r := f.Reading
	if r.TotalBytes > 0 {
		r.UsedPct = percent(r.UsedBytes, r.TotalBytes)
	}
	return r, nil
}

// SnapshotGauge returns a sequence of readings and then repeats the last one, so a
// test can assert "the first drop brought usage back under budget".
type SnapshotGauge struct {
	Readings []Usage
	i        int
}

// Usage implements Gauge.
func (s *SnapshotGauge) Usage(context.Context) (Usage, error) {
	if len(s.Readings) == 0 {
		return Usage{Source: "none"}, nil
	}
	i := s.i
	if i >= len(s.Readings) {
		i = len(s.Readings) - 1
	}
	s.i++
	r := s.Readings[i]
	if r.TotalBytes > 0 {
		r.UsedPct = percent(r.UsedBytes, r.TotalBytes)
	}
	return r, nil
}

// Now is a convenience for tests that need a fixed clock.
func Now(t time.Time) func() time.Time { return func() time.Time { return t } }
