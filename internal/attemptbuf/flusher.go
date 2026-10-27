package attemptbuf

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/m7jay/pigeon/internal/config"
	"github.com/m7jay/pigeon/internal/metrics"
	"github.com/m7jay/pigeon/internal/store"
)

// Sink is the Postgres side of the flush: one call inserts one batch in ONE
// transaction (FR-815). *store.Store implements it against the batched
// SECURITY DEFINER function in 0009.
//
// It returns how many rows the transaction actually inserted; a row Postgres
// already had (a replay of the same buffered attempt after a crash) is not an
// error and is not counted, which is what makes the flush idempotent.
type Sink interface {
	RecordAttempts(ctx context.Context, batch []store.AttemptRecord) (int, error)
}

// FlusherOptions configures a Flusher. Zero values take their documented
// defaults, with AttemptFlushBatch's floor applied the same way config.Load
// applies it: a batch under config.MinFlushBatch is raised, because FR-815 is a
// floor on batching and not a target.
type FlusherOptions struct {
	// Batch is the maximum rows per transaction (FR-815: at least 2,000 at the
	// reference load). Defaults to config.MinFlushBatch.
	Batch int
	// Interval is how long an idle flusher waits between passes. It is also the
	// worst-case lag between an attempt happening and its row being visible,
	// which §6 bounds at 1 s. Defaults to 1 s.
	Interval time.Duration
	// StaleAfter is how long another consumer's pending batch must have been
	// untouched before this flusher takes it over (a peer replica that died).
	// Defaults to 30 s.
	StaleAfter time.Duration
	// FailBackoff is how long to wait after a failed insert before retrying, so a
	// database that is down does not spin. Defaults to 1 s.
	FailBackoff time.Duration
	// DrainGrace bounds the shutdown drain (D-3): the flusher keeps flushing
	// after the parent context is cancelled until the buffer is empty or this
	// elapses. Defaults to 10 s.
	DrainGrace time.Duration
	Logger     *slog.Logger
	Metrics    *metrics.Metrics
}

// Flusher drains the buffer into Postgres in batches. One Flusher per process
// (the process may push from many goroutines, but only this one reads).
type Flusher struct {
	buf     *Buffer
	sink    Sink
	opts    FlusherOptions
	log     *slog.Logger
	metrics *metrics.Metrics

	// stuck holds the batch whose insert failed, so it is retried on the next
	// pass instead of waiting for StaleAfter to reclaim it. It is NOT the
	// buffer: those entries are still pending in Redis, and this process dying
	// loses nothing (they are reclaimed on the next start). It is bounded by
	// Batch, so a database that is down applies backpressure rather than
	// accumulating memory (FR-818).
	stuck []Entry
}

// NewFlusher builds a Flusher. A nil sink is rejected: there is nothing to
// flush into, and silently discarding evidence is the one thing this path must
// not do.
func NewFlusher(buf *Buffer, sink Sink, opts FlusherOptions) (*Flusher, error) {
	if buf == nil {
		return nil, errors.New("flusher: nil buffer")
	}
	if sink == nil {
		return nil, errors.New("flusher: nil sink")
	}
	if opts.Batch < config.MinFlushBatch {
		opts.Batch = config.MinFlushBatch
	}
	if opts.Interval <= 0 {
		opts.Interval = time.Second
	}
	if opts.StaleAfter < 0 {
		opts.StaleAfter = 30 * time.Second
	}
	if opts.FailBackoff <= 0 {
		opts.FailBackoff = time.Second
	}
	if opts.DrainGrace <= 0 {
		opts.DrainGrace = 10 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Flusher{buf: buf, sink: sink, opts: opts, log: opts.Logger, metrics: opts.Metrics}, nil
}

// Run drains the buffer until ctx is cancelled, then flushes what is left before
// returning (D-3: "on SIGTERM, the flusher MUST flush before exit"). It returns
// ctx.Err().
//
// The startup pass reclaims with no idle requirement, because the entries it
// finds are exactly the ones a process that died mid-flush left pending: on a
// restart they belong to nobody else (D-1's "0 lost on worker crash").
func (f *Flusher) Run(ctx context.Context) error {
	if n, err := f.reclaim(ctx, 0); err != nil {
		if ctx.Err() == nil {
			f.log.Warn("flusher: could not reclaim a previous flusher's pending batch", "error", err)
		}
	} else if n > 0 {
		f.log.Info("flusher: recovered a pending batch left by a previous flusher", "rows", n)
	}

	for {
		if ctx.Err() != nil {
			break
		}
		n, err := f.pass(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			f.metrics.Inc(metrics.FlushErrors)
			f.log.Warn("flusher: flush pass failed; the batch stays buffered", "error", err)
			if !sleep(ctx, f.opts.FailBackoff) {
				break
			}
			continue
		}
		if n == 0 && !sleep(ctx, f.opts.Interval) {
			break
		}
	}

	// D-3: the parent context is done, so the drain runs on its own bounded
	// context. A cancelled context cannot be used to revoke the promise that the
	// buffer is flushed before exit.
	drainCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), f.opts.DrainGrace)
	defer cancel()
	n, err := f.Drain(drainCtx)
	switch {
	case err != nil:
		f.log.Error("flusher: shutdown drain did not finish; the remainder stays in Redis and is flushed on the next start",
			"flushed_rows", n, "error", err)
	case n > 0:
		f.log.Info("flusher: drained the attempt buffer on shutdown (D-3)", "rows", n)
	}
	return ctx.Err()
}

// Drain flushes until the buffer is empty or ctx expires, and returns how many
// rows it wrote. It is D-3's operation: call it with a bounded, non-cancelled
// context.
func (f *Flusher) Drain(ctx context.Context) (int, error) {
	total := 0
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		n, err := f.pass(ctx)
		if err != nil {
			return total, err
		}
		total += n
		if n == 0 {
			return total, nil
		}
	}
}

// FlushOnce performs exactly one batch: read up to Batch entries, insert them in
// one transaction, acknowledge them. It returns the number of rows Postgres
// inserted (0 when the buffer was empty). Tests drive it directly; Run loops it.
func (f *Flusher) FlushOnce(ctx context.Context) (int, error) {
	inserted, _, err := f.flushBatch(ctx)
	return inserted, err
}

// flushBatch is FlushOnce plus the number of entries the batch carried, which is
// what decides whether the backlog is drained (a short read) or another batch
// should follow immediately.
func (f *Flusher) flushBatch(ctx context.Context) (inserted, read int, err error) {
	entries, err := f.readBatch(ctx)
	if err != nil {
		return 0, 0, err
	}
	if len(entries) == 0 {
		return 0, 0, nil
	}
	n, err := f.insert(ctx, entries)
	return n, len(entries), err
}

// pass drains the backlog: it keeps flushing while the buffer keeps handing back
// full batches, and stops as soon as a read comes up short. That is what keeps
// the transaction size at or above FR-815's floor under load instead of flushing
// whatever a single poll happened to see.
func (f *Flusher) pass(ctx context.Context) (int, error) {
	total := 0
	for {
		n, read, err := f.flushBatch(ctx)
		if err != nil {
			return total, err
		}
		total += n
		if read < f.opts.Batch {
			return total, nil
		}
		if err := ctx.Err(); err != nil {
			return total, err
		}
	}
}

// reclaim takes over a pending batch nobody finished and flushes it, returning
// the rows written.
func (f *Flusher) reclaim(ctx context.Context, minIdle time.Duration) (int, error) {
	if len(f.stuck) == 0 {
		entries, err := f.buf.Reclaim(ctx, f.opts.Batch, minIdle)
		if err != nil {
			return 0, err
		}
		if len(entries) == 0 {
			return 0, nil
		}
		f.stuck = entries
	}
	return f.insert(ctx, f.stuck)
}

// readBatch returns the next batch to insert: the retry of a batch whose insert
// failed, then anything a peer flusher left pending, then new entries.
func (f *Flusher) readBatch(ctx context.Context) ([]Entry, error) {
	if len(f.stuck) > 0 {
		return f.stuck, nil
	}
	entries, err := f.buf.Reclaim(ctx, f.opts.Batch, f.opts.StaleAfter)
	if err != nil {
		return nil, err
	}
	if len(entries) > 0 {
		return entries, nil
	}
	return f.buf.Read(ctx, f.opts.Batch)
}

// insert writes one batch in one transaction and retires it from the buffer.
// The order is the whole durability argument: insert first, acknowledge second.
// A failure to insert leaves the batch pending — in Redis and in f.stuck — so it
// is retried rather than lost.
func (f *Flusher) insert(ctx context.Context, entries []Entry) (int, error) {
	recs := Records(entries)
	skipped := Unreadable(entries)

	inserted := 0
	if len(recs) > 0 {
		n, err := f.sink.RecordAttempts(ctx, recs)
		if err != nil {
			// Not acknowledged: the entries stay pending until the next attempt.
			f.stuck = entries
			return 0, fmt.Errorf("insert %d attempt rows in one transaction: %w", len(recs), err)
		}
		inserted = n
	}
	f.stuck = nil
	f.metrics.Inc(metrics.FlushBatches)
	f.metrics.Add(metrics.AttemptsFlushed, int64(inserted))
	if dup := len(recs) - inserted; dup > 0 {
		// Not lost evidence: the row was already there, which the buffer's own
		// replay after a crash produces on purpose. Still an anomaly, so it is
		// counted and logged rather than ignored (LOG-2: failures and state
		// transitions always log at warn).
		f.metrics.Add(metrics.AttemptsConflicted, int64(dup))
		f.log.Warn("flusher: buffered attempts were already in Postgres; the flush is idempotent on (delivery_id, attempt)",
			"duplicates", dup, "rows", len(recs))
	}
	if len(skipped) > 0 {
		// Retired with a warning: the payload is unreadable, so nothing can ever
		// insert it, and leaving it pending would re-read it on every pass.
		f.log.Warn("flusher: retiring unreadable buffered attempt entries; the payload does not decode",
			"entries", len(skipped), "first_id", skipped[0].ID, "raw", skipped[0].Raw)
	}
	if err := f.buf.Ack(ctx, IDs(entries)); err != nil {
		// The rows are in Postgres, so this is not a data loss: the entries stay
		// pending and are re-inserted later, which the primary key makes a no-op.
		return inserted, fmt.Errorf("retire %d flushed attempts: %w", len(entries), err)
	}
	f.depth(ctx)
	return inserted, nil
}

// depth records the buffer depth gauge. A failure to read it is not worth an
// error the caller must handle: the gauge is diagnostics, not evidence.
func (f *Flusher) depth(ctx context.Context) {
	n, err := f.buf.Depth(ctx)
	if err != nil {
		f.log.Debug("flusher: could not read the buffer depth", "error", err)
		return
	}
	f.metrics.Set(metrics.AttemptBufferPending, n)
}

// sleep waits for d or until ctx is done, reporting whether the wait completed.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
