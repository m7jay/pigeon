// Package relay is the process that turns accepted events into published
// envelopes (FR-701). It leases outbox rows with SKIP LOCKED (FR-702), publishes
// each to the broker, and marks the row published only after the broker acks
// (FR-701). It listens on Postgres for the fast path and polls as the durable
// fallback (D-4), so a missed notification never strands an event.
//
// The drain is a BATCH drain (§8.1's L1, FR-818 as amended by `SPEC.md` A-11): one
// iteration leases many rows, publishes them, resolves their delivery sets
// together and writes the whole fan-out - the delivery rows, the §7.5a routing
// outcomes and the outbox marks - in one transaction per pack that fills the write
// budget's row bound, through the process's write budget. Before this the loop
// leased one row per iteration and ran publish -> fan-out -> mark serially per
// event, which bounded the fan-out rate at one round-trip chain per event. On this
// project's quiet 2 vCPU box (1,000 accepted events at 10 subscriptions, the
// instance idle of ingest, the same in-process probe either side of the change)
// that was 41.5 events/s (414.9 deliveries/s) before and 496.7 events/s (4,966.8
// deliveries/s) after - a lower bound on the shape rather than a §16 graded number,
// since §16 puts the generator off the machine under test.
//
// The relay is a system worker: it has no producer scope and reaches the outbox
// only through the store's SECURITY DEFINER functions, exactly as the lease and
// mark operations require.
package relay

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m7jay/pigeon/internal/broker"
	"github.com/m7jay/pigeon/internal/envelope"
	"github.com/m7jay/pigeon/internal/metrics"
	"github.com/m7jay/pigeon/internal/store"
)

// defaultLeaseBatch is how many outbox rows one drain pass leases when the caller
// names no size: enough that a batch reaches FR-815's rows per transaction at the
// 10-subscription reference model (256 events fan out to 12 rows each, ~3,000 rows,
// so a pass fills the write budget's 2,000-row buffer instead of underfeeding it),
// and small enough that one pass holds a bounded amount of the outbox under lease.
const defaultLeaseBatch = 256

// Options configures a Worker. Zero values take their documented defaults.
type Options struct {
	// Lease is how long a claimed row is held before it is claimable again. A
	// crashed relay lets the lease lapse and another relay re-claims the row
	// (D-6, FR-809). Defaults to 30 s.
	Lease time.Duration
	// Poll   is the fallback scan interval when no notification arrives. Defaults
	// to 1 s (D-4). Zero disables polling, leaving only the listen fast path.
	Poll   time.Duration
	Logger *slog.Logger
	// Metrics records the fan-out non-delivery reasons (FR-508). A nil Metrics
	// disables recording, which the tests that care only about delivery want.
	Metrics *metrics.Metrics
	// BatchSize is how many outbox rows one drain pass leases at once: one lease
	// round trip per pass (FR-702's batch lease). The fan-out those rows resolve to
	// is written one transaction per pack that fills the write budget's row bound
	// (FR-818), so a pass is several transactions when it is bigger than the bound,
	// not one. Defaults to defaultLeaseBatch. It is a code-level default rather than
	// a configuration knob because the batch's row bound is the write budget's,
	// which is configured; this is only how much of the outbox one pass claims.
	BatchSize int
}

// Worker leases, publishes, and marks outbox rows. It is safe to construct once
// and Run for the life of the process; tests drive ProcessPending directly.
type Worker struct {
	store      *store.Store
	broker     broker.Broker
	notify     <-chan string
	lease      time.Duration
	poll       time.Duration
	leaseBatch int
	log        *slog.Logger
	metrics    *metrics.Metrics
}

// New builds a Worker. A nil logger is replaced rather than panicked on.
func New(st *store.Store, br broker.Broker, opts Options) *Worker {
	if opts.Lease <= 0 {
		opts.Lease = 30 * time.Second
	}
	if opts.Poll == 0 {
		opts.Poll = time.Second
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = defaultLeaseBatch
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Metrics == nil {
		opts.Metrics = metrics.New()
	}
	return &Worker{
		store:      st,
		broker:     br,
		lease:      opts.Lease,
		poll:       opts.Poll,
		leaseBatch: opts.BatchSize,
		log:        opts.Logger,
		metrics:    opts.Metrics,
	}
}

// SetNotifier wires the LISTEN/NOTIFY fast path (D-4). The channel is fed by a
// Postgres notification listener; a nil channel (the default) means polling
// alone, which is correct and complete, just slower to the first scan.
func (w *Worker) SetNotifier(ch <-chan string) { w.notify = ch }

// Run is the long-running loop. It drains on every notification (fast path) and
// on every poll tick (durable fallback), and exits cleanly when ctx is cancelled.
// It never returns nil on its own: the loop only ends on ctx.Done or a fatal
// lease error, which it returns so the caller can decide whether to restart.
func (w *Worker) Run(ctx context.Context) error {
	notify := w.notify
	var ticker <-chan time.Time
	if w.poll > 0 {
		t := time.NewTicker(w.poll)
		defer t.Stop()
		ticker = t.C
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-notify:
			// A closed listener channel means the subscription is gone; drop to
			// polling alone rather than spinning on the now-ready channel.
			if !ok {
				notify = nil
				continue
			}
			w.process(ctx)
		case <-ticker:
			w.process(ctx)
		}
	}
}

// process drains whatever is leaseable now, logging a warning on any failure
// rather than abandoning the loop: one bad row must not stop the relay.
func (w *Worker) process(ctx context.Context) {
	if n, err := w.ProcessPending(ctx); err != nil {
		w.log.Warn("relay: drain pass stopped", "published", n, "error", err)
	} else if n > 0 {
		w.log.Info("relay: drain pass", "published", n)
	}
}

// ProcessPending leases up to BatchSize rows at a time and delivers every
// currently-leaseable row, returning how many it published. It stops at the first
// delivery failure and returns the error, so a broker that is down does not churn
// rows: the failed row stays pending under its lease and is retried once the lease
// lapses. When the outbox is empty (or every row is still leased elsewhere) it
// returns 0, nil.
//
// The fan-out of one pass is packed into submissions that fill the write budget's
// row bound: at the reference fan-out (ten subscriptions, twelve rows per event) a
// pass of 256 rows resolves to ~3,000 rows and is written as a full pack plus the
// pack holding its remainder, so at 250 events/s the loop commits a couple of
// transactions per second rather than one per event. Carrying a pass's remainder
// into the next pass would trade that for a partial transaction at the end of every
// call (the outbox runs dry before a pack fills) and would leave BatchSize's rows
// unrelated to the transaction they are written in, so the remainder is written
// where it was produced.
//
// A publish that fails part way through a batch does not discard the batch's
// prefix: those rows were acknowledged by the broker, so they are fanned out and
// marked, and the error is returned after them. Leaving an acknowledged row
// unmarked would only re-publish it once its lease lapsed.
func (w *Worker) ProcessPending(ctx context.Context) (int, error) {
	published := 0
	for {
		rows, err := w.store.LeaseOutboxBatch(ctx, w.lease, w.leaseBatch)
		if err != nil {
			return published, err
		}
		if len(rows) == 0 {
			return published, nil
		}
		ready, publishErr := w.publish(ctx, rows)
		published += len(ready)
		if len(ready) > 0 {
			if err := w.fanOut(ctx, ready); err != nil {
				return published, err
			}
		}
		if publishErr != nil {
			return published, publishErr
		}
	}
}

// publish hands each leased row's §10 envelope to the broker, stopping at the
// first rejection. It returns the prefix the broker acknowledged and the error, so
// the caller can honour what was accepted and leave the rest to the lease.
func (w *Worker) publish(ctx context.Context, rows []store.LeasedOutbox) ([]store.LeasedOutbox, error) {
	for i := range rows {
		ev := buildEnvelope(rows[i])
		if err := w.broker.Publish(ctx, &ev); err != nil {
			w.log.Warn("relay: publish failed; row left pending for retry",
				"event_id", rows[i].EventID, "error", err)
			return rows[:i], err
		}
	}
	return rows, nil
}

// fanOut resolves a published batch to its delivery sets and writes them through
// the process's write budget (FR-818, `SPEC.md` A-11): the deliveries, the §7.5a
// routing outcomes and the outbox marks of one submission go in ONE transaction,
// so the batch - not the event - is the unit §8.1's L1 and FR-815's rows per
// transaction are stated in.
//
// A submission never carries more rows than the budget's accumulation buffer holds
// (PackFanOut splits on that bound). A single event whose own fan-out set is larger
// than the bound is left to the single-event path: at that size one transaction per
// event is already at §8.1's floor, and splitting one event across transactions is
// the only thing on this path that could lose rows, because the mark in the first
// transaction would claim deliveries the second had not written.
func (w *Worker) fanOut(ctx context.Context, rows []store.LeasedOutbox) error {
	plans, err := w.store.PlanFanOut(ctx, rows)
	if err != nil {
		return err
	}
	capacity := w.store.FanOutCapacity()
	batched := make([]store.FanOutPlan, 0, len(plans))
	for _, p := range plans {
		if p.Rows() > capacity {
			res, err := w.store.FanOutEventAndMark(ctx, p.Scope(), p.OutboxID, p.EventID)
			if err != nil {
				return err
			}
			recordFanout(w.metrics, res)
			continue
		}
		batched = append(batched, p)
	}
	for _, pack := range store.PackFanOut(batched, capacity) {
		if err := w.writeFanOut(ctx, pack); err != nil {
			return err
		}
	}
	return nil
}

// writeFanOut submits one pack and advances FR-508's reason counters only after
// the transaction that carried the rows has committed, so a counter never reports
// a fan-out the database did not take.
func (w *Worker) writeFanOut(ctx context.Context, pack []store.FanOutPlan) error {
	if len(pack) == 0 {
		return nil
	}
	if err := w.store.SubmitFanOut(ctx, store.NewRelayBatch(pack)); err != nil {
		return err
	}
	for _, p := range pack {
		recordFanout(w.metrics, p.Result())
	}
	return nil
}

// recordFanout advances one counter per distinct non-delivery reason in the
// result: the single event-level reason (unroutable, or no matching
// subscription), plus one per skipped endpoint. Each reason is its own metric
// label (FR-508, FR-509), so "an un-onboarded consumer" and "a paused endpoint"
// are separable in the counter. It is a free function so the label mapping is
// testable without a broker.
func recordFanout(m *metrics.Metrics, r store.FanOutResult) {
	if r.EventReason != "" {
		m.RecordNonDelivery(r.EventReason)
	}
	for _, s := range r.Skipped {
		m.RecordNonDelivery(s.Reason)
	}
}

// buildEnvelope renders the leased row into the §10 wire shape. The event id is
// the webhook-id (FR-1001) and is stable across the retries a crash produces, so
// a consumer that dedupes on it treats a re-published event as already seen.
func buildEnvelope(row store.LeasedOutbox) envelope.Envelope {
	return envelope.Envelope{
		ID:          row.EventID,
		Type:        row.EventType,
		Version:     fmt.Sprintf("%d.0", row.PayloadVersion),
		ProducerID:  row.ProducerID,
		ConsumerRef: row.ConsumerRef,
		CreatedAt:   row.OccurredAt,
		Data:        row.Payload,
		Class:       row.EventClass,
	}
}

// Notifier returns a channel fed by a Postgres LISTEN on the relay channel
// (D-4's fast path). It opens its own single connection with an OnNotification
// callback that pushes each payload onto the channel, and holds that connection
// for the life of the process so the subscription survives. The relay's poll
// fallback covers a dropped subscription, so a failed listen here is a
// performance loss, not a correctness one. Callers pass the result to
// Worker.SetNotifier and cancel the context to stop it.
func Notifier(ctx context.Context, url string) <-chan string {
	ch := make(chan string, 16)
	if url == "" {
		close(ch)
		return ch
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		close(ch)
		return ch
	}
	// One connection: the LISTEN subscription and the notification reader live on
	// a single socket for the process's lifetime; a pool would hand the socket to
	// a borrower and the relay would never see the notification.
	cfg.MaxConns = 1
	cfg.ConnConfig.OnNotification = func(_ *pgconn.PgConn, n *pgconn.Notification) {
		select {
		case ch <- n.Payload:
		default:
			// Notifications arriving faster than the relay drains: skip the
			// overflow and let the next poll tick catch up rather than backing
			// the channel up unbounded.
		}
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		close(ch)
		return ch
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		pool.Close()
		close(ch)
		return ch
	}
	if _, err := conn.Exec(ctx, `listen pigeon_outbox`); err != nil {
		conn.Release()
		pool.Close()
		close(ch)
		return ch
	}
	// Never release the connection: holding it keeps the LISTEN subscription and
	// its reader goroutine alive. Closing the pool on ctx.Done tears it down.
	go func() {
		<-ctx.Done()
		pool.Close()
	}()
	return ch
}
