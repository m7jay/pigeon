// Package relay is the process that turns accepted events into published
// envelopes (FR-701). It leases outbox rows with SKIP LOCKED (FR-702), publishes
// each to the broker, and marks the row published only after the broker acks
// (FR-701). It listens on Postgres for the fast path and polls as the durable
// fallback (D-4), so a missed notification never strands an event.
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
}

// Worker leases, publishes, and marks outbox rows. It is safe to construct once
// and Run for the life of the process; tests drive ProcessPending directly.
type Worker struct {
	store   *store.Store
	broker  broker.Broker
	notify  <-chan string
	lease   time.Duration
	poll    time.Duration
	log     *slog.Logger
	metrics *metrics.Metrics
}

// New builds a Worker. A nil logger is replaced rather than panicked on.
func New(st *store.Store, br broker.Broker, opts Options) *Worker {
	if opts.Lease <= 0 {
		opts.Lease = 30 * time.Second
	}
	if opts.Poll == 0 {
		opts.Poll = time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Metrics == nil {
		opts.Metrics = metrics.New()
	}
	return &Worker{
		store:   st,
		broker:  br,
		lease:   opts.Lease,
		poll:    opts.Poll,
		log:     opts.Logger,
		metrics: opts.Metrics,
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

// ProcessPending leases and delivers every currently-leaseable row once,
// returning how many it published. It stops at the first delivery failure and
// returns the error, so a broker that is down does not churn rows: the failed row
// stays pending under its lease and is retried once the lease lapses. When the
// outbox is empty (or every row is still leased elsewhere) it returns 0, nil.
func (w *Worker) ProcessPending(ctx context.Context) (int, error) {
	published := 0
	for {
		row, got, err := w.store.LeaseOutbox(ctx, w.lease)
		if err != nil {
			return published, err
		}
		if !got {
			return published, nil
		}
		if err := w.deliver(ctx, row); err != nil {
			return published, err
		}
		published++
	}
}

// deliver is one row's whole lifecycle: build the §10 envelope, publish it,
// fan the event out to its deliveries, and only then mark the row published. The
// order is the durability contract - the mark is the claim that the broker holds
// the message, so it can never precede the publish (FR-701); fan-out sits between
// because a delivery set that must be created is part of honouring the message,
// and if it fails the row stays pending to be re-leased and re-fanned-out
// (idempotent on (event_id, endpoint_id), FR-704).
func (w *Worker) deliver(ctx context.Context, row store.LeasedOutbox) error {
	ev := buildEnvelope(row)
	if err := w.broker.Publish(ctx, &ev); err != nil {
		w.log.Warn("relay: publish failed; row left pending for retry",
			"event_id", row.EventID, "error", err)
		return err
	}
	if err := w.fanOut(ctx, row); err != nil {
		w.log.Warn("relay: fan-out failed; row left pending for retry",
			"event_id", row.EventID, "error", err)
		return err
	}
	if err := w.store.MarkOutboxPublished(ctx, row.OutboxID); err != nil {
		return fmt.Errorf("relay: mark published for %s: %w", row.EventID, err)
	}
	return nil
}

// fanOut resolves the just-published event to its delivery rows (FR-703) and
// records the non-delivery reasons (FR-508). It runs inside the relay's lease
// iteration, so a crash between publish and mark re-leases and re-fans-out rather
// than losing the delivery set.
func (w *Worker) fanOut(ctx context.Context, row store.LeasedOutbox) error {
	result, err := w.store.FanOutEvent(ctx, store.Scope{ProducerID: row.ProducerID, Env: row.Env}, row.EventID)
	if err != nil {
		return err
	}
	recordFanout(w.metrics, result)
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
