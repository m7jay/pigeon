package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m7jay/pigeon/internal/envelope"
	"github.com/m7jay/pigeon/internal/store"
	"github.com/m7jay/pigeon/internal/writeq"
)

// This file is the relay's batch drain end to end (card `t_b9850e1b`, FR-818 as
// amended by SPEC.md A-11): one lease in, ONE fan-out transaction out, the acked
// prefix honoured when a publish fails part way through a batch, and the
// single-event path taken when one event's fan-out set is bigger than the write
// budget's batch.

// seedFanOutConsumer registers a consumer-routed type with `subs` verified
// subscribed endpoints and publishes `events` events to it, returning the
// producer, the consumer ref and the event ids.
func seedFanOutConsumer(t *testing.T, st *store.Store, admin *pgxpool.Pool, slug, typeName string, subs, events int) (string, []string) {
	t.Helper()
	ctx := context.Background()
	producerID := seedProducer(t, admin, slug)
	sc := store.Scope{ProducerID: producerID, Env: "live"}

	if _, err := st.CreateEventType(ctx, sc, store.NewEventType{
		Name: typeName, PayloadSchema: json.RawMessage(`{"type":"object"}`), RoutingMode: store.RoutingConsumer,
	}); err != nil {
		t.Fatalf("CreateEventType: %v", err)
	}
	con, err := st.CreateConsumer(ctx, sc, "ref-"+slug, "Consumer "+slug)
	if err != nil {
		t.Fatalf("CreateConsumer: %v", err)
	}
	for i := 0; i < subs; i++ {
		created, err := st.CreateEndpoint(ctx, sc, store.NewEndpoint{
			ConsumerID: con.ID, Name: fmt.Sprintf("e%02d", i),
			URL: fmt.Sprintf("https://8.8.8.%d/hook", i+1),
		})
		if err != nil {
			t.Fatalf("CreateEndpoint(%d): %v", i, err)
		}
		if err := st.VerifyEndpoint(ctx, sc, created.Endpoint.ID, created.VerificationToken); err != nil {
			t.Fatalf("VerifyEndpoint(%d): %v", i, err)
		}
		if _, err := st.CreateSubscription(ctx, sc, store.NewSubscription{
			EndpointID: created.Endpoint.ID, EventType: typeName,
		}); err != nil {
			t.Fatalf("CreateSubscription(%d): %v", i, err)
		}
	}

	ids := make([]string, events)
	for i := range ids {
		pub, err := st.PublishEvent(ctx, sc, store.PublishInput{
			EventType: typeName, Payload: json.RawMessage(`{"n":1}`), ConsumerID: &con.ID,
		})
		if err != nil {
			t.Fatalf("PublishEvent(%d): %v", i, err)
		}
		ids[i] = pub.ID
	}
	return producerID, ids
}

// countingBroker accepts publishes until its failAt-th call, which it rejects -
// a broker that goes down in the middle of a batch.
type countingBroker struct {
	calls  int
	failAt int
	got    []string
}

func (b *countingBroker) Publish(_ context.Context, ev *envelope.Envelope) error {
	b.calls++
	if b.calls == b.failAt {
		return fmt.Errorf("broker down at publish %d", b.calls)
	}
	b.got = append(b.got, ev.ID)
	return nil
}

func (b *countingBroker) Close() error { return nil }

// TestRelayDrainsABatchInOneTransaction is the whole point of the card on the
// relay's side: 40 events at 10 subscriptions each drain in ONE write-budget
// transaction (FR-815, FR-818), with every delivery row and every outbox mark
// committed by it. The loop this replaces wrote 40 transactions.
func TestRelayDrainsABatchInOneTransaction(t *testing.T) {
	st, admin := harness(t)
	const (
		subs   = 10
		events = 40
	)
	_, ids := seedFanOutConsumer(t, st, admin, "relay-batch", "signup.completed", subs, events)

	w := New(st, &recordingBroker{}, Options{Poll: 10 * time.Millisecond, BatchSize: 200})
	before := st.Writes().Stats()
	n, err := w.ProcessPending(context.Background())
	if err != nil {
		t.Fatalf("ProcessPending: %v", err)
	}
	after := st.Writes().Stats()
	if n != events {
		t.Fatalf("published %d events, want %d", n, events)
	}
	if drains := after.Drains - before.Drains; drains != 1 {
		t.Fatalf("the batch drained as %d transactions, want 1 (FR-815: one transaction per batch)", drains)
	}
	if rows := after.Rows - before.Rows; rows != int64(events*(subs+2)) {
		t.Fatalf("the batch wrote %d rows, want %d", rows, events*(subs+2))
	}
	for _, id := range ids {
		if status := outboxStatus(t, admin, id); status != "published" {
			t.Fatalf("outbox row for %s = %q, want published", id, status)
		}
		if got := countEventDeliveries(t, admin, id); got != subs {
			t.Fatalf("event %s has %d delivery rows, want %d", id, got, subs)
		}
	}
	// A second drain finds nothing: every leased row was marked by the batch.
	if n, err := w.ProcessPending(context.Background()); err != nil || n != 0 {
		t.Fatalf("second drain published %d (err %v), want 0", n, err)
	}
}

// TestRelayFanOutSplitsAPassIntoPacksAtTheRowBound is the other half of the batch
// shape: a pass whose fan-out is bigger than the write budget's row bound is
// written as several transactions that fill the bound, not one oversized submission
// (which the budget refuses) and not one transaction per event (the loop this card
// replaced). Twelve events at ten subscriptions are 144 rows into a 40-row bound:
// four transactions of a full pack each, three events per pack.
func TestRelayFanOutSplitsAPassIntoPacksAtTheRowBound(t *testing.T) {
	st, admin := harness(t)
	const (
		subs   = 10
		events = 12
	)
	_, ids := seedFanOutConsumer(t, st, admin, "relay-packs", "signup.completed", subs, events)

	// A budget of 40 rows per submission: one event's fan-out is 12 rows, so a pack
	// holds three events and one pass of twelve events is four packs.
	app := st.Pool()
	q := store.NewWriteQueue(app, writeq.Options{Depth: 4, Workers: 1, Pending: 40, Pool: 5})
	t.Cleanup(q.Close)
	small := store.New(app, store.WithWriteQueue(q))
	t.Cleanup(small.Close)

	before := q.Stats()
	w := New(small, &recordingBroker{}, Options{Poll: 10 * time.Millisecond, BatchSize: events})
	if n, err := w.ProcessPending(context.Background()); err != nil || n != events {
		t.Fatalf("drain published %d (err %v), want %d", n, err, events)
	}
	stats := q.Stats()
	wantRows := int64(events * (subs + 2))
	rows := stats.Rows - before.Rows
	if rows != wantRows {
		t.Fatalf("the drain wrote %d rows, want %d", rows, wantRows)
	}
	drains := stats.Drains - before.Drains
	if drains != 4 {
		t.Fatalf("the drain opened %d transactions for %d rows into a 40-row bound, want 4 full packs",
			drains, wantRows)
	}
	if perTx := rows / drains; perTx != 36 {
		t.Fatalf("a transaction carried %d rows on average, want a full pack of 36", perTx)
	}
	for _, id := range ids {
		if status := outboxStatus(t, admin, id); status != "published" {
			t.Fatalf("outbox row for %s = %q, want published", id, status)
		}
		if got := countEventDeliveries(t, admin, id); got != subs {
			t.Fatalf("event %s has %d delivery rows, want %d", id, got, subs)
		}
	}
}

// TestRelayBatchSizeOneDrainsPerEvent is the control half of the option's
// contract: with BatchSize 1 the drain is the loop this card replaced, one
// transaction per event.
func TestRelayBatchSizeOneDrainsPerEvent(t *testing.T) {
	st, admin := harness(t)
	_, ids := seedFanOutConsumer(t, st, admin, "relay-bsize1", "signup.completed", 1, 3)

	w := New(st, &recordingBroker{}, Options{Poll: 10 * time.Millisecond, BatchSize: 1})
	before := st.Writes().Stats()
	if n, err := w.ProcessPending(context.Background()); err != nil || n != len(ids) {
		t.Fatalf("one-row batches published %d (err %v), want %d", n, err, len(ids))
	}
	if drains := st.Writes().Stats().Drains - before.Drains; drains != int64(len(ids)) {
		t.Fatalf("BatchSize 1 drained %d transactions for %d events, want one each", drains, len(ids))
	}
}

// TestRelayBatchSizeFormsTheTransaction is the other half: BatchSize is what turns
// "one transaction per event" into "one transaction per batch".
func TestRelayBatchSizeFormsTheTransaction(t *testing.T) {
	st, admin := harness(t)
	const events = 3
	_, ids := seedFanOutConsumer(t, st, admin, "relay-bsize3", "signup.completed", 1, events)

	w := New(st, &recordingBroker{}, Options{Poll: 10 * time.Millisecond, BatchSize: events})
	before := st.Writes().Stats()
	if n, err := w.ProcessPending(context.Background()); err != nil || n != len(ids) {
		t.Fatalf("batched drain published %d (err %v), want %d", n, err, len(ids))
	}
	if drains := st.Writes().Stats().Drains - before.Drains; drains != 1 {
		t.Fatalf("BatchSize %d drained %d transactions for one full batch, want 1", events, drains)
	}
}

// TestRelayBatchHonoursTheAcknowledgedPrefix is FR-701 inside a batch: a broker
// that rejects one publish does not make the relay forget the publishes it already
// acknowledged. The prefix is fanned out and marked, the rejected row stays pending
// under its lease for the retry, and the error is still reported.
func TestRelayBatchHonoursTheAcknowledgedPrefix(t *testing.T) {
	st, admin := harness(t)
	const subs = 2
	_, ids := seedFanOutConsumer(t, st, admin, "relay-prefix", "signup.completed", subs, 3)

	brk := &countingBroker{failAt: 2}
	w := New(st, brk, Options{Lease: 30 * time.Second, Poll: 10 * time.Millisecond, BatchSize: 10})
	n, err := w.ProcessPending(context.Background())
	if err == nil {
		t.Fatal("ProcessPending returned nil despite a broker rejecting the second publish")
	}
	if n != 1 {
		t.Fatalf("published %d events, want the one the broker acknowledged", n)
	}
	if len(brk.got) != 1 || brk.got[0] != ids[0] {
		t.Fatalf("broker acknowledged %v, want [%s]", brk.got, ids[0])
	}

	// The acknowledged event is honoured end to end: marked, with its deliveries.
	if status := outboxStatus(t, admin, ids[0]); status != "published" {
		t.Fatalf("outbox row for the acknowledged event = %q, want published (FR-701)", status)
	}
	if got := countEventDeliveries(t, admin, ids[0]); got != subs {
		t.Fatalf("the acknowledged event has %d delivery rows, want %d", got, subs)
	}
	// The rest are untouched: still pending, no deliveries, and still leased - so
	// the next pass leaves them to the lease (D-6) rather than re-publishing them
	// behind a broken broker.
	for _, id := range ids[1:] {
		if status := outboxStatus(t, admin, id); status != "pending" {
			t.Fatalf("outbox row for %s = %q, want pending", id, status)
		}
		if got := countEventDeliveries(t, admin, id); got != 0 {
			t.Fatalf("event %s has %d delivery rows, want 0: it was never published", id, got)
		}
	}
	if n, err := w.ProcessPending(context.Background()); err != nil || n != 0 {
		t.Fatalf("drain behind a broken broker published %d (err %v), want 0 while the rows are leased", n, err)
	}
}

// TestRelayOversizedEventTakesItsOwnTransaction is the escape hatch: an event whose
// fan-out set alone is larger than the write budget's batch is written by the
// single-event path (one transaction, already at §8.1's floor) instead of being
// split across transactions - a split would put its outbox mark in one transaction
// and could lose rows the mark had already claimed.
func TestRelayOversizedEventTakesItsOwnTransaction(t *testing.T) {
	st, admin := harness(t)
	const subs = 10
	_, ids := seedFanOutConsumer(t, st, admin, "relay-oversize", "signup.completed", subs, 1)

	// A budget whose whole accumulation buffer is four rows: this event's fan-out
	// (10 deliveries, the routing outcome and the mark) cannot fit it.
	app := st.Pool()
	q := store.NewWriteQueue(app, writeq.Options{Depth: 4, Workers: 1, Pending: 4, Pool: 5})
	t.Cleanup(q.Close)
	small := store.New(app, store.WithWriteQueue(q))
	t.Cleanup(small.Close)

	before := q.Stats()
	w := New(small, &recordingBroker{}, Options{Poll: 10 * time.Millisecond, BatchSize: 10})
	n, err := w.ProcessPending(context.Background())
	if err != nil {
		t.Fatalf("ProcessPending: %v", err)
	}
	if n != 1 {
		t.Fatalf("published %d events, want 1", n)
	}
	if drains := q.Stats().Drains - before.Drains; drains != 0 {
		t.Fatalf("the oversized event went through the batch path (%d drains), want its own transaction", drains)
	}
	if status := outboxStatus(t, admin, ids[0]); status != "published" {
		t.Fatalf("outbox row = %q, want published", status)
	}
	if got := countEventDeliveries(t, admin, ids[0]); got != subs {
		t.Fatalf("event has %d delivery rows, want %d", got, subs)
	}
}
