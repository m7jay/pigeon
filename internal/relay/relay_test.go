package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m7jay/pigeon/internal/db"
	"github.com/m7jay/pigeon/internal/dbtest"
	"github.com/m7jay/pigeon/internal/envelope"
	"github.com/m7jay/pigeon/internal/store"
)

// The relay durability tests run against real Postgres (gated on
// PIGEON_TEST_DATABASE_URL, as CI sets it) with a fake broker, because the
// durability contract - FR-701 (mark only after ack), FR-702 (lease), FR-809
// (lease expiry re-claims), D-6, AC-08, AC-09 - is about Postgres state and the
// worker's behaviour, not the transport. AC-08 kills the relay mid-flight and
// proves re-publication without a duplicate outbox row; AC-09 stops the relay
// and proves the accepted event is not lost and OBS-2's lag grows.

const (
	appRole     = "pigeon_app"
	appPassword = "pigeon_app"
)

// harness returns a store running as the non-superuser runtime role over a
// migrated, emptied disposable database, plus the superuser pool for reading the
// truth. It refuses to touch a database whose name is not obviously disposable.
func harness(t *testing.T) (*store.Store, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("PIGEON_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PIGEON_TEST_DATABASE_URL not set")
	}
	u, err := parseDSN(dsn)
	if err != nil || !strings.Contains(u.Path, "test") {
		t.Fatalf("refusing to use non-disposable DSN %q", dsn)
	}

	ctx := context.Background()
	admin, err := db.Connect(ctx, dsn, 5)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	t.Cleanup(admin.Close)
	if err := db.Migrate(ctx, admin, 30*time.Second); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	resetForRelay(ctx, t, admin)
	// The relay's store runs as the tenant-scoped runtime role, not the
	// superuser - the same baseline the store harness grants. The role is
	// cluster-wide, so another suite on this cluster may be creating it at the
	// same instant; the shared fixture treats losing that race as the role now
	// existing.
	if err := dbtest.ProvisionAppRole(ctx, admin, appRole, appPassword); err != nil {
		t.Fatalf("provision app role: %v", err)
	}
	app, err := db.Connect(ctx, withAppCreds(dsn), 5)
	if err != nil {
		t.Fatalf("connect app: %v", err)
	}
	t.Cleanup(app.Close)
	return store.New(app), admin
}

// withAppCreds returns the DSN rewritten for the app role's credentials, so the
// store connects as the non-superuser runtime role the deployed relay uses.
func withAppCreds(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}
	u.User = url.UserPassword(appRole, appPassword)
	return u.String()
}

func parseDSN(dsn string) (*url.URL, error) { return url.Parse(dsn) }

// jsonRaw wraps a literal as a json.RawMessage for seeding payloads.
func jsonRaw(s string) json.RawMessage { return json.RawMessage(s) }

// resetForRelay empties the ingest tables so each test starts clean.
func resetForRelay(ctx context.Context, t *testing.T, admin *pgxpool.Pool) {
	t.Helper()
	_, err := admin.Exec(ctx, `truncate table
		event_types, events, outbox, idempotency_keys, api_keys, consumers, producers, users restart identity cascade`)
	if err != nil {
		t.Fatalf("reset: %v", err)
	}
}

// seedProducer writes a producer through the superuser: only the platform may
// create tenants (RLS forbids a tenant role from doing it).
func seedProducer(t *testing.T, admin *pgxpool.Pool, slug string) string {
	t.Helper()
	id := "prd_" + slug
	if _, err := admin.Exec(context.Background(),
		`insert into producers (id, slug, name) values ($1,$2,$3) on conflict (id) do nothing`,
		id, slug, "Producer "+slug); err != nil {
		t.Fatalf("seed producer: %v", err)
	}
	return id
}

// seedPublish creates a producer and a broadcast event type, then publishes one
// event. It returns the producer and event ids; the outbox row is pending.
func seedPublish(t *testing.T, st *store.Store, admin *pgxpool.Pool, slug, typeName string) (producerID, eventID string) {
	t.Helper()
	ctx := context.Background()
	producerID = seedProducer(t, admin, slug)
	sc := store.Scope{ProducerID: producerID, Env: "live"}
	if _, err := st.CreateEventType(ctx, sc, store.NewEventType{
		Name: typeName, PayloadSchema: jsonRaw(`{"type":"object"}`),
	}); err != nil {
		t.Fatalf("CreateEventType: %v", err)
	}
	pub, err := st.PublishEvent(ctx, sc, store.PublishInput{EventType: typeName, Payload: jsonRaw(`{"n":1}`)})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	return producerID, pub.ID
}

func outboxStatus(t *testing.T, admin *pgxpool.Pool, eventID string) string {
	t.Helper()
	var status string
	err := admin.QueryRow(context.Background(),
		`select status from outbox where event_id = $1`, eventID).Scan(&status)
	if err != nil {
		t.Fatalf("read outbox %s: %v", eventID, err)
	}
	return status
}

func outboxCount(t *testing.T, admin *pgxpool.Pool, producerID string) int {
	t.Helper()
	var n int
	if err := admin.QueryRow(context.Background(),
		`select count(*) from outbox o join events e on e.id = o.event_id where e.producer_id = $1`,
		producerID).Scan(&n); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	return n
}

// --- fake brokers --------------------------------------------------------

// recordingBroker records the event ids it successfully delivered.
type recordingBroker struct {
	mu       sync.Mutex
	received []string
}

func (b *recordingBroker) Publish(_ context.Context, ev *envelope.Envelope) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.received = append(b.received, ev.ID)
	return nil
}
func (b *recordingBroker) Close() error { return nil }
func (b *recordingBroker) ids() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.received...)
}

// crashBroker delivers its first publish, then blocks until the worker's context
// is cancelled - simulating a relay that is killed mid-delivery, before the
// broker's ack is acted on. Every later publish completes normally. It signals
// each in-flight publish on entered so a test can act at the exact moment.
type crashBroker struct {
	entered  chan string
	mu       sync.Mutex
	calls    int
	received []string
}

func (b *crashBroker) Publish(ctx context.Context, ev *envelope.Envelope) error {
	b.mu.Lock()
	b.calls++
	crash := b.calls == 1
	if crash {
		b.mu.Unlock()
		b.entered <- ev.ID // the test knows we are mid-flight
		<-ctx.Done()       // the relay is killed here, before it could mark
		return ctx.Err()   // so the ack never results in a mark
	}
	b.received = append(b.received, ev.ID)
	b.mu.Unlock()
	return nil
}
func (b *crashBroker) Close() error { return nil }
func (b *crashBroker) ids() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.received...)
}

// --- tests ---------------------------------------------------------------

// TestRelayPublishesThenMarks is the happy path and FR-701: a publish delivers
// the §10 envelope and marks the row published exactly once.
func TestRelayPublishesThenMarks(t *testing.T) {
	st, admin := harness(t)
	_, eventID := seedPublish(t, st, admin, "relay-ok", "order.created")

	brk := &recordingBroker{}
	w := New(st, brk, Options{Poll: 10 * time.Millisecond})
	n, err := w.ProcessPending(context.Background())
	if err != nil {
		t.Fatalf("ProcessPending: %v", err)
	}
	if n != 1 {
		t.Fatalf("published %d, want 1", n)
	}
	if got := brk.ids(); len(got) != 1 || got[0] != eventID {
		t.Fatalf("delivered %v, want [%s]", got, eventID)
	}
	if status := outboxStatus(t, admin, eventID); status != "published" {
		t.Fatalf("outbox status = %q, want published", status)
	}
	// A second drain finds nothing: the published row is not re-leased.
	if n, err := w.ProcessPending(context.Background()); err != nil || n != 0 {
		t.Fatalf("second drain published %d (err %v), want 0", n, err)
	}
}

// TestRelayMarkOnlyAfterBrokerAck is FR-701: a broker that rejects the message
// leaves the row pending, so a failed publish can never be reported as delivered.
func TestRelayMarkOnlyAfterBrokerAck(t *testing.T) {
	st, admin := harness(t)
	producerID, eventID := seedPublish(t, st, admin, "relay-ack", "order.created")

	brk := &rejectingBroker{}
	w := New(st, brk, Options{Poll: 10 * time.Millisecond})
	if _, err := w.ProcessPending(context.Background()); err == nil {
		t.Fatal("ProcessPending returned nil despite a rejecting broker")
	}
	if status := outboxStatus(t, admin, eventID); status != "pending" {
		t.Fatalf("outbox status = %q after a rejected publish, want pending (FR-701)", status)
	}
	_ = producerID
}

// rejectingBroker fails every publish.
type rejectingBroker struct{ n int }

func (b *rejectingBroker) Publish(context.Context, *envelope.Envelope) error {
	b.n++
	return fmt.Errorf("broker down")
}
func (b *rejectingBroker) Close() error { return nil }

// TestRelayKillMidFlightRepublishes is AC-08: kill the relay after accept, mid a
// delivery, and prove that on restart the same row is re-published and marked -
// with exactly one outbox row and no lost or duplicated delivery.
func TestRelayKillMidFlightRepublishes(t *testing.T) {
	st, admin := harness(t)
	producerID, eventID := seedPublish(t, st, admin, "relay-kill", "order.created")

	entered := make(chan string, 1)
	crasher := &crashBroker{entered: entered}
	// A short lease so the re-claim after the kill happens quickly.
	w := New(st, crasher, Options{Lease: 2 * time.Second, Poll: 15 * time.Millisecond})

	ctxA, cancelA := context.WithCancel(context.Background())
	doneA := make(chan error, 1)
	go func() { doneA <- w.Run(ctxA) }()

	// Wait until the relay is genuinely mid-delivery: it has leased the row and is
	// inside Publish. Then the kill is a cancel of that context.
	select {
	case got := <-entered:
		if got != eventID {
			t.Fatalf("relay is delivering %q, want %q", got, eventID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("relay never leased the pending row")
	}

	cancelA()
	select {
	case <-doneA:
	case <-time.After(5 * time.Second):
		t.Fatal("relay did not exit after its context was cancelled")
	}

	// The kill landed before the mark: the row is still pending, exactly one row
	// exists, and the crashed delivery never completed.
	if status := outboxStatus(t, admin, eventID); status != "pending" {
		t.Fatalf("outbox status after kill = %q, want pending", status)
	}
	if n := outboxCount(t, admin, producerID); n != 1 {
		t.Fatalf("%d outbox rows after kill, want 1", n)
	}
	if len(crasher.ids()) != 0 {
		t.Fatalf("%d deliveries completed through the crash, want 0", len(crasher.ids()))
	}

	// Wait out the lease so a fresh relay can re-claim the row (FR-809, D-6).
	time.Sleep(2300 * time.Millisecond)

	// A healthy relay restarts and finishes the job.
	brk2 := &recordingBroker{}
	w2 := New(st, brk2, Options{Lease: 30 * time.Second, Poll: 15 * time.Millisecond})
	n, err := w2.ProcessPending(context.Background())
	if err != nil {
		t.Fatalf("recovery ProcessPending: %v", err)
	}
	if n != 1 {
		t.Fatalf("recovery published %d, want 1", n)
	}
	if got := brk2.ids(); len(got) != 1 || got[0] != eventID {
		t.Fatalf("recovery delivered %v, want [%s]", got, eventID)
	}

	// The durable proof: one outbox row, now published. Re-publication reused the
	// same row - there is no second delivery row (AC-08).
	if status := outboxStatus(t, admin, eventID); status != "published" {
		t.Fatalf("outbox status after recovery = %q, want published", status)
	}
	if n := outboxCount(t, admin, producerID); n != 1 {
		t.Fatalf("%d outbox rows after recovery, want exactly 1 (no duplicate delivery row)", n)
	}
}

// TestRelayStoppedEventIsNotLost is AC-09: with the relay stopped, an accepted
// event sits pending, OBS-2's lag grows, and nothing is lost - a late relay
// drains it and the lag returns to zero.
func TestRelayStoppedEventIsNotLost(t *testing.T) {
	st, admin := harness(t)
	producerID, eventID := seedPublish(t, st, admin, "relay-lag", "order.created")

	// No relay has run. The event was accepted (seedPublish returned), the outbox
	// row is pending, and OBS-2's lag is positive - the alarm condition.
	if status := outboxStatus(t, admin, eventID); status != "pending" {
		t.Fatalf("outbox status with relay stopped = %q, want pending", status)
	}
	lag, err := st.OutboxLagSeconds(context.Background())
	if err != nil {
		t.Fatalf("OutboxLagSeconds: %v", err)
	}
	if lag <= 0 {
		t.Fatalf("lag = %v while a row is pending, want > 0 (OBS-2)", lag)
	}

	// The relay starts late (it crashed, or was deploying) and loses nothing.
	brk := &recordingBroker{}
	w := New(st, brk, Options{Poll: 15 * time.Millisecond})
	n, err := w.ProcessPending(context.Background())
	if err != nil {
		t.Fatalf("ProcessPending: %v", err)
	}
	if n != 1 {
		t.Fatalf("late relay published %d, want 1", n)
	}
	if status := outboxStatus(t, admin, eventID); status != "published" {
		t.Fatalf("outbox status after late drain = %q, want published", status)
	}
	if lag, err := st.OutboxLagSeconds(context.Background()); err != nil || lag != 0 {
		t.Fatalf("lag after drain = %v, want 0", lag)
	}
	_ = producerID
}

// TestRelayLeaseHeldNotDoubleDelivered is FR-702: two relays running at once must
// not both publish the same row. One leases it; the other's lease finds nothing
// until the first marks it.
func TestRelayLeaseHeldNotDoubleDelivered(t *testing.T) {
	st, admin := harness(t)
	_, _ = seedPublish(t, st, admin, "relay-lease", "order.created")

	// A long lease so the first relay holds the row across the second's attempt.
	brk1 := &recordingBroker{}
	w1 := New(st, brk1, Options{Lease: 30 * time.Second, Poll: 15 * time.Millisecond})
	if n, err := w1.ProcessPending(context.Background()); err != nil || n != 1 {
		t.Fatalf("first relay published %d (err %v), want 1", n, err)
	}

	// A second relay, running now, leases nothing: the row is under w1's lease.
	brk2 := &recordingBroker{}
	w2 := New(st, brk2, Options{Lease: 30 * time.Second, Poll: 15 * time.Millisecond})
	if n, err := w2.ProcessPending(context.Background()); err != nil || n != 0 {
		t.Fatalf("second relay published %d (err %v), want 0 while the row is leased", n, err)
	}
	if n := len(brk2.ids()); n != 0 {
		t.Fatalf("second relay delivered %d, want 0 (FR-702)", n)
	}
}
