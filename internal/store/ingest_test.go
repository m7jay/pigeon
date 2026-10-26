package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// isInvalid is the store-test spelling of a 422: the caller must fix a value.
func isInvalid(err error) bool { return errors.Is(err, ErrInvalid) }

// newAppStore is the runtime-role store: every assertion about what a producer
// can do runs through it, so RLS is in force exactly as in production.
func newAppStore(t *testing.T, admin *pgxpool.Pool) *Store {
	t.Helper()
	return New(appPool(t, admin))
}

// scanOutboxStatus is the admin-side read that proves what the accept wrote: the
// outbox row exists, is pending until the relay marks it, and there is exactly
// one per event.
func scanOutboxStatus(t *testing.T, admin *pgxpool.Pool, eventID string) (status string, exists bool) {
	t.Helper()
	err := admin.QueryRow(context.Background(),
		`select status from outbox where event_id = $1`, eventID).Scan(&status)
	if err != nil {
		return "", false
	}
	return status, true
}

func countOutbox(t *testing.T, admin *pgxpool.Pool, producerID string) int {
	t.Helper()
	var n int
	if err := admin.QueryRow(context.Background(),
		`select count(*) from outbox o join events e on e.id = o.event_id
		 where e.producer_id = $1`, producerID).Scan(&n); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	return n
}

const paymentSchema = `{"type":"object","required":["amount"],"properties":{"amount":{"type":"integer"}}}`

// resetOutboxForTest wipes the outbox and events so a relay test starts from a
// clean slate. The relay's LeaseOutbox and the OBS-2 lag are global (the relay is
// one system worker over the whole outbox), and a prior test may leave a row that
// is pending-but-leased (a crashed relay leaves its lease in place until it
// expires) — that row is invisible to a fresh lease yet still counts toward lag,
// so a lease-based drain can't clear it. On this disposable test database a
// truncate is the honest reset. Event types, producers, consumers and idempotency
// keys are untouched, so each test still creates its own fixtures.
func resetOutboxForTest(t *testing.T, admin *pgxpool.Pool) {
	t.Helper()
	if _, err := admin.Exec(context.Background(), `truncate table outbox, events cascade`); err != nil {
		t.Fatalf("reset outbox: %v", err)
	}
}

func TestCreateEventTypeDefaultsAndPublish(t *testing.T) {
	admin := testPool(t)
	st := newAppStore(t, admin)
	ctx := context.Background()
	pid, _ := seedProducer(t, admin, "et-ingest")
	sc := Scope{ProducerID: pid, Env: "live"}

	et, err := st.CreateEventType(ctx, sc, NewEventType{
		Name:          "payment.succeeded",
		PayloadSchema: json.RawMessage(paymentSchema),
	})
	if err != nil {
		t.Fatalf("CreateEventType: %v", err)
	}
	if et.PayloadVersion != 1 || !et.Enabled || et.RoutingMode != RoutingBroadcast || et.EventClass != EventClassNormal {
		t.Fatalf("defaults wrong: %+v", et)
	}

	pub, err := st.PublishEvent(ctx, sc, PublishInput{
		EventType: "payment.succeeded",
		Payload:   json.RawMessage(`{"amount":49900}`),
	})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	if len(pub.ID) < 5 || pub.ID[:4] != "evt_" {
		t.Fatalf("event id %q is not a ULID", pub.ID)
	}
	if pub.RoutingMode != RoutingBroadcast || pub.PayloadVersion != 1 {
		t.Fatalf("published event %+v has wrong routing/version", pub)
	}

	status, ok := scanOutboxStatus(t, admin, pub.ID)
	if !ok || status != "pending" {
		t.Fatalf("outbox status = %q (exists %v), want pending; the event and outbox row must commit together (FR-604)", status, ok)
	}
	if n := countOutbox(t, admin, pid); n != 1 {
		t.Fatalf("outbox has %d rows, want 1", n)
	}
}

func TestPublishSchemaViolationIsInvalid(t *testing.T) {
	admin := testPool(t)
	st := newAppStore(t, admin)
	ctx := context.Background()
	pid, _ := seedProducer(t, admin, "et-schema")
	sc := Scope{ProducerID: pid, Env: "live"}
	if _, err := st.CreateEventType(ctx, sc, NewEventType{Name: "payment.succeeded", PayloadSchema: json.RawMessage(paymentSchema)}); err != nil {
		t.Fatalf("CreateEventType: %v", err)
	}
	// amount is a string, not an integer: FR-303, and the store must refuse it
	// before writing anything.
	_, err := st.PublishEvent(ctx, sc, PublishInput{EventType: "payment.succeeded", Payload: json.RawMessage(`{"amount":"oops"}`)})
	if err == nil || !isInvalid(err) {
		t.Fatalf("err = %v, want ErrInvalid (schema violation)", err)
	}
	if n := countOutbox(t, admin, pid); n != 0 {
		t.Fatalf("a rejected payload wrote %d outbox rows, want 0", n)
	}
}

func TestPublishUnknownTypeIsInvalid(t *testing.T) {
	admin := testPool(t)
	st := newAppStore(t, admin)
	ctx := context.Background()
	pid, _ := seedProducer(t, admin, "et-unknown")
	sc := Scope{ProducerID: pid, Env: "live"}
	_, err := st.PublishEvent(ctx, sc, PublishInput{EventType: "ghost.happened", Payload: json.RawMessage(`{}`)})
	if err == nil || !isInvalid(err) {
		t.Fatalf("err = %v, want ErrInvalid for an unknown event type", err)
	}
}

func TestPublishDisabledTypeIsInvalid(t *testing.T) {
	admin := testPool(t)
	st := newAppStore(t, admin)
	ctx := context.Background()
	pid, _ := seedProducer(t, admin, "et-disabled")
	sc := Scope{ProducerID: pid, Env: "live"}
	if _, err := st.CreateEventType(ctx, sc, NewEventType{Name: "thing.happens", PayloadSchema: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("CreateEventType: %v", err)
	}
	// There is no disable route in the store yet, but the column and check exist;
	// a disabled type must be refused at publish. Simulate by flipping the flag.
	if _, err := admin.Exec(ctx, `update event_types set enabled = false where name = 'thing.happens'`); err != nil {
		t.Fatalf("disable: %v", err)
	}
	_, err := st.PublishEvent(ctx, sc, PublishInput{EventType: "thing.happens", Payload: json.RawMessage(`{}`)})
	if err == nil || !isInvalid(err) {
		t.Fatalf("err = %v, want ErrInvalid for a disabled event type", err)
	}
}

// ptr is a small generic for the optional PATCH fields.
func ptr[T any](v T) *T { return &v }

// TestPatchEventType is FR-304/FR-305 at the store layer: PATCH flips enabled
// (and a disabled type is then refused at publish), bumps the version, preserves
// an omitted field, and returns ErrNotFound for a name the scope does not own -
// including the other environment, where the same name does not exist.
func TestPatchEventType(t *testing.T) {
	admin := testPool(t)
	st := newAppStore(t, admin)
	ctx := context.Background()
	pid, _ := seedProducer(t, admin, "et-patch")
	live := Scope{ProducerID: pid, Env: "live"}
	test := Scope{ProducerID: pid, Env: "test"}

	if _, err := st.CreateEventType(ctx, live, NewEventType{Name: "billing.invoice", PayloadSchema: json.RawMessage(`{"type":"object"}`)}); err != nil {
		t.Fatalf("CreateEventType: %v", err)
	}

	// Disable, then a disabled type is refused at publish (FR-305).
	disabled, err := st.PatchEventType(ctx, live, "billing.invoice", EventTypePatch{Enabled: ptr(false)})
	if err != nil {
		t.Fatalf("PatchEventType disable: %v", err)
	}
	if disabled.Enabled {
		t.Fatalf("disabled type %+v still enabled", disabled)
	}
	if _, err := st.PublishEvent(ctx, live, PublishInput{EventType: "billing.invoice", Payload: json.RawMessage(`{}`)}); err == nil || !isInvalid(err) {
		t.Fatalf("publish to a disabled type err = %v, want ErrInvalid", err)
	}

	// Version bump, and the omitted enabled field is preserved (still false, not
	// reset to true by a patch that only names payload_version).
	bumped, err := st.PatchEventType(ctx, live, "billing.invoice", EventTypePatch{PayloadVersion: ptr(2)})
	if err != nil {
		t.Fatalf("PatchEventType bump: %v", err)
	}
	if bumped.PayloadVersion != 2 || bumped.Enabled {
		t.Fatalf("bumped type %+v, want v2 with enabled preserved (still false)", bumped)
	}

	// An unknown name in the caller's scope is ErrNotFound, never a write.
	if _, err := st.PatchEventType(ctx, live, "ghost.happened", EventTypePatch{Enabled: ptr(false)}); err != ErrNotFound {
		t.Fatalf("patch unknown err = %v, want ErrNotFound", err)
	}

	// Event types are scoped by environment: the same name in the other
	// environment is a different (absent) resource, also ErrNotFound.
	if _, err := st.PatchEventType(ctx, test, "billing.invoice", EventTypePatch{Enabled: ptr(false)}); err != ErrNotFound {
		t.Fatalf("patch in the wrong env err = %v, want ErrNotFound", err)
	}
}

func TestPublishBroadcastWithConsumerKeyIsInvalid(t *testing.T) {
	admin := testPool(t)
	st := newAppStore(t, admin)
	ctx := context.Background()
	pid, _ := seedProducer(t, admin, "et-bcast")
	sc := Scope{ProducerID: pid, Env: "live"}
	if _, err := st.CreateEventType(ctx, sc, NewEventType{Name: "ops.ping", PayloadSchema: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("CreateEventType: %v", err)
	}
	ref := "sch_1"
	_, err := st.PublishEvent(ctx, sc, PublishInput{EventType: "ops.ping", Payload: json.RawMessage(`{}`), ConsumerRef: &ref})
	if err == nil || !isInvalid(err) {
		t.Fatalf("err = %v, want ErrInvalid: broadcast types take no consumer key", err)
	}
}

func TestPublishConsumerRequiresExactlyOneKey(t *testing.T) {
	admin := testPool(t)
	st := newAppStore(t, admin)
	ctx := context.Background()
	pid, _ := seedProducer(t, admin, "et-consumer")
	sc := Scope{ProducerID: pid, Env: "live"}
	if _, err := st.CreateEventType(ctx, sc, NewEventType{Name: "signup.completed", RoutingMode: RoutingConsumer, PayloadSchema: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("CreateEventType: %v", err)
	}
	// Neither key.
	if _, err := st.PublishEvent(ctx, sc, PublishInput{EventType: "signup.completed", Payload: json.RawMessage(`{}`)}); err == nil || !isInvalid(err) {
		t.Fatalf("neither key: err = %v, want ErrInvalid (AC-25)", err)
	}
	// Both keys.
	id, ref := "con_1", "sch_1"
	if _, err := st.PublishEvent(ctx, sc, PublishInput{EventType: "signup.completed", Payload: json.RawMessage(`{}`), ConsumerID: &id, ConsumerRef: &ref}); err == nil || !isInvalid(err) {
		t.Fatalf("both keys: err = %v, want ErrInvalid (AC-25)", err)
	}
}

func TestPublishConsumerResolvesConsumerIDToRef(t *testing.T) {
	admin := testPool(t)
	st := newAppStore(t, admin)
	ctx := context.Background()
	pid, _ := seedProducer(t, admin, "et-ref")
	sc := Scope{ProducerID: pid, Env: "live"}
	if _, err := st.CreateEventType(ctx, sc, NewEventType{Name: "signup.completed", RoutingMode: RoutingConsumer, PayloadSchema: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("CreateEventType: %v", err)
	}
	// Register a consumer, then publish with its id: the envelope must carry the
	// consumer's ref, because consumer_ref is the routing key and the §10 field.
	c, err := st.CreateConsumer(ctx, sc, "sch_4412", "Acme")
	if err != nil {
		t.Fatalf("CreateConsumer: %v", err)
	}
	pub, err := st.PublishEvent(ctx, sc, PublishInput{EventType: "signup.completed", Payload: json.RawMessage(`{}`), ConsumerID: &c.ID})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	if pub.ConsumerRef != "sch_4412" {
		t.Fatalf("consumer_id resolved to ref %q, want sch_4412", pub.ConsumerRef)
	}
}

func TestPublishIsIdempotentOnKey(t *testing.T) {
	admin := testPool(t)
	st := newAppStore(t, admin)
	ctx := context.Background()
	pid, _ := seedProducer(t, admin, "et-idem")
	sc := Scope{ProducerID: pid, Env: "live"}
	if _, err := st.CreateEventType(ctx, sc, NewEventType{Name: "payment.succeeded", PayloadSchema: json.RawMessage(paymentSchema)}); err != nil {
		t.Fatalf("CreateEventType: %v", err)
	}
	in := PublishInput{EventType: "payment.succeeded", Payload: json.RawMessage(`{"amount":1}`), IdempotencyKey: "abc-123"}
	first, err := st.PublishEvent(ctx, sc, in)
	if err != nil {
		t.Fatalf("first publish: %v", err)
	}
	second, err := st.PublishEvent(ctx, sc, in)
	if err != nil {
		t.Fatalf("second publish: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("replay returned %q, want original %q (FR-603)", second.ID, first.ID)
	}
	if n := countOutbox(t, admin, pid); n != 1 {
		t.Fatalf("idempotent replay created %d outbox rows, want 1", n)
	}
}

func TestRelayLeasesMarksAndClears(t *testing.T) {
	admin := testPool(t)
	st := newAppStore(t, admin)
	ctx := context.Background()
	pid, _ := seedProducer(t, admin, "relay-basic")
	sc := Scope{ProducerID: pid, Env: "live"}
	if _, err := st.CreateEventType(ctx, sc, NewEventType{Name: "payment.succeeded", PayloadSchema: json.RawMessage(paymentSchema)}); err != nil {
		t.Fatalf("CreateEventType: %v", err)
	}
	resetOutboxForTest(t, admin)
	pub, err := st.PublishEvent(ctx, sc, PublishInput{EventType: "payment.succeeded", Payload: json.RawMessage(`{"amount":1}`)})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}

	// Nothing leased before the event existed; now the relay claims exactly one.
	row, got, err := st.LeaseOutbox(ctx, 30*time.Second)
	if err != nil || !got {
		t.Fatalf("LeaseOutbox: got=%v err=%v, want one leased row", got, err)
	}
	if row.EventID != pub.ID {
		t.Fatalf("leased %q, want %q", row.EventID, pub.ID)
	}
	if row.PayloadSchemaMissing() {
		t.Fatal("leased row lost its payload")
	}
	if status, _ := scanOutboxStatus(t, admin, pub.ID); status != "pending" {
		t.Fatalf("status after lease = %q, want still pending (mark only after broker ack, FR-701)", status)
	}
	// A second lease in the same window gets nothing: the row is under lease
	// (FR-702 SKIP LOCKED), so two relays never publish the same row.
	if _, got, err = st.LeaseOutbox(ctx, 30*time.Second); err != nil || got {
		t.Fatalf("second lease got=%v err=%v, want nothing while leased", got, err)
	}

	if err := st.MarkOutboxPublished(ctx, row.OutboxID); err != nil {
		t.Fatalf("MarkOutboxPublished: %v", err)
	}
	if status, _ := scanOutboxStatus(t, admin, pub.ID); status != "published" {
		t.Fatalf("status after mark = %q, want published", status)
	}
	// Marking again is a no-op, not an error (idempotent success).
	if err := st.MarkOutboxPublished(ctx, row.OutboxID); err != nil {
		t.Fatalf("idempotent re-mark: %v", err)
	}
	if _, got, _ := st.LeaseOutbox(ctx, 30*time.Second); got {
		t.Fatal("a published row was leased again")
	}
}

func TestRelayLeaseExpiresSoACrashedRelayIsRecoverable(t *testing.T) {
	admin := testPool(t)
	st := newAppStore(t, admin)
	ctx := context.Background()
	pid, _ := seedProducer(t, admin, "relay-lease")
	sc := Scope{ProducerID: pid, Env: "live"}
	if _, err := st.CreateEventType(ctx, sc, NewEventType{Name: "ops.ping", PayloadSchema: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("CreateEventType: %v", err)
	}
	resetOutboxForTest(t, admin)
	if _, err := st.PublishEvent(ctx, sc, PublishInput{EventType: "ops.ping", Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}

	// A very short lease: the first relay "crashed" without marking. Once the
	// lease lapses the row is claimable again (D-6, FR-809), so nothing is lost.
	row, got, err := st.LeaseOutbox(ctx, 1*time.Second)
	if err != nil || !got {
		t.Fatalf("first lease: got=%v err=%v", got, err)
	}
	time.Sleep(1300 * time.Millisecond)
	row2, got2, err := st.LeaseOutbox(ctx, 30*time.Second)
	if err != nil || !got2 {
		t.Fatalf("after lease expiry: got=%v err=%v, want the row claimable again", got2, err)
	}
	if row2.OutboxID != row.OutboxID {
		t.Fatalf("re-leased %q, want the same row %d", row2.EventID, row.OutboxID)
	}
}

func TestOutboxLagReportsAge(t *testing.T) {
	admin := testPool(t)
	st := newAppStore(t, admin)
	ctx := context.Background()
	pid, _ := seedProducer(t, admin, "relay-lag")
	sc := Scope{ProducerID: pid, Env: "live"}
	if _, err := st.CreateEventType(ctx, sc, NewEventType{Name: "ops.ping", PayloadSchema: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("CreateEventType: %v", err)
	}
	resetOutboxForTest(t, admin)
	if _, err := st.PublishEvent(ctx, sc, PublishInput{EventType: "ops.ping", Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	lag, err := st.OutboxLagSeconds(ctx)
	if err != nil {
		t.Fatalf("OutboxLagSeconds: %v", err)
	}
	if lag <= 0 {
		t.Fatalf("lag = %v, want > 0 while a row is pending (OBS-2)", lag)
	}
	// Drain it and confirm the lag returns to zero.
	row, got, _ := st.LeaseOutbox(ctx, 30*time.Second)
	if !got {
		t.Fatal("expected a pending row to drain")
	}
	if err := st.MarkOutboxPublished(ctx, row.OutboxID); err != nil {
		t.Fatalf("mark: %v", err)
	}
	if lag, err := st.OutboxLagSeconds(ctx); err != nil || lag != 0 {
		t.Fatalf("lag after drain = %v, want 0", lag)
	}
}

// payloadSchemaMissing is a small guard on the leased row: the payload column is
// the whole point of the join in pigeon_lease_outbox, so a nil/empty payload is a
// contract break, not a benign zero.
func (r LeasedOutbox) PayloadSchemaMissing() bool { return len(r.Payload) == 0 }
