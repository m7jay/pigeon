package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m7jay/pigeon/internal/db"
	"github.com/m7jay/pigeon/internal/writeq"
)

// This file is the relay's batch drain against a real Postgres: the fan-out of a
// whole leased batch written in ONE transaction (FR-818 as amended by SPEC.md A-11,
// FR-815's rows per transaction), the tenant scope still enforced per producer,
// idempotency on a re-submitted batch, the §7.5a reasons surviving the batched
// path, and a failed batch leaving the outbox claimable. Card `t_b9850e1b`.

// batchStore builds a store over a write budget whose accumulation buffer the test
// names, so the row bound the relay packs against is the test's own number.
func batchStore(t *testing.T, admin *pgxpool.Pool, pending int) (*Store, *writeq.Queue) {
	t.Helper()
	app := appPool(t, admin)
	q := NewWriteQueue(app, writeq.Options{Depth: 8, Workers: 2, Pending: pending, Pool: 5})
	st := New(app, WithWriteQueue(q))
	t.Cleanup(st.Close)
	return st, q
}

// batchScope seeds a producer whose slug is unique to this run: this package's
// harness keeps producers between runs, so a fixed slug would collide.
func batchScope(t *testing.T, admin *pgxpool.Pool, slug string) Scope {
	t.Helper()
	return Scope{ProducerID: seedRoutingProducer(t, admin, slug), Env: EnvLive}
}

// subscribedConsumer registers one consumer with n verified endpoints subscribed
// to eventType, which is the reference model's fan-out (one event, n deliveries).
func subscribedConsumer(t *testing.T, st *Store, sc Scope, slug, eventType string, n int) (Consumer, []string) {
	t.Helper()
	con := mustConsumer(t, st, sc, "ref-"+slug, "Batch consumer "+slug)
	eps := make([]string, n)
	for i := range eps {
		ep := mustVerifiedEndpoint(t, st, sc, con.ID,
			fmt.Sprintf("%s-ep-%02d", slug, i), fmt.Sprintf("https://8.8.8.%d/hook", i+1))
		eps[i] = ep.ID
		mustSubscribe(t, st, sc, ep.ID, eventType)
	}
	return con, eps
}

// publishN accepts n events routed to one consumer.
func publishN(t *testing.T, st *Store, sc Scope, eventType, consumerID string, n int) []string {
	t.Helper()
	ctx := context.Background()
	ids := make([]string, n)
	for i := range ids {
		pub, err := st.PublishEvent(ctx, sc, PublishInput{
			EventType: eventType, Payload: json.RawMessage(`{"n":1}`), ConsumerID: &consumerID,
		})
		if err != nil {
			t.Fatalf("PublishEvent %d: %v", i, err)
		}
		ids[i] = pub.ID
	}
	return ids
}

func countAllDeliveries(t *testing.T, admin *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := admin.QueryRow(context.Background(), `select count(*) from deliveries`).Scan(&n); err != nil {
		t.Fatalf("count deliveries: %v", err)
	}
	return n
}

// TestRelayFanOutBatchWritesOneTransaction is the relay's half of FR-815 as A-11
// states it: a leased batch of events, each fanning out to its subscriptions, is
// written in ONE transaction. Before this card the drain loop wrote one
// transaction per event, which is what bounded the fan-out at ~1/latency.
func TestRelayFanOutBatchWritesOneTransaction(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	st, q := batchStore(t, admin, 2000)
	ctx := context.Background()

	slug := uniqueSlug("relaybatch")
	sc := batchScope(t, admin, slug)
	et := mustConsumerEvent(t, st, sc, strings.ReplaceAll(slug, "-", "_")+".created")
	con, eps := subscribedConsumer(t, st, sc, slug, et.Name, 10)

	const events = 100
	ids := publishN(t, st, sc, et.Name, con.ID, events)

	leased, err := st.LeaseOutboxBatch(ctx, 30*time.Second, 200)
	if err != nil {
		t.Fatalf("LeaseOutboxBatch: %v", err)
	}
	if len(leased) != events {
		t.Fatalf("leased %d rows, want %d", len(leased), events)
	}

	plans, err := st.PlanFanOut(ctx, leased)
	if err != nil {
		t.Fatalf("PlanFanOut: %v", err)
	}
	if len(plans) != events {
		t.Fatalf("planned %d events, want %d", len(plans), events)
	}
	seenOutbox := map[int64]bool{}
	for _, p := range plans {
		if len(p.Deliverable) != len(eps) || p.RoutingState != "routable" || p.EventReason != "" {
			t.Fatalf("plan %s: %d deliverable, state %q, reason %q; want %d, routable, no reason",
				p.EventID, len(p.Deliverable), p.RoutingState, p.EventReason, len(eps))
		}
		if p.ConsumerID != con.ID {
			t.Fatalf("plan %s resolved consumer %q, want %q", p.EventID, p.ConsumerID, con.ID)
		}
		if seenOutbox[p.OutboxID] {
			t.Fatalf("outbox row %d appears twice in one plan set", p.OutboxID)
		}
		seenOutbox[p.OutboxID] = true
	}

	before := q.Stats()
	if err := st.SubmitFanOut(ctx, NewRelayBatch(plans)); err != nil {
		t.Fatalf("SubmitFanOut: %v", err)
	}
	after := q.Stats()
	if drains := after.Drains - before.Drains; drains != 1 {
		t.Fatalf("the batch drained as %d transactions, want 1 (FR-815: one transaction per batch)", drains)
	}
	// One delivery row per (event, endpoint), plus the event's routing outcome and
	// the outbox mark.
	wantRows := int64(events * (len(eps) + 2))
	if rows := after.Rows - before.Rows; rows != wantRows {
		t.Fatalf("the batch wrote %d rows, want %d", rows, wantRows)
	}

	// Durable: every event has one delivery per subscription, and every outbox row
	// is marked published - the fan-out is committed, not announced.
	for _, id := range ids {
		if n := countDeliveries(t, admin, id); n != len(eps) {
			t.Fatalf("event %s has %d delivery rows, want %d", id, n, len(eps))
		}
		if status, _ := scanOutboxStatus(t, admin, id); status != "published" {
			t.Fatalf("outbox row for %s = %q, want published", id, status)
		}
	}
	if n := countAllDeliveries(t, admin); n != events*len(eps) {
		t.Fatalf("%d delivery rows in total, want %d", n, events*len(eps))
	}
}

// TestRelayFanOutBatchKeepsEachProducersScope proves the batched path is still
// tenant-scoped. One batch can span producers - the relay is a system worker with
// no tenant of its own - so the batch sets the scope per group and the RLS
// policies on deliveries refuse any row that belongs to another producer.
func TestRelayFanOutBatchKeepsEachProducersScope(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	st, _ := batchStore(t, admin, 2000)
	ctx := context.Background()

	slugA, slugB := uniqueSlug("batcha"), uniqueSlug("batchb")
	a := batchScope(t, admin, slugA)
	b := batchScope(t, admin, slugB)
	etA := mustConsumerEvent(t, st, a, "batch.created")
	etB := mustConsumerEvent(t, st, b, "batch.created")
	conA, _ := subscribedConsumer(t, st, a, slugA, etA.Name, 2)
	conB, _ := subscribedConsumer(t, st, b, slugB, etB.Name, 3)

	idsA := publishN(t, st, a, etA.Name, conA.ID, 1)
	idsB := publishN(t, st, b, etB.Name, conB.ID, 1)

	leased, err := st.LeaseOutboxBatch(ctx, 30*time.Second, 10)
	if err != nil {
		t.Fatalf("LeaseOutboxBatch: %v", err)
	}
	if len(leased) != 2 {
		t.Fatalf("leased %d rows, want both producers' rows in one batch", len(leased))
	}
	plans, err := st.PlanFanOut(ctx, leased)
	if err != nil {
		t.Fatalf("PlanFanOut: %v", err)
	}
	if err := st.SubmitFanOut(ctx, NewRelayBatch(plans)); err != nil {
		t.Fatalf("SubmitFanOut: %v", err)
	}

	if n := countDeliveries(t, admin, idsA[0]); n != 2 {
		t.Fatalf("producer A's event has %d deliveries, want 2", n)
	}
	if n := countDeliveries(t, admin, idsB[0]); n != 3 {
		t.Fatalf("producer B's event has %d deliveries, want 3", n)
	}

	// The tenant read is the independent check: as producer A the runtime role can
	// see exactly A's two rows, whatever the batch wrote.
	conn, err := st.Pool().Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer conn.Release()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := db.SetProducerScope(ctx, tx, a.ProducerID); err != nil {
		t.Fatalf("set producer scope: %v", err)
	}
	if err := db.SetEnvScope(ctx, tx, a.Env); err != nil {
		t.Fatalf("set env scope: %v", err)
	}
	var visible int
	if err := tx.QueryRow(ctx, `select count(*) from deliveries`).Scan(&visible); err != nil {
		t.Fatalf("scoped count: %v", err)
	}
	if visible != 2 {
		t.Fatalf("producer A sees %d delivery rows, want 2: the batch wrote outside the scope it set", visible)
	}
}

// TestRelayFanOutBatchIsIdempotentOnResubmit is FR-704 on the batched path: the
// same plans written twice create no second delivery set and mark nothing twice,
// which is what makes a re-leased batch after a crash harmless.
func TestRelayFanOutBatchIsIdempotentOnResubmit(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	st, _ := batchStore(t, admin, 2000)
	ctx := context.Background()

	slug := uniqueSlug("batchidem")
	sc := batchScope(t, admin, slug)
	et := mustConsumerEvent(t, st, sc, "batch.created")
	con, eps := subscribedConsumer(t, st, sc, slug, et.Name, 3)
	ids := publishN(t, st, sc, et.Name, con.ID, 1)

	leased, err := st.LeaseOutboxBatch(ctx, 30*time.Second, 10)
	if err != nil {
		t.Fatalf("LeaseOutboxBatch: %v", err)
	}
	plans, err := st.PlanFanOut(ctx, leased)
	if err != nil {
		t.Fatalf("PlanFanOut: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := st.SubmitFanOut(ctx, NewRelayBatch(plans)); err != nil {
			t.Fatalf("SubmitFanOut %d: %v", i+1, err)
		}
	}
	if n := countDeliveries(t, admin, ids[0]); n != len(eps) {
		t.Fatalf("after two submissions the event has %d delivery rows, want %d", n, len(eps))
	}
	state, reason := scanEventRouting(t, admin, ids[0])
	if state != "routable" || reason != "" {
		t.Fatalf("event routing = (%q, %q), want (routable, no reason)", state, reason)
	}
}

// TestRelayFanOutBatchRecordsReasonsAndIdleEvents is FR-508/FR-509 and §7.5a on
// the batched path: the four outcomes - routed, no matching subscription,
// unroutable, broadcast - each land on the durable rows the batched transaction
// writes, with the endpoint-level reasons on their own skipped rows.
func TestRelayFanOutBatchRecordsReasonsAndIdleEvents(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	st, _ := batchStore(t, admin, 2000)
	ctx := context.Background()

	slug := uniqueSlug("batchreasons")
	sc := batchScope(t, admin, slug)
	et := mustConsumerEvent(t, st, sc, "batch.created")
	con, _ := subscribedConsumer(t, st, sc, slug, et.Name, 0)

	// Four endpoints with four outcomes: deliverable, deliverable, paused (and
	// therefore skipped), never-verified in live (skipped for its own reason).
	deliverable := []string{}
	for i, endpoint := range []struct {
		name   string
		url    string
		paused bool
		verify bool
	}{
		{"d1", "https://8.8.8.1/hook", false, true},
		{"d2", "https://8.8.8.2/hook", false, true},
		{"p1", "https://8.8.8.3/hook", true, true},
		{"u1", "https://8.8.8.4/hook", false, false},
	} {
		created, err := st.CreateEndpoint(ctx, sc, NewEndpoint{
			ConsumerID: con.ID, Name: fmt.Sprintf("%s-%s", slug, endpoint.name), URL: endpoint.url,
		})
		if err != nil {
			t.Fatalf("CreateEndpoint(%s): %v", endpoint.name, err)
		}
		if endpoint.verify {
			if err := st.VerifyEndpoint(ctx, sc, created.Endpoint.ID, created.VerificationToken); err != nil {
				t.Fatalf("VerifyEndpoint(%s): %v", endpoint.name, err)
			}
		}
		if endpoint.paused {
			if _, err := st.SetEndpointPaused(ctx, sc, created.Endpoint.ID, true); err != nil {
				t.Fatalf("SetEndpointPaused(%s): %v", endpoint.name, err)
			}
		}
		mustSubscribe(t, st, sc, created.Endpoint.ID, et.Name)
		if i < 2 {
			deliverable = append(deliverable, created.Endpoint.ID)
		}
	}

	// A second consumer with no subscription for this type (AC-24), an unknown ref
	// in live (AC-23's unroutable case), and a broadcast type's event (§7.5a - a
	// consumer-routed type requires a consumer key, so broadcast needs its own).
	mustConsumer(t, st, sc, "ref-"+slug+"-nosub", "Nothing subscribed")
	unroutableRef := "ref-" + slug + "-nobody"
	nosubRef := "ref-" + slug + "-nosub"
	routed, err := st.PublishEvent(ctx, sc, PublishInput{EventType: et.Name, Payload: json.RawMessage(`{}`), ConsumerRef: &con.Ref})
	if err != nil {
		t.Fatalf("PublishEvent(routed): %v", err)
	}
	nosub, err := st.PublishEvent(ctx, sc, PublishInput{EventType: et.Name, Payload: json.RawMessage(`{}`), ConsumerRef: &nosubRef})
	if err != nil {
		t.Fatalf("PublishEvent(no subscription): %v", err)
	}
	unroutable, err := st.PublishEvent(ctx, sc, PublishInput{EventType: et.Name, Payload: json.RawMessage(`{}`), ConsumerRef: &unroutableRef})
	if err != nil {
		t.Fatalf("PublishEvent(unroutable): %v", err)
	}
	broadcastType, err := st.CreateEventType(ctx, sc, NewEventType{
		Name: "batch.broadcast", PayloadSchema: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("CreateEventType(broadcast): %v", err)
	}
	broadcast, err := st.PublishEvent(ctx, sc, PublishInput{EventType: broadcastType.Name, Payload: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatalf("PublishEvent(broadcast): %v", err)
	}

	leased, err := st.LeaseOutboxBatch(ctx, 30*time.Second, 10)
	if err != nil {
		t.Fatalf("LeaseOutboxBatch: %v", err)
	}
	if len(leased) != 4 {
		t.Fatalf("leased %d rows, want 4", len(leased))
	}
	plans, err := st.PlanFanOut(ctx, leased)
	if err != nil {
		t.Fatalf("PlanFanOut: %v", err)
	}
	byEvent := map[string]FanOutPlan{}
	for _, p := range plans {
		byEvent[p.EventID] = p
	}

	if p := byEvent[routed.ID]; len(p.Deliverable) != 2 || len(p.Skipped) != 2 ||
		p.RoutingState != "routable" || p.EventReason != "" {
		t.Fatalf("routed plan = %d deliverable, %d skipped, state %q, reason %q; want 2, 2, routable, none",
			len(p.Deliverable), len(p.Skipped), p.RoutingState, p.EventReason)
	}
	reasons := map[string]string{}
	for _, s := range byEvent[routed.ID].Skipped {
		reasons[s.EndpointID] = s.Reason
	}
	for _, ep := range deliverable {
		if _, ok := reasons[ep]; ok {
			t.Fatalf("deliverable endpoint %s was also recorded as skipped", ep)
		}
	}
	if p := byEvent[nosub.ID]; len(p.Deliverable) != 0 || p.EventReason != ReasonNoMatchingSubscription || p.RoutingState != "routable" {
		t.Fatalf("no-subscription plan = %d deliverable, state %q, reason %q", len(p.Deliverable), p.RoutingState, p.EventReason)
	}
	if p := byEvent[unroutable.ID]; p.EventReason != ReasonUnroutableNotRegistered || p.RoutingState != "unroutable" {
		t.Fatalf("unroutable plan = state %q, reason %q", p.RoutingState, p.EventReason)
	}
	if p := byEvent[broadcast.ID]; len(p.Deliverable) != 0 || len(p.Skipped) != 0 || p.RoutingState != "" {
		t.Fatalf("broadcast plan = %d deliverable, %d skipped, state %q; want nothing fanned out and nothing recorded",
			len(p.Deliverable), len(p.Skipped), p.RoutingState)
	}

	if err := st.SubmitFanOut(ctx, NewRelayBatch(plans)); err != nil {
		t.Fatalf("SubmitFanOut: %v", err)
	}

	// Durable rows: the skipped pair carries its own reason each, and the event's
	// §7.5a outcome is on the event row.
	var pausedRows, unverifiedRows int
	if err := admin.QueryRow(ctx, `
		select count(*) filter (where non_delivery_reason = $2),
		       count(*) filter (where non_delivery_reason = $3)
		from deliveries where event_id = $1`,
		routed.ID, ReasonEndpointPaused, ReasonEndpointUnverified,
	).Scan(&pausedRows, &unverifiedRows); err != nil {
		t.Fatalf("scan skipped rows: %v", err)
	}
	if pausedRows != 1 || unverifiedRows != 1 {
		t.Fatalf("skipped rows = %d paused, %d unverified; want 1 and 1", pausedRows, unverifiedRows)
	}
	state, reason := scanEventRouting(t, admin, nosub.ID)
	if state != "routable" || reason != ReasonNoMatchingSubscription {
		t.Fatalf("no-subscription event routing = (%q, %q)", state, reason)
	}
	state, reason = scanEventRouting(t, admin, unroutable.ID)
	if state != "unroutable" || reason != ReasonUnroutableNotRegistered {
		t.Fatalf("unroutable event routing = (%q, %q)", state, reason)
	}
	if n := countDeliveries(t, admin, broadcast.ID); n != 0 {
		t.Fatalf("broadcast event has %d delivery rows, want 0", n)
	}
	for _, id := range []string{routed.ID, nosub.ID, unroutable.ID, broadcast.ID} {
		if status, _ := scanOutboxStatus(t, admin, id); status != "published" {
			t.Fatalf("outbox row for %s = %q, want published", id, status)
		}
	}
}

// TestRelayFanOutBatchFailureRollsBackAndLeavesTheOutboxPending is the ordering
// contract FR-701 needs from the batched path: the outbox mark is written in the
// same transaction AFTER the delivery rows, so a transaction that fails anywhere
// leaves every row of the batch pending and re-leaseable - nothing is half-fanned
// out and nothing is claimed as published.
func TestRelayFanOutBatchFailureRollsBackAndLeavesTheOutboxPending(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	st, _ := batchStore(t, admin, 2000)
	ctx := context.Background()

	slug := uniqueSlug("batchfail")
	sc := batchScope(t, admin, slug)
	et := mustConsumerEvent(t, st, sc, "batch.created")
	con, eps := subscribedConsumer(t, st, sc, slug, et.Name, 2)
	ids := publishN(t, st, sc, et.Name, con.ID, 2)

	leased, err := st.LeaseOutboxBatch(ctx, 30*time.Second, 10)
	if err != nil {
		t.Fatalf("LeaseOutboxBatch: %v", err)
	}
	plans, err := st.PlanFanOut(ctx, leased)
	if err != nil {
		t.Fatalf("PlanFanOut: %v", err)
	}
	if len(plans) != 2 {
		t.Fatalf("planned %d events, want 2", len(plans))
	}
	// The second plan's event id does not exist, so the batch's one insert
	// statement violates deliveries' foreign key to events.
	plans[1].EventID = "evt_does_not_exist"

	if err := st.SubmitFanOut(ctx, NewRelayBatch(plans)); err == nil {
		t.Fatal("SubmitFanOut accepted a batch whose delivery rows cannot exist")
	}
	if n := countAllDeliveries(t, admin); n != 0 {
		t.Fatalf("%d delivery rows survived the failed batch, want 0: the batch is one transaction", n)
	}
	for _, id := range ids {
		if status, _ := scanOutboxStatus(t, admin, id); status != "pending" {
			t.Fatalf("outbox row for %s = %q after a failed batch, want pending (FR-701)", id, status)
		}
	}

	// The happy path still works on the same rows once the plan is right, so the
	// failure left nothing behind that blocks the retry.
	replanned, err := st.PlanFanOut(ctx, leased)
	if err != nil {
		t.Fatalf("PlanFanOut after the failure: %v", err)
	}
	if err := st.SubmitFanOut(ctx, NewRelayBatch(replanned)); err != nil {
		t.Fatalf("SubmitFanOut (retry): %v", err)
	}
	for _, id := range ids {
		if n := countDeliveries(t, admin, id); n != len(eps) {
			t.Fatalf("event %s has %d delivery rows after the retry, want %d", id, n, len(eps))
		}
		if status, _ := scanOutboxStatus(t, admin, id); status != "published" {
			t.Fatalf("outbox row for %s = %q after the retry, want published", id, status)
		}
	}
}

// TestPackFanOutSplitsAtTheRowBound is the packing rule: a submission never
// carries more rows than the budget's accumulation buffer holds, order is
// preserved, and no plan is ever split (splitting one event across transactions
// would put its mark in the first and could lose the rest of its delivery set).
func TestPackFanOutSplitsAtTheRowBound(t *testing.T) {
	plan := func(id string, deliveries int, routed bool) FanOutPlan {
		p := FanOutPlan{EventID: id}
		if routed {
			p.RoutingState = "routable"
		}
		for i := 0; i < deliveries; i++ {
			p.Deliverable = append(p.Deliverable, fmt.Sprintf("%s-ep-%d", id, i))
		}
		return p
	}
	// Rows: 3 deliveries + routing + mark = 5 for a routed plan, 3 + mark = 4 for
	// a broadcast one.
	plans := []FanOutPlan{plan("a", 3, true), plan("b", 3, true), plan("c", 3, false), plan("d", 0, false)}
	if got := plans[0].Rows(); got != 5 {
		t.Fatalf("routed 3-delivery plan Rows() = %d, want 5", got)
	}
	if got := plans[2].Rows(); got != 4 {
		t.Fatalf("broadcast 3-delivery plan Rows() = %d, want 4", got)
	}

	packs := PackFanOut(plans, 9)
	var flat []string
	for _, pack := range packs {
		rows := 0
		for _, p := range pack {
			rows += p.Rows()
			flat = append(flat, p.EventID)
		}
		if rows > 9 {
			t.Fatalf("a pack carries %d rows, want <= 9 (the budget's accumulation buffer)", rows)
		}
	}
	if strings.Join(flat, "") != "abcd" {
		t.Fatalf("packs flattened to %v, want the plans in order and exactly once", flat)
	}
	// 5 fits alone (5 + 5 = 10 > 9), 5 + 4 fits, and the 1-row plan starts a third
	// submission rather than overfilling the second.
	if len(packs) != 3 {
		t.Fatalf("packed into %d submissions, want 3", len(packs))
	}
	if got := []string{packs[0][0].EventID, packs[1][0].EventID, packs[1][1].EventID, packs[2][0].EventID}; strings.Join(got, "") != "abcd" {
		t.Fatalf("pack boundaries = %v, want [a] [b c] [d]", got)
	}
	// A capacity that cannot hold a plan is not the packer's to fix: the plan stays
	// whole and one submission overflows, which the caller compares against
	// FanOutCapacity and handles with the single-event path.
	oversized := []FanOutPlan{plan("big", 10, true)}
	packs = PackFanOut(oversized, 4)
	if len(packs) != 1 || len(packs[0]) != 1 {
		t.Fatalf("an oversized plan was split into %d submissions, want it left whole", len(packs))
	}
}
