package store

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/m7jay/pigeon/internal/db"
	"github.com/m7jay/pigeon/internal/writeq"
)

// This file is the relay's drain unit: the batch of outbox rows one drain pass
// claimed, resolved to the delivery set each of them fans out to, and written
// through FR-818's write budget (SPEC.md A-11) as ONE transaction.
//
// The defect it exists for (card `t_b9850e1b`, found by S10's load test): the
// relay leased one outbox row per iteration and ran publish -> fan-out -> mark
// serially per event, so its rate was bounded by one whole round-trip chain per
// event - 41.5 events/s (414.9 deliveries/s) on this project's quiet 2 vCPU box,
// 1,000 accepted events at 10 subscriptions with the instance idle of ingest, and
// ~1.3-1.4 events/s on the loaded box S10 measured - against §8.1's 2,500
// deliveries/s fan-out row at the 250 events/s reference rate. The same probe on
// the batched drain below gives 496.7 events/s (4,966.8 deliveries/s). §8.1's L1
// asks for batched inserts (>= 2,000 rows per transaction) and FR-818 says the
// fan-out delivery rows, the delivery completion/terminal updates and the outbox
// marks are batched into one transaction per batch, so a batch is what the relay
// has to form: one lease in, ONE transaction out.
//
// Three pieces, and the split matters:
//
//   - PlanFanOut resolves a leased batch to its delivery sets. It READS only, so
//     it is not a write-budget item: one query per (producer, env) group, because
//     the tenant policies need the scope set per transaction.
//   - RelayBatch is the writeq.Batch the fan-out rows are submitted as. Its Apply
//     writes every row of every plan in the one transaction the budget opened:
//     the deliveries, the skipped rows, the §7.5a routing outcome on `events`, and
//     the outbox marks (FR-701's claim, which still follows the broker's ack
//     because the relay publishes before it submits).
//   - PackFanOut splits a drained set into submissions that fit the budget's row
//     capacity, so no submission is ever refused for being too large.

// FanOutPlan is one leased event resolved to the deliveries it fans out to. It is
// the unit RelayBatch writes; its fields mirror FanOutResult so the caller can
// advance FR-508's counters from the plan without re-reading.
type FanOutPlan struct {
	OutboxID   int64
	EventID    string
	ProducerID string
	Env        string
	// EventAt is the event's created_at: the deliveries partition key, and the
	// value a retried fan-out still dedupes on (FR-704, SB-1).
	EventAt time.Time
	// RoutingMode is the event's declared routing. Anything but RoutingConsumer
	// is a broadcast event, which §7.5a fans out to nothing.
	RoutingMode string
	// ConsumerID is the consumer the event's ref resolved to; empty when the event
	// is unroutable (FR-508).
	ConsumerID string
	// Deliverable is the endpoint ids this event becomes a delivery row for.
	Deliverable []string
	// Skipped is the matched endpoints that cannot take the event now, each with
	// its own endpoint-level reason (FR-509).
	Skipped []SkippedDelivery
	// RoutingState and EventReason are the §7.5a outcome recorded on the event
	// ("routable" or "unroutable", plus the reason code when there is one). Both
	// are empty for a broadcast event, which records nothing.
	RoutingState string
	EventReason  string
}

// Scope is the tenant scope this plan's rows belong to, and therefore the scope
// the transaction writing them is set to.
func (p FanOutPlan) Scope() Scope { return Scope{ProducerID: p.ProducerID, Env: p.Env} }

// Rows is how many rows this plan writes: one per delivery (or skipped delivery),
// plus the outbox mark, plus the routing outcome when the event records one. It is
// the count FR-815's floor and the budget's accumulation buffer are stated in, so
// it counts rows actually written rather than a per-event constant.
func (p FanOutPlan) Rows() int {
	rows := len(p.Deliverable) + len(p.Skipped) + 1
	if p.RoutingState != "" {
		rows++
	}
	return rows
}

// Result is the plan read back as the fan-out outcome the metric layer records
// (FR-508), so the relay's reason counters work the same on this path as on the
// single-event one.
func (p FanOutPlan) Result() FanOutResult {
	return FanOutResult{
		EventID:      p.EventID,
		RoutingMode:  p.RoutingMode,
		RoutingState: p.RoutingState,
		EventReason:  p.EventReason,
		Deliveries:   len(p.Deliverable),
		Skipped:      p.Skipped,
		Idle:         p.RoutingMode != RoutingConsumer,
	}
}

// PlanFanOut resolves a leased batch to one plan per row, in the order it was
// leased. An event that is not visible in the row's own scope (which cannot happen
// for a row the outbox function just leased, but would mean marking a row
// published with no delivery set behind it) is ErrNotFound rather than a silent
// skip.
//
// The reads are grouped by (producer, env) because the tenant policies read those
// two settings per transaction (MT-1, FR-103): one scoped read transaction and one
// query per group, whatever the size of the batch.
func (s *Store) PlanFanOut(ctx context.Context, rows []LeasedOutbox) ([]FanOutPlan, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	groups := groupLeases(rows)
	plans := make([]FanOutPlan, 0, len(rows))
	for _, g := range groups {
		var resolved []FanOutPlan
		err := s.scoped(ctx, g.scope, func(tx pgx.Tx) error {
			var err error
			resolved, err = s.plansInTx(ctx, tx, g.scope, g.rows)
			return err
		})
		if err != nil {
			return nil, err
		}
		plans = append(plans, resolved...)
	}
	return plans, nil
}

// leaseGroup is the leases of one (producer, env): the unit one scoped read
// transaction can serve.
type leaseGroup struct {
	scope Scope
	rows  []LeasedOutbox
}

// groupLeases splits a leased batch by scope, keeping the leases in their own
// order. One batch can legitimately span producers - the relay is a system worker
// with no tenant of its own - so the grouping is what keeps every read and write
// honest about whose rows it is touching, rather than averaging the scope over the
// batch.
func groupLeases(rows []LeasedOutbox) []leaseGroup {
	var groups []leaseGroup
	at := map[Scope]int{}
	for _, r := range rows {
		sc := Scope{ProducerID: r.ProducerID, Env: r.Env}
		i, ok := at[sc]
		if !ok {
			groups = append(groups, leaseGroup{scope: sc})
			i = len(groups) - 1
			at[sc] = i
		}
		groups[i].rows = append(groups[i].rows, r)
	}
	return groups
}

// plansInTx runs the one resolution query for a group and folds its rows - one per
// (event, matched endpoint), the same intersection queryFanOutMatches reads for a
// single event - back into one plan per event. The classification of each match
// (deliverable, paused, unverified) is FanOutEvent's, row for row: FR-509's
// endpoint-level reasons are recorded rather than dropped.
func (s *Store) plansInTx(ctx context.Context, tx pgx.Tx, sc Scope, rows []LeasedOutbox) ([]FanOutPlan, error) {
	outboxIDs := make([]int64, len(rows))
	eventIDs := make([]string, len(rows))
	index := make(map[int64]int, len(rows))
	plans := make([]FanOutPlan, len(rows))
	for i, r := range rows {
		outboxIDs[i] = r.OutboxID
		eventIDs[i] = r.EventID
		index[r.OutboxID] = i
		plans[i] = FanOutPlan{
			OutboxID:    r.OutboxID,
			EventID:     r.EventID,
			ProducerID:  r.ProducerID,
			Env:         r.Env,
			RoutingMode: r.RoutingMode,
		}
	}

	seen := make(map[int64]bool, len(rows))
	result, err := tx.Query(ctx, `
		select x.outbox_id, e.created_at, e.routing_mode,
		       coalesce(e.consumer_ref, ''), c.id,
		       m.endpoint_id, m.status, m.verified, m.sub_status
		from unnest($1::bigint[], $2::text[]) as x(outbox_id, event_id)
		join events e on e.producer_id = $3 and e.env = $4 and e.id = x.event_id
		left join consumers c on c.producer_id = e.producer_id and c.env = e.env
		                     and c.ref = e.consumer_ref
		left join lateral (
			select ep.id as endpoint_id, ep.status,
			       (ep.verified_at is not null) as verified, s.status as sub_status
			from subscriptions s
			join endpoints ep on ep.producer_id = s.producer_id and ep.env = s.env
			                 and ep.id = s.endpoint_id
			where s.producer_id = e.producer_id and s.env = e.env
			  and s.event_type = e.event_type and ep.consumer_id = c.id
			  and s.status <> 'deleted' and ep.status <> 'deleted'
		) m on true
		order by x.outbox_id, m.endpoint_id`,
		outboxIDs, eventIDs, sc.ProducerID, sc.Env)
	if err != nil {
		return nil, fmt.Errorf("plan fan-out: %w", err)
	}
	defer result.Close()
	for result.Next() {
		var outboxID int64
		var at time.Time
		var routingMode, consumerRef string
		var consumerID, endpointID, endpointStatus, subStatus *string
		var verified *bool
		if err := result.Scan(&outboxID, &at, &routingMode, &consumerRef, &consumerID,
			&endpointID, &endpointStatus, &verified, &subStatus); err != nil {
			return nil, fmt.Errorf("plan fan-out: %w", err)
		}
		i, ok := index[outboxID]
		if !ok {
			return nil, fmt.Errorf("plan fan-out: outbox row %d was not in the batch", outboxID)
		}
		seen[outboxID] = true
		p := &plans[i]
		p.EventAt, p.RoutingMode = at, routingMode
		if consumerID != nil {
			p.ConsumerID = *consumerID
		}
		if routingMode != RoutingConsumer {
			// Broadcast events carry no consumer (§7.5a): there is nothing to
			// intersect, so they fan out to nothing and record nothing.
			continue
		}
		switch {
		case consumerID == nil:
			p.RoutingState, p.EventReason = "unroutable", ReasonUnroutableNotRegistered
		case endpointID == nil:
			// The consumer exists but nothing is subscribed to this type (AC-24).
			// A matched row later in the same event overwrites this with the
			// routed outcome, so the order the matches arrive in does not matter.
			if p.RoutingState == "" {
				p.RoutingState, p.EventReason = "routable", ReasonNoMatchingSubscription
			}
		default:
			// The event routed, so any stale non-delivery reason is cleared
			// (FanOutEvent does the same on a re-fan-out of a since-onboarded
			// event).
			p.RoutingState, p.EventReason = "routable", ""
			switch {
			case strOrEmpty(subStatus) == "paused" || strOrEmpty(endpointStatus) == "paused":
				p.Skipped = append(p.Skipped, SkippedDelivery{
					EndpointID: strOrEmpty(endpointID), Reason: ReasonEndpointPaused,
				})
			case !truthy(verified) && sc.Env == EnvLive:
				p.Skipped = append(p.Skipped, SkippedDelivery{
					EndpointID: strOrEmpty(endpointID), Reason: ReasonEndpointUnverified,
				})
			default:
				p.Deliverable = append(p.Deliverable, strOrEmpty(endpointID))
			}
		}
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("plan fan-out: %w", err)
	}
	for _, r := range rows {
		if !seen[r.OutboxID] {
			return nil, fmt.Errorf("%w: fan-out plan for outbox row %d: event %s is not visible in %s/%s",
				ErrNotFound, r.OutboxID, r.EventID, r.ProducerID, r.Env)
		}
	}
	return plans, nil
}

func strOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func truthy(b *bool) bool { return b != nil && *b }

// RelayBatch is the fan-out of one drained lease batch, submitted to FR-818's
// write budget as one unit: every delivery row, every skipped row, every event's
// §7.5a outcome and every outbox mark in the batch is written in the ONE
// transaction the budget opened, which is what §8.1's L1 and FR-815's rows per
// transaction are about.
//
// It implements writeq.Batch and writeq.Coalescer: the relay's own drain submits
// one batch per pass, and two passes (or two relay processes sharing a budget -
// they cannot share leases, only transactions) fold into one.
type RelayBatch struct {
	groups []*relayBatchGroup
}

// relayBatchGroup is the plans of one (producer, env). The write transaction sets
// the tenant scope per group, because the RLS policies on `deliveries` and
// `events` read it (MT-1): a batch that spans producers is written as one statement
// per group rather than one statement that the policy would refuse.
type relayBatchGroup struct {
	scope Scope
	plans []FanOutPlan
}

// NewRelayBatch starts a batch over the given plans. A plan's Scope decides which
// group it lands in; the plans keep their order.
func NewRelayBatch(plans []FanOutPlan) *RelayBatch {
	b := &RelayBatch{}
	for _, p := range plans {
		b.add(p)
	}
	return b
}

func (b *RelayBatch) add(p FanOutPlan) {
	sc := p.Scope()
	for _, g := range b.groups {
		if g.scope == sc {
			g.plans = append(g.plans, p)
			return
		}
	}
	b.groups = append(b.groups, &relayBatchGroup{scope: sc, plans: []FanOutPlan{p}})
}

// Kind is the writeq coalescing key: every relay fan-out batch is the same kind of
// work, so a drain writes one transaction.
func (b *RelayBatch) Kind() string { return "relay-fanout" }

// Rows is how many rows the batch writes: the deliveries, the skipped deliveries,
// the events routing outcomes and the outbox marks (FR-815 counts rows, not
// events).
func (b *RelayBatch) Rows() int {
	n := 0
	for _, g := range b.groups {
		for _, p := range g.plans {
			n += p.Rows()
		}
	}
	return n
}

// Plans is the batch's plans, flattened in batch order. It is how a caller reads
// back what was written after the submit returned.
func (b *RelayBatch) Plans() []FanOutPlan {
	var out []FanOutPlan
	for _, g := range b.groups {
		out = append(out, g.plans...)
	}
	return out
}

// Days is the distinct event days the batch writes deliveries for: the partitions
// that must exist before its transaction inserts (the batched path cannot rely on
// FanOutEvent's per-event check).
func (b *RelayBatch) Days() []time.Time {
	seen := map[string]time.Time{}
	for _, g := range b.groups {
		for _, p := range g.plans {
			if len(p.Deliverable) == 0 && len(p.Skipped) == 0 {
				continue
			}
			day := p.EventAt.UTC()
			seen[day.Format("2006-01-02")] = day
		}
	}
	days := make([]time.Time, 0, len(seen))
	for _, d := range seen {
		days = append(days, d)
	}
	sort.Slice(days, func(i, j int) bool { return days[i].Before(days[j]) })
	return days
}

// Coalesce folds another relay fan-out batch into this one, so one drain writes
// one transaction. A batch of another kind is refused rather than guessed at: the
// queue then applies it separately, in the same transaction.
func (b *RelayBatch) Coalesce(other writeq.Batch) bool {
	o, ok := other.(*RelayBatch)
	if !ok || o == nil {
		return false
	}
	for _, g := range o.groups {
		for _, p := range g.plans {
			b.add(p)
		}
	}
	return true
}

// Apply writes the whole batch in tx, in the one transaction the queue opened for
// this drain. It never commits or rolls back: the queue owns the transaction
// (FR-815's "one transaction per batch").
//
// The statements inside a group run in a fixed order - the delivery rows, their
// skipped siblings, the events routing outcomes, then the outbox marks - and what
// makes the batch safe is that they are ONE transaction: the mark is FR-701's claim
// that the event has been honoured, so it commits together with the rows it claims,
// or not at all. A failure anywhere leaves every row of the batch pending and
// re-leaseable, and a re-fan-out of a re-leased row is a no-op on
// (event_id, endpoint_id) (FR-704).
func (b *RelayBatch) Apply(ctx context.Context, tx pgx.Tx) error {
	for _, g := range b.groups {
		if err := db.SetProducerScope(ctx, tx, g.scope.ProducerID); err != nil {
			return fmt.Errorf("relay batch: set producer scope: %w", err)
		}
		if err := db.SetEnvScope(ctx, tx, g.scope.Env); err != nil {
			return fmt.Errorf("relay batch: set env scope: %w", err)
		}
		if err := g.insertDeliveries(ctx, tx); err != nil {
			return err
		}
		if err := g.insertSkipped(ctx, tx); err != nil {
			return err
		}
		if err := g.updateRouting(ctx, tx); err != nil {
			return err
		}
		if err := markOutbox(ctx, tx, g.outboxIDs()); err != nil {
			return err
		}
	}
	return nil
}

// deliveries flattens the group's deliverable endpoints into the four parallel
// arrays one statement inserts: (event_id, endpoint_id, consumer_id, created_at)
// per row.
func (g *relayBatchGroup) deliveries() (events, endpoints, consumers []string, days []time.Time) {
	for _, p := range g.plans {
		for _, ep := range p.Deliverable {
			events = append(events, p.EventID)
			endpoints = append(endpoints, ep)
			consumers = append(consumers, p.ConsumerID)
			days = append(days, p.EventAt)
		}
	}
	return events, endpoints, consumers, days
}

// insertDeliveries batch-inserts one pending delivery per (event, endpoint) in one
// statement, exactly as FanOutEvent's insertDeliveries does for one event: the
// lease is set at creation (FR-813), the event's own created_at is the partition
// key and the dedupe key (SB-1, FR-704), and ON CONFLICT (event_id, endpoint_id,
// created_at) makes a re-fan-out a no-op.
func (g *relayBatchGroup) insertDeliveries(ctx context.Context, tx pgx.Tx) error {
	events, endpoints, consumers, days := g.deliveries()
	if len(events) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `
		insert into deliveries
			(event_id, endpoint_id, producer_id, env, consumer_id, status,
			 lease_expires_at, next_attempt_at, created_at)
		select x.event_id, x.endpoint_id, $1, $2, x.consumer_id, 'pending',
		       now(), now(), x.created_at
		from unnest($3::text[], $4::text[], $5::text[], $6::timestamptz[])
		     as x(event_id, endpoint_id, consumer_id, created_at)
		on conflict (event_id, endpoint_id, created_at)
			where replay_of_delivery_id is null do nothing`,
		g.scope.ProducerID, g.scope.Env, events, endpoints, consumers, days)
	if err != nil {
		return fmt.Errorf("relay batch: insert %d deliveries: %w", len(events), err)
	}
	return nil
}

// insertSkipped batch-inserts the matched-but-undeliverable rows with their
// endpoint-level reason (FR-509), same batching and idempotency rules.
func (g *relayBatchGroup) insertSkipped(ctx context.Context, tx pgx.Tx) error {
	var events, endpoints, consumers, reasons []string
	var days []time.Time
	for _, p := range g.plans {
		for _, s := range p.Skipped {
			events = append(events, p.EventID)
			endpoints = append(endpoints, s.EndpointID)
			consumers = append(consumers, p.ConsumerID)
			reasons = append(reasons, s.Reason)
			days = append(days, p.EventAt)
		}
	}
	if len(events) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `
		insert into deliveries
			(event_id, endpoint_id, producer_id, env, consumer_id, status,
			 non_delivery_reason, lease_expires_at, created_at)
		select x.event_id, x.endpoint_id, $1, $2, x.consumer_id, 'skipped',
		       x.reason, null, x.created_at
		from unnest($3::text[], $4::text[], $5::text[], $6::text[], $7::timestamptz[])
		     as x(event_id, endpoint_id, consumer_id, reason, created_at)
		on conflict (event_id, endpoint_id, created_at)
			where replay_of_delivery_id is null do nothing`,
		g.scope.ProducerID, g.scope.Env, events, endpoints, consumers, reasons, days)
	if err != nil {
		return fmt.Errorf("relay batch: insert %d skipped deliveries: %w", len(events), err)
	}
	return nil
}

// updateRouting writes each event's §7.5a outcome - routing_state plus the reason
// for the two non-delivery cases - in one statement. Live events that record
// nothing (broadcast ones) are not in the statement at all, exactly as
// FanOutEvent skips them.
func (g *relayBatchGroup) updateRouting(ctx context.Context, tx pgx.Tx) error {
	var ids, states, reasons []string
	for _, p := range g.plans {
		if p.RoutingState == "" {
			continue
		}
		ids = append(ids, p.EventID)
		states = append(states, p.RoutingState)
		reasons = append(reasons, p.EventReason)
	}
	if len(ids) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `
		update events e set routing_state = x.routing_state,
		                    non_delivery_reason = nullif(x.reason, '')
		from unnest($1::text[], $2::text[], $3::text[])
		     as x(event_id, routing_state, reason)
		where e.producer_id = $4 and e.env = $5 and e.id = x.event_id`,
		ids, states, reasons, g.scope.ProducerID, g.scope.Env)
	if err != nil {
		return fmt.Errorf("relay batch: record %d routing outcomes: %w", len(ids), err)
	}
	return nil
}

// outboxIDs is the group's outbox rows, the batch's claim on the broker's acks.
func (g *relayBatchGroup) outboxIDs() []int64 {
	ids := make([]int64, len(g.plans))
	for i, p := range g.plans {
		ids[i] = p.OutboxID
	}
	return ids
}

// markOutbox marks every row published in one statement (FR-701). The function is
// idempotent on a still-pending row, so a re-submitted batch is a no-op rather
// than an error.
func markOutbox(ctx context.Context, tx pgx.Tx, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	rows, err := tx.Query(ctx,
		`select pigeon_mark_outbox_published(x) from unnest($1::bigint[]) as x`, ids)
	if err != nil {
		return fmt.Errorf("mark %d outbox rows published: %w", len(ids), err)
	}
	defer rows.Close()
	for rows.Next() {
		// Drained so the statement runs to completion server-side; the function
		// returns void.
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("mark %d outbox rows published: %w", len(ids), err)
	}
	return nil
}

// PackFanOut splits a drained set into the submissions the budget can carry: each
// submission writes at most capacity rows, because FR-815's floor is a minimum and
// the budget's accumulation buffer refuses a batch larger than it - a drain is
// split rather than refused. Order is preserved, so the batches are submitted in the
// order the rows were leased.
//
// A plan is never split. A single event whose fan-out set alone exceeds the
// capacity is the caller's to handle - `Store.FanOutCapacity` is what it is
// compared against, and RelayBatch will refuse it - because splitting one event
// across two transactions would put its outbox mark in the first and leave the
// rest of its delivery set unwritten if the second failed.
func PackFanOut(plans []FanOutPlan, capacity int) [][]FanOutPlan {
	if capacity < 1 {
		capacity = 1
	}
	var packs [][]FanOutPlan
	var current []FanOutPlan
	rows := 0
	for _, p := range plans {
		if len(current) > 0 && rows+p.Rows() > capacity {
			packs = append(packs, current)
			current, rows = nil, 0
		}
		current = append(current, p)
		rows += p.Rows()
	}
	if len(current) > 0 {
		packs = append(packs, current)
	}
	return packs
}

// FanOutCapacity is how many rows one fan-out submission may carry: the write
// budget's accumulation buffer (FR-818). The relay leases and packs against it,
// so a drained batch is split rather than refused.
func (s *Store) FanOutCapacity() int { return s.writes.Stats().Pending }

// SubmitFanOut hands a fan-out batch to FR-818's write budget and returns once the
// transaction that carried it has committed, so a caller that gets nil can trust
// that the delivery rows exist and the outbox rows are marked.
//
// It ensures the partitions the batch writes into first: the delivery rows are
// partitioned by the event's day (SB-1), and the batched path cannot rely on
// FanOutEvent's per-event check.
func (s *Store) SubmitFanOut(ctx context.Context, b *RelayBatch) error {
	if b == nil || b.Rows() == 0 {
		return nil
	}
	for _, day := range b.Days() {
		if err := s.EnsurePartitionCached(ctx, "deliveries", day); err != nil {
			return fmt.Errorf("relay batch: ensure deliveries partition: %w", err)
		}
	}
	if err := s.writes.Submit(ctx, b); err != nil {
		return fmt.Errorf("relay batch: submit %d rows: %w", b.Rows(), err)
	}
	return nil
}
