package store

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// This file is S7's read surface and its one write: the delivery list, the
// delivery detail with its ordered attempts, the event-level view with its
// rollup (FR-1101, FR-1102, FR-1109, FR-1110, FR-1111), the dedupe answer
// (FR-903) and replay (FR-1106, FR-1107, FR-1108).
//
// Two properties hold for every method here, and each is enforced twice:
//
//   - the tenant scope is in the WHERE clause *and* in `pigeon.producer_id`/
//     `pigeon.env` for RLS (MT-1), so a forgotten filter returns nothing rather
//     than another producer's history;
//   - a foreign id is ErrNotFound, never a 403 (FR-108), because a lookup that
//     misses in the caller's scope is indistinguishable from one that does not
//     exist.
//
// The consumer-scoped reads (FR-1105) are the same methods with ConsumerID set
// from the caller's session claim: the filter lives in this layer so the handler
// cannot forget it, and nothing in a request body can move it.

// Rollup is FR-1109's per-event fan-out outcome. The five values are the whole
// vocabulary: an event is either still in flight, fully delivered, partly or
// wholly failed, or unroutable because its consumer_ref matched no consumer
// (§7.5a).
const (
	RollupAllDelivered    = "all_delivered"
	RollupPartiallyFailed = "partially_failed"
	RollupFailed          = "failed"
	RollupPending         = "pending"
	RollupUnroutable      = "unroutable"
)

// Delivery statuses this file reasons about. The column's check constraint is
// the source of truth (0007, and S5 adds FR-1201's `failed_terminal`); these
// three names are how a status is *classified* here:
//
//   - pending             in flight, so the event's outcome is not settled;
//   - delivered           attempted and accepted (FR-803);
//   - anything else       terminal without a delivery: `failed_terminal`
//     (0007 wrote `failed`; 0013 renamed it, I-4/FR-1201), `skipped`, and
//     whatever terminal status a later slice adds.
const (
	statusPending   = "pending"
	statusDelivered = "delivered"
	statusSkipped   = "skipped"
)

// ReplayMaxPerRequest is FR-1107's bound. It is named here rather than in the
// handler so the refusal message and the store's own check cannot drift.
const ReplayMaxPerRequest = 1000

// ReplayReleasePerSecond is FR-1107's "50/s per endpoint" (and FR-1308's "the
// same endpoint limits as first attempts"). A replay does not POST anything
// itself: it creates pending rows and lets the delivery worker do the work, so
// the rate is enforced where the work is released - the new rows' leases are
// spaced 1/50 s apart per endpoint, which makes a 1,000-delivery replay to one
// endpoint cost that endpoint 20 s of releases instead of a burst. The worker's
// own per-endpoint limits (S5's leased concurrency and token bucket) then govern
// the in-flight attempts exactly as they do for a first attempt.
const ReplayReleasePerSecond = 50

// ErrReplayBound is FR-1107's refusal: the selection is larger than one request
// may replay. It wraps ErrInvalid so the API answers 422, and its message names
// the bound, which is what AC-13 asserts.
var ErrReplayBound = fmt.Errorf("%w: replay is bounded to %d deliveries per request", ErrInvalid, ReplayMaxPerRequest)

// DeliveryFilter selects deliveries. Every field is optional and an empty one
// means "no restriction", so the zero value is "everything in this tenant".
//
// Status is passed through rather than validated against an enum: the column's
// check constraint is the source of truth, and a status a later slice adds (FR-1201
// `failed_terminal`) must be filterable without an API change.
type DeliveryFilter struct {
	EndpointID string     `json:"endpoint_id,omitempty"`
	EventType  string     `json:"event_type,omitempty"`
	ConsumerID string     `json:"consumer_id,omitempty"`
	Status     string     `json:"status,omitempty"`
	From       *time.Time `json:"from,omitempty"`
	To         *time.Time `json:"to,omitempty"`
}

// DeliverySummary is one delivery as the list and the detail report it. The
// four FR-811 columns are here in full, which is what makes FR-1110 possible: a
// successful first attempt has no attempt row, so its status, timestamp and
// duration have to come from the delivery row itself.
type DeliverySummary struct {
	ID                 int64
	EventID            string
	EventType          string
	EndpointID         string
	ConsumerID         string
	Status             string
	Attempts           int
	FirstAttemptAt     *time.Time
	DeliveredAt        *time.Time
	CreatedAt          time.Time
	NonDeliveryReason  *string
	ReplayOfDeliveryID *int64
	// HeldBackSince and HeldBackReason are FR-1304: this delivery is being held
	// back by its endpoint's limits rather than attempted, since the given
	// instant and for the given reason. Both are nil on a delivery that is not
	// being held, which is every delivery whose attempt has started.
	HeldBackSince  *time.Time
	HeldBackReason *string
}

// DurationMs is how long the delivery took: the wait between its first attempt
// and its acceptance. It is nil while either endpoint of that interval is
// unknown, which is the honest answer for a delivery still in flight.
func (d DeliverySummary) DurationMs() *int {
	if d.FirstAttemptAt == nil || d.DeliveredAt == nil {
		return nil
	}
	ms := int(d.DeliveredAt.Sub(*d.FirstAttemptAt).Milliseconds())
	if ms < 0 {
		ms = 0
	}
	return &ms
}

// Attempt is one failed attempt (FR-1103): number, timestamp, duration, status,
// truncated body, error class and next retry time. A successful first attempt is
// deliberately absent (FR-811) - see DeliveryDetail.EffectiveAttempt.
type Attempt struct {
	Attempt        int
	OccurredAt     time.Time
	DurationMs     int
	ResponseStatus *int
	ResponseBody   *string
	ErrorClass     string
	Outcome        string
	NextRetryAt    *time.Time
}

// EffectiveAttempt is the attempt a delivery had that left no `attempts` row:
// the successful first attempt FR-811 records as columns on the delivery. It is
// what FR-1110 requires the trail to show instead of an empty one, and it is
// marked as synthesized so no caller can mistake it for a row.
type EffectiveAttempt struct {
	Attempt    int
	OccurredAt time.Time
	DurationMs *int
	Status     string
	Outcome    string
}

// DeliveryDetail is FR-1102: a delivery with its ordered attempts. Trail() is
// the whole answer to FR-1110 - the attempt rows, or the one effective attempt
// when there are none.
type DeliveryDetail struct {
	Delivery         DeliverySummary
	Attempts         []Attempt
	EffectiveAttempt *EffectiveAttempt
	// ReplayedBy are the ids of deliveries created by replaying this one
	// (FR-1106/FR-1108): the trail distinguishes a replay from a first attempt
	// in both directions.
	ReplayedBy []int64
}

// Trail is the ordered trail FR-1110 promises never to leave empty: the attempt
// rows as they are, plus the synthesized first attempt for the deliveries that
// have no rows (FR-811's successful first attempt, and a skipped delivery, which
// was never attempted at all).
func (d DeliveryDetail) Trail() []Attempt {
	if len(d.Attempts) > 0 {
		return d.Attempts
	}
	if d.EffectiveAttempt == nil {
		return nil
	}
	out := Attempt{
		Attempt:    d.EffectiveAttempt.Attempt,
		OccurredAt: d.EffectiveAttempt.OccurredAt,
		Outcome:    d.EffectiveAttempt.Outcome,
	}
	if d.EffectiveAttempt.DurationMs != nil {
		out.DurationMs = *d.EffectiveAttempt.DurationMs
	}
	return []Attempt{out}
}

// DeliveryPage is one page of the list plus the cursor for the next one. The
// cursor is a delivery id: ids are bigserial, so the order is total and a page
// boundary can never skip or repeat a row.
type DeliveryPage struct {
	Deliveries []DeliverySummary
	NextCursor *int64
}

// ListDeliveries is FR-1101: filters plus cursor pagination, newest first.
//
// The page is fetched as limit+1 rows so "is there a next page" is answered by
// the same query rather than a second count, which also makes the answer exact
// under concurrent writes.
func (s *Store) ListDeliveries(ctx context.Context, sc Scope, f DeliveryFilter, limit int, cursor *int64) (DeliveryPage, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	var page DeliveryPage
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			select d.id, d.event_id, e.event_type, d.endpoint_id, d.consumer_id, d.status,
			       d.attempts, d.first_attempt_at, d.delivered_at, d.created_at,
			       d.non_delivery_reason, d.replay_of_delivery_id,
			       d.held_back_since, d.held_back_reason
			from deliveries d
			join events e on e.id = d.event_id
			where d.producer_id = $1 and d.env = $2
			  and ($3 = '' or d.endpoint_id = $3)
			  and ($4 = '' or d.consumer_id = $4)
			  and ($5 = '' or e.event_type = $5)
			  and ($6 = '' or d.status = $6)
			  and ($7::timestamptz is null or d.created_at >= $7)
			  and ($8::timestamptz is null or d.created_at <= $8)
			  and ($9::bigint is null or d.id < $9)
			order by d.id desc
			limit $10`,
			sc.ProducerID, sc.Env, f.EndpointID, f.ConsumerID, f.EventType, f.Status,
			f.From, f.To, cursor, limit+1)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var d DeliverySummary
			if err := rows.Scan(&d.ID, &d.EventID, &d.EventType, &d.EndpointID, &d.ConsumerID,
				&d.Status, &d.Attempts, &d.FirstAttemptAt, &d.DeliveredAt, &d.CreatedAt,
				&d.NonDeliveryReason, &d.ReplayOfDeliveryID,
				&d.HeldBackSince, &d.HeldBackReason); err != nil {
				return err
			}
			page.Deliveries = append(page.Deliveries, d)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(page.Deliveries) > limit {
			page.Deliveries = page.Deliveries[:limit]
			last := page.Deliveries[len(page.Deliveries)-1].ID
			page.NextCursor = &last
		}
		return nil
	})
	if err != nil {
		return DeliveryPage{}, err
	}
	return page, nil
}

// GetDelivery is FR-1102/FR-1103: one delivery with its ordered attempts, the
// effective attempt FR-1110 requires, and the deliveries a replay created from
// it (FR-1108).
func (s *Store) GetDelivery(ctx context.Context, sc Scope, id int64) (DeliveryDetail, error) {
	var out DeliveryDetail
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		var d DeliverySummary
		err := tx.QueryRow(ctx, `
			select d.id, d.event_id, e.event_type, d.endpoint_id, d.consumer_id, d.status,
			       d.attempts, d.first_attempt_at, d.delivered_at, d.created_at,
			       d.non_delivery_reason, d.replay_of_delivery_id,
			       d.held_back_since, d.held_back_reason
			from deliveries d
			join events e on e.id = d.event_id
			where d.producer_id = $1 and d.env = $2 and d.id = $3`,
			sc.ProducerID, sc.Env, id).Scan(&d.ID, &d.EventID, &d.EventType, &d.EndpointID,
			&d.ConsumerID, &d.Status, &d.Attempts, &d.FirstAttemptAt, &d.DeliveredAt,
			&d.CreatedAt, &d.NonDeliveryReason, &d.ReplayOfDeliveryID,
			&d.HeldBackSince, &d.HeldBackReason)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		out.Delivery = d
		if out.Attempts, err = attemptsFor(ctx, tx, d.ID); err != nil {
			return err
		}
		out.EffectiveAttempt = effectiveAttempt(d)
		out.ReplayedBy, err = replayedBy(ctx, tx, d.ID)
		return err
	})
	if err != nil {
		return DeliveryDetail{}, err
	}
	return out, nil
}

// attemptsFor reads one delivery's attempt rows in attempt order (FR-1102's
// "ordered attempts", which is the (delivery_id, attempt) unique index's own
// order in 0008). 0011 gives the tenant a select policy on `attempts`, which is
// what makes this a read rather than a definer function: the write path stays
// the definer functions in 0008, and the policy's predicate is the delivery's
// own scope, so a row of another tenant's delivery is invisible here too.
func attemptsFor(ctx context.Context, tx pgx.Tx, deliveryID int64) ([]Attempt, error) {
	rows, err := tx.Query(ctx, `
		select attempt, occurred_at, duration_ms, response_status, response_body,
		       error_class, outcome, next_retry_at
		from attempts where delivery_id = $1 order by attempt`, deliveryID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Attempt
	for rows.Next() {
		var a Attempt
		if err := rows.Scan(&a.Attempt, &a.OccurredAt, &a.DurationMs, &a.ResponseStatus,
			&a.ResponseBody, &a.ErrorClass, &a.Outcome, &a.NextRetryAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// effectiveAttempt synthesizes the attempt a delivery had when it left no
// attempt row (FR-1110, FR-811): the successful attempt, whose status, timestamp
// and duration live on the delivery row. A skipped delivery gets one too - it
// was never attempted, and an empty trail would read as "nothing happened"
// rather than "this endpoint could not take the event".
//
// A `failed_terminal` delivery needs no synthesis: its last attempt is a row.
func effectiveAttempt(d DeliverySummary) *EffectiveAttempt {
	switch d.Status {
	case statusDelivered:
		attemptNo := d.Attempts
		if attemptNo < 1 {
			attemptNo = 1
		}
		at := d.CreatedAt
		switch {
		case d.DeliveredAt != nil:
			at = *d.DeliveredAt
		case d.FirstAttemptAt != nil:
			at = *d.FirstAttemptAt
		}
		out := &EffectiveAttempt{Attempt: attemptNo, OccurredAt: at, Status: d.Status, Outcome: d.Status}
		// The duration is the successful attempt's own only when it was the
		// first attempt (FR-1110): after a retry, first_attempt_at is when the
		// delivery started, not when this attempt did, and delivery.DurationMs
		// carries the total for whoever wants it.
		if d.Attempts <= 1 && d.FirstAttemptAt != nil {
			out.DurationMs = d.DurationMs()
		}
		return out
	case statusSkipped:
		outcome := statusSkipped
		if d.NonDeliveryReason != nil {
			outcome = *d.NonDeliveryReason
		}
		return &EffectiveAttempt{Attempt: 1, OccurredAt: d.CreatedAt, Status: d.Status, Outcome: outcome}
	}
	return nil
}

// replayedBy lists the deliveries created by replaying this one, so the trail
// distinguishes a replay from a first attempt in both directions (FR-1108).
func replayedBy(ctx context.Context, tx pgx.Tx, deliveryID int64) ([]int64, error) {
	rows, err := tx.Query(ctx,
		`select id from deliveries where replay_of_delivery_id = $1 order by id`, deliveryID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// EventView is FR-1109's event-level view: the event, the fan-out outcome it
// recorded (§7.5a) and, per delivery, its status and trail - which is how FR-1111
// ("did my event succeed?") is answered from the API and the database rather than
// from application logs.
type EventView struct {
	EventID           string
	EventType         string
	RoutingMode       string
	ConsumerID        *string
	ConsumerRef       *string
	PayloadVersion    int
	OccurredAt        time.Time
	CreatedAt         time.Time
	RoutingState      *string
	NonDeliveryReason *string
	Rollup            string
	Deliveries        []DeliveryDetail
}

// EventView reads one event with every delivery it produced and computes the
// rollup (FR-1109). A foreign event id is ErrNotFound (FR-108).
func (s *Store) EventView(ctx context.Context, sc Scope, eventID string) (EventView, error) {
	var out EventView
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		out.EventID = eventID
		err := tx.QueryRow(ctx, `
			select event_type, routing_mode, consumer_id, consumer_ref, payload_version,
			       occurred_at, created_at, routing_state, non_delivery_reason
			from events
			where producer_id = $1 and env = $2 and id = $3`,
			sc.ProducerID, sc.Env, eventID).Scan(&out.EventType, &out.RoutingMode,
			&out.ConsumerID, &out.ConsumerRef, &out.PayloadVersion, &out.OccurredAt,
			&out.CreatedAt, &out.RoutingState, &out.NonDeliveryReason)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		out.Deliveries, err = deliveriesForEvent(ctx, tx, sc, eventID)
		if err != nil {
			return err
		}
		out.Rollup = Rollup(out.RoutingState, out.NonDeliveryReason, statusesOf(out.Deliveries))
		return nil
	})
	if err != nil {
		return EventView{}, err
	}
	return out, nil
}

// deliveriesForEvent reads every delivery of one event - the first fan-out rows
// and the ones a replay created - with its trail. It is the query
// deliveries_event_idx exists for.
//
// The deliveries are read to completion before the trails are read: pgx runs one
// statement per connection at a time, so a query issued inside an open row
// iterator fails the whole call with "conn busy" rather than streaming.
func deliveriesForEvent(ctx context.Context, tx pgx.Tx, sc Scope, eventID string) ([]DeliveryDetail, error) {
	rows, err := tx.Query(ctx, `
		select d.id, d.event_id, e.event_type, d.endpoint_id, d.consumer_id, d.status,
		       d.attempts, d.first_attempt_at, d.delivered_at, d.created_at,
		       d.non_delivery_reason, d.replay_of_delivery_id,
		       d.held_back_since, d.held_back_reason
		from deliveries d
		join events e on e.id = d.event_id
		where d.producer_id = $1 and d.env = $2 and d.event_id = $3
		order by d.id`,
		sc.ProducerID, sc.Env, eventID)
	if err != nil {
		return nil, err
	}
	var out []DeliveryDetail
	for rows.Next() {
		var detail DeliveryDetail
		var d DeliverySummary
		if err := rows.Scan(&d.ID, &d.EventID, &d.EventType, &d.EndpointID, &d.ConsumerID,
			&d.Status, &d.Attempts, &d.FirstAttemptAt, &d.DeliveredAt, &d.CreatedAt,
			&d.NonDeliveryReason, &d.ReplayOfDeliveryID,
			&d.HeldBackSince, &d.HeldBackReason); err != nil {
			rows.Close()
			return nil, err
		}
		detail.Delivery = d
		out = append(out, detail)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		if out[i].Attempts, err = attemptsFor(ctx, tx, out[i].Delivery.ID); err != nil {
			return nil, err
		}
		out[i].EffectiveAttempt = effectiveAttempt(out[i].Delivery)
	}
	return out, nil
}

func statusesOf(ds []DeliveryDetail) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = d.Delivery.Status
	}
	return out
}

// Rollup is FR-1109's rollup, in one place so the API and any future UI cannot
// disagree about it. Precedence, and the reason for it:
//
//  1. `unroutable` - §7.5a's consumer_ref matched no consumer. The event never
//     had a destination, so no deliveries exist and none will.
//  2. `pending` - at least one delivery is still in flight, so the outcome is
//     not settled yet and a "did it succeed?" answer would be premature.
//  3. `all_delivered` - every delivery of the event is `delivered`. A `skipped`
//     row breaks this: a matched endpoint that could not take the event means
//     the event did not fully deliver, whatever the other rows say.
//  4. `failed` - nothing was delivered: every row is terminal without a
//     delivery (`failed_terminal`, `skipped`), or the event produced no rows
//     and recorded an event-level reason of its own (§7.5a's `not_delivered:*`).
//  5. `partially_failed` - a mix: something was delivered and something was not.
//
// Two shapes worth naming. An event whose fan-out has not run yet has no rows
// and no reason, and reads `pending` - correctly, because the relay has not
// resolved it. A broadcast event reads `pending` too: M0's endpoint model is
// consumer-owned, so a broadcast event has no fan-out target and therefore no
// deliveries to report (SPEC.md, S3).
func Rollup(routingState *string, reason *string, statuses []string) string {
	if routingState != nil && *routingState == "unroutable" {
		return RollupUnroutable
	}
	if len(statuses) == 0 {
		if reason != nil && *reason != "" {
			return RollupFailed
		}
		return RollupPending
	}
	pending, delivered := 0, 0
	for _, st := range statuses {
		switch st {
		case statusPending:
			pending++
		case statusDelivered:
			delivered++
		}
	}
	switch {
	case pending > 0:
		return RollupPending
	case delivered == len(statuses):
		return RollupAllDelivered
	case delivered == 0:
		return RollupFailed
	default:
		return RollupPartiallyFailed
	}
}

// ReplayInput is one replay request. RequestKey is FR-1107's idempotency key:
// the same key twice is one set of new deliveries (AC-12). DryRun answers the
// count without creating anything, which is what a confirmation dialog needs
// (UI-5) and what makes the bound visible before the caller commits to it.
type ReplayInput struct {
	Filter     DeliveryFilter
	RequestKey string
	DryRun     bool
}

// ReplayResult is what a replay did, or what it would do on a dry run.
type ReplayResult struct {
	// Matched is how many existing deliveries the filter selected.
	Matched int
	// Created is how many new deliveries this call created; 0 on a repeat of a
	// request key, and 0 on a dry run.
	Created int
	// DeliveryIDs are the new deliveries: this call's own, or the ones the
	// first call with this key created. A repeat returns the same ids (AC-12).
	DeliveryIDs []int64
	// Repeated is true when the request key had already been used, so nothing
	// was created and DeliveryIDs is the original set.
	Repeated bool
	// ReleasePerSecond is the rate the new deliveries are released at, so the
	// caller can state it (UI-5: "count and rate").
	ReleasePerSecond int
}

// ReplayDeliveries is FR-1106/FR-1107/FR-1108.
//
// It creates a NEW delivery per selected row, linked by replay_of_delivery_id
// and carrying the same event, endpoint and consumer; it never touches the row
// it repairs - no status, no timestamp, no attempt row (FR-1106). The new rows
// are pending with their lease set, so the delivery worker picks them up exactly
// as it picks up a first attempt, spaced at ReplayReleasePerSecond per endpoint.
//
// What is replayable: a delivery that was attempted and reached a terminal
// state - `delivered` or `failed_terminal`, plus any terminal status a later
// slice adds.
// `pending` is in flight and must not be duplicated. `skipped` is refused on
// purpose: it was never attempted, and its reasons (an unverified endpoint, a
// paused endpoint or subscription) are §7.5a conditions whose fix is to change
// the endpoint's state, not to POST to it - replaying one would deliver to an
// endpoint FR-403 says must not receive live events.
//
// The whole call is one transaction. If the selection exceeds FR-1107's bound it
// returns ErrReplayBound and the transaction rolls back, so a refused request
// leaves no record and the same key can be used once the filter is narrowed.
func (s *Store) ReplayDeliveries(ctx context.Context, sc Scope, in ReplayInput) (ReplayResult, error) {
	if len(in.RequestKey) == 0 {
		return ReplayResult{}, fmt.Errorf("%w: a replay needs an idempotency key", ErrInvalid)
	}
	keyHash := sha256.Sum256([]byte(in.RequestKey))
	filterJSON, err := json.Marshal(in.Filter)
	if err != nil {
		return ReplayResult{}, fmt.Errorf("encode replay filter: %w", err)
	}

	var out ReplayResult
	out.ReleasePerSecond = ReplayReleasePerSecond
	err = s.scoped(ctx, sc, func(tx pgx.Tx) error {
		// How many rows the filter selects, in the same statement as the write
		// that depends on it (below), so a concurrent fan-out cannot move the
		// number between the check and the insert.
		var matched int
		if err := tx.QueryRow(ctx, selectionCountSQL, selectionArgs(sc, in.Filter)...).Scan(&matched); err != nil {
			return err
		}
		out.Matched = matched

		if in.DryRun {
			if matched > ReplayMaxPerRequest {
				return ErrReplayBound
			}
			return nil
		}

		// FR-1107: record the request key before creating anything, and treat a
		// conflict as "this request already ran" - the answer is the set it
		// created, which is what makes a retry safe (AC-12). A refused request
		// returns an error, so this row never commits.
		var inserted bool
		err := tx.QueryRow(ctx, `
			insert into replay_requests (producer_id, env, request_key_hash, filter)
			values ($1, $2, $3, $4)
			on conflict (producer_id, env, request_key_hash) do nothing
			returning true`,
			sc.ProducerID, sc.Env, keyHash[:], filterJSON).Scan(&inserted)
		if errors.Is(err, pgx.ErrNoRows) {
			// The key was used before: return that request's answer unchanged.
			// `created` is deliberately not read back - this call created
			// nothing, and reporting the first call's count here would tell a
			// retrying client it wrote rows twice (AC-12).
			err := tx.QueryRow(ctx, `
				select matched, delivery_ids from replay_requests
				where producer_id = $1 and env = $2 and request_key_hash = $3`,
				sc.ProducerID, sc.Env, keyHash[:]).Scan(&out.Matched, &out.DeliveryIDs)
			if err != nil {
				return err
			}
			out.Repeated = true
			return nil
		}
		if err != nil {
			return err
		}

		ids, err := replayInsert(ctx, tx, sc, in.Filter)
		if err != nil {
			return err
		}
		if len(ids) == 0 && matched > ReplayMaxPerRequest {
			// The insert declined to create anything because the selection is
			// over the bound; say so rather than reporting a successful no-op.
			return ErrReplayBound
		}
		out.Created = len(ids)
		out.DeliveryIDs = ids

		_, err = tx.Exec(ctx, `
			update replay_requests set matched = $4, created = $5, delivery_ids = $6
			where producer_id = $1 and env = $2 and request_key_hash = $3`,
			sc.ProducerID, sc.Env, keyHash[:], out.Matched, out.Created, ids)
		return err
	})
	if err != nil {
		return ReplayResult{}, err
	}
	if out.DeliveryIDs == nil {
		out.DeliveryIDs = []int64{}
	}
	return out, nil
}

// selectionWhere is the one definition of "the deliveries a replay selects",
// shared by the count and the insert so they can never disagree.
//
// `status not in ('pending', 'skipped')` is "attempted and terminal": it is
// written as an exclusion rather than a list of statuses so a terminal status a
// later slice adds (FR-1201's `failed_terminal`) is replayable without an edit
// here.
const selectionWhere = `
	d.producer_id = $1 and d.env = $2
	and d.status not in ('pending', 'skipped')
	and ($3 = '' or d.endpoint_id = $3)
	and ($4 = '' or d.consumer_id = $4)
	and ($5 = '' or e.event_type = $5)
	and ($6 = '' or d.status = $6)
	and ($7::timestamptz is null or d.created_at >= $7)
	and ($8::timestamptz is null or d.created_at <= $8)`

func selectionArgs(sc Scope, f DeliveryFilter) []any {
	return []any{sc.ProducerID, sc.Env, f.EndpointID, f.ConsumerID, f.EventType, f.Status, f.From, f.To}
}

const selectionCountSQL = `
	select count(*)
	from deliveries d
	join events e on e.id = d.event_id
	where ` + selectionWhere

// replayInsert creates the replay rows in one statement, and refuses to create
// anything when the selection is over FR-1107's bound - the bound is part of the
// write rather than a check that precedes it, so there is no window in which a
// larger selection could slip through.
//
// The lease spacing is FR-1107's 50/s per endpoint: row k of an endpoint's
// selection is released k/50 s after now, which is what the worker's claim gate
// reads (pigeon_claim_delivery). Ids are allocated in selection order per
// endpoint, so the release order is the replay's own order.
func replayInsert(ctx context.Context, tx pgx.Tx, sc Scope, f DeliveryFilter) ([]int64, error) {
	args := append(selectionArgs(sc, f), ReplayMaxPerRequest, ReplayReleasePerSecond)
	var ids []int64
	err := tx.QueryRow(ctx, `
		with candidates as (
			select d.id, d.event_id, d.endpoint_id, d.producer_id, d.env, d.consumer_id,
			       row_number() over (partition by d.endpoint_id order by d.id) as rn
			from deliveries d
			join events e on e.id = d.event_id
			where `+selectionWhere+`
		),
		inserted as (
			insert into deliveries
				(event_id, endpoint_id, producer_id, env, consumer_id, status,
				 replay_of_delivery_id, lease_expires_at)
			select c.event_id, c.endpoint_id, c.producer_id, c.env, c.consumer_id, 'pending',
			       c.id,
			       now() + make_interval(secs => (c.rn - 1)::double precision / $10::double precision)
			from candidates c
			where (select count(*) from candidates) <= $9
			returning id
		)
		select coalesce(array_agg(id order by id), '{}') from inserted`,
		args...).Scan(&ids)
	if err != nil {
		return nil, err
	}
	return ids, nil
}

// DedupeAnswer is FR-903's support answer: was this event a duplicate for this
// endpoint, and if so since when.
type DedupeAnswer struct {
	EndpointID string
	EventID    string
	// Duplicate is whether the pair is present at all: the 30-day answer, from
	// the deliveries row while its partition exists and from S8's archive once
	// it has been dropped (A-4).
	Duplicate bool
	// FirstDeliveryID and FirstSeenAt are the first delivery of the pair and
	// when it was created, or zero values when Duplicate is false.
	FirstDeliveryID int64
	FirstSeenAt     *time.Time
	// Deliveries is how many deliveries of that pair exist on the live side. A
	// number above 1 is a replay (FR-1106) or a re-fan-out, so it is part of the
	// answer rather than a detail: it is what distinguishes "we sent it twice"
	// from "we sent it once".
	Deliveries int
}

// DedupeAnswer answers FR-903 for one (endpoint, event) pair, read through the
// single name S8's archive joins into - `delivery_dedupe_all` - so the caller
// does not care which side answered. An endpoint outside the caller's scope is
// ErrNotFound (FR-108); a pair with no deliveries is not an error, it is
// `duplicate: false`.
func (s *Store) DedupeAnswer(ctx context.Context, sc Scope, endpointID, eventID string) (DedupeAnswer, error) {
	out := DedupeAnswer{EndpointID: endpointID, EventID: eventID}
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		var firstID *int64
		var firstSeen *time.Time
		err := tx.QueryRow(ctx, `
			select dd.first_delivery_id, dd.first_seen_at,
			       (select count(*) from deliveries d
			         where d.producer_id = $1 and d.env = $2
			           and d.endpoint_id = ep.id and d.event_id = $4)
			from endpoints ep
			left join delivery_dedupe_all dd
			       on dd.endpoint_id = ep.id and dd.event_id = $4
			where ep.producer_id = $1 and ep.env = $2 and ep.id = $3`,
			sc.ProducerID, sc.Env, endpointID, eventID).Scan(&firstID, &firstSeen, &out.Deliveries)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if firstID != nil {
			out.Duplicate = true
			out.FirstDeliveryID = *firstID
			out.FirstSeenAt = firstSeen
		}
		return nil
	})
	if err != nil {
		return DedupeAnswer{}, err
	}
	return out, nil
}
