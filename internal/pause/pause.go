// Package pause decides the two failure thresholds of §7.12 / FR-1202 / FR-1203
// and keeps the counters they are evaluated on.
//
// The decided model for gap 1 (SPEC.md A-1, as amended in FR-1202/FR-1203/
// FR-1209/FR-1210/FR-1211 on docs/spec-gap-closure, not the AC-14 sentence it
// replaced) is:
//
//   - a SUBSCRIPTION pauses on depth: 10 failed attempts for one
//     (endpoint, event_type) inside a sliding hour (FR-1202);
//   - an ENDPOINT pauses on breadth: N = min(active, max(3, ceil(active/4)))
//     distinct subscriptions each having accumulated 3 failed attempts inside the
//     same sliding hour, where active is the endpoint's count of unpaused
//     subscriptions and active >= 2 (FR-1203). A depth trigger was deleted
//     rather than kept beside it: a paused subscription stops producing
//     failures, so an endpoint accumulates at most 10 per subscription and a
//     fixed number measures a subscription count, not a failure signal.
//
// A "failure" is one attempt that did not return 2xx - transport error, timeout
// or a non-2xx response, retriable or terminal. Successes never touch a counter,
// which is why every Observe call is a failure by construction.
//
// The counters are Redis, and the pause they decide is NOT (D-8): losing these
// keys can delay a pause by up to one window, and can never invent one, because
// the flag lives in Postgres and the windows only feed the decision.
//
// Membership in the breadth set is a PROJECTION of the per-subscription windows,
// re-derived on every observation rather than maintained as a second state
// machine. Two independent records of the same fact is how FR-1203's predicate
// ("has accumulated 3 within the hour") and the set's membership drift apart -
// and the drift is visible: a subscription whose window lapses must stop
// counting, and nothing would tell the set.
package pause

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Thresholds is A-1's numeric model, in one place so the numbers in the SPEC and
// the numbers in the code cannot disagree.
type Thresholds struct {
	// SubscriptionDepth is FR-1202's 10 failures for one (endpoint, event_type).
	SubscriptionDepth int
	// BreadthFloor is the 3-attempt floor below which a subscription does not
	// count as failing - it is what stops one bad event type from pausing an
	// endpoint (FR-1203).
	BreadthFloor int
	// BreadthDivisor is the 4 in ceil(active/4): a quarter of the endpoint's live
	// subscription surface.
	BreadthDivisor int
	// MinActive is the smallest endpoint the breadth rule applies to (FR-1203:
	// active >= 2). Below it, only the subscription level can pause anything.
	MinActive int
	// Window is the sliding hour both thresholds are measured over (FR-1202,
	// FR-1203, FR-1209).
	Window time.Duration
}

// DefaultThresholds is the decided model.
func DefaultThresholds() Thresholds {
	return Thresholds{
		SubscriptionDepth: 10,
		BreadthFloor:      3,
		BreadthDivisor:    4,
		MinActive:         2,
		Window:            time.Hour,
	}
}

// BreadthTarget is FR-1203's N for an endpoint with `active` unpaused
// subscriptions, or 0 when the endpoint level does not apply at all (fewer than
// MinActive subscriptions). 10 subscriptions give 3, 2 give 2, 50 give 13.
func (t Thresholds) BreadthTarget(active int) int {
	if active < t.MinActive {
		return 0
	}
	divisor := t.BreadthDivisor
	if divisor < 1 {
		divisor = 1
	}
	quarter := int(math.Ceil(float64(active) / float64(divisor)))
	if quarter < t.BreadthFloor {
		quarter = t.BreadthFloor
	}
	if quarter > active {
		quarter = active
	}
	return quarter
}

// Tracker keeps the failure windows in Redis.
type Tracker struct {
	rdb *redis.Client
	thr Thresholds
	now func() time.Time
}

// NewTracker wraps a connected client. A nil client yields a nil Tracker whose
// methods are safe to call and report nothing observed, so a deployment without
// Redis keeps working exactly as it did before S5 (no thresholds) instead of
// panicking on a path a P0 requirement does not cover today.
func NewTracker(rdb *redis.Client, thr Thresholds) *Tracker {
	if thr.SubscriptionDepth <= 0 {
		thr = DefaultThresholds()
	}
	return &Tracker{rdb: rdb, thr: thr, now: time.Now}
}

// WithClock replaces the time source; tests use it to move a window's boundary.
func (t *Tracker) WithClock(now func() time.Time) *Tracker {
	t.now = now
	return t
}

// Thresholds returns the model this Tracker evaluates.
func (t *Tracker) Thresholds() Thresholds { return t.thr }

// Enabled reports whether the tracker has a store to count in.
func (t *Tracker) Enabled() bool { return t != nil && t.rdb != nil }

// WindowKey is the per-subscription window: one member per failed attempt, scored
// by the attempt's time. The subscription IS the (endpoint, event_type) pair
// (FR-501), so this key names exactly one subscription.
func WindowKey(endpointID, eventType string) string {
	return "pigeon:pause:sub:" + endpointID + ":" + eventType
}

// BreadthKey is the endpoint's breadth set: one member per subscription that
// currently holds BreadthFloor or more failures, scored by its latest failure.
func BreadthKey(endpointID string) string { return "pigeon:pause:subs:" + endpointID }

// Observation is one failed attempt entering the windows.
type Observation struct {
	EndpointID string
	EventType  string
	// At is when the attempt happened. It is the score of the window member and
	// the right edge of the sliding window.
	At time.Time
}

// Verdict is what the windows say after the observation.
type Verdict struct {
	// Depth is the subscription's failure count inside the window.
	Depth int
	// FailingSubscriptions is how many distinct subscriptions on the endpoint
	// currently hold BreadthFloor or more failures.
	FailingSubscriptions int
}

// observeScript records one failed attempt and re-derives the subscription's
// membership in the breadth set in the same atomic step, so the set can never be
// a step behind its window.
//
// KEYS[1] the subscription window, KEYS[2] the endpoint's breadth set.
// ARGV: now_ms, cutoff_ms, member, latest_ms, event_type, floor, ttl_ms.
const observeScript = `
local window, breadth = KEYS[1], KEYS[2]
local now      = tonumber(ARGV[1])
local cutoff   = tonumber(ARGV[2])
local member   = ARGV[3]
local latest   = tonumber(ARGV[4])
local event    = ARGV[5]
local floor    = tonumber(ARGV[6])
local ttl      = tonumber(ARGV[7])

-- Drop everything that has fallen out of the sliding hour, from the window and
-- from the endpoint's breadth set, before counting either (FR-1202, FR-1203).
redis.call('ZREMRANGEBYSCORE', window, '-inf', cutoff)
redis.call('ZREMRANGEBYSCORE', breadth, '-inf', cutoff)

redis.call('ZADD', window, now, member)
local depth = redis.call('ZCARD', window)

-- Membership is the projection: in while the window holds >= floor, out as soon
-- as it does not.
if depth >= floor then
  redis.call('ZADD', breadth, latest, event)
else
  redis.call('ZREM', breadth, event)
end

redis.call('PEXPIRE', window, ttl)
redis.call('PEXPIRE', breadth, ttl)
return depth
`

var observeCmd = redis.NewScript(observeScript)

// Observe records one failed attempt for a subscription and returns the counts
// the thresholds are evaluated on. The endpoint-level count is re-derived from
// the member windows, so a subscription that has stopped failing - or whose
// hour has simply lapsed - is not counted (see the package comment).
func (t *Tracker) Observe(ctx context.Context, o Observation) (Verdict, error) {
	if !t.Enabled() {
		return Verdict{}, nil
	}
	at := o.At
	if at.IsZero() {
		at = t.now()
	}
	cutoff := at.Add(-t.thr.Window)
	depth, err := observeCmd.Run(ctx, t.rdb,
		[]string{WindowKey(o.EndpointID, o.EventType), BreadthKey(o.EndpointID)},
		at.UnixMilli(), cutoff.UnixMilli(),
		fmt.Sprintf("%d-%d", at.UnixNano(), attemptMemberSeq()), at.UnixMilli(),
		o.EventType, t.thr.BreadthFloor, (t.thr.Window + time.Minute).Milliseconds(),
	).Int()
	if err != nil {
		return Verdict{}, fmt.Errorf("pause: observe %s/%s: %w", o.EndpointID, o.EventType, err)
	}
	failing, err := t.FailingSubscriptions(ctx, o.EndpointID, at)
	if err != nil {
		return Verdict{}, err
	}
	return Verdict{Depth: depth, FailingSubscriptions: failing}, nil
}

// FailingSubscriptions counts the endpoint's subscriptions currently inside the
// breadth set, and prunes any member whose own window has fallen below the floor.
// Pruning here as well as in the write path is what makes the count independent
// of which subscription happens to fail next.
func (t *Tracker) FailingSubscriptions(ctx context.Context, endpointID string, at time.Time) (int, error) {
	if !t.Enabled() {
		return 0, nil
	}
	if at.IsZero() {
		at = t.now()
	}
	cutoff := at.Add(-t.thr.Window)
	members, err := t.members(ctx, endpointID, cutoff)
	if err != nil {
		return 0, err
	}
	if len(members) == 0 {
		return 0, nil
	}
	// One pipeline: count each member's window, then forget the ones that no
	// longer qualify. Fewer round trips than the number of subscriptions
	// matters, because this runs at most once per failed attempt.
	pipe := t.rdb.Pipeline()
	counts := make([]*redis.IntCmd, len(members))
	for i, m := range members {
		counts[i] = pipe.ZCount(ctx, WindowKey(endpointID, m), fmt.Sprintf("(%d", cutoff.UnixMilli()), "+inf")
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, fmt.Errorf("pause: count windows for %s: %w", endpointID, err)
	}
	failing := 0
	stale := make([]any, 0, len(members))
	for i, m := range members {
		n, err := counts[i].Result()
		if err != nil {
			return 0, fmt.Errorf("pause: count window %s: %w", m, err)
		}
		if int(n) >= t.thr.BreadthFloor {
			failing++
			continue
		}
		stale = append(stale, m)
	}
	if len(stale) > 0 {
		if err := t.rdb.ZRem(ctx, BreadthKey(endpointID), stale...).Err(); err != nil {
			return 0, fmt.Errorf("pause: prune lapsed breadth members for %s: %w", endpointID, err)
		}
	}
	return failing, nil
}

// FailingMembers returns the endpoint's failing subscriptions right now, for
// logging and for the notification an endpoint pause names.
func (t *Tracker) FailingMembers(ctx context.Context, endpointID string) ([]string, error) {
	if !t.Enabled() {
		return nil, nil
	}
	return t.members(ctx, endpointID, t.now().Add(-t.thr.Window))
}

func (t *Tracker) members(ctx context.Context, endpointID string, cutoff time.Time) ([]string, error) {
	if err := t.rdb.ZRemRangeByScore(ctx, BreadthKey(endpointID), "-inf", fmt.Sprintf("%d", cutoff.UnixMilli())).Err(); err != nil {
		return nil, fmt.Errorf("pause: trim breadth for %s: %w", endpointID, err)
	}
	members, err := t.rdb.ZRange(ctx, BreadthKey(endpointID), 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("pause: read breadth for %s: %w", endpointID, err)
	}
	return members, nil
}

// ClearSubscription is FR-1211's subscription half: resuming a subscription
// deletes its attempt window and its membership in the endpoint's breadth set, so
// it re-qualifies only after accumulating BreadthFloor fresh failures.
func (t *Tracker) ClearSubscription(ctx context.Context, endpointID, eventType string) error {
	if !t.Enabled() {
		return nil
	}
	pipe := t.rdb.Pipeline()
	pipe.Del(ctx, WindowKey(endpointID, eventType))
	pipe.ZRem(ctx, BreadthKey(endpointID), eventType)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("pause: clear window %s/%s: %w", endpointID, eventType, err)
	}
	return nil
}

// ClearBreadth is FR-1211's endpoint half: it removes the breadth set AND the
// window of every subscription that was in it, returning those event types so
// the caller can report what a resume forgave. Clearing only the set would leave
// those windows at or above the floor, FR-1203's predicate would still hold, and
// the endpoint would re-pause after one further failure per subscription -
// exactly the recurrence the requirement says resume prevents.
func (t *Tracker) ClearBreadth(ctx context.Context, endpointID string) ([]string, error) {
	if !t.Enabled() {
		return nil, nil
	}
	members, err := t.rdb.ZRange(ctx, BreadthKey(endpointID), 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("pause: read breadth for %s: %w", endpointID, err)
	}
	pipe := t.rdb.Pipeline()
	pipe.Del(ctx, BreadthKey(endpointID))
	for _, eventType := range members {
		pipe.Del(ctx, WindowKey(endpointID, eventType))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("pause: clear breadth for %s: %w", endpointID, err)
	}
	return members, nil
}

// Notifier is the seam FR-1204's "configured address" will plug into. v1.0 has
// no mailer until the portals land (S9), so the shipped implementation logs and
// the durable record is the pause_notifications row the store writes in the same
// transaction as the pause itself: that row is what makes "notify once" true and
// testable, and it is what a mailer will read if the process dies between the
// pause and the send.
type Notifier interface {
	NotifyPaused(ctx context.Context, n Notification) error
}

// Notification is the FR-1204 message: the endpoint, and either the event type
// (subscription pause) or the count of failing subscriptions (endpoint pause),
// plus the first and last failing response in the window that tripped it.
type Notification struct {
	Kind                 string // KindSubscription | KindEndpoint
	ProducerID           string
	Env                  string
	EndpointID           string
	EventType            string
	FailingSubscriptions int
	FirstResponse        string
	LastResponse         string
	PausedAt             time.Time
}

// The two pause kinds FR-1204's message distinguishes: which subscription (and
// which event type) tripped, or how many subscriptions did.
const (
	KindSubscription = "subscription"
	KindEndpoint     = "endpoint"
)

var memberSeq atomic.Int64

func attemptMemberSeq() int64 {
	// A counter is enough to keep two attempts with the same nanosecond apart;
	// the member only has to be unique inside one endpoint's window.
	return memberSeq.Add(1)
}
