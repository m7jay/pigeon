package pause

import (
	"context"
	"fmt"
	"time"

	"github.com/m7jay/pigeon/internal/store"
)

// Resume is FR-1205 and FR-1211: ONE action per scope, and no replay.
//
// It spans two stores on purpose, and the two halves are ordered deliberately.
// The durable flag lives in Postgres (D-8, the pause cannot be invented or lost by
// a Redis flush) and the counters that decide a pause live in Redis. FR-1211 says
// resume clears "the pause and that key's counter state", so a resume that only
// cleared the flag would leave membership and windows intact - FR-1203's predicate
// would still hold and the endpoint would re-pause after one further failure per
// subscription, which is exactly the recurrence the requirement says resume
// prevents.
//
// The counters go first. If the flag write then fails, a retry of the same action
// completes it and nothing was lost: the scope is still paused (the conservative
// state) with clean counters. The reverse order risks reporting a successful
// resume that re-pauses on the next failure, which is the one outcome FR-1211
// exists to forbid.
//
// Nothing here replays anything (FR-1205). Held deliveries resume where their
// schedule left off, with the paused period added back to their window (FR-1207),
// which the SQL half does.
type Resumer struct {
	store    *store.Store
	counters *Tracker
}

// NewResumer wires the durable half (Postgres) to the decision half (Redis).
func NewResumer(st *store.Store, counters *Tracker) *Resumer {
	return &Resumer{store: st, counters: counters}
}

// ResumeOutcome reports what a resume did, in the terms an operator sees.
type ResumeOutcome struct {
	Scope      string // KindSubscription | KindEndpoint
	EndpointID string
	EventType  string
	ProducerID string
	Env        string
	Resumed    bool
	// PausedFor is the period FR-1207 adds back to every held delivery's window.
	PausedFor time.Duration
	// Forgiven lists the event types whose attempt windows an endpoint resume
	// cleared (FR-1211). Empty for a subscription resume and for an endpoint that
	// had no failing members.
	Forgiven []string
}

// ResumeSubscription resumes one (endpoint, event_type): its own window and its
// membership in the endpoint's breadth set go, and nothing else is touched.
func (r *Resumer) ResumeSubscription(ctx context.Context, endpointID, eventType string) (ResumeOutcome, error) {
	out := ResumeOutcome{Scope: KindSubscription, EndpointID: endpointID, EventType: eventType}
	if r.counters != nil && r.counters.Enabled() {
		if err := r.counters.ClearSubscription(ctx, endpointID, eventType); err != nil {
			return out, fmt.Errorf("resume subscription: clear counters: %w", err)
		}
	}
	res, err := r.store.ResumeSubscription(ctx, endpointID, eventType)
	if err != nil {
		return out, err
	}
	out.Resumed, out.ProducerID, out.Env, out.PausedFor = res.Resumed, res.ProducerID, res.Env, res.PausedFor
	return out, nil
}

// ResumeEndpoint resumes an endpoint: the breadth set AND the windows of its former
// members go (FR-1211), and the subscriptions that had tripped their own depth
// threshold stay paused, because the endpoint pause never wrote rows for them
// (FR-1210). That is what makes an endpoint resume restore every subscription that
// had not tripped its own threshold and leave behind exactly those that did.
func (r *Resumer) ResumeEndpoint(ctx context.Context, endpointID string) (ResumeOutcome, error) {
	out := ResumeOutcome{Scope: KindEndpoint, EndpointID: endpointID}
	if r.counters != nil && r.counters.Enabled() {
		forgiven, err := r.counters.ClearBreadth(ctx, endpointID)
		if err != nil {
			return out, fmt.Errorf("resume endpoint: clear counters: %w", err)
		}
		out.Forgiven = forgiven
	}
	res, err := r.store.ResumeEndpoint(ctx, endpointID)
	if err != nil {
		return out, err
	}
	out.Resumed, out.ProducerID, out.Env, out.PausedFor = res.Resumed, res.ProducerID, res.Env, res.PausedFor
	return out, nil
}
