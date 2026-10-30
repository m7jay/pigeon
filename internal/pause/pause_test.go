package pause

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newTestTracker returns a Tracker on an in-process Redis plus a clock the test
// owns, so the sliding hour can be crossed without waiting one.
func newTestTracker(t *testing.T) (*Tracker, func(time.Duration)) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	now := time.Now()
	tr := NewTracker(rdb, DefaultThresholds()).WithClock(func() time.Time { return now })
	return tr, func(d time.Duration) { now = now.Add(d) }
}

// FR-1203's N, exactly as A-1 states it: min(active, max(3, ceil(active/4))), and
// not applicable below two subscriptions where only the depth rule can act.
func TestBreadthTargetIsTheDecidedRule(t *testing.T) {
	thr := DefaultThresholds()
	for _, tc := range []struct {
		active int
		want   int
	}{
		{1, 0}, // no endpoint-level rule below MinActive
		{2, 2}, // "every subscription if there are fewer than three"
		{3, 3},
		{4, 3},
		{9, 3},
		{10, 3}, // A-1's example
		{12, 3},
		{13, 4},
		{50, 13}, // A-1's example
		{132, 33},
		{133, 34}, // the size at which the deleted depth trigger would have fired
		{200, 50},
	} {
		if got := thr.BreadthTarget(tc.active); got != tc.want {
			t.Errorf("BreadthTarget(active=%d) = %d, want %d (FR-1203)", tc.active, got, tc.want)
		}
	}
	// The floor exists so one bad event type cannot pause a small endpoint, and
	// the subscription depth A-1 names is 10, not AC-14's stale 50.
	if thr.SubscriptionDepth != 10 || thr.BreadthFloor != 3 {
		t.Errorf("thresholds = %+v, want depth 10 and a 3-attempt floor (FR-1202, FR-1203)", thr)
	}
}

// FR-1209: the window is a sliding hour, and a success never touches it - which
// is why Observe is only called for a failed attempt.
func TestWindowSlidesAndForgets(t *testing.T) {
	tr, advance := newTestTracker(t)
	ctx := context.Background()
	obs := Observation{EndpointID: "ep_1", EventType: "order.created", At: time.Now()}

	for i := 0; i < 3; i++ {
		v, err := tr.Observe(ctx, obs)
		if err != nil {
			t.Fatalf("Observe: %v", err)
		}
		if v.Depth != i+1 {
			t.Fatalf("depth after %d failures = %d, want %d", i+1, v.Depth, i+1)
		}
	}

	// 61 minutes later every earlier failure has left the window.
	advance(61 * time.Minute)
	obs.At = obs.At.Add(61 * time.Minute)
	v, err := tr.Observe(ctx, obs)
	if err != nil {
		t.Fatalf("Observe after the window: %v", err)
	}
	if v.Depth != 1 {
		t.Errorf("depth after the hour elapsed = %d, want 1: the window slides (FR-1209)", v.Depth)
	}
	// And a subscription whose window lapsed is no longer failing, even though it
	// was a member of the breadth set a moment ago.
	if v.FailingSubscriptions != 0 {
		t.Errorf("failing subscriptions = %d, want 0: a lapsed window is not a failure", v.FailingSubscriptions)
	}
}

// FR-1203's breadth set holds distinct subscriptions that each cleared the floor,
// and a subscription that fails repeatedly still costs exactly one member.
func TestBreadthCountsSubscriptionsNotAttempts(t *testing.T) {
	tr, _ := newTestTracker(t)
	ctx := context.Background()
	at := time.Now()

	// One subscription failing ten times is one failing subscription, not ten -
	// and it does not count at all until it clears the 3-attempt floor, which is
	// what keeps one bad event from pausing anything (A-1).
	for i := 0; i < 10; i++ {
		v, err := tr.Observe(ctx, Observation{EndpointID: "ep_1", EventType: "a.b", At: at})
		if err != nil {
			t.Fatalf("Observe: %v", err)
		}
		wantFailing := 0
		if i+1 >= 3 {
			wantFailing = 1
		}
		if v.FailingSubscriptions != wantFailing {
			t.Fatalf("after %d failures on one subscription: %d failing, want %d",
				i+1, v.FailingSubscriptions, wantFailing)
		}
	}
	// FR-1203's count includes a PAUSED subscription: a pause writes the durable
	// flag and never touches these windows, and a membership only goes on resume,
	// so the subscription that just tripped its own depth of ten still counts
	// toward the endpoint's breadth.
	if v, _ := tr.FailingSubscriptions(ctx, "ep_1", at); v != 1 {
		t.Errorf("after ten failures on one subscription: %d failing, want 1 - the count includes paused subscriptions (FR-1203)",
			v)
	}
	// Two more subscriptions reach the 3-attempt floor: three fail in total.
	for _, et := range []string{"c.d", "e.f"} {
		for i := 0; i < 3; i++ {
			if _, err := tr.Observe(ctx, Observation{EndpointID: "ep_1", EventType: et, At: at}); err != nil {
				t.Fatalf("Observe %s: %v", et, err)
			}
		}
	}
	v, err := tr.FailingSubscriptions(ctx, "ep_1", at)
	if err != nil {
		t.Fatalf("FailingSubscriptions: %v", err)
	}
	if v != 3 {
		t.Errorf("failing subscriptions = %d, want 3 (three distinct subscriptions over the floor)", v)
	}
	// A subscription below the floor does not count: two failures on a fourth.
	for i := 0; i < 2; i++ {
		if _, err := tr.Observe(ctx, Observation{EndpointID: "ep_1", EventType: "g.h", At: at}); err != nil {
			t.Fatalf("Observe g.h: %v", err)
		}
	}
	v, _ = tr.FailingSubscriptions(ctx, "ep_1", at)
	if v != 3 {
		t.Errorf("failing subscriptions = %d, want 3: the 3-attempt floor keeps one bad event from counting", v)
	}
	// And the windows are per (endpoint, event type): another endpoint shares none.
	if _, err := tr.Observe(ctx, Observation{EndpointID: "ep_2", EventType: "a.b", At: at}); err != nil {
		t.Fatalf("Observe ep_2: %v", err)
	}
	if v, _ := tr.FailingSubscriptions(ctx, "ep_2", at); v != 0 {
		t.Errorf("failing subscriptions on the untouched endpoint = %d, want 0", v)
	}
}

// FR-1211: resuming a subscription clears its window and its breadth membership;
// resuming an endpoint clears the set AND the windows of its former members, so
// the endpoint cannot re-pause on one further failure each.
func TestResumeClearsTheCounterState(t *testing.T) {
	tr, _ := newTestTracker(t)
	ctx := context.Background()
	at := time.Now()

	for _, et := range []string{"a.b", "c.d", "e.f"} {
		for i := 0; i < 3; i++ {
			if _, err := tr.Observe(ctx, Observation{EndpointID: "ep_1", EventType: et, At: at}); err != nil {
				t.Fatalf("Observe %s: %v", et, err)
			}
		}
	}
	if v, _ := tr.FailingSubscriptions(ctx, "ep_1", at); v != 3 {
		t.Fatalf("precondition: %d failing subscriptions, want 3", v)
	}

	// A subscription resume forgives that subscription only.
	if err := tr.ClearSubscription(ctx, "ep_1", "a.b"); err != nil {
		t.Fatalf("ClearSubscription: %v", err)
	}
	if v, _ := tr.FailingSubscriptions(ctx, "ep_1", at); v != 2 {
		t.Errorf("after one subscription resume: %d failing, want 2", v)
	}
	if v, err := tr.Observe(ctx, Observation{EndpointID: "ep_1", EventType: "a.b", At: at}); err != nil || v.Depth != 1 {
		t.Errorf("the resumed subscription's window = %d (err %v), want a fresh 1", v.Depth, err)
	}

	// An endpoint resume forgives every former member and reports which.
	forgiven, err := tr.ClearBreadth(ctx, "ep_1")
	if err != nil {
		t.Fatalf("ClearBreadth: %v", err)
	}
	if len(forgiven) != 2 {
		t.Errorf("ClearBreadth reported %v, want the two remaining members (FR-1211)", forgiven)
	}
	if v, _ := tr.FailingSubscriptions(ctx, "ep_1", at); v != 0 {
		t.Errorf("after an endpoint resume: %d failing, want 0", v)
	}
	// Each former member now needs a fresh floor: two failures leave it out.
	for _, et := range []string{"c.d", "e.f"} {
		for i := 0; i < 2; i++ {
			if _, err := tr.Observe(ctx, Observation{EndpointID: "ep_1", EventType: et, At: at}); err != nil {
				t.Fatalf("Observe %s: %v", et, err)
			}
		}
	}
	if v, _ := tr.FailingSubscriptions(ctx, "ep_1", at); v != 0 {
		t.Errorf("%d subscriptions re-qualified after two fresh failures each; FR-1211 wants none until three", v)
	}
	for _, et := range []string{"c.d", "e.f"} {
		if _, err := tr.Observe(ctx, Observation{EndpointID: "ep_1", EventType: et, At: at}); err != nil {
			t.Fatalf("Observe %s: %v", et, err)
		}
	}
	if v, _ := tr.FailingSubscriptions(ctx, "ep_1", at); v != 2 {
		t.Errorf("%d subscriptions re-qualified after three fresh failures each, want 2", v)
	}
}

// A worker with no Redis disables the thresholds instead of panicking on a path
// no P0 requirement covers today (§9's limits are Redis; the deployment without
// one simply has no pause detection, which the boot path says out loud).
func TestDisabledTrackerIsInert(t *testing.T) {
	tr := NewTracker(nil, DefaultThresholds())
	if tr.Enabled() {
		t.Fatal("a tracker with no client must report itself disabled")
	}
	ctx := context.Background()
	if v, err := tr.Observe(ctx, Observation{EndpointID: "e", EventType: "t", At: time.Now()}); err != nil || v.Depth != 0 {
		t.Errorf("Observe on a disabled tracker = (%+v, %v), want a zero verdict and no error", v, err)
	}
	if forgiven, err := tr.ClearBreadth(ctx, "e"); err != nil || forgiven != nil {
		t.Errorf("ClearBreadth on a disabled tracker = (%v, %v), want nothing and no error", forgiven, err)
	}
	if err := tr.ClearSubscription(ctx, "e", "t"); err != nil {
		t.Errorf("ClearSubscription on a disabled tracker: %v", err)
	}
}
