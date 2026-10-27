package limit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// The per-endpoint limits: cluster-wide, atomic, separate (FR-1302, FR-1306,
// FR-1307), and AC-41's observable - the effective capacity does not grow with the
// number of replicas sharing the Redis.

func newRedisRegistry(t *testing.T) (*Redis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	r, err := NewRedisFrom(rdb)
	if err != nil {
		t.Fatalf("NewRedisFrom: %v", err)
	}
	return r, rdb
}

// helper: hold a slot, failing the test if it is refused.
func mustAcquire(t *testing.T, ctx context.Context, r Registry, endpoint string, cap int, ttl time.Duration, holder string) Lease {
	t.Helper()
	lease, ok, wait, err := r.Acquire(ctx, endpoint, cap, ttl, holder)
	if err != nil {
		t.Fatalf("Acquire(%s by %s): %v", endpoint, holder, err)
	}
	if !ok {
		t.Fatalf("Acquire(%s by %s) refused with wait=%s; the cap was not reached", endpoint, holder, wait)
	}
	return lease
}

// AC-41: "the endpoint never observes more than 10 in-flight requests; observed
// count is identical with 1 and 2 replicas". Two Registry values over one Redis is
// exactly the two-replica case, and the third is the 1-replica case.
func TestCapHoldsAcrossReplicas(t *testing.T) {
	ctx := context.Background()
	replicaA, rdb := newRedisRegistry(t)
	replicaB, err := NewRedisFrom(rdb)
	if err != nil {
		t.Fatalf("second replica: %v", err)
	}
	const capN, ttl, endpoint = 10, 30 * time.Second, "ep_shared"

	// One replica alone reaches exactly the cap, no more.
	oneReplica := make([]Lease, 0, capN)
	for i := 0; i < capN; i++ {
		oneReplica = append(oneReplica, mustAcquire(t, ctx, replicaA, endpoint, capN, ttl, "replicaA"))
	}
	if _, ok, _, err := replicaA.Acquire(ctx, endpoint, capN, ttl, "replicaA"); err != nil || ok {
		t.Fatalf("the 11th slot on one replica = (ok=%v, err=%v), want refused (FR-1302)", ok, err)
	}
	for _, l := range oneReplica {
		l.Release(ctx)
	}
	if n, err := rdb.ZCard(ctx, ConcurrencyKey(endpoint)).Result(); err != nil || n != 0 {
		t.Fatalf("after releasing every slot the set holds %d (err %v), want 0", n, err)
	}

	// Now the two replicas together: ten slots are all they get, whichever one
	// asks, so the endpoint's in-flight count is the same number.
	held := make([]Lease, 0, capN)
	for i := 0; i < capN; i++ {
		r := replicaA
		holder := "replicaA"
		if i%2 == 1 {
			r, holder = replicaB, "replicaB"
		}
		held = append(held, mustAcquire(t, ctx, r, endpoint, capN, ttl, holder))
	}
	if n, err := rdb.ZCard(ctx, ConcurrencyKey(endpoint)).Result(); err != nil || n != capN {
		t.Fatalf("in-flight slots with two replicas = %d (err %v), want %d", n, err, capN)
	}
	// Both replicas see the shared set as full: the limit is the endpoint's.
	for _, r := range []*Redis{replicaA, replicaB} {
		if _, ok, _, err := r.Acquire(ctx, endpoint, capN, ttl, "late"); err != nil || ok {
			t.Errorf("a replica acquired an 11th slot (ok=%v, err=%v); the cap must not scale with replicas (FR-1307)", ok, err)
		}
	}
	// A release by one replica frees capacity for the other, which is what makes
	// the shared set the authority rather than a local count.
	held[0].Release(ctx)
	mustAcquire(t, ctx, replicaB, endpoint, capN, ttl, "replicaB")
	for _, l := range held[1:] {
		l.Release(ctx)
	}
}

// FR-1302: "a crashed worker's slots expire". A worker that dies holding a slot
// leaks nothing - the member is scored by its expiry and purged by the next
// acquisition.
func TestCrashedHolderSlotsExpire(t *testing.T) {
	ctx := context.Background()
	r, rdb := newRedisRegistry(t)
	const endpoint = "ep_crash"

	// Held with a 60 ms lease and never released, the way a SIGKILL leaks one.
	mustAcquire(t, ctx, r, endpoint, 1, 60*time.Millisecond, "dead")
	if _, ok, _, err := r.Acquire(ctx, endpoint, 1, 60*time.Millisecond, "next"); err != nil || ok {
		t.Fatalf("cap 1 with one live holder = (ok=%v, err=%v), want refused", ok, err)
	}
	time.Sleep(90 * time.Millisecond)
	mustAcquire(t, ctx, r, endpoint, 1, 60*time.Millisecond, "next")
	if n, err := rdb.ZCard(ctx, ConcurrencyKey(endpoint)).Result(); err != nil || n != 1 {
		t.Fatalf("after the dead holder expired the set holds %d (err %v), want 1", n, err)
	}
}

// FR-1306: RPS is a rate over time, not a counter per second, and it is a
// DIFFERENT mechanism from the concurrency cap - a bucket bounds how many start,
// a slot bounds how many are in flight.
func TestRateBucketIsContinuousAndSeparateFromConcurrency(t *testing.T) {
	ctx := context.Background()
	r, _ := newRedisRegistry(t)
	const endpoint, rps, burst = "ep_rate", 5.0, 5

	// A burst is available immediately, then the bucket is empty.
	for i := 0; i < burst; i++ {
		ok, wait, err := r.Allow(ctx, endpoint, rps, burst)
		if err != nil || !ok {
			t.Fatalf("token %d of the burst = (ok=%v, wait=%s, err=%v)", i+1, ok, wait, err)
		}
	}
	ok, wait, err := r.Allow(ctx, endpoint, rps, burst)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if ok {
		t.Fatal("the bucket served a 6th token from a burst of 5")
	}
	// At 5 RPS a token is 200 ms away; the wait must reflect the rate rather than a
	// whole second, which is the difference between a bucket and a counter.
	if wait < 100*time.Millisecond || wait > 400*time.Millisecond {
		t.Errorf("wait for the next token = %s, want about 200 ms at 5 RPS (FR-1306)", wait)
	}
	// And the bucket refills continuously.
	time.Sleep(250 * time.Millisecond)
	if ok, _, err := r.Allow(ctx, endpoint, rps, burst); err != nil || !ok {
		t.Errorf("after 250 ms a token was refused (ok=%v, err=%v); the bucket refills continuously", ok, err)
	}

	// The two mechanisms do not share state: exhausting the concurrency cap leaves
	// the rate bucket untouched, and vice versa (FR-1306 forbids conflating them).
	mustAcquire(t, ctx, r, endpoint, 1, 30*time.Second, "holder")
	if _, ok, _, err := r.Acquire(ctx, endpoint, 1, 30*time.Second, "holder"); err != nil || ok {
		t.Fatalf("the concurrency cap did not hold on its own (ok=%v, err=%v)", ok, err)
	}
	time.Sleep(250 * time.Millisecond) // let the bucket refill
	if ok, _, err := r.Allow(ctx, endpoint, rps, burst); err != nil || !ok {
		t.Errorf("a full concurrency cap refused a rate token (ok=%v, err=%v): the caps are independent", ok, err)
	}
}

// FR-1307: the limit is a property of the ENDPOINT. Two endpoints do not share a
// cap, and an endpoint's cap is not the process's.
func TestLimitsAreKeyedByEndpoint(t *testing.T) {
	ctx := context.Background()
	r, _ := newRedisRegistry(t)
	mustAcquire(t, ctx, r, "ep_a", 1, 30*time.Second, "h")
	if _, ok, _, err := r.Acquire(ctx, "ep_a", 1, 30*time.Second, "h"); err != nil || ok {
		t.Fatalf("ep_a's cap 1 did not hold (ok=%v, err=%v)", ok, err)
	}
	// ep_b is untouched by ep_a's saturation.
	mustAcquire(t, ctx, r, "ep_b", 1, 30*time.Second, "h")
	if ok, _, err := r.Allow(ctx, "ep_b", 1, 1); err != nil || !ok {
		t.Errorf("ep_b's bucket was affected by ep_a (ok=%v, err=%v)", ok, err)
	}
}

// A non-positive cap is a caller bug, not "unlimited": FR-1307 forbids the
// unbounded case, so the limiter refuses loudly instead of silently uncapping.
func TestInvalidCapIsRefused(t *testing.T) {
	ctx := context.Background()
	r, _ := newRedisRegistry(t)
	if _, ok, _, err := r.Acquire(ctx, "ep", 0, time.Second, "h"); err == nil || ok {
		t.Error("a zero cap was accepted; FR-1307 has no unbounded case")
	}
	if _, ok, _, err := r.Acquire(ctx, "ep", 1, 0, "h"); err == nil || ok {
		t.Error("a zero slot ttl was accepted; a slot that never expires cannot free")
	}
}

// The in-process fallback is honest about being per-process (FR-1307): it holds
// the same shape, and its slots expire too.
func TestLocalFallbackHoldsAndExpires(t *testing.T) {
	ctx := context.Background()
	l := NewLocal()
	mustAcquire(t, ctx, l, "ep", 2, 50*time.Millisecond, "h")
	lease := mustAcquire(t, ctx, l, "ep", 2, 50*time.Millisecond, "h")
	if _, ok, _, err := l.Acquire(ctx, "ep", 2, 50*time.Millisecond, "h"); err != nil || ok {
		t.Fatalf("the local cap of 2 did not hold (ok=%v, err=%v)", ok, err)
	}
	lease.Release(ctx)
	mustAcquire(t, ctx, l, "ep", 2, 50*time.Millisecond, "h")
	// Release twice is safe, and an unexpired slot still frees by expiry.
	lease.Release(ctx)
	if ok, _, err := l.Allow(ctx, "ep", 2, 2); err != nil || !ok {
		t.Errorf("the local bucket refused a token (ok=%v, err=%v)", ok, err)
	}
	if err := l.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
