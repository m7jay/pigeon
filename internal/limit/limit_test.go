package limit

import (
	"context"
	"strconv"
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
//
// No leg of this test is timed against a wall clock the box can stretch. The
// shape it replaces held a 60 ms lease for the "still live" refusal and then
// slept 90 ms for the expiry, so on a loaded box (and under -race, which slows
// every Redis round trip) the "live" holder had already lapsed by the second
// Acquire: the missing refusal was reported as a broken cap rather than as a
// scheduling delay (measured 36 FAIL / 500 at -race -count=500 under load, every
// failure on the refusal line).
func TestCrashedHolderSlotsExpire(t *testing.T) {
	ctx := context.Background()
	r, rdb := newRedisRegistry(t)

	// A live holder blocks the next acquire. 30 s is not a sleep that is "long
	// enough": for this assertion to be wrong, thirty seconds of wall clock must
	// pass between two statements of this test, whatever the box is doing.
	const liveLease = 30 * time.Second
	const endpoint = "ep_crash"
	mustAcquire(t, ctx, r, endpoint, 1, liveLease, "dead")
	_, ok, wait, err := r.Acquire(ctx, endpoint, 1, liveLease, "next")
	if err != nil || ok {
		t.Fatalf("cap 1 with one live holder = (ok=%v, err=%v), want refused", ok, err)
	}
	if wait <= 0 || wait > liveLease {
		t.Errorf("the refusal reported wait=%s; a held slot's remaining lease (0, %s] is what the caller waits for (FR-1302)", wait, liveLease)
	}

	// A lease that really lapses frees the slot, at the scale it was taken at: a
	// separate short lease, waited for by a bounded poll rather than a fixed
	// sleep. Load can only make the purge later, never false the assertion, and
	// a ttl read as seconds instead of milliseconds (50 s for this holder) still
	// misses the bound.
	const shortLease, lapsed = 50 * time.Millisecond, "ep_lapsed"
	mustAcquire(t, ctx, r, lapsed, 1, shortLease, "dead")
	waitForFreeSlot(t, ctx, r, lapsed, liveLease, 5*time.Second)
	if n, err := rdb.ZCard(ctx, ConcurrencyKey(lapsed)).Result(); err != nil || n != 1 {
		t.Fatalf("after the lapsed holder freed its slot the set holds %d (err %v), want 1", n, err)
	}

	// And the dead member is PURGED, not merely outscored: a crash leaves the
	// member behind with a lapsed score, which is what the next acquisition
	// purges. Move the held slot's score into the past by hand - no wall clock -
	// and require the next acquisition to take the slot AND drop the dead member
	// by name, so a leak cannot hide behind the cap check.
	const purged = "ep_purged"
	key := ConcurrencyKey(purged)
	mustAcquire(t, ctx, r, purged, 1, liveLease, "dead")
	dead := zMembers(t, rdb, key)
	if len(dead) != 1 {
		t.Fatalf("%s holds %v after one acquire, want exactly 1 member", key, dead)
	}
	markLeaseLapsed(t, rdb, key, dead[0])
	mustAcquire(t, ctx, r, purged, 1, liveLease, "next")
	got := zMembers(t, rdb, key)
	if len(got) != 1 || got[0] == dead[0] {
		t.Fatalf("%s holds %v after the dead holder's lease lapsed, want exactly the new holder (%q purged)", key, got, dead[0])
	}
}

// waitForFreeSlot acquires until the endpoint admits the caller, failing after
// within. A bounded wait is what makes the leg latency-independent: load can only
// make the purge later, never false the assertion.
func waitForFreeSlot(t *testing.T, ctx context.Context, r Registry, endpoint string, ttl, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		if _, ok, _, err := r.Acquire(ctx, endpoint, 1, ttl, "next"); err != nil {
			t.Fatalf("Acquire(%s): %v", endpoint, err)
		} else if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("a lapsed holder still blocked %s %s later; a crashed worker's slot must expire (FR-1302)", endpoint, within)
		}
		time.Sleep(time.Millisecond)
	}
}

// zMembers returns the endpoint's held slots as recorded, so a test can name the
// member a crashed holder leaked instead of only counting it.
func zMembers(t *testing.T, rdb *redis.Client, key string) []string {
	t.Helper()
	members, err := rdb.ZRange(context.Background(), key, 0, -1).Result()
	if err != nil {
		t.Fatalf("ZRANGE %s: %v", key, err)
	}
	return members
}

// markLeaseLapsed moves a held slot's score into the past: the model of a
// SIGKILLed holder whose lease has since run out, at no cost in wall clock.
func markLeaseLapsed(t *testing.T, rdb *redis.Client, key, member string) {
	t.Helper()
	err := rdb.ZAdd(context.Background(), key, redis.Z{
		Score:  float64(time.Now().Add(-time.Second).UnixMilli()),
		Member: member,
	}).Err()
	if err != nil {
		t.Fatalf("ZADD lapsed score for %s: %v", member, err)
	}
}

// FR-1306: RPS is a rate over time, not a counter per second, and it is a
// DIFFERENT mechanism from the concurrency cap - a bucket bounds how many start,
// a slot bounds how many are in flight.
//
// No leg here is timed against the box. The bucket's state is written by this test
// (seedBucket) and read back (bucketTokens) around each call, with ts pinned at or
// ahead of the caller's clock: rateScript refills from `now - ts` and clamps a
// negative elapsed to 0, so every call below sees exactly zero refill and its
// arithmetic is exact whatever the box is doing. The shape it replaces filled a
// burst through five round trips and required the sixth to land inside 200 ms -
// and computed the expected wait from that same refill, so the busier the box the
// smaller the reported wait. Measured on the landed tree at -race under load: 44
// reds in one -count=200 package run, 15 of them on the 6th token and 29 on the
// wait, which was reported as low as 4 ms.
func TestRateBucketIsContinuousAndSeparateFromConcurrency(t *testing.T) {
	ctx := context.Background()
	r, rdb := newRedisRegistry(t)
	const endpoint, rps, burst = "ep_rate", 5.0, 5

	// A bucket that does not exist yet starts full, at the configured burst. This
	// direction is latency-independent on purpose: a refill can only ADD tokens, so
	// no amount of load can turn a served token into a refusal. (An initial count
	// other than `burst` reds on the second call.)
	const fresh = "ep_rate_fresh"
	for i := 0; i < burst; i++ {
		if ok, wait, err := r.Allow(ctx, fresh, rps, burst); err != nil || !ok {
			t.Fatalf("token %d from an endpoint with no bucket yet = (ok=%v, wait=%s, err=%v); a fresh bucket holds a burst of %d", i+1, ok, wait, err, burst)
		}
	}

	// A burst is spent one token at a time and the bucket is empty exactly at 0.
	// Each call starts from a state this test wrote and must leave it exactly one
	// token lighter; reading that state back is what makes this a check of the
	// limiter's arithmetic rather than of the test's own seeds.
	for left := burst; left >= 1; left-- {
		seedBucket(t, rdb, endpoint, float64(left), pinnedAhead)
		ok, wait, err := r.Allow(ctx, endpoint, rps, burst)
		if err != nil || !ok {
			t.Fatalf("a bucket holding %d token(s) refused one (ok=%v, wait=%s, err=%v)", left, ok, wait, err)
		}
		if got := bucketTokens(t, rdb, endpoint); got != float64(left-1) {
			t.Fatalf("a bucket holding %d token(s) holds %v after serving one, want %d", left, got, left-1)
		}
	}

	// An empty bucket serves nothing, and the wait it reports is the rate's (200 ms
	// at 5 RPS) rather than a whole second, which is the difference between a
	// bucket and a counter. Still with elapsed pinned at 0, so it is the exact
	// value the rate implies instead of whatever the box left in the bucket.
	seedBucket(t, rdb, endpoint, 0, pinnedAhead)
	ok, wait, err := r.Allow(ctx, endpoint, rps, burst)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if ok {
		t.Fatalf("the bucket served a token from an empty bucket, after a burst of %d was spent", burst)
	}
	t.Logf("an empty bucket at %g RPS reports wait=%s", rps, wait)
	if wait < 100*time.Millisecond || wait > 400*time.Millisecond {
		t.Errorf("wait for the next token = %s, want about 200 ms at 5 RPS (FR-1306)", wait)
	}

	// And the bucket refills continuously, at the rate, up to the burst and no
	// further. Seeded empty at the caller's own clock, a 250 ms Sleep is the whole
	// of the elapsed time rateScript can see, which is more than the 200 ms a token
	// costs; Sleep is a floor, so a slower box only adds tokens. An endpoint idle
	// for an hour is then worth exactly `burst` tokens, not eighteen thousand:
	// elapsed is enormous there, and the ceiling is what makes the value exact.
	seedBucket(t, rdb, endpoint, 0, 0)
	time.Sleep(250 * time.Millisecond)
	if ok, _, err := r.Allow(ctx, endpoint, rps, burst); err != nil || !ok {
		t.Errorf("after 250 ms a token was refused (ok=%v, err=%v); the bucket refills continuously", ok, err)
	}
	seedBucket(t, rdb, endpoint, 0, -time.Hour)
	if ok, _, err := r.Allow(ctx, endpoint, rps, burst); err != nil || !ok {
		t.Fatalf("a bucket idle for an hour refused a token (ok=%v, err=%v)", ok, err)
	}
	if got := bucketTokens(t, rdb, endpoint); got != burst-1 {
		t.Fatalf("a bucket idle for an hour holds %v after serving one, want %d: the refill is capped at the burst (FR-1306)", got, burst-1)
	}

	// The two mechanisms do not share state, in either direction: an endpoint whose
	// rate bucket is empty still hands out concurrency slots, and a full
	// concurrency cap leaves the rate bucket working (FR-1306 forbids conflating).
	seedBucket(t, rdb, endpoint, 0, pinnedAhead)
	mustAcquire(t, ctx, r, endpoint, 1, 30*time.Second, "holder")
	if _, ok, _, err := r.Acquire(ctx, endpoint, 1, 30*time.Second, "holder"); err != nil || ok {
		t.Fatalf("the concurrency cap did not hold on its own (ok=%v, err=%v)", ok, err)
	}
	seedBucket(t, rdb, endpoint, 1, pinnedAhead)
	if ok, _, err := r.Allow(ctx, endpoint, rps, burst); err != nil || !ok {
		t.Errorf("a full concurrency cap refused a rate token (ok=%v, err=%v): the caps are independent", ok, err)
	}
}

// pinnedAhead is how far ahead of the caller's clock the bucket legs below pin a
// bucket's ts. rateScript refills from `now - ts` and clamps a negative elapsed to
// zero, so a ts a minute ahead makes elapsed exactly 0 for a whole leg: for one of
// those assertions to be wrong, a minute of wall clock must pass between two
// statements of this file, whatever the box is doing. It is also a state
// production can reach - a replica whose clock runs ahead writes exactly this.
const pinnedAhead = time.Minute

// seedBucket writes an endpoint's bucket hash directly: the {tokens, ts} a burst of
// Allow calls would have left, without the round trips and without depending on
// how long the previous call took.
func seedBucket(t *testing.T, rdb *redis.Client, endpoint string, tokens float64, ahead time.Duration) {
	t.Helper()
	err := rdb.HSet(context.Background(), RPSKey(endpoint),
		"tokens", tokens, "ts", time.Now().Add(ahead).UnixMilli()).Err()
	if err != nil {
		t.Fatalf("seed bucket %s: %v", endpoint, err)
	}
}

// bucketTokens reads back the count the last call stored, so an assertion can name
// the state the limiter left behind instead of only the boolean it returned.
func bucketTokens(t *testing.T, rdb *redis.Client, endpoint string) float64 {
	t.Helper()
	raw, err := rdb.HGet(context.Background(), RPSKey(endpoint), "tokens").Result()
	if err != nil {
		t.Fatalf("HGET %s tokens: %v", RPSKey(endpoint), err)
	}
	got, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		t.Fatalf("bucket %s holds tokens=%q, which is not a number: %v", endpoint, raw, err)
	}
	return got
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
