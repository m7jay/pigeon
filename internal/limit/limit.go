// Package limit holds the per-endpoint limits that must hold CLUSTER-WIDE: the
// concurrency cap (FR-1302, default 10) and the RPS cap (FR-1306, default 25).
//
// The two are deliberately separate mechanisms, because FR-1306 forbids
// conflating them: a leased slot bounds how many requests are IN FLIGHT for an
// endpoint, a token bucket bounds how many START per second. One slow request
// holds a slot for its whole duration and consumes one token; fifty fast ones
// consume fifty tokens and hold one slot each for a few milliseconds.
//
// Both live in Redis, keyed by endpoint id alone, which is what makes the limit
// a property of the ENDPOINT rather than of the process (FR-1307): N core
// replicas hit the same keys, so adding replicas does not add capacity. AC-41 is
// that claim turned into an observable - two replicas sharing one Redis observe
// exactly the same in-flight count as one.
//
// Both operations are single Lua scripts, so acquire-and-count is atomic: two
// workers racing for the last slot cannot both win it. A worker that dies
// holding a slot leaks nothing - the slot is a sorted-set member scored by its
// expiry, so the next acquisition purges it (FR-1302: "a crashed worker's slots
// expire").
//
// The in-process semaphore the worker also keeps is BACKPRESSURE, never the
// authority (FR-816): it bounds one process's goroutines and connections, and
// the number it bounds is not the endpoint's promise.
package limit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/m7jay/pigeon/internal/redisconn"
)

// KeyPrefix namespaces the limiter's keys so they are recognisable next to the
// broker's streams and the pause counters.
const KeyPrefix = "pigeon:limit"

// Lease is one held concurrency slot. Release is idempotent and safe to call
// after the process has already lost the slot (an expiry, a Redis restart): the
// slot it names is simply gone.
type Lease interface {
	Release(ctx context.Context)
}

// Registry enforces the per-endpoint limits. Every method is safe for concurrent
// use, and both implementations are interchangeable from the caller's side.
type Registry interface {
	// Acquire takes one concurrency slot for an endpoint, or reports that the
	// endpoint is at its cap together with how long until a slot may free.
	Acquire(ctx context.Context, endpointID string, cap int, ttl time.Duration, holder string) (lease Lease, ok bool, wait time.Duration, err error)
	// Allow consumes one token from an endpoint's bucket, or reports how long
	// until one token is available.
	Allow(ctx context.Context, endpointID string, rps float64, burst int) (ok bool, wait time.Duration, err error)
	// Close releases the client. Only the Redis implementation owns one.
	Close() error
}

// ConcurrencyKey is the sorted set holding an endpoint's held slots: one member
// per in-flight request, scored by the instant its lease expires.
func ConcurrencyKey(endpointID string) string { return KeyPrefix + ":conc:" + endpointID }

// RPSKey is the hash holding an endpoint's bucket ({tokens, ts}).
func RPSKey(endpointID string) string { return KeyPrefix + ":rps:" + endpointID }

// holderID returns a process-unique prefix for slot members. A member must be
// unique per lease, or one worker's release would free another worker's slot.
func holderID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		// rand.Read failing is not a reason to refuse to deliver; the timestamp
		// plus the pid is unique enough to keep two members apart.
		return fmt.Sprintf("p%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// ---------------------------------------------------------------------------
// Redis: the authority (FR-1302, FR-1306, FR-1307).
// ---------------------------------------------------------------------------

// Redis enforces both limits in one shared Redis, so the caps hold across every
// replica. It implements Registry.
type Redis struct {
	rdb   *redis.Client
	owned bool // whether Close should close the client
}

// NewRedis connects a client to url. It PINGs, so a wrong URL fails at boot
// rather than silently disabling the P0 limits. url is the documented
// PIGEON_REDIS_URL form (redis://host:port/db) or a bare host:port, and the
// database index the URL names is the one the limits live in - FR-1307's promise
// is that replicas sharing a Redis share its keyspace, so a client in a database
// other than the one the DSN named would break exactly that, silently.
func NewRedis(ctx context.Context, url string) (*Redis, error) {
	rdb, err := redisconn.Client(url)
	if err != nil {
		return nil, err
	}
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("limit: ping %s: %w", url, err)
	}
	return &Redis{rdb: rdb, owned: true}, nil
}

// NewRedisFrom wraps an already-connected client (tests, and the broker's
// connection when one process already holds it).
func NewRedisFrom(rdb *redis.Client) (*Redis, error) {
	if rdb == nil {
		return nil, fmt.Errorf("limit: nil redis client")
	}
	return &Redis{rdb: rdb}, nil
}

// Close closes a client this Registry created. A wrapped client is left alone.
func (r *Redis) Close() error {
	if r.owned {
		return r.rdb.Close()
	}
	return nil
}

// Client exposes the underlying client, so a second Redis-backed collaborator -
// the pause counters of FR-1202/FR-1203 - shares this connection pool instead of
// opening a second one to the same server.
func (r *Redis) Client() *redis.Client { return r.rdb }

// acquireScript takes one slot if the endpoint is below its cap, purging lapsed
// leases first so a crashed worker's slot is reusable immediately rather than at
// the end of some grace period.
//
// KEYS[1] the slot set. ARGV: now_ms, ttl_ms, member, cap.
// Returns {1, 0} on success, {0, ms_until_a_slot_may_free} when saturated.
const acquireScript = `
local key   = KEYS[1]
local now   = tonumber(ARGV[1])
local ttl   = tonumber(ARGV[2])
local member = ARGV[3]
local cap   = tonumber(ARGV[4])

-- A member's score is the instant its lease expires. Everything scored at or
-- below now belongs to a worker that finished (or died) without releasing.
redis.call('ZREMRANGEBYSCORE', key, '-inf', now)

if redis.call('ZCARD', key) >= cap then
  local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
  local wait = 0
  if oldest[2] then wait = tonumber(oldest[2]) - now end
  if wait < 0 then wait = 0 end
  return {0, wait}
end

redis.call('ZADD', key, now + ttl, member)
redis.call('PEXPIRE', key, ttl + 1000)
return {1, 0}
`

// releaseScript frees one slot. KEYS[1] the slot set; ARGV[1] the member.
const releaseScript = `
local key = KEYS[1]
local member = ARGV[1]
redis.call('ZREM', key, member)
return redis.call('ZCARD', key)
`

// rateScript is a token bucket refilled at a fixed rate and decremented
// atomically (FR-1306). Continuous refill is what makes it a rate over time
// rather than a per-second counter: a burst of 25 at the start of a second does
// not leave the endpoint idle for the rest of it.
//
// KEYS[1] the bucket hash. ARGV: now_ms, rps, burst.
// Returns {1, 0} when a token was taken, {0, ms_until_one_token} otherwise.
const rateScript = `
local key   = KEYS[1]
local now   = tonumber(ARGV[1])
local rps   = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])

local vals   = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(vals[1])
local ts     = tonumber(vals[2])
if tokens == nil or ts == nil then
  tokens = burst
  ts = now
end

local elapsed = now - ts
if elapsed < 0 then elapsed = 0 end
tokens = tokens + (elapsed / 1000.0) * rps
if tokens > burst then tokens = burst end

local ok = 0
local wait = 0
if tokens >= 1 then
  tokens = tokens - 1
  ok = 1
else
  wait = math.ceil((1 - tokens) / rps * 1000.0)
  if wait < 1 then wait = 1 end
end

redis.call('HSET', key, 'tokens', tokens, 'ts', now)
-- Keep the bucket for as long as it takes to refill from empty, plus a minute:
-- forgetting an idle endpoint's bucket loses nothing (a fresh bucket starts
-- full), so this is hygiene, not correctness.
redis.call('PEXPIRE', key, math.ceil(burst / rps * 1000.0) + 60000)
return {ok, wait}
`

var (
	acquireCmd = redis.NewScript(acquireScript)
	releaseCmd = redis.NewScript(releaseScript)
	rateCmd    = redis.NewScript(rateScript)
)

// Acquire implements Registry.
func (r *Redis) Acquire(ctx context.Context, endpointID string, cap int, ttl time.Duration, holder string) (Lease, bool, time.Duration, error) {
	if cap <= 0 {
		// A non-positive cap is a caller bug, not "unlimited" (FR-1307 forbids
		// the unbounded case); refusing loudly beats silently uncapping.
		return nil, false, 0, fmt.Errorf("limit: concurrency cap must be >= 1, got %d", cap)
	}
	if ttl <= 0 {
		return nil, false, 0, fmt.Errorf("limit: slot ttl must be positive, got %s", ttl)
	}
	member := holder + "." + holderID()
	now := time.Now().UnixMilli()
	res, err := acquireCmd.Run(ctx, r.rdb, []string{ConcurrencyKey(endpointID)},
		now, ttl.Milliseconds(), member, cap).Result()
	if err != nil {
		return nil, false, 0, fmt.Errorf("limit: acquire slot: %w", err)
	}
	ok, wait, err := twoInts(res)
	if err != nil {
		return nil, false, 0, fmt.Errorf("limit: acquire slot reply: %w", err)
	}
	if ok != 1 {
		return nil, false, time.Duration(wait) * time.Millisecond, nil
	}
	return &redisLease{rdb: r.rdb, key: ConcurrencyKey(endpointID), member: member}, true, 0, nil
}

// Allow implements Registry.
func (r *Redis) Allow(ctx context.Context, endpointID string, rps float64, burst int) (bool, time.Duration, error) {
	if rps <= 0 {
		return true, 0, nil // no rate configured: the bucket does not exist
	}
	if burst < 1 {
		burst = 1
	}
	res, err := rateCmd.Run(ctx, r.rdb, []string{RPSKey(endpointID)},
		time.Now().UnixMilli(), rps, burst).Result()
	if err != nil {
		return false, 0, fmt.Errorf("limit: rate bucket: %w", err)
	}
	ok, wait, err := twoInts(res)
	if err != nil {
		return false, 0, fmt.Errorf("limit: rate bucket reply: %w", err)
	}
	if ok != 1 {
		return false, time.Duration(wait) * time.Millisecond, nil
	}
	return true, 0, nil
}

type redisLease struct {
	rdb    *redis.Client
	key    string
	member string
}

func (l *redisLease) Release(ctx context.Context) {
	// Best effort by design: a failed release costs at most the slot's ttl,
	// because the next acquisition purges it by score.
	_ = releaseCmd.Run(ctx, l.rdb, []string{l.key}, l.member).Err()
}

// twoInts reads the {ok, wait} pair a script returns. go-redis hands a Lua table
// back as []interface{} of int64.
func twoInts(res any) (int64, int64, error) {
	items, ok := res.([]any)
	if !ok || len(items) != 2 {
		return 0, 0, fmt.Errorf("want a two-element reply, got %T", res)
	}
	ok0, err := toInt(items[0])
	if err != nil {
		return 0, 0, err
	}
	wait, err := toInt(items[1])
	if err != nil {
		return 0, 0, err
	}
	return ok0, wait, nil
}

func toInt(v any) (int64, error) {
	switch n := v.(type) {
	case int64:
		return n, nil
	case int:
		return int64(n), nil
	case float64:
		return int64(n), nil
	default:
		return 0, fmt.Errorf("want an integer, got %T", v)
	}
}

// ---------------------------------------------------------------------------
// Local: the degraded path when no Redis is configured.
// ---------------------------------------------------------------------------

// Local enforces both limits inside one process. It exists because the worker
// must run somewhere Redis is not configured (the API-only deployment), and it
// is honest about what it is: per-process, so N replicas allow N times the cap -
// which is precisely what FR-1302 and FR-1307 forbid for a real deployment. The
// boot path logs a warning naming FR-1302 when it selects this.
type Local struct {
	mu      sync.Mutex
	slots   map[string]map[string]time.Time
	buckets map[string]*localBucket
	now     func() time.Time
}

type localBucket struct {
	tokens float64
	last   time.Time
}

// NewLocal returns the in-process fallback.
func NewLocal() *Local {
	return &Local{
		slots:   make(map[string]map[string]time.Time),
		buckets: make(map[string]*localBucket),
		now:     time.Now,
	}
}

// Close implements Registry.
func (l *Local) Close() error { return nil }

// Acquire implements Registry with an expiring member map.
func (l *Local) Acquire(_ context.Context, endpointID string, cap int, ttl time.Duration, holder string) (Lease, bool, time.Duration, error) {
	if cap <= 0 {
		return nil, false, 0, fmt.Errorf("limit: concurrency cap must be >= 1, got %d", cap)
	}
	if ttl <= 0 {
		return nil, false, 0, fmt.Errorf("limit: slot ttl must be positive, got %s", ttl)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	set, ok := l.slots[endpointID]
	if !ok {
		set = make(map[string]time.Time)
		l.slots[endpointID] = set
	}
	soonest := time.Time{}
	for member, expiry := range set {
		if !expiry.After(now) {
			delete(set, member) // a crashed holder's slot (FR-1302)
			continue
		}
		if soonest.IsZero() || expiry.Before(soonest) {
			soonest = expiry
		}
	}
	if len(set) >= cap {
		return nil, false, soonest.Sub(now), nil
	}
	member := holder + "." + holderID()
	set[member] = now.Add(ttl)
	return &localLease{l: l, endpointID: endpointID, member: member}, true, 0, nil
}

// Allow implements Registry with a local token bucket.
func (l *Local) Allow(_ context.Context, endpointID string, rps float64, burst int) (bool, time.Duration, error) {
	if rps <= 0 {
		return true, 0, nil
	}
	if burst < 1 {
		burst = 1
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	b, ok := l.buckets[endpointID]
	if !ok {
		b = &localBucket{tokens: float64(burst), last: now}
		l.buckets[endpointID] = b
	} else {
		elapsed := now.Sub(b.last).Seconds()
		if elapsed > 0 {
			b.tokens += elapsed * rps
			if b.tokens > float64(burst) {
				b.tokens = float64(burst)
			}
			b.last = now
		}
	}
	if b.tokens >= 1 {
		b.tokens--
		return true, 0, nil
	}
	wait := time.Duration((1 - b.tokens) / rps * float64(time.Second))
	if wait < time.Millisecond {
		wait = time.Millisecond
	}
	return false, wait, nil
}

type localLease struct {
	l          *Local
	endpointID string
	member     string
}

func (ls *localLease) Release(context.Context) {
	ls.l.mu.Lock()
	defer ls.l.mu.Unlock()
	delete(ls.l.slots[ls.endpointID], ls.member)
}
