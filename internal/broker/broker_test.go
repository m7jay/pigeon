package broker

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/m7jay/pigeon/internal/envelope"
)

func jsonRaw(s string) json.RawMessage { return json.RawMessage(s) }

// The §2 per-class stream keys this package writes to (FR-705). They are the
// literal names the contract fixes - envelope.Stream() yields "events:normal"
// or "events:priority" and Redis.stream prefixes "pigeon:" - and a test that
// publishes to them has to own them: one redis-server is shared by every run on
// the box (PIGEON_TEST_REDIS_URL points at it), so an entry left behind is
// another run's failure.
const (
	normalStreamKey   = "pigeon:events:normal"
	priorityStreamKey = "pigeon:events:priority"
)

// classStreamKeys is what a publishing fixture clears before it asserts a count
// and deletes again when it is done.
var classStreamKeys = []string{normalStreamKey, priorityStreamKey}

// newTestRedisClient dials the real Redis CI provides (PIGEON_TEST_REDIS_URL)
// when it is set, and a fresh in-process miniredis otherwise, so the same
// assertion holds in both places and a skip is never how a run passes.
//
// The whole DSN is honoured, its database index included: with
// redis://host:6399/9 this package works in DB 9 while the rest of that server
// stays in DB 0. Building the client from u.Host instead silently drops the
// index and lands every run in DB 0, sharing one keyspace with everything else
// logged into that server - the first of the two defects that made this
// fixture pass once and fail on every run after (t_2e8d8b86).
func newTestRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	raw := os.Getenv("PIGEON_TEST_REDIS_URL")
	if raw == "" {
		mr, err := miniredis.Run()
		if err != nil {
			t.Fatalf("miniredis: %v", err)
		}
		t.Cleanup(mr.Close)
		rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		return rdb
	}

	// A bare host:port is accepted too, the shape attemptbuf's clientFromURL
	// takes (PIGEON_REDIS_URL allows it), so pointing this package at either
	// form works.
	opts := &redis.Options{Addr: raw}
	if strings.Contains(raw, "://") {
		var err error
		if opts, err = redis.ParseURL(raw); err != nil {
			t.Fatalf("parse PIGEON_TEST_REDIS_URL %q: %v", raw, err)
		}
		// The index the DSN names has to reach the client this fixture reads
		// and writes its keyspace with - that is the defect, and a passing
		// count cannot show it, so assert it rather than trust the parse.
		if want := dsnDB(t, raw); opts.DB != want {
			t.Fatalf("PIGEON_TEST_REDIS_URL %q: client is on DB %d, want DB %d", raw, opts.DB, want)
		}
	}
	// ContextTimeoutEnabled so the caller's deadline is a hard bound rather
	// than a hint the client retries past. Same server, same choice as the
	// client attemptbuf dials with.
	opts.ContextTimeoutEnabled = true

	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// dsnDB is the database index a redis:// DSN names, 0 when it names none.
func dsnDB(t *testing.T, raw string) int {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse PIGEON_TEST_REDIS_URL %q: %v", raw, err)
	}
	path := strings.TrimPrefix(u.Path, "/")
	if path == "" {
		return 0
	}
	n, err := strconv.Atoi(path)
	if err != nil {
		t.Fatalf("database index in %q: %v", raw, err)
	}
	return n
}

// resetClassStreams makes the run own the state it asserts on: both class
// streams are emptied before the test publishes, so what it counts afterwards
// is its own entry and nothing else. Without that reset the first run against a
// clean server passes, leaves its entry, and every later run reads XLEN 2 and
// fails - on a shared server that is every run after the first, which is what
// turned a full local `go test -p 1 ./...` red.
func resetClassStreams(t *testing.T, rdb *redis.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Del(ctx, classStreamKeys...).Err(); err != nil {
		t.Fatalf("reset class streams: %v", err)
	}
	// The reset is an assertion, not decoration: stop clearing one of these
	// keys and this line is what goes red, instead of a later count that reads
	// like a transport bug.
	for _, key := range classStreamKeys {
		if n, err := rdb.XLen(ctx, key).Result(); err != nil || n != 0 {
			t.Fatalf("reset left %s at len %d (err %v), want 0", key, n, err)
		}
	}
}

// clearClassStreams deletes both class streams, leaving the server as the run
// found it. It is deferred, not a t.Cleanup: the broker's Close() closes the
// client it was handed, and t.Cleanup runs after every defer, so a cleanup
// registered here would find its connection already closed and leak the entry
// (measured: "cleanup: DEL ...: redis: client is closed", key left at XLEN 1).
func clearClassStreams(t *testing.T, rdb *redis.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Del(ctx, classStreamKeys...).Err(); err != nil {
		t.Logf("cleanup: DEL %v: %v", classStreamKeys, err)
	}
}

// TestRedisBrokerPublishesToStream proves the transport contract: a Publish
// durably lands the envelope on the class stream before it returns, so the
// relay's nil return really is the ack (FR-701). It runs against the real Redis
// CI provides (PIGEON_TEST_REDIS_URL) when set, and against an in-process
// miniredis otherwise, so the same assertion holds in both places. It owns the
// class streams: emptied before it publishes, deleted again afterwards, so a
// second run on the same shared server sees the empty streams the first run saw
// and leaves nothing behind for the next one.
func TestRedisBrokerPublishesToStream(t *testing.T) {
	rdb := newTestRedisClient(t)
	resetClassStreams(t, rdb)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	b, err := NewRedisFrom(ctx, rdb)
	if err != nil {
		t.Fatalf("NewRedisFrom: %v", err)
	}
	defer b.Close()
	defer clearClassStreams(t, rdb) // before b.Close() (defers unwind LIFO)

	ev := &envelope.Envelope{
		ID:         "evt_01J9Z6Q2X8",
		Type:       "payment.succeeded",
		Version:    "1.0",
		ProducerID: "prd_8Kd2",
		CreatedAt:  time.Unix(1700000000, 0).UTC(),
		Data:       jsonRaw(`{"amount":49900}`),
		Class:      "normal",
	}
	if err := b.Publish(ctx, ev); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// The entry is in the normal-class stream, and priority is a separate stream
	// (FR-705), empty here.
	if n, err := rdb.XLen(ctx, normalStreamKey).Result(); err != nil || n != 1 {
		t.Fatalf("normal stream len = %d (err %v), want 1", n, err)
	}
	if n, err := rdb.XLen(ctx, priorityStreamKey).Result(); err != nil || n != 0 {
		t.Fatalf("priority stream len = %d (err %v), want 0 (separate stream, FR-705)", n, err)
	}

	// The delivered bytes are exactly the §10 envelope.
	msgs, err := rdb.XRange(ctx, normalStreamKey, "-", "+").Result()
	if err != nil || len(msgs) != 1 {
		t.Fatalf("XRange: %v (%d msgs)", err, len(msgs))
	}
	if got := msgs[0].Values["event"]; got != string(envelope.MustMarshal(*ev)) {
		t.Fatalf("delivered envelope mismatch:\n got %s\nwant %s", got, msgs[0].Values["event"])
	}
}

// TestRedisBrokerPublishFailureIsAnError proves the failure path: a broker that
// refuses the message returns an error, which is what keeps the relay from
// marking the outbox published.
func TestRedisBrokerPublishFailureIsAnError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// A client pointed at a dead address: Publish must surface the error rather
	// than pretending the message was accepted.
	b := &Redis{rdb: redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})}
	ev := &envelope.Envelope{ID: "evt_x", Type: "x.y", Class: "normal"}
	if err := b.Publish(ctx, ev); err == nil {
		t.Fatal("Publish to a dead broker returned nil, want an error")
	}
}
