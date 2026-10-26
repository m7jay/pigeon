package broker

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/m7jay/pigeon/internal/envelope"
)

func jsonRaw(s string) json.RawMessage { return json.RawMessage(s) }

// TestRedisBrokerPublishesToStream proves the transport contract: a Publish
// durably lands the envelope on the class stream before it returns, so the
// relay's nil return really is the ack (FR-701). It runs against the real Redis
// CI provides (PIGEON_TEST_REDIS_URL) when set, and against an in-process
// miniredis otherwise, so the same assertion holds in both places.
func TestRedisBrokerPublishesToStream(t *testing.T) {
	addr := os.Getenv("PIGEON_TEST_REDIS_URL")
	var rdb *redis.Client
	cleanup := func() {}
	if addr != "" {
		u, err := url.Parse(addr)
		if err != nil {
			t.Fatalf("parse PIGEON_TEST_REDIS_URL: %v", err)
		}
		rdb = redis.NewClient(&redis.Options{Addr: u.Host})
		cleanup = func() { rdb.Close() }
	} else {
		mr, err := miniredis.Run()
		if err != nil {
			t.Fatalf("miniredis: %v", err)
		}
		rdb = redis.NewClient(&redis.Options{Addr: mr.Addr()})
		cleanup = func() { mr.Close(); rdb.Close() }
	}
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	b, err := NewRedisFrom(ctx, rdb)
	if err != nil {
		t.Fatalf("NewRedisFrom: %v", err)
	}
	defer b.Close()

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
	if n, err := rdb.XLen(ctx, "pigeon:events:normal").Result(); err != nil || n != 1 {
		t.Fatalf("normal stream len = %d (err %v), want 1", n, err)
	}
	if n, err := rdb.XLen(ctx, "pigeon:events:priority").Result(); err != nil || n != 0 {
		t.Fatalf("priority stream len = %d (err %v), want 0 (separate stream, FR-705)", n, err)
	}

	// The delivered bytes are exactly the §10 envelope.
	msgs, err := rdb.XRange(ctx, "pigeon:events:normal", "-", "+").Result()
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
