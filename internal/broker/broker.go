// Package broker delivers a published envelope into the events stream.
//
// The relay publishes accepted events here (FR-701); the fan-out consumer reads
// them later. It is an interface because the durability contract is what matters,
// not the transport: the relay marks an outbox row published only after Publish
// returns, so the transport's job is to promise the message is durable before
// that return. Redis Streams is the transport (§2): durable append, one stream
// per event class (FR-705), at ~1 GB less RSS than Kafka for the reference box.
package broker

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/m7jay/pigeon/internal/envelope"
)

// Broker accepts a published envelope onto the events stream.
//
// Publish MUST return nil only once the message is durably accepted by the
// broker - the relay treats a nil return as the ack (FR-701) and marks the
// outbox row published only then. A non-nil error means the message was NOT
// accepted, so the relay leaves the row pending and it is retried.
type Broker interface {
	Publish(ctx context.Context, ev *envelope.Envelope) error
	Close() error
}

// Redis publishes envelopes to Redis Streams. Each event class has its own
// stream so priority and normal events use separate consumer pools (FR-705).
type Redis struct {
	rdb *redis.Client
}

// NewRedis connects to the broker at url and PINGs it, so a misconfigured URL
// fails the boot (OPS-7) rather than silently dropping every publish.
func NewRedis(ctx context.Context, url string) (*Redis, error) {
	rdb := redis.NewClient(&redis.Options{Addr: url})
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("broker ping: %w", err)
	}
	return &Redis{rdb: rdb}, nil
}

// NewRedisFrom wraps an already-connected client. Tests use it to run against an
// in-process miniredis or a shared CI Redis without going through the URL, so the
// transport contract is asserted the same everywhere.
func NewRedisFrom(_ context.Context, rdb *redis.Client) (*Redis, error) {
	if rdb == nil {
		return nil, fmt.Errorf("broker: nil client")
	}
	return &Redis{rdb: rdb}, nil
}

// stream is the §2 per-class events stream (FR-705): pigeon:events:normal and
// pigeon:events:priority, each an independent stream with its own consumer group.
func (r *Redis) stream(ev *envelope.Envelope) string {
	return "pigeon:" + ev.Stream()
}

// Publish XADDs the envelope to its class stream. XADD is durable and atomic -
// the entry is in the stream before XAdd returns, so a nil error is the ack.
func (r *Redis) Publish(ctx context.Context, ev *envelope.Envelope) error {
	data := envelope.MustMarshal(*ev)
	if err := r.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: r.stream(ev),
		Values: map[string]any{"event": string(data)},
	}).Err(); err != nil {
		return fmt.Errorf("broker publish to %s: %w", r.stream(ev), err)
	}
	return nil
}

// Close releases the broker connection.
func (r *Redis) Close() error { return r.rdb.Close() }
