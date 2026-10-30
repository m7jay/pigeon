// Package attemptbuf is the attempt-evidence buffer and the flusher that drains
// it (S6).
//
// §6 puts attempt evidence in a different durability class from control state:
// the row and its truncated response body may be up to 1 s behind, and losing
// the tail is acceptable — while the delivery itself is driven by its lease and
// never by this buffer (D-2). D-1 fixes where that buffer lives: **Redis, never
// worker process memory**, so a worker crash loses nothing but the tail of an
// unclean Redis crash.
//
// The structure is a Redis Stream with a consumer group, not a bare list. §6's
// table says "Redis list → Postgres batches"; what that describes is an
// append-only buffer in Redis plus the operation "read a batch, insert it, then
// remove exactly what was inserted". A list cannot do that safely: LPop hands the
// batch to the flusher and removes it at the same instant, so a process killed
// between the pop and the commit loses the batch, which is precisely the loss
// D-1 says is zero. A stream does: XAdd appends, XReadGroup hands the flusher a
// batch that STAYS in the group's pending-entry list, and XAck retires it only
// after the Postgres transaction has committed. A flusher (or worker) killed
// anywhere in that window loses nothing — the entry is pending, and
// XAutoClaim reclaims it on the next start. That pending-entry list is also what
// D-2 means by the queue's own delivery accounting.
//
// Nothing in this package is on the delivery critical path: a failed push is
// reported to the caller, which writes the row inline instead (see
// internal/delivery). Losing Redis entirely degrades evidence to the pre-S6
// behaviour rather than stopping delivery.
package attemptbuf

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/m7jay/pigeon/internal/metrics"
	"github.com/m7jay/pigeon/internal/redisconn"
	"github.com/m7jay/pigeon/internal/store"
)

const (
	// DefaultKey is the buffer's stream (§6's "Redis list", D-1).
	DefaultKey = "pigeon:attempts"
	// DefaultGroup is the consumer group the flusher reads through. Its
	// pending-entry list is the "inserted but not yet acknowledged" set.
	DefaultGroup = "flusher"
	// recordField is the single stream field each entry carries: the attempt
	// record as JSON. One field keeps the entry readable with redis-cli XRANGE
	// without knowing the schema.
	recordField = "r"
)

// Options configures a Buffer. Zero values take their documented defaults.
type Options struct {
	// Key is the stream name. Defaults to DefaultKey.
	Key string
	// Group is the consumer group. Defaults to DefaultGroup.
	Group string
	// Consumer names this flusher inside the group. It defaults to the host
	// name, which is stable across restarts of the same replica: a restarted
	// flusher reclaims its own pending batch rather than orphaning it.
	Consumer string
	Logger   *slog.Logger
	Metrics  *metrics.Metrics
}

// Entry is one buffered attempt as read from the stream: its stream id (the
// acknowledgement handle) and the record it carries.
//
// An entry whose payload cannot be decoded is returned with Unreadable set and
// Raw carrying a truncated copy. It is still returned so the flusher can retire
// it: an entry nobody can read would otherwise sit in the pending list forever
// and be re-read on every pass.
type Entry struct {
	ID     string
	Record store.AttemptRecord
	// Unreadable marks a stream entry that does not decode into a record.
	Unreadable bool
	// Raw is a truncated copy of an unreadable entry's payload, for the warning
	// the flusher logs. Empty for a readable entry.
	Raw string
}

// IDs returns the acknowledgement handles for a batch.
func IDs(entries []Entry) []string {
	ids := make([]string, 0, len(entries))
	for _, e := range entries {
		ids = append(ids, e.ID)
	}
	return ids
}

// Records returns the decodable records of a batch.
func Records(entries []Entry) []store.AttemptRecord {
	recs := make([]store.AttemptRecord, 0, len(entries))
	for _, e := range entries {
		if e.Unreadable {
			continue
		}
		recs = append(recs, e.Record)
	}
	return recs
}

// Unreadable returns the entries that could not be decoded.
func Unreadable(entries []Entry) []Entry {
	var out []Entry
	for _, e := range entries {
		if e.Unreadable {
			out = append(out, e)
		}
	}
	return out
}

// Buffer is the Redis-side attempt buffer. It is safe for concurrent use: the
// delivery workers push from many goroutines, the flusher reads from one.
type Buffer struct {
	rdb      *redis.Client
	key      string
	group    string
	consumer string
	log      *slog.Logger
	metrics  *metrics.Metrics

	// groupMu serializes group creation. Push runs from many worker
	// goroutines and Read/Reclaim from the flusher, so the "created yet?"
	// check and the create must not race (the -race detector flags the bare
	// bool).
	groupMu    sync.Mutex
	groupReady bool
}

// New builds a Buffer over an already-connected client. A nil client is
// rejected rather than panicked on later.
func New(rdb *redis.Client, opts Options) (*Buffer, error) {
	if rdb == nil {
		return nil, errors.New("attemptbuf: nil redis client")
	}
	if opts.Key == "" {
		opts.Key = DefaultKey
	}
	if opts.Group == "" {
		opts.Group = DefaultGroup
	}
	if opts.Consumer == "" {
		host, err := os.Hostname()
		if err != nil || host == "" {
			host = fmt.Sprintf("flusher-%d", os.Getpid())
		}
		opts.Consumer = host
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Buffer{
		rdb:      rdb,
		key:      opts.Key,
		group:    opts.Group,
		consumer: opts.Consumer,
		log:      opts.Logger,
		metrics:  opts.Metrics,
	}, nil
}

// Dial connects to url and PINGs it, so a misconfigured buffer fails the boot
// (OPS-7) rather than dropping every push into a Redis nobody can reach. That is
// the same class of refusal as the broker's ping on the events stream.
//
// url is the documented PIGEON_REDIS_URL form (redis://host:port/db) or a bare
// host:port.
func Dial(ctx context.Context, url string, opts Options) (*Buffer, error) {
	rdb, err := clientFromURL(url)
	if err != nil {
		return nil, err
	}
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("ping attempt buffer at %s: %w", url, err)
	}
	return New(rdb, opts)
}

// NewFromClient wraps an already-connected client, the way broker.NewRedisFrom
// does: tests run the same assertions against miniredis and against the real
// Redis CI provides.
func NewFromClient(rdb *redis.Client, opts Options) (*Buffer, error) {
	return New(rdb, opts)
}

// clientFromURL accepts both the documented URL form and a bare host:port. The
// parsing is redisconn's, which is the one place in this tree that decides what
// PIGEON_REDIS_URL means; this wrapper adds the buffer's own deadline policy.
//
// ContextTimeoutEnabled is turned on here, because without it go-redis ignores
// the caller's context deadline and rides the client's own ReadTimeout instead:
// measured, a 200 ms deadline against a socket that accepts and never answers
// returned after 3.001 s, the read timeout. A Push, or the flusher's bounded
// retirement (AckTimeout), would then be waiting on a bound nobody set (D-9).
func clientFromURL(raw string) (*redis.Client, error) {
	o, err := redisconn.Options(raw)
	if err != nil {
		return nil, err
	}
	o.ContextTimeoutEnabled = true
	return redis.NewClient(o), nil
}

// Close releases the buffer's connection.
func (b *Buffer) Close() error { return b.rdb.Close() }

// Key is the stream name, exported for the monitoring query an operator runs.
func (b *Buffer) Key() string { return b.key }

// Consumer is this flusher's name inside the group.
func (b *Buffer) Consumer() string { return b.consumer }

// Push appends one attempt record. It returns nil only once Redis has
// acknowledged the append: a nil return means the record is in the buffer and
// survives a worker crash (D-1). The caller MUST NOT drop the record on a
// non-nil error — internal/delivery writes it inline instead.
func (b *Buffer) Push(ctx context.Context, rec store.AttemptRecord) error {
	raw, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("encode attempt record: %w", err)
	}
	if err := b.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: b.key,
		Values: map[string]any{recordField: string(raw)},
	}).Err(); err != nil {
		return fmt.Errorf("buffer attempt %d/%d: %w", rec.DeliveryID, rec.Attempt, err)
	}
	return nil
}

// Depth is how many entries are in the buffer (not how many are unacknowledged);
// it is the gauge an operator watches when the flusher falls behind (OBS-9).
func (b *Buffer) Depth(ctx context.Context) (int64, error) {
	n, err := b.rdb.XLen(ctx, b.key).Result()
	if err != nil {
		return 0, fmt.Errorf("buffer depth: %w", err)
	}
	return n, nil
}

// ensureGroup creates the consumer group at the start of the stream if it does
// not exist. It is idempotent (BUSYGROUP) and is called before any read, so a
// flusher that starts before the first push still sees every entry.
func (b *Buffer) ensureGroup(ctx context.Context) error {
	b.groupMu.Lock()
	defer b.groupMu.Unlock()
	if b.groupReady {
		return nil
	}
	if err := b.rdb.XGroupCreateMkStream(ctx, b.key, b.group, "0").Err(); err != nil {
		// An existing group answers BUSYGROUP; that is success here.
		if !isBusyGroup(err) {
			return fmt.Errorf("create consumer group %s on %s: %w", b.group, b.key, err)
		}
	}
	b.groupReady = true
	return nil
}

// isBusyGroup reports whether err is Redis' "group already exists" answer. The
// match is on the message because the server returns a plain string error and
// go-redis exports no sentinel for it.
func isBusyGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

// Read returns up to count NEW entries and leaves them pending until Ack, so a
// flusher that dies after reading loses nothing: the entries are reclaimable.
func (b *Buffer) Read(ctx context.Context, count int) ([]Entry, error) {
	if err := b.ensureGroup(ctx); err != nil {
		return nil, err
	}
	streams, err := b.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    b.group,
		Consumer: b.consumer,
		Streams:  []string{b.key, ">"},
		Count:    int64(count),
		// Block must be negative to omit BLOCK entirely. go-redis emits
		// `block <ms>` for any Block >= 0, and the zero value would therefore
		// send BLOCK 0 — block forever — which would hang the flusher on its
		// first empty pass instead of returning "nothing new". The cadence is
		// the caller's sleep (Interval), not a blocking read.
		Block: -1,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil // nothing new; not an error
	}
	if err != nil {
		return nil, fmt.Errorf("read buffered attempts: %w", err)
	}
	return decode(streams), nil
}

// Reclaim takes over pending entries that have been idle for at least minIdle,
// which is how a flusher recovers the batch a crashed one left behind (D-1's
// "0 lost on worker crash"). At startup minIdle is 0 — every pending entry is
// this process's to finish — and in the steady state it is the configured
// staleness, so two healthy replicas do not keep re-doing each other's batches.
func (b *Buffer) Reclaim(ctx context.Context, count int, minIdle time.Duration) ([]Entry, error) {
	if err := b.ensureGroup(ctx); err != nil {
		return nil, err
	}
	msgs, _, err := b.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   b.key,
		Group:    b.group,
		Consumer: b.consumer,
		MinIdle:  minIdle,
		Start:    "0-0",
		Count:    int64(count),
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reclaim buffered attempts: %w", err)
	}
	return decodeMessages(msgs), nil
}

// Ack retires entries from the pending list and removes them from the stream. It
// is called only after the Postgres transaction has committed, so an entry is
// never retired before the row exists. A duplicate ack (two flushers that
// reclaimed the same batch) is a no-op in Redis.
//
// The two commands ride one MULTI/EXEC, so a cancellation or a dropped
// connection applies both or neither (D-9). That matters in one direction only:
// an entry acked without its removal has left the pending list, where no reclaim
// can reach it, and only XLEN - the depth gauge OBS-9 watches - still counts it,
// for the life of the stream. Removing the acked entries is what keeps the
// stream from growing without bound, and it costs a peer that still holds one of
// these ids in its pending list nothing: the peer already re-inserted the row
// (the unique (delivery_id, attempt) key makes that a no-op) and its XAck of a
// deleted id answers 0 rather than failing.
func (b *Buffer) Ack(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	// TxPipelined is MULTI/EXEC: both commands are queued, sent together, and
	// applied by the server in one transaction, in this order.
	_, err := b.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.XAck(ctx, b.key, b.group, ids...)
		pipe.XDel(ctx, b.key, ids...)
		return nil
	})
	if err != nil {
		return fmt.Errorf("ack and trim %d buffered attempts: %w", len(ids), err)
	}
	return nil
}

// SweepStranded removes entries that have left the pending list but not the
// stream, and returns how many it removed. Run issues it at startup, after the
// initial reclaim, and then at most once per StaleAfter for as long as it runs
// (D-9): the call is the same either way, and what a sweep can retire is decided
// by the bound it reads, not by when it runs.
//
// A strand is the tear a transaction cannot cover: an acknowledgement applied in
// halves by a hard kill between the two commands inside MULTI/EXEC, or by a Redis
// crash whose AOF replay stopped after the XACK. The entry is out of the pending
// list, where XAUTOCLAIM can never see it again, and out of the unread range,
// where Read(">") never returns it either. Only XLEN still counts it, for the
// life of the stream: the queue-depth gauge OBS-9 watches over-reports forever
// (NFR-6 can never hold again) until a sweep trims it. This is that trim.
//
// The entire safety argument is the bound, because XTRIM does not consult the
// pending list: every entry below it has been delivered (last-delivered-id only
// moves forward, and only a read or a claim moves it) and is not pending, so it
// has already been acknowledged, while every entry at or above it is pending or
// unread and is kept. The bound is the smallest pending id when anything is
// pending - anything older than the oldest unacknowledged entry is delivered and
// not pending, so it is acknowledged - and otherwise the id after the group's
// last delivery, because with nothing pending every delivered entry has been
// acknowledged and everything after the last delivery is still unread.
func (b *Buffer) SweepStranded(ctx context.Context) (int, error) {
	if err := b.ensureGroup(ctx); err != nil {
		return 0, err
	}
	before, err := b.Depth(ctx)
	if err != nil {
		return 0, err
	}
	if before == 0 {
		return 0, nil
	}
	groups, err := b.rdb.XInfoGroups(ctx, b.key).Result()
	if err != nil {
		return 0, fmt.Errorf("sweep stranded attempt entries: read consumer groups: %w", err)
	}
	var group *redis.XInfoGroup
	for i := range groups {
		if groups[i].Name == b.group {
			group = &groups[i]
			break
		}
	}
	if group == nil {
		return 0, fmt.Errorf("sweep stranded attempt entries: consumer group %s is not in XINFO GROUPS", b.group)
	}
	// XLEN is pending plus unread plus stranded, so XLEN <= pending means there
	// is neither an unread entry nor a strand. That is the steady state, and it
	// must not issue a destructive command.
	if before <= group.Pending {
		return 0, nil
	}

	var bound string
	if group.Pending > 0 {
		sum, err := b.rdb.XPending(ctx, b.key, b.group).Result()
		if err != nil {
			return 0, fmt.Errorf("sweep stranded attempt entries: read the pending summary: %w", err)
		}
		bound = sum.Lower
		if bound == "" {
			// Never guess an id for a destructive command.
			return 0, fmt.Errorf("sweep stranded attempt entries: %d pending entries but XPENDING reported no smallest id", group.Pending)
		}
	} else {
		bound = nextStreamID(group.LastDeliveredID)
	}
	if _, err := b.rdb.XTrimMinID(ctx, b.key, bound).Result(); err != nil {
		return 0, fmt.Errorf("sweep stranded attempt entries below %s: %w", bound, err)
	}
	after, err := b.Depth(ctx)
	if err != nil {
		return 0, err
	}
	return int(before - after), nil
}

// nextStreamID returns the smallest stream id greater than id: the first id an
// XTRIM MINID bound may keep. A stream id is <ms>-<seq> with both parts compared
// as unsigned integers, so the sequence steps first and a full sequence steps the
// millisecond and restarts it. An id that does not parse is treated as 0-0, whose
// next id trims nothing - the safe answer when the input is not understood.
func nextStreamID(id string) string {
	ms, seq, ok := parseStreamID(id)
	if !ok {
		return "0-1"
	}
	if seq == ^uint64(0) {
		return fmt.Sprintf("%d-0", ms+1)
	}
	return fmt.Sprintf("%d-%d", ms, seq+1)
}

// parseStreamID splits a stream id into its millisecond and sequence parts.
func parseStreamID(id string) (ms, seq uint64, ok bool) {
	dash := strings.IndexByte(id, '-')
	if dash <= 0 {
		return 0, 0, false
	}
	ms, err := strconv.ParseUint(id[:dash], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	seq, err = strconv.ParseUint(id[dash+1:], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return ms, seq, true
}

// decode flattens XReadGroup's per-stream shape into entries.
func decode(streams []redis.XStream) []Entry {
	var out []Entry
	for _, s := range streams {
		out = append(out, decodeMessages(s.Messages)...)
	}
	return out
}

// maxRawInLog bounds how much of an unreadable entry the warning may carry. The
// payload is an attempt record whose body is already truncated to the configured
// cap, but a corrupt entry is by definition not trustworthy, so the log line is
// bounded independently.
const maxRawInLog = 200

// decodeMessages turns stream messages into entries. A message that does not
// decode is marked Unreadable rather than dropped, so the flusher can retire it
// and say so: it is evidence that is already gone (nothing can read it), and
// leaving it in the pending list would re-read it on every pass forever.
func decodeMessages(msgs []redis.XMessage) []Entry {
	out := make([]Entry, 0, len(msgs))
	for _, m := range msgs {
		e := Entry{ID: m.ID}
		raw, ok := m.Values[recordField].(string)
		if ok {
			var rec store.AttemptRecord
			if err := json.Unmarshal([]byte(raw), &rec); err == nil && rec.DeliveryID > 0 && rec.Attempt > 0 {
				e.Record = rec
				out = append(out, e)
				continue
			}
		}
		e.Unreadable = true
		if len(raw) > maxRawInLog {
			raw = raw[:maxRawInLog]
		}
		e.Raw = raw
		out = append(out, e)
	}
	return out
}
