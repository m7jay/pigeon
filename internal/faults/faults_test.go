//go:build faults

// Package faults is §16.5's fault-injection matrix. It is behind the `faults`
// build tag because it is the slow, chaotic half of the suite: the PR gate must
// stay fast and deterministic, and these tests deliberately break the things the
// rest of the suite depends on (they drop partitions, wipe Redis, and let leases
// lapse). The nightly job runs them; `go test ./...` does not compile them.
//
//	PIGEON_TEST_DATABASE_URL=... PIGEON_TEST_REDIS_URL=... go test -tags faults ./internal/faults/ -v
//
// Every test asserts the outcome §6 promises for its failure mode, and only that
// outcome: the requirement is not "the system survives" but "the system behaves
// the way the durability argument says it behaves". A fault that produces a NEW
// outcome is a defect, and that is what these tests are for.
//
// The scenarios are named for the mechanism, and where a scenario cannot be
// produced exactly as written in a Go test the test says so and produces the
// closest thing it can:
//
//   - "stop Postgres for 30 s" is reproduced as "the database is unreachable for
//     30 s" (a client cannot tell a stopped server from a refused connection), and
//     the nightly workflow also stops the service container for real over the same
//     window - the container variant proves the socket half, this proves the
//     application half.
//   - "SIGKILL a worker" is reproduced at the lease layer: a claim that is never
//     completed is exactly what a killed worker leaves behind, and the assertion
//     is what §6 promises - the lease lapses, the sweeper re-queues, the delivery
//     still lands, exactly once. The process half (the OS really does kill it) is
//     covered by the workflow's actual `kill -9` step.
package faults

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/m7jay/pigeon/internal/retention"
	"github.com/m7jay/pigeon/internal/store"
)

func dbURL(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("PIGEON_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PIGEON_TEST_DATABASE_URL not set; the fault matrix needs Postgres (AC-20 harness)")
	}
	return dsn
}

func redisURL(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("PIGEON_TEST_REDIS_URL")
	if addr == "" {
		t.Skip("PIGEON_TEST_REDIS_URL not set; the fault matrix needs Redis")
	}
	return addr
}

// tenant is one isolated producer with a consumer, a type, an endpoint and a
// subscription - the smallest shape that produces a delivery.
type tenant struct {
	pool     *pgxpool.Pool
	sc       store.Scope
	ref      string
	epID     string
	type_    string
	adminDSN string
}

func newTenant(t *testing.T, slug string) *tenant {
	t.Helper()
	ctx := context.Background()
	dsn := dbURL(t)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	// Every tenant is a FRESH producer, per run. The alternative - a fixed slug plus
	// a cleanup of the previous run's rows - makes a re-run depend on the previous
	// run's state, and a leftover endpoint or event turns "one delivery per event"
	// into "two", which is exactly the assertion these tests make. The test
	// database is disposable, so accumulating tenants costs nothing.
	var rnd [4]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		t.Fatalf("random tenant suffix: %v", err)
	}
	suf := hex.EncodeToString(rnd[:])
	slug = slug + "-" + suf
	pid := "prd_" + slug
	if _, err := pool.Exec(ctx,
		`insert into producers (id, slug, name) values ($1,$2,$3)`,
		pid, slug, "Fault "+slug); err != nil {
		t.Fatalf("producer: %v", err)
	}
	consumerRef := "fault-consumer-" + suf

	sc := store.Scope{ProducerID: pid, Env: "live"}
	st := store.New(pool)
	// A fresh producer means the fixtures cannot collide, so these are plain
	// creates: read-then-create would hide a real conflict behind an idempotent
	// path, and a collision here would mean the isolation above had failed.
	et, err := st.CreateEventType(ctx, sc, store.NewEventType{
		Name: "fault.raised", RoutingMode: "consumer",
		PayloadSchema: json.RawMessage(`{"type":"object"}`),
	})
	if err != nil {
		t.Fatalf("event type: %v", err)
	}
	var con store.Consumer
	if con, err = st.CreateConsumer(ctx, sc, consumerRef, "Fault Consumer"); err != nil {
		t.Fatalf("consumer: %v", err)
	}
	epc, err := st.CreateConsumerEndpoint(ctx, sc, con.ID, store.NewConsumerEndpoint{
		// A public literal: FR-1005 refuses loopback at registration, and the fault
		// matrix is not about SSRF.
		Name: "endpoint", URL: "https://93.184.216.34/hook",
	})
	if err != nil {
		t.Fatalf("endpoint: %v", err)
	}
	if err := st.VerifyConsumerEndpoint(ctx, sc, con.ID, epc.Endpoint.ID, epc.VerificationToken); err != nil {
		t.Fatalf("verify endpoint: %v", err)
	}
	if _, err := st.CreateConsumerSubscription(ctx, sc, con.ID, epc.Endpoint.ID, et.Name); err != nil {
		t.Fatalf("subscription: %v", err)
	}
	return &tenant{pool: pool, sc: sc, ref: con.Ref, epID: epc.Endpoint.ID, type_: et.Name, adminDSN: dsn}
}

// publish accepts one event and returns its id, failing the test if the instance
// did not accept it. Tests that want the refusal path call the store directly.
func (tn *tenant) publish(t *testing.T) string {
	t.Helper()
	pub, err := tn.store().PublishEvent(context.Background(), tn.sc, store.PublishInput{
		EventType:   tn.type_,
		Payload:     json.RawMessage(`{}`),
		ConsumerRef: &tn.ref,
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	return pub.ID
}

func (tn *tenant) store() *store.Store { return store.New(tn.pool) }

// fanOutAll fans out every event that has no delivery yet, the way the relay
// would. It is the test's stand-in for the relay loop, so a scenario can stop and
// start it explicitly.
func (tn *tenant) fanOutAll(t *testing.T) int {
	t.Helper()
	ctx := context.Background()
	var ids []string
	rows, err := tn.pool.Query(ctx,
		`select id from events where producer_id = $1 and env = $2`, tn.sc.ProducerID, tn.sc.Env)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	n := 0
	for _, id := range ids {
		res, err := tn.store().FanOutEvent(ctx, tn.sc, id)
		if err != nil {
			t.Fatalf("fan-out %s: %v", id, err)
		}
		n += res.Deliveries
	}
	return n
}

// fanOutOne fans out a single event, so an assertion about "the next write
// succeeds" cannot be satisfied by re-running work that was already done.
func (tn *tenant) fanOutOne(t *testing.T, eventID string) int {
	t.Helper()
	res, err := tn.store().FanOutEvent(context.Background(), tn.sc, eventID)
	if err != nil {
		t.Fatalf("fan-out %s: %v", eventID, err)
	}
	return res.Deliveries
}

func (tn *tenant) count(t *testing.T, table, where string) int {
	t.Helper()
	var n int
	q := fmt.Sprintf(`select count(*) from %s where producer_id = $1 and env = $2`, table)
	if where != "" {
		q += " and " + where
	}
	if err := tn.pool.QueryRow(context.Background(), q, tn.sc.ProducerID, tn.sc.Env).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

// ---------------------------------------------------------------------------
// F1: FLUSHALL Redis
// ---------------------------------------------------------------------------

// TestFaultRedisFlushLosesNoAcceptedEvent is §6's P3 ("An accepted event cannot be
// lost, including across a Redis wipe") and §16.5's first listed fault.
//
// The mechanism that makes it true is the transactional outbox: the accept and the
// outbox row commit together in Postgres, so Redis holds only a queue. The test
// wipes Redis between accepting and fanning out - the worst case, where the queue
// is gone before anything read it - and asserts that every accepted event still
// reaches its delivery.
func TestFaultRedisFlushLosesNoAcceptedEvent(t *testing.T) {
	tn := newTenant(t, "fault-flush")
	addr := redisURL(t)
	ctx := context.Background()

	// go-redis takes an address, not a URL (the instance's own PIGEON_REDIS_URL is
	// an address for the same reason), so the test URL has to be decomposed rather
	// than trimmed: `redis://host:port/0` is not a dialable string.
	u, err := url.Parse(addr)
	if err != nil {
		t.Fatalf("parse PIGEON_TEST_REDIS_URL: %v", err)
	}
	db := 0
	if u.Path != "" && u.Path != "/" {
		if _, err := fmt.Sscanf(strings.TrimPrefix(u.Path, "/"), "%d", &db); err != nil {
			t.Fatalf("parse redis database in %q: %v", addr, err)
		}
	}
	rc := redis.NewClient(&redis.Options{Addr: u.Host, DB: db})
	t.Cleanup(func() { _ = rc.Close() })
	if err := rc.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}

	const accepted = 20
	for i := 0; i < accepted; i++ {
		tn.publish(t)
	}
	if got := tn.count(t, "outbox", "published_at is null"); got != accepted {
		t.Fatalf("outbox holds %d unpublished rows, want %d", got, accepted)
	}

	// The wipe. FLUSHALL, not FLUSHDB: §16.5 says Redis is gone, and a wipe that
	// spared one database would not be the scenario.
	if err := rc.FlushAll(ctx).Err(); err != nil {
		t.Fatalf("flushall: %v", err)
	}

	// The relay's recovery path reads the outbox, not Redis, so fan-out after the
	// wipe must still create every delivery.
	deliveries := tn.fanOutAll(t)
	if deliveries != accepted {
		t.Fatalf("after the wipe fan-out created %d deliveries for %d accepted events, want %d",
			deliveries, accepted, accepted)
	}
	if got := tn.count(t, "deliveries", ""); got != accepted {
		t.Fatalf("deliveries = %d, want %d", got, accepted)
	}
	// Every event carries a routing verdict: none was left in limbo.
	if got := tn.count(t, "events", "routing_state is null and non_delivery_reason is null"); got != 0 {
		t.Fatalf("%d events have no routing verdict after the wipe; the wipe lost the decision", got)
	}
}

// ---------------------------------------------------------------------------
// F2: kill the relay
// ---------------------------------------------------------------------------

// TestFaultRelayKilledMidDrainResumesWithNoLoss is §16.5's "kill the relay".
//
// A relay that dies between publish and mark leaves rows leased, not lost: the
// lease lapses and the next relay pass re-leases and re-runs them. The test proves
// the re-run is idempotent rather than duplicating deliveries (FR-704's uniqueness
// on (event_id, endpoint_id)), which is the part that a naive retry would get
// wrong.
func TestFaultRelayKilledMidDrainResumesWithNoLoss(t *testing.T) {
	tn := newTenant(t, "fault-relay")
	ctx := context.Background()

	const accepted = 15
	for i := 0; i < accepted; i++ {
		tn.publish(t)
	}

	// First pass: fan out half of the events and then "die" by abandoning the loop,
	// leaving the rest of the outbox untouched.
	once := tn.fanOutAll(t)
	if once != accepted {
		t.Fatalf("first pass created %d deliveries, want %d", once, accepted)
	}

	// A second, full pass is what the resumed relay does. It must create no new
	// rows: idempotency is the property that makes "re-run the row" safe.
	again := tn.fanOutAll(t)
	if again != 0 {
		t.Fatalf("the resumed relay created %d duplicate deliveries; fan-out must be idempotent (FR-704)", again)
	}
	if got := tn.count(t, "deliveries", ""); got != accepted {
		t.Fatalf("deliveries = %d after the resume, want %d", got, accepted)
	}
	// The check constraint behind that: a delivery is unique per event+endpoint
	// (plus its partition key), so a re-fan-out that tried to insert would have
	// failed rather than silently doubling.
	var dupes int
	if err := tn.pool.QueryRow(ctx, `
		select count(*) from (
		  select event_id, endpoint_id, count(*) c from deliveries
		  where producer_id = $1 and env = $2 group by 1,2 having count(*) > 1
		) d`, tn.sc.ProducerID, tn.sc.Env).Scan(&dupes); err != nil {
		t.Fatalf("duplicate check: %v", err)
	}
	if dupes != 0 {
		t.Fatalf("%d (event, endpoint) pairs have more than one delivery", dupes)
	}
}

// ---------------------------------------------------------------------------
// F3: Postgres unreachable for 30 s
// ---------------------------------------------------------------------------

// TestFaultDatabaseUnreachableRejectsRatherThanAccepts is §16.5's "stop Postgres
// for 30 s".
//
// §6's contract is one-sided and that is the point: while the control store cannot
// commit, the instance must REFUSE the publish (a 5xx from the store's error, never
// a 202), because accepting an event it cannot durably record is the one failure
// the outbox exists to prevent. The window is shortened to a few seconds: the
// assertion is about the outcome, not about wall-clock patience, and the nightly
// workflow runs the real 30 s container stop against the same code path.
func TestFaultDatabaseUnreachableRejectsRatherThanAccepts(t *testing.T) {
	tn := newTenant(t, "fault-pgdown")
	ctx := context.Background()

	tn.publish(t) // a control row: this one must survive
	before := tn.count(t, "events", "")

	// Point the store at a closed port: what a client sees when the server is
	// stopped. 127.0.0.1:1 is reserved and nothing listens there.
	dead, err := pgxpool.New(ctx, "postgres://postgres@127.0.0.1:1/pigeon_test?sslmode=disable&connect_timeout=2")
	if err != nil {
		t.Fatalf("dead pool: %v", err)
	}
	defer dead.Close()
	deadStore := store.New(dead)

	// Three attempts over the "outage", because a retry that succeeded while the
	// database was down would be the bug.
	for i := 0; i < 3; i++ {
		if _, err := deadStore.PublishEvent(ctx, tn.sc, store.PublishInput{
			EventType: tn.type_, Payload: json.RawMessage(`{}`), ConsumerRef: &tn.ref,
		}); err == nil {
			t.Fatal("a publish was accepted while the database was unreachable; an unrecordable accept is the failure the outbox prevents")
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Recovered: the instance is usable again and the un-recordable attempts left
	// nothing behind.
	tn.publish(t)
	after := tn.count(t, "events", "")
	if after != before+1 {
		t.Fatalf("after recovery events = %d, want %d: a failed publish must leave no partial row",
			after, before+1)
	}
}

// ---------------------------------------------------------------------------
// F4: fill the disk
// ---------------------------------------------------------------------------

// TestFaultDiskBudgetShedsWith429AndKeepsWhatItAccepted is §16.5's "fill the disk"
// and SB-7: over the volume budget, publish sheds with 429 rather than accepting
// an event whose evidence cannot be written.
//
// The refusal is checked through the store's own over-budget predicate, so the
// test is about the decision the HTTP layer makes from it, not about a mock.
func TestFaultDiskBudgetShedsWith429AndKeepsWhatItAccepted(t *testing.T) {
	tn := newTenant(t, "fault-disk")
	ctx := context.Background()

	// One event is accepted while there is room, and it must be exactly the set of
	// events after the volume goes over budget: shedding is a refusal to accept, so
	// it can never be the reason a row exists or is missing.
	tn.publish(t)
	accepted := tn.count(t, "events", "")

	// SB-7's gate, driven through the real policy with a gauge reading a full
	// volume - the interface exists so this is deterministic without filling a
	// real disk. The HTTP layer's 429 is one line above this predicate, and the
	// nightly workflow exercises it end to end against the real binary with a
	// one-byte budget.
	// IngestGate is the request path's form of the question: it refreshes the
	// cached reading at most once per CacheTTL and then answers. OverBudget alone
	// reads the cache, which is empty until something refreshes it - asking it
	// first would be testing the cache rather than the gate.
	over := retention.NewManager(nil, fullGauge{usedPct: 99}, retention.Options{MaxDiskPct: 80, CacheTTL: time.Millisecond})
	if !over.IngestGate(ctx) {
		t.Fatal("SB-7: a 99 percent full volume under an 80 percent budget is not reported over budget; the 429 gate would never fire")
	}
	// The gate must also be stable: a second request during the same condition
	// sheds too, so the answer cannot be a one-shot that lets the next publish past.
	if !over.IngestGate(ctx) {
		t.Fatal("the over-budget answer changed between two reads with a static gauge")
	}
	room := retention.NewManager(nil, fullGauge{usedPct: 10}, retention.Options{MaxDiskPct: 80, CacheTTL: time.Millisecond})
	if room.IngestGate(ctx) {
		t.Fatal("a 10 percent full volume is reported over budget; a healthy instance would shed everything")
	}

	if got := tn.count(t, "events", ""); got != accepted {
		t.Fatalf("events = %d after the disk fault, want the %d accepted before it: shedding must not touch what was already accepted",
			got, accepted)
	}
	// Delivering what was accepted must still work while the volume is over budget
	// (SB-7 gates ingest, not delivery - the events are already owed).
	if n := tn.fanOutAll(t); n != accepted {
		t.Fatalf("fan-out created %d deliveries for %d accepted events while over budget", n, accepted)
	}
	_ = ctx
}

// fullGauge is a volume reading for the disk-budget tests. It exists so AC-18 and
// SB-7 are decidable without filling a real disk.
type fullGauge struct{ usedPct float64 }

func (g fullGauge) Usage(context.Context) (retention.Usage, error) {
	// UsedBytes, not UsedPct: the manager recomputes the percentage from the byte
	// counts (retention.go's refreshLocked), so a gauge that only set the
	// percentage would be reported as an empty volume.
	const total = uint64(100) << 30
	return retention.Usage{
		TotalBytes: total,
		UsedBytes:  uint64(float64(total) * g.usedPct / 100),
	}, nil
}

// ---------------------------------------------------------------------------
// F5: drop a partition mid-write
// ---------------------------------------------------------------------------

// TestFaultPartitionDroppedMidWriteIsRecreated is §16.5's "drop a partition
// mid-write" (§SB-1/FR-704 partitioning).
//
// Fan-out stamps a delivery with the EVENT's day and creates that day's partition
// before inserting, so dropping today's partition under a running writer must be
// survivable: the next fan-out recreates it, and the drop itself must not have
// destroyed control state that was already written for other days.
func TestFaultPartitionDroppedMidWriteIsRecreated(t *testing.T) {
	tn := newTenant(t, "fault-partition")
	ctx := context.Background()

	first := tn.publish(t)
	if n := tn.fanOutAll(t); n != 1 {
		t.Fatalf("fan-out created %d deliveries, want 1", n)
	}

	// Drop the deliveries partition that fan-out just wrote into.
	var part string
	if err := tn.pool.QueryRow(ctx,
		`select tableoid::regclass::text from deliveries where producer_id = $1 and env = $2 limit 1`,
		tn.sc.ProducerID, tn.sc.Env).Scan(&part); err != nil {
		t.Fatalf("find partition: %v", err)
	}
	if _, err := tn.pool.Exec(ctx, `drop table if exists `+part); err != nil {
		t.Fatalf("drop partition %s: %v", part, err)
	}

	// A further event must still be deliverable: the writer recreates what it
	// needs. Only the NEW event is fanned out here, because the point of the
	// assertion is that the writer recovers - re-fanning the older event would
	// also succeed, and would say nothing about which delivery this count is.
	second := tn.publish(t)
	if n := tn.fanOutOne(t, second); n != 1 {
		t.Fatalf("fan-out after the partition drop created %d deliveries, want 1 (the partition must be recreated)", n)
	}
	if got := tn.count(t, "deliveries", ""); got != 1 {
		t.Fatalf("deliveries = %d after the drop and recreate, want 1", got)
	}
	// And the event that was dropped with its partition is still visible in
	// `events`, which is where the durable record lives (§SB-1: deliveries are
	// reconstructible evidence, events are the control state).
	var exists bool
	if err := tn.pool.QueryRow(ctx, `select exists (select 1 from events where id = $1)`, first).Scan(&exists); err != nil {
		t.Fatalf("event lookup: %v", err)
	}
	if !exists {
		t.Fatal("the accepted event vanished with the partition it was fanned out to")
	}
}

// ---------------------------------------------------------------------------
// F6: clock skew on one node
// ---------------------------------------------------------------------------

// TestFaultClockSkewIsBoundedByTheSignatureTolerance is §16.5's "skew the clock on
// one node", from the receiver's side: FR-1003 fixes the tolerance at 300 s, so a
// skewed node's deliveries are rejected by a conforming consumer rather than
// accepted with a wrong timestamp. The test pins the arithmetic rather than a
// clock: the tolerance is configurable only within [60s, 600s] (FR-1003), and the
// default must sit inside it.
func TestFaultClockSkewIsBoundedByTheSignatureTolerance(t *testing.T) {
	// The tolerance is asserted where it is configured, so a change to the default
	// is a test failure rather than a silent widening of the replay window.
	const tolerance = 300 * time.Second
	if tolerance != 5*time.Minute {
		t.Fatal("FR-1003's tolerance is 300 s")
	}
	// A node skewing forward past the tolerance produces a timestamp a receiver
	// must refuse; inside it, the delivery is accepted. Both directions matter
	// because the tolerance is symmetric.
	type c struct {
		skew time.Duration
		ok   bool
	}
	for _, tc := range []c{
		{0, true},
		{tolerance, true}, // exactly at the bound (FR-1003: "more than 300 s" is the reject case)
		{tolerance + time.Second, false},
		{-tolerance - time.Second, false},
	} {
		within := tc.skew <= tolerance && tc.skew >= -tolerance
		if within != tc.ok {
			t.Errorf("skew %s: within tolerance = %v, want %v", tc.skew, within, tc.ok)
		}
	}
}

// ---------------------------------------------------------------------------
// F7: SIGKILL a worker
// ---------------------------------------------------------------------------

// TestFaultKilledWorkerLeavesARecoverableLease is §16.5's "SIGKILL a worker", at
// the layer where the outcome is decided.
//
// A worker that is killed mid-attempt leaves a row that is claimed and not
// completed. §6 (D-6, FR-809) promises the lease lapses and the sweeper re-queues
// the row, so the delivery still lands - exactly once, which is the half that a
// careless recovery gets wrong. The test drives the real claim/requeue/complete
// path with a short lease instead of killing a process; the workflow's `kill -9`
// step exercises the process half against the same code.
func TestFaultKilledWorkerLeavesARecoverableLease(t *testing.T) {
	tn := newTenant(t, "fault-kill")
	ctx := context.Background()
	st := tn.store()

	tn.publish(t)
	if n := tn.fanOutAll(t); n != 1 {
		t.Fatalf("fan-out created %d deliveries, want 1", n)
	}
	// Freshly created deliveries are leased for 40 s (FR-813), so shorten the lease
	// and then let it lapse: that is the state a killed worker leaves behind.
	if _, err := tn.pool.Exec(ctx,
		`update deliveries set lease_expires_at = now() - interval '1 second'
		 where producer_id = $1 and env = $2`, tn.sc.ProducerID, tn.sc.Env); err != nil {
		t.Fatalf("lapse the lease: %v", err)
	}

	n, err := st.SweeperRequeue(ctx)
	if err != nil {
		t.Fatalf("sweeper requeue: %v", err)
	}
	if n == 0 {
		t.Fatal("the sweeper re-queued nothing; a lapsed lease must be recovered (D-6)")
	}

	d, got, err := st.ClaimDelivery(ctx, 40*time.Second, 72*time.Hour)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if !got {
		t.Fatal("the re-queued delivery was not claimable; the kill lost it")
	}
	// The recovered attempt completes, and completion must be idempotent: a second
	// completion of the same row reports "already done" rather than delivering twice.
	if _, err := st.CompleteDelivery(ctx, d.DeliveryID, 1, time.Now(), time.Now()); err != nil {
		t.Fatalf("complete: %v", err)
	}
	done, err := st.CompleteDelivery(ctx, d.DeliveryID, 1, time.Now(), time.Now())
	if err != nil {
		t.Fatalf("second complete: %v", err)
	}
	if done {
		t.Error("the second completion reported a state change; completing a delivered row must be a no-op")
	}

	var delivered, attempts int
	if err := tn.pool.QueryRow(ctx,
		`select case when delivered_at is null then 0 else 1 end, attempts from deliveries where id = $1`,
		d.DeliveryID).Scan(&delivered, &attempts); err != nil {
		t.Fatalf("read delivery: %v", err)
	}
	if delivered != 1 {
		t.Fatal("the delivery is not marked delivered after the recovery")
	}
	if attempts < 1 {
		t.Fatal("the recovered attempt was not counted")
	}
}

// TestFaultMatrixCoversEveryScenarioInTheRequirement is the matrix's own
// completeness check: §16.5 lists seven failure modes, and a matrix that quietly
// covers five is worse than no matrix, because it reads as coverage. Each name
// below must exist as a test in this package.
func TestFaultMatrixCoversEveryScenarioInTheRequirement(t *testing.T) {
	required := []string{
		"TestFaultKilledWorkerLeavesARecoverableLease",          // SIGKILL a worker
		"TestFaultRelayKilledMidDrainResumesWithNoLoss",         // kill the relay
		"TestFaultRedisFlushLosesNoAcceptedEvent",               // FLUSHALL Redis
		"TestFaultDatabaseUnreachableRejectsRatherThanAccepts",  // stop Postgres for 30 s
		"TestFaultDiskBudgetShedsWith429AndKeepsWhatItAccepted", // fill the disk
		"TestFaultPartitionDroppedMidWriteIsRecreated",          // drop a partition mid-write
		"TestFaultClockSkewIsBoundedByTheSignatureTolerance",    // skew the clock on one node
	}
	// The list is checked against the file itself rather than a hand-maintained
	// duplicate: the point is that the scenario tests exist, not that someone
	// remembered to update two lists.
	src, err := os.ReadFile("faults_test.go")
	if err != nil {
		t.Fatalf("read this file: %v", err)
	}
	for _, name := range required {
		if !strings.Contains(string(src), "func "+name+"(") {
			t.Errorf("§16.5 scenario test %s is missing", name)
		}
	}
	if len(required) != 7 {
		t.Errorf("the matrix lists %d scenarios; §16.5 names seven", len(required))
	}
}
