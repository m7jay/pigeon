package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m7jay/pigeon/internal/db"
	"github.com/m7jay/pigeon/internal/writeq"
)

// This file is AC-5: the batched control-state path measured against FR-815 on a
// real Postgres, because the property is about transactions the database counts
// (pg_stat_database.xact_commit), not about the queue's own bookkeeping. The
// queue's bookkeeping is asserted in internal/writeq; here the transaction count
// is the database's, exactly as cmd/loadgen samples it.

// fanOutBatch is the relay's drain unit as this slice must be able to carry it
// (SPEC.md A-11 decision 4a): N delivery inserts written in ONE statement, in the
// transaction the write budget opened. The relay's own batch shape belongs to card
// t_b9850e1b - this stands in for it so the queue's contract with that shape (one
// transaction, FR-815's rows per transaction, RLS still enforcing the scope) is
// proven before that card lands rather than after.
type fanOutBatch struct {
	producerID string
	env        string
	eventID    string
	consumerID string
	createdAt  time.Time
	endpoints  []string
	// inserted collects the ids the statement created, so the completion leg can
	// drive exactly those rows.
	inserted []int64
}

func (b *fanOutBatch) Kind() string { return "fanout-deliveries" }
func (b *fanOutBatch) Rows() int    { return len(b.endpoints) }

func (b *fanOutBatch) Apply(ctx context.Context, tx pgx.Tx) error {
	// The insert runs as the runtime role, so RLS is live: both scope variables
	// have to be set on this transaction or the policy's WITH CHECK refuses every
	// row. Setting them is the batch's job, not the queue's.
	if err := db.SetProducerScope(ctx, tx, b.producerID); err != nil {
		return err
	}
	if err := db.SetEnvScope(ctx, tx, b.env); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `
		insert into deliveries (event_id, endpoint_id, producer_id, env, consumer_id, status, created_at)
		select $1, ep, $2, $3, $4, 'pending', $5
		  from unnest($6::text[]) as ep
		returning id`,
		b.eventID, b.producerID, b.env, b.consumerID, b.createdAt, b.endpoints)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return err
		}
		b.inserted = append(b.inserted, id)
	}
	return rows.Err()
}

// xactCommit reads the database's own committed-transaction count. It is the same
// observable cmd/loadgen reads for FR-815's rows-per-transaction.
func xactCommit(t *testing.T, admin *pgxpool.Pool) int64 {
	t.Helper()
	var n int64
	if err := admin.QueryRow(context.Background(),
		`select xact_commit from pg_stat_database where datname = current_database()`).Scan(&n); err != nil {
		t.Fatalf("read xact_commit: %v", err)
	}
	return n
}

// statsSettle bounds how long a sample waits for the database's statistics to
// catch up with a commit that has already happened.
const statsSettle = 1500 * time.Millisecond

// settleStats waits for the database's per-backend statistics to reach shared
// memory. Postgres flushes them on a timer rather than at commit, so a sample
// taken immediately after a workload can read a value that has not moved yet -
// which is how a batched write would look like no write at all, and why the
// fan-out assertion below is a bound rather than an exact 1: this test's own
// sampling transactions are counted in the same window. Polling instead of
// sleeping would not help - every poll is a transaction, and the poll loop
// measures itself.
func settleStats() { time.Sleep(statsSettle) }

// uniqueSlug makes a fixture name unique within one database, because this suite
// can run more than once against the same one (a developer re-running a single
// test, or a retry). seedProducer upserts the producer, but consumers, event
// types and endpoints are only created: a name a previous run left behind is a
// conflict, not a fresh fixture.
func uniqueSlug(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano()%1_000_000_000)
}

// TestControlStateWritesAreBatched is AC-5. It drives a synthetic fan-out +
// completion workload through the write budget and measures it the way FR-815
// defines the floor: rows per transaction, with the transaction count taken from
// pg_stat_database rather than from the queue.
//
// FR-815's binding row set is the fan-out insert (§8.1), and A-11 decision 4
// fixes the shape: the relay's drain unit IS the batch, so a floor-sized fan-out
// is one transaction of 2,000 rows. The completion path cannot reach 2,000 rows
// per transaction by construction, and the card's decision 4 says why and what
// FR-815 is really about there: "FR-815's floor is the commit rate observable,
// not a per-call shape". A completion caller blocks until its row is committed
// (FR-818: never drop, and its callers get a retryable error), so the rows one
// completion transaction can carry are bounded by how many attempts a worker has
// in flight - PIGEON_DELIVERY_CONCURRENCY, 64 - and the property that must hold is
// that the commit rate projected onto the reference load stays in the tens per
// second. Both halves are asserted below, and the projection is printed so the
// gap between 64 and 2,000 is visible rather than hidden behind a tolerance.
func TestControlStateWritesAreBatched(t *testing.T) {
	ctx := context.Background()
	admin := testPool(t)
	app := appPool(t, admin)

	// 64 concurrent completions, matching the delivery worker's per-process cap
	// (FR-816), and a budget wide enough that the budget itself is never the
	// thing being measured.
	const (
		concurrency = 64
		rows        = 2000
	)
	q := NewWriteQueue(app, writeq.Options{
		Depth:        16,
		Workers:      2,
		Pending:      rows,
		AdmitTimeout: 100 * time.Millisecond,
	})
	st := New(app, WithWriteQueue(q))
	t.Cleanup(st.Close)

	slug := uniqueSlug("s12batch")
	sc := Scope{ProducerID: seedRoutingProducer(t, admin, slug), Env: EnvLive}
	consumer := mustConsumer(t, st, sc, "ref-"+slug, "S12 batch consumer")
	// An event-type name matches ^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$, so the
	// unique suffix has to be underscored rather than hyphenated.
	et := mustConsumerEvent(t, st, sc, strings.ReplaceAll(slug, "-", "_")+".batch")
	ep := mustVerifiedEndpoint(t, st, sc, consumer.ID, slug+"-ep", "https://8.8.8.8/hook")
	mustSubscribe(t, st, sc, ep.ID, et.Name)

	// One real event, so the 2,000 deliveries have a real parent row (the FK is
	// the only one deliveries still has).
	published, err := st.PublishEvent(ctx, sc, PublishInput{
		EventType:  et.Name,
		Payload:    json.RawMessage(`{"n":1}`),
		ConsumerID: &consumer.ID,
	})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}

	// The fan-out's partition. Fan-out stamps deliveries.created_at from the
	// event's, so the day is the event's own.
	day := time.Now().UTC()
	if _, err := st.EnsurePartition(ctx, "deliveries", day); err != nil {
		t.Fatalf("EnsurePartition(deliveries): %v", err)
	}
	createdAt := day

	endpoints := make([]string, rows)
	for i := range endpoints {
		endpoints[i] = fmt.Sprintf("%s-ep-%05d", slug, i)
	}

	// --- leg 1: the fan-out batch, one transaction ---------------------------
	fanOut := &fanOutBatch{
		producerID: sc.ProducerID,
		env:        sc.Env,
		eventID:    published.ID,
		consumerID: consumer.ID,
		createdAt:  createdAt,
		endpoints:  endpoints,
	}
	// The queue's own counter is the exact record of what the budget did and is
	// asserted exactly; the database's counter is the observable FR-815 is
	// stated in and is asserted as a bound, because this database has other
	// committers inside the same window (autovacuum after the fixtures, the
	// pools' health checks, and this measurement's own sampling).
	statsBefore := q.Stats()
	before := xactCommit(t, admin)
	if err := q.Submit(ctx, fanOut); err != nil {
		t.Fatalf("submit the fan-out batch: %v", err)
	}
	settleStats()
	after := xactCommit(t, admin)
	statsAfter := q.Stats()
	if len(fanOut.inserted) != rows {
		t.Fatalf("the fan-out batch inserted %d rows, want %d", len(fanOut.inserted), rows)
	}
	if drains, wrote := statsAfter.Drains-statsBefore.Drains, statsAfter.Rows-statsBefore.Rows; drains != 1 || wrote != rows {
		t.Fatalf("a %d-row fan-out batch drained as %d transactions of %d rows, want one transaction of %d", rows, drains, wrote, rows)
	}
	// 2,000 rows in one transaction is FR-815's floor; 2,000 single-row commits
	// is the failure it exists to prevent. Ten rows per transaction is the
	// loosest bound that still separates the two by two orders of magnitude.
	if dbTxns := after - before; dbTxns > int64(rows/10) {
		t.Fatalf("a %d-row fan-out batch committed inside a window of %d database transactions: FR-815's floor is 2,000 rows per transaction", rows, dbTxns)
	} else {
		t.Logf("fan-out leg: %d rows, 1 queue drain, %d database transactions in the window", rows, dbTxns)
	}

	// --- leg 2: completions, batched across the workers ----------------------
	statsBefore = q.Stats()
	base := xactCommit(t, admin)
	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)
	next := make(chan int64, rows)
	go func() {
		defer close(next)
		for _, id := range fanOut.inserted {
			next <- id
		}
	}()
	start := time.Now()
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range next {
				done, err := st.CompleteDelivery(ctx, id, 1, start, start)
				if err != nil {
					errCh <- fmt.Errorf("complete %d: %w", id, err)
					return
				}
				if !done {
					errCh <- fmt.Errorf("complete %d: the row had already left pending", id)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("%v", err)
	}
	elapsed := time.Since(start)
	settleStats()
	dbTxns := xactCommit(t, admin) - base
	stats := q.Stats()
	completionRows := stats.Rows - statsBefore.Rows
	completionTxns := stats.Drains - statsBefore.Drains
	if completionRows != rows {
		t.Fatalf("the completion leg wrote %d rows, want %d", completionRows, rows)
	}
	if completionTxns == 0 {
		t.Fatal("the completion leg committed nothing")
	}

	// Every row is really delivered, and none of them twice.
	var delivered int
	if err := admin.QueryRow(ctx,
		`select count(*) from deliveries where producer_id = $1 and status = 'delivered'`,
		sc.ProducerID).Scan(&delivered); err != nil {
		t.Fatalf("count delivered: %v", err)
	}
	if delivered != rows {
		t.Fatalf("deliveries marked delivered = %d, want %d", delivered, rows)
	}

	// No single-row completion write remains on the batched path: the only
	// caller of the completion function is the batch's statement, which is a
	// source-level property and therefore asserted as one.
	assertNoSingleRowControlWrites(t)

	rowsPerCommit := float64(completionRows) / float64(completionTxns)
	t.Logf("completion leg: %d rows in %d transactions (%.1f rows/transaction, %.2f s, %.0f completions/s; %d database transactions in the window)",
		completionRows, completionTxns, rowsPerCommit, elapsed.Seconds(), float64(completionRows)/elapsed.Seconds(), dbTxns)
	// The database's own counter, with the background it also counts, must still
	// be far below one transaction per row: this is the same claim from the side
	// the reviewer can reproduce with pg_stat_database alone.
	if dbTxns > 200 {
		t.Fatalf("the completion leg ran inside a window of %d database transactions for %d rows: the batched path is not visible as batching", dbTxns, completionRows)
	}

	// The commit rate FR-815 is actually about, projected onto the reference
	// load: 250 events/s x 10 subscriptions = 2,500 delivery rows/s. The
	// completion path cannot reach the fan-out's 2,000 rows per transaction by
	// construction - a completion caller blocks until its row commits, so a
	// drain can only carry the attempts in flight (PIGEON_DELIVERY_CONCURRENCY,
	// 64) - and FR-815's own statement of the failure ("commit rate in the
	// thousands") is what the bound has to separate. The fan-out leg above is
	// where the literal 2,000-rows-per-transaction floor is asserted.
	const referenceRowsPerSecond = 2500
	projected := float64(referenceRowsPerSecond) / rowsPerCommit
	if projected > 250 {
		t.Fatalf("at the reference load this path would commit %.0f times/s (%.1f rows/transaction): FR-815 exists because single-row commits put the commit rate in the thousands", projected, rowsPerCommit)
	}
	t.Logf("completion leg projected at the reference load (2,500 rows/s): %.1f commits/s", projected)
	if rowsPerCommit < 4 {
		t.Fatalf("completions committed %.1f rows per transaction: the queue is not batching across the %d workers in flight", rowsPerCommit, concurrency)
	}
}

// TestCompleteDeliveryIsIdempotentThroughTheBatch pins the outcome semantics the
// batched path owes its callers: a transition that has already happened reports
// false rather than failing, and the second caller is not an error. The SQL
// function returns NULL for that case, which is the shape the batch reads; the
// single-row caller it replaced scanned that NULL into a bool and reported a
// scan failure, turning a normal no-op into a logged error on the delivery path.
func TestCompleteDeliveryIsIdempotentThroughTheBatch(t *testing.T) {
	ctx := context.Background()
	admin := testPool(t)
	app := appPool(t, admin)
	st := New(app)
	t.Cleanup(st.Close)

	slug := uniqueSlug("s12idem")
	sc := Scope{ProducerID: seedRoutingProducer(t, admin, slug), Env: EnvLive}
	id := seedPendingDelivery(t, st, admin, sc, slug)

	now := time.Now().UTC()
	done, err := st.CompleteDelivery(ctx, id, 1, now, now)
	if err != nil {
		t.Fatalf("first CompleteDelivery: %v", err)
	}
	if !done {
		t.Fatal("the first CompleteDelivery reported no transition")
	}
	done, err = st.CompleteDelivery(ctx, id, 1, now, now)
	if err != nil {
		t.Fatalf("second CompleteDelivery: %v, want a no-op rather than an error", err)
	}
	if done {
		t.Fatal("the second CompleteDelivery reported a second transition")
	}
}

// TestControlStateWritesGoThroughOneBudget is the shared-budget claim: a
// completion submitted while every slot is held waits for a slot rather than
// taking a pool connection of its own, and a publish is admitted by the same
// budget - so "full" means the process's write capacity, not one subsystem's
// queue.
func TestControlStateWritesGoThroughOneBudget(t *testing.T) {
	ctx := context.Background()
	admin := testPool(t)
	app := appPool(t, admin)

	q := NewWriteQueue(app, writeq.Options{
		Depth: 1, Workers: 1, Pending: 16, AdmitTimeout: 40 * time.Millisecond,
	})
	st := New(app, WithWriteQueue(q))
	t.Cleanup(st.Close)

	slug := uniqueSlug("s12onebudget")
	sc := Scope{ProducerID: seedRoutingProducer(t, admin, slug), Env: EnvLive}
	id := seedPendingDelivery(t, st, admin, sc, slug)

	// Hold the only slot, then ask the budget both ways.
	held, err := q.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if _, err := q.Admit(ctx); err == nil {
		t.Fatal("Admit was admitted while the only slot was held")
	} else if !strings.Contains(err.Error(), "saturated") {
		t.Fatalf("Admit = %v, want the saturation error the publish path turns into a 429", err)
	}

	completed := make(chan error, 1)
	go func() {
		now := time.Now().UTC()
		_, err := st.CompleteDelivery(ctx, id, 1, now, now)
		completed <- err
	}()
	select {
	case err := <-completed:
		t.Fatalf("a control-state write returned (%v) while the budget was full: it did not go through the budget", err)
	case <-time.After(80 * time.Millisecond):
	}

	held()
	select {
	case err := <-completed:
		if err != nil {
			t.Fatalf("CompleteDelivery after the slot freed = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CompleteDelivery did not return after a slot freed")
	}
}

// assertNoSingleRowControlWrites refuses a direct, single-row call to the
// completion, terminal or requeue function anywhere in the package's non-test
// sources outside the batch's own statements. It is AC-5's "no single-row
// completion write remains on the batched path" read as what it is: a property of
// the code, which a timing sample cannot prove. It matches the CALL form
// (name followed by an open paren), because prose about the function is not a
// call and delivery.go documents what the function does.
func assertNoSingleRowControlWrites(t *testing.T) {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	var offenders []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		if name == "deliverybatch.go" {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(".", name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for _, fn := range []string{"pigeon_complete_delivery", "pigeon_finalize_delivery", "pigeon_requeue_delivery"} {
			if strings.Contains(string(raw), fn+"(") {
				offenders = append(offenders, name+" calls "+fn)
			}
		}
	}
	if len(offenders) > 0 {
		t.Fatalf("single-row control-state writes remain outside the batch: %v", offenders)
	}
}

// seedPendingDelivery creates one published event, one consumer-routed type, one
// verified endpoint and one pending delivery in the caller's scope, and returns
// the delivery id. It is the smallest fixture that a completion can be driven
// against, and it creates no state another test in the package owns.
func seedPendingDelivery(t *testing.T, st *Store, admin *pgxpool.Pool, sc Scope, slug string) int64 {
	t.Helper()
	ctx := context.Background()
	consumer := mustConsumer(t, st, sc, "ref-"+slug, "consumer "+slug)
	et := mustConsumerEvent(t, st, sc, strings.ReplaceAll(slug, "-", "_")+".evt")
	ep := mustVerifiedEndpoint(t, st, sc, consumer.ID, slug+"-ep", "https://8.8.8.8/hook"+slug)
	mustSubscribe(t, st, sc, ep.ID, et.Name)

	published, err := st.PublishEvent(ctx, sc, PublishInput{
		EventType:  et.Name,
		Payload:    json.RawMessage(`{}`),
		ConsumerID: &consumer.ID,
	})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	day := time.Now().UTC()
	if _, err := st.EnsurePartition(ctx, "deliveries", day); err != nil {
		t.Fatalf("EnsurePartition(deliveries): %v", err)
	}

	var id int64
	if err := admin.QueryRow(ctx, `
		insert into deliveries (event_id, endpoint_id, producer_id, env, consumer_id, status, created_at)
		values ($1, $2, $3, $4, $5, 'pending', $6)
		returning id`,
		published.ID, slug+"-ep", sc.ProducerID, sc.Env, consumer.ID, day).Scan(&id); err != nil {
		t.Fatalf("insert pending delivery: %v", err)
	}
	return id
}
