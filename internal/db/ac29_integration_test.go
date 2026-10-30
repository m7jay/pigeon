package db

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestEvidenceAndControlTxAreDistinguishableInPgStatActivity is AC-29's proof.
//
// FR-812 splits the write classes by commit durability: evidence commits with
// synchronous_commit=off, control with on. AC-29 asks for that split to be
// verifiable from pg_stat_activity *under load* - so this test runs one
// EvidenceTx and one ControlTx concurrently, both blocked inside their
// transaction on pg_sleep, and reads the server's own view from a third
// connection while both are in flight. It asserts each class is visible under
// its own marker simultaneously, and that the marker reverts when the
// transaction ends (the setting is transaction-local, not pooled).
//
// The marker is not the setting. db.go stamps the marker from a literal that
// is independent of the value it hands to set_config for the GUC, so a split
// broken at that set_config call leaves the marker - and the operator reading
// it out of pg_stat_activity - naming a durability class the backend is not
// running. Each class therefore reads current_setting('synchronous_commit')
// back from INSIDE its own transaction and is required to be running the value
// FR-812 pins, and, once both commit, the setting is read back on the pooled
// connection itself to show it did not outlive its transaction (see
// assertSyncCommitDoesNotLeakPastTheClassTransaction).
//
// pg_stat_activity is CLUSTER-wide, and every read here is scoped twice: to
// the database this pool is connected to, and to the pids this pool's own two
// transactions report from inside themselves. Without the first, a healthy
// evidence or control transaction in another database on the same server
// (another suite, another worker, another tenant's box sharing the cluster) is
// counted as this pool's backend; without the second, an unrelated session of
// this same database is. Either one both satisfies a poll this pool should
// have failed and, worse, reports a leak that is not one. The claim AC-29
// makes is about *this* pool's write classes.
//
// Gated on PIGEON_TEST_DATABASE_URL like the rest of the DB-backed suite.
func TestEvidenceAndControlTxAreDistinguishableInPgStatActivity(t *testing.T) {
	dsn := os.Getenv("PIGEON_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PIGEON_TEST_DATABASE_URL not set; skipping the AC-29 pg_stat_activity proof")
	}
	ctx := context.Background()
	pool, err := Connect(ctx, dsn, 10)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer pool.Close()

	const hold = 2 * time.Second
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	start := make(chan struct{})

	// live is filled from INSIDE the two transactions: the backend each pooled
	// connection is running on, and the synchronous_commit that backend has in
	// force. Those pids - not whoever else happens to be connected to this
	// database - are the backends AC-29's leak claim is about, and the only
	// ones this test may accuse.
	type liveTx struct {
		class string
		pid   int32
		guc   string
	}
	live := make(chan liveTx, 2)

	run := func(class string, tx func(context.Context, func(tx pgx.Tx) error) error) {
		defer wg.Done()
		<-start
		errs <- tx(ctx, func(tx pgx.Tx) error {
			sample := liveTx{class: class}
			if err := tx.QueryRow(ctx, "select pg_backend_pid()").Scan(&sample.pid); err != nil {
				return err
			}
			// The GUC this transaction is actually running with, read from
			// inside it. set_config has already applied it - db.go sets both
			// the setting and the marker before fn runs.
			if err := tx.QueryRow(ctx, "select current_setting('synchronous_commit')").Scan(&sample.guc); err != nil {
				return err
			}
			live <- sample
			_, err := tx.Exec(ctx, "select pg_sleep($1)", hold.Seconds())
			return err
		})
	}

	wg.Add(2)
	go run(TxClassEvidence, func(c context.Context, fn func(pgx.Tx) error) error {
		return EvidenceTx(c, pool, fn)
	})
	go run(TxClassControl, func(c context.Context, fn func(pgx.Tx) error) error {
		return ControlTx(c, pool, fn)
	})
	close(start)

	own := map[int32]bool{}
	inForce := map[string]string{}
	for len(own) < 2 {
		select {
		case sample, ok := <-live:
			if !ok {
				t.Fatalf("both transactions must report the backend they run on; got %d pid(s)", len(own))
			}
			own[sample.pid] = true
			inForce[sample.class] = sample.guc
		case <-time.After(3 * time.Second):
			t.Fatalf("both transactions must report the backend they run on; got %d pid(s)", len(own))
		}
	}

	// FR-812's split, asserted against the SETTING and not the label db.go
	// writes next to it: inside its own transaction the evidence class must be
	// running synchronous_commit=off and the control class on. The marker
	// constants spell the same two values out, but nothing in db.go couples
	// them - the marker comes from the `class` argument and the GUC from the
	// `syncCommit` argument - so only a read of the GUC can see a split broken
	// at the set_config call. Measured before this assertion existed: flipping
	// EvidenceTx's argument from "off" to "on" left this test green, and left
	// pg_stat_activity reporting `synchronous_commit=off` for a backend that
	// had been running `on`.
	for _, want := range []struct{ class, guc string }{
		{TxClassEvidence, "off"},
		{TxClassControl, "on"},
	} {
		if got := inForce[want.class]; got != want.guc {
			t.Errorf("in-flight %q transaction ran with synchronous_commit=%q, want %q: the durability split FR-812 pins is broken (the class marker still reads %q, so pg_stat_activity names a class this backend is not running)",
				want.class, got, want.guc, want.class)
		}
	}

	// Poll while both transactions are parked in pg_sleep. The marker is set
	// transaction-locally before fn runs, so both must be readable at once.
	//
	// Both scopes are load-bearing, because pg_stat_activity is CLUSTER-wide:
	// the database filter keeps another database's evidence/control backend on
	// the same server (another suite, another worker, another process on the
	// box) from standing in for this pool's writer, and the pid set keeps a
	// foreign session of THIS database from doing the same. AC-29's claim is
	// about this pool's write classes; only `own` is this pool.
	deadline := time.Now().Add(1500 * time.Millisecond)
	var evidence, control, foreign int
	for time.Now().Before(deadline) {
		rows, err := pool.Query(ctx,
			`select pid, application_name
			   from pg_stat_activity
			  where state = 'active'
			    and datname = current_database()
			    and application_name in ($1, $2)`,
			TxClassEvidence, TxClassControl)
		if err != nil {
			t.Fatalf("read pg_stat_activity: %v", err)
		}
		evidence, control, foreign = 0, 0, 0
		for rows.Next() {
			var pid int32
			var name string
			if err := rows.Scan(&pid, &name); err != nil {
				rows.Close()
				t.Fatalf("read pg_stat_activity: %v", err)
			}
			if !own[pid] {
				foreign++
				continue
			}
			if name == TxClassEvidence {
				evidence++
			} else {
				control++
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatalf("read pg_stat_activity: %v", err)
		}
		if evidence > 0 && control > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if evidence == 0 {
		t.Errorf("no active backend showed application_name=%q; the evidence marker is not visible under load", TxClassEvidence)
	}
	if control == 0 {
		t.Errorf("no active backend showed application_name=%q; the control marker is not visible under load", TxClassControl)
	}
	pids := make([]int32, 0, len(own))
	for pid := range own {
		pids = append(pids, pid)
	}
	t.Logf("pg_stat_activity under load: this pool's pids %v showed %d evidence %q and %d control %q; %d other marker-bearing backend(s) in this database (not this pool)",
		pids, evidence, TxClassEvidence, control, TxClassControl, foreign)

	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("in-flight transaction failed: %v", err)
		}
	}

	// The marker is transaction-local: once both commits land, the backends
	// that were stamped must have reverted it. That is what makes the split
	// safe on a pooled connection (FR-812) rather than a leak into the next
	// borrower.
	//
	// Poll with a deadline instead of taking one sample. A backend's view of a
	// just-committed peer is updated around the commit, not inside the client's
	// wait for it, so the first sample taken the instant wg.Wait() returns can
	// still read a marker the commit has already reverted - a race in this
	// assertion, not a leak in the split (reported from CI-shaped runs at 1
	// failure in 5). A real leak is not transient: a marker that survives its
	// transaction stays on that pooled connection for its whole idle life, and
	// that is the borrower the next transaction inherits, so waiting up to
	// `settle` cannot hide one.
	//
	// The count is this pool's own backends and nothing else - not every
	// pigeon-% backend in the database. FR-812's promise is that a stamped
	// transaction does not leak its setting to the next borrower of the same
	// pooled connection, and an unrelated session running a write-class
	// transaction against this database right now is not this pool's business
	// (measured: with the database filter but without this scope, a marker held
	// by another session of the same database made this test red).
	const settle = 500 * time.Millisecond
	leakDeadline := time.Now().Add(settle)
	var lingering int
	var survivors string
	for {
		if err := pool.QueryRow(ctx,
			`select count(*),
			        coalesce(string_agg(format('pid %s %s state=%s', pid, application_name, state), ', '), '')
			   from pg_stat_activity
			  where pid = any($1::int[])
			    and application_name like 'pigeon-%(%)'`,
			pids,
		).Scan(&lingering, &survivors); err != nil {
			t.Fatalf("read pg_stat_activity after commit: %v", err)
		}
		if lingering == 0 || !time.Now().Before(leakDeadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if lingering != 0 {
		// Name the survivors: a leak report that does not say which backend
		// holds which marker costs the reviewer an hour of archaeology.
		t.Errorf("%d backend(s) still carry a class marker %s after the transactions committed; the setting leaked: %s",
			lingering, settle, survivors)
	}

	// The assertions above read the marker; the setting it names is pinned
	// here, including the one property the marker cannot witness at all -
	// set_config's third argument. application_name is applied with `true` and
	// reverts even when synchronous_commit is applied with `false` and does not.
	assertSyncCommitDoesNotLeakPastTheClassTransaction(t, ctx, dsn)
}

// assertSyncCommitDoesNotLeakPastTheClassTransaction pins the half of FR-812
// the class marker cannot see.
//
// txWith applies the setting with set_config('synchronous_commit', $1, true):
// transaction-local, so the value dies with the commit and the next borrower of
// that pooled connection gets the session's own value back. Only the third
// argument decides that, and it is invisible from inside the transaction -
// `false`, the session-level form, would leave the connection running
// synchronous_commit=off (or on) for the rest of its life, which is exactly
// what FR-812 forbids. The marker assertion cannot catch it: application_name
// is applied by its own transaction-local set_config and reverts even when the
// GUC does not.
//
// The readback has to land on the backend the class transaction ran on, so each
// case runs against a pool capped at ONE connection: the class transaction and
// the readback before and after it are then the same session by construction
// rather than by luck of the pool's idle list. The session's own value is
// pinned to the class's opposite first, so a cluster default that happens to
// equal the class value (postgres:16's is `on`, which would quietly excuse an
// evidence-class leak) cannot satisfy the assertion either.
func assertSyncCommitDoesNotLeakPastTheClassTransaction(t *testing.T, ctx context.Context, dsn string) {
	t.Helper()
	// Either direction of a session-level leak has to be visible, so each case
	// pins the session value to the class's OPPOSITE before running it: on a
	// cluster whose default is `on` the evidence case is the one a leak shows
	// up in, on a cluster defaulting to `off` it is the control case.
	for _, tc := range []struct {
		class    string
		want     string // the synchronous_commit FR-812 pins for this class
		baseline string // the session value pinned first, deliberately the other one
		run      func(context.Context, *pgxpool.Pool, func(pgx.Tx) error) error
	}{
		{class: TxClassEvidence, want: "off", baseline: "on", run: EvidenceTx},
		{class: TxClassControl, want: "on", baseline: "off", run: ControlTx},
	} {
		t.Run(tc.class, func(t *testing.T) {
			pool, err := Connect(ctx, dsn, 1)
			if err != nil {
				t.Fatalf("Connect (single-connection pool): %v", err)
			}
			defer pool.Close()

			// One round trip that both pins the session-level value and names
			// the backend this readback is about.
			var pid int32
			var pinned string
			if err := pool.QueryRow(ctx,
				"select pg_backend_pid(), set_config('synchronous_commit', $1, false)", tc.baseline,
			).Scan(&pid, &pinned); err != nil {
				t.Fatalf("pin the session-level synchronous_commit=%s: %v", tc.baseline, err)
			}
			if pinned != tc.baseline {
				t.Fatalf("pinned the session-level setting to %q but the server reports %q", tc.baseline, pinned)
			}

			var before string
			if err := pool.QueryRow(ctx, "select current_setting('synchronous_commit')").Scan(&before); err != nil {
				t.Fatalf("read the pinned session-level setting back: %v", err)
			}

			// The class transaction on that same connection.
			var inTx string
			if err := tc.run(ctx, pool, func(tx pgx.Tx) error {
				return tx.QueryRow(ctx, "select current_setting('synchronous_commit')").Scan(&inTx)
			}); err != nil {
				t.Fatalf("%s transaction on the single-connection pool: %v", tc.class, err)
			}
			if inTx != tc.want {
				t.Errorf("in-flight %q transaction ran with synchronous_commit=%q, want %q", tc.class, inTx, tc.want)
			}

			var after string
			if err := pool.QueryRow(ctx, "select current_setting('synchronous_commit')").Scan(&after); err != nil {
				t.Fatalf("read the setting back after the class transaction committed: %v", err)
			}
			if after != before {
				t.Errorf("pid %d still reports synchronous_commit=%q after the %q class transaction committed on it, but the session value pinned before that transaction was %q: the setting escaped its transaction (set_config(..., false) instead of true), so the next borrower of this pooled connection inherits it, which FR-812 forbids",
					pid, after, tc.class, before)
			}
		})
	}
}
