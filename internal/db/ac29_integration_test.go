package db

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
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
	// ownPids is filled from INSIDE the two transactions: the backend each
	// pooled connection is running on. Those pids - not whoever else happens to
	// be connected to this database - are the backends AC-29's leak claim is
	// about, and the only ones this test may accuse.
	ownPids := make(chan int32, 2)

	run := func(tx func(context.Context, func(tx pgx.Tx) error) error) {
		defer wg.Done()
		<-start
		errs <- tx(ctx, func(tx pgx.Tx) error {
			var pid int32
			if err := tx.QueryRow(ctx, "select pg_backend_pid()").Scan(&pid); err != nil {
				return err
			}
			ownPids <- pid
			_, err := tx.Exec(ctx, "select pg_sleep($1)", hold.Seconds())
			return err
		})
	}

	wg.Add(2)
	go run(func(c context.Context, fn func(pgx.Tx) error) error {
		return EvidenceTx(c, pool, fn)
	})
	go run(func(c context.Context, fn func(pgx.Tx) error) error {
		return ControlTx(c, pool, fn)
	})
	close(start)

	own := map[int32]bool{}
	for len(own) < 2 {
		select {
		case pid, ok := <-ownPids:
			if !ok {
				t.Fatalf("both transactions must report the backend they run on; got %d pid(s)", len(own))
			}
			own[pid] = true
		case <-time.After(3 * time.Second):
			t.Fatalf("both transactions must report the backend they run on; got %d pid(s)", len(own))
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
}
