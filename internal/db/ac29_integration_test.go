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

	run := func(tx func(context.Context, func(tx pgx.Tx) error) error) {
		defer wg.Done()
		<-start
		errs <- tx(ctx, func(tx pgx.Tx) error {
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

	// Poll while both transactions are parked in pg_sleep. The marker is set
	// transaction-locally before fn runs, so both must be readable at once.
	deadline := time.Now().Add(1500 * time.Millisecond)
	var evidence, control int
	for time.Now().Before(deadline) {
		if err := pool.QueryRow(ctx,
			`select count(*) filter (where application_name = $1),
			        count(*) filter (where application_name = $2)
			   from pg_stat_activity
			  where state = 'active'`,
			TxClassEvidence, TxClassControl,
		).Scan(&evidence, &control); err != nil {
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
	t.Logf("pg_stat_activity under load: %d evidence backend(s) %q, %d control backend(s) %q",
		evidence, TxClassEvidence, control, TxClassControl)

	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("in-flight transaction failed: %v", err)
		}
	}

	// The marker is transaction-local: once both commits land, no backend may
	// still advertise a class marker. This is what makes the split safe on a
	// pooled connection (FR-812) rather than a leak into the next borrower.
	var lingering int
	if err := pool.QueryRow(ctx,
		`select count(*) from pg_stat_activity where application_name like 'pigeon-%(%)'`,
	).Scan(&lingering); err != nil {
		t.Fatalf("read pg_stat_activity after commit: %v", err)
	}
	if lingering != 0 {
		t.Errorf("%d backend(s) still carry a class marker after the transactions committed; the setting leaked", lingering)
	}
}
