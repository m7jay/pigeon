package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/m7jay/pigeon/internal/writeq"
)

// This file is the batched half of the delivery worker's control-state writes
// (FR-818, SPEC.md A-11 decision 4b). Before it, every completion, terminal
// transition and requeue was its own ControlTx: one transaction, one row, at the
// rate the worker completes attempts. FR-815's floor for this path is 2,000 rows
// per transaction at the reference load, and the only way to reach it is to let
// the rows accumulate across workers - which is what DeliveryBatch plus the write
// queue does.
//
// The write itself is one statement per operation for the whole batch: the rows
// travel as parallel arrays zipped positionally by a single multi-argument
// unnest, exactly the shape pigeon_record_attempts (0010) and store.RecordAttempts
// already use for buffered evidence. That is not only fewer round trips: a
// positional zip is what makes it impossible for one delivery's parameters to be
// paired with another delivery's row (SB-4's rule, applied to control state).
//
// The delivery worker's own API is unchanged. CompleteDelivery, FinalizeDelivery
// and RequeueDelivery still take one delivery and still return whether the
// transition happened; they now submit one-row batches to the shared budget and
// wait for the transaction that carried them, so a caller that got `nil` can
// still trust that the row is committed (FR-604's rule for control state: the
// commit is what makes the write true).

// ControlOp is one control-state transition on a delivery row.
type ControlOp string

const (
	// ControlComplete is FR-811's successful attempt: status, attempts,
	// first_attempt_at, delivered_at, lease cleared.
	ControlComplete ControlOp = "complete"
	// ControlFinalize is FR-806's terminal failure.
	ControlFinalize ControlOp = "finalize"
	// ControlRequeue is FR-805/FR-1208's retry schedule.
	ControlRequeue ControlOp = "requeue"
)

// DeliveryWrite is one pending control-state write: one delivery, one
// transition, and the parameters that transition needs.
type DeliveryWrite struct {
	Op         ControlOp
	DeliveryID int64
	// Attempts and FirstAttempt are what Complete and Finalize set on the row
	// (FR-811). Requeue ignores them.
	Attempts     int
	FirstAttempt time.Time
	// At is the instant this operation carries: for Complete it is the
	// delivered_at FR-811 promises, for Requeue it is the next attempt's instant
	// (FR-1208). Finalize carries none.
	At time.Time
	// RetryDeadline is FR-1201's window (Requeue only).
	RetryDeadline time.Time
}

// applied is a pointer so that a batch folded into another batch by Coalesce
// still sees its own row's outcome: the drain writes the result through the
// pointer the original caller is holding.
type deliveryWrite struct {
	DeliveryWrite
	applied bool
}

// deliveryControlKind is the coalescing key. Every DeliveryBatch shares it, so
// every control-state write accumulated in one drain is written in ONE
// transaction (the statements inside it are per operation, not per batch).
const deliveryControlKind = "delivery-control"

// DeliveryBatch is a batch of delivery control-state writes submitted to
// FR-818's write budget as one unit.
//
// It implements writeq.Batch and writeq.Coalescer: a drain folds the batches of
// every worker that happened to submit together into one batch, so N workers
// completing one delivery each cost one transaction and (per operation) one
// statement, not N of either.
type DeliveryBatch struct {
	writes []*deliveryWrite
}

// NewDeliveryBatch starts an empty batch.
func NewDeliveryBatch() *DeliveryBatch { return &DeliveryBatch{} }

// Add appends one write and returns its index, which is how the caller reads the
// outcome back with Applied after the batch commits. The writes of one batch
// keep their order, and every index stays valid across Coalesce: folding appends
// to the same slice the results are recorded in.
func (b *DeliveryBatch) Add(w DeliveryWrite) int {
	b.writes = append(b.writes, &deliveryWrite{DeliveryWrite: w})
	return len(b.writes) - 1
}

// Applied reports whether the write at i transitioned its row. A completed
// delivery, a terminal transition and a requeue are all idempotent (the SQL
// functions only move a still-pending row), so false means "someone else already
// moved it", which is a normal outcome and not an error.
func (b *DeliveryBatch) Applied(i int) bool {
	if i < 0 || i >= len(b.writes) {
		return false
	}
	return b.writes[i].applied
}

// Kind is the writeq coalescing key.
func (b *DeliveryBatch) Kind() string { return deliveryControlKind }

// Rows is the number of rows this batch writes: one per delivery transition,
// which is what FR-815's floor is counted in.
func (b *DeliveryBatch) Rows() int { return len(b.writes) }

// Coalesce folds another batch of the same kind into this one, so one drain
// writes one transaction. It refuses a different type rather than guessing: the
// queue then applies that batch separately, in the same transaction, which costs
// a statement and never a row.
func (b *DeliveryBatch) Coalesce(other writeq.Batch) bool {
	o, ok := other.(*DeliveryBatch)
	if !ok || o == nil {
		return false
	}
	b.writes = append(b.writes, o.writes...)
	return true
}

// opGroup is the writes of one operation, in batch order, with the batch index of
// each so the statement's ordinality can be mapped back to the caller.
type opGroup struct {
	op  ControlOp
	idx []int
}

// groups splits the batch by operation, preserving first-appearance order so a
// batch's statements are deterministic.
func (b *DeliveryBatch) groups() []opGroup {
	var groups []opGroup
	at := map[ControlOp]int{}
	for i, w := range b.writes {
		gi, ok := at[w.Op]
		if !ok {
			groups = append(groups, opGroup{op: w.Op})
			gi = len(groups) - 1
			at[w.Op] = gi
		}
		groups[gi].idx = append(groups[gi].idx, i)
	}
	return groups
}

// Apply writes every row of the batch in tx: one statement per operation, all
// inside the single transaction the queue opened for this drain, so the drain is
// one commit (FR-815).
func (b *DeliveryBatch) Apply(ctx context.Context, tx pgx.Tx) error {
	if len(b.writes) == 0 {
		return nil
	}
	for _, g := range b.groups() {
		var err error
		switch g.op {
		case ControlComplete:
			err = b.applyComplete(ctx, tx, g)
		case ControlFinalize:
			err = b.applyFinalize(ctx, tx, g)
		case ControlRequeue:
			err = b.applyRequeue(ctx, tx, g)
		default:
			// Unreachable through the store's own callers; a refusal rather than
			// a silent no-op, because silently dropping a control-state write is
			// the one thing this path must never do.
			return fmt.Errorf("delivery batch: unknown control op %q", g.op)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// applyComplete writes every completion in the group with one call to
// pigeon_complete_delivery per row, in one statement.
func (b *DeliveryBatch) applyComplete(ctx context.Context, tx pgx.Tx, g opGroup) error {
	ids := make([]int64, len(g.idx))
	attempts := make([]int32, len(g.idx))
	firsts := make([]time.Time, len(g.idx))
	ats := make([]time.Time, len(g.idx))
	for n, i := range g.idx {
		w := b.writes[i].DeliveryWrite
		ids[n] = w.DeliveryID
		attempts[n] = int32(w.Attempts)
		firsts[n] = w.FirstAttempt
		ats[n] = w.At
	}
	rows, err := tx.Query(ctx, `
		select x.ord,
		       pigeon_complete_delivery(x.id, x.attempts, x.first_attempt, x.at)
		  from unnest($1::bigint[], $2::int[], $3::timestamptz[], $4::timestamptz[])
		       with ordinality as x(id, attempts, first_attempt, at, ord)`,
		ids, attempts, firsts, ats)
	if err != nil {
		return fmt.Errorf("complete %d deliveries: %w", len(ids), err)
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var ord int64
		// Nullable: the function's `returning true` yields NULL when its UPDATE
		// matched no row, which is the idempotent no-op, not a failure.
		var applied *bool
		if err := rows.Scan(&ord, &applied); err != nil {
			return fmt.Errorf("complete %d deliveries: %w", len(ids), err)
		}
		seen++
		if ord < 1 || int(ord) > len(g.idx) {
			return fmt.Errorf("complete %d deliveries: result %d is outside the batch", len(ids), ord)
		}
		b.writes[g.idx[ord-1]].applied = applied != nil && *applied
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("complete %d deliveries: %w", len(ids), err)
	}
	if seen != len(g.idx) {
		return fmt.Errorf("complete %d deliveries: %d results came back", len(ids), seen)
	}
	return nil
}

// applyFinalize writes every terminal transition in the group in one statement.
func (b *DeliveryBatch) applyFinalize(ctx context.Context, tx pgx.Tx, g opGroup) error {
	ids := make([]int64, len(g.idx))
	attempts := make([]int32, len(g.idx))
	firsts := make([]time.Time, len(g.idx))
	for n, i := range g.idx {
		w := b.writes[i].DeliveryWrite
		ids[n] = w.DeliveryID
		attempts[n] = int32(w.Attempts)
		firsts[n] = w.FirstAttempt
	}
	rows, err := tx.Query(ctx, `
		select x.ord,
		       pigeon_finalize_delivery(x.id, x.attempts, x.first_attempt)
		  from unnest($1::bigint[], $2::int[], $3::timestamptz[])
		       with ordinality as x(id, attempts, first_attempt, ord)`,
		ids, attempts, firsts)
	if err != nil {
		return fmt.Errorf("finalize %d deliveries: %w", len(ids), err)
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var ord int64
		var applied *bool
		if err := rows.Scan(&ord, &applied); err != nil {
			return fmt.Errorf("finalize %d deliveries: %w", len(ids), err)
		}
		seen++
		if ord < 1 || int(ord) > len(g.idx) {
			return fmt.Errorf("finalize %d deliveries: result %d is outside the batch", len(ids), ord)
		}
		b.writes[g.idx[ord-1]].applied = applied != nil && *applied
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("finalize %d deliveries: %w", len(ids), err)
	}
	if seen != len(g.idx) {
		return fmt.Errorf("finalize %d deliveries: %d results came back", len(ids), seen)
	}
	return nil
}

// applyRequeue writes every retry schedule in the group in one statement.
// pigeon_requeue_delivery returns void, so there is no per-row outcome to map
// back: the transition is idempotent and its result is not an input to anything
// the worker does next.
func (b *DeliveryBatch) applyRequeue(ctx context.Context, tx pgx.Tx, g opGroup) error {
	ids := make([]int64, len(g.idx))
	nexts := make([]time.Time, len(g.idx))
	deadlines := make([]time.Time, len(g.idx))
	attempts := make([]int32, len(g.idx))
	for n, i := range g.idx {
		w := b.writes[i].DeliveryWrite
		ids[n] = w.DeliveryID
		nexts[n] = w.At
		deadlines[n] = w.RetryDeadline
		attempts[n] = int32(w.Attempts)
	}
	rows, err := tx.Query(ctx, `
		select pigeon_requeue_delivery(x.id, x.next_attempt_at, x.retry_deadline, x.attempt)
		  from unnest($1::bigint[], $2::timestamptz[], $3::timestamptz[], $4::int[])
		       as x(id, next_attempt_at, retry_deadline, attempt)`,
		ids, nexts, deadlines, attempts)
	if err != nil {
		return fmt.Errorf("requeue %d deliveries: %w", len(ids), err)
	}
	defer rows.Close()
	for rows.Next() {
		// Drained so the statement runs to completion server-side; nothing is
		// read from the void result.
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("requeue %d deliveries: %w", len(ids), err)
	}
	return nil
}
