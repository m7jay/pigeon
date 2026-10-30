package delivery

import (
	"testing"

	"github.com/m7jay/pigeon/internal/store"
)

// TestLaneAllowancePartitionsEveryAcceptedProcessCap pins FR-1303's arithmetic
// (SPEC.md A-10): R = clamp(configured, 0, C-1) over a configured value the config
// loader has already validated >= 1, so
//
//   - the two allowances PARTITION C (they never add up to C + R: the shipped
//     per-process ceiling is not raised by this slice),
//   - the first-attempt lane keeps at least one slot at every accepted C, and
//   - the clamp reaches R = 0 only at C = 1, where one lane serves both classes.
func TestLaneAllowancePartitionsEveryAcceptedProcessCap(t *testing.T) {
	cases := []struct {
		name                   string
		process, retry         int
		wantFirst, wantRetries int
	}{
		{"the shipped defaults", 64, 16, 48, 16},
		{"the smallest C with two lanes", 2, 1, 1, 1},
		{"C = 1 and the shipped R: the clamp reaches R = 0", 1, 16, 1, 0},
		{"C = 1 and R = 1: the clamp reaches R = 0", 1, 1, 1, 0},
		{"an over-large R is clamped, not refused", 8, 8, 1, 7},
		{"an over-large R at a small C", 4, 99, 1, 3},
		{"an unset C is guarded to one slot", 0, 5, 1, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			first, retries := laneAllowance(tc.process, tc.retry)
			if first != tc.wantFirst || retries != tc.wantRetries {
				t.Fatalf("laneAllowance(%d, %d) = (%d, %d), want (%d, %d)",
					tc.process, tc.retry, first, retries, tc.wantFirst, tc.wantRetries)
			}
			if first < 1 {
				t.Errorf("laneAllowance(%d, %d) leaves the first-attempt lane %d slots (FR-1303)",
					tc.process, tc.retry, first)
			}
			if want := max(tc.process, 1); first+retries != want {
				t.Errorf("laneAllowance(%d, %d) sums to %d, want the process cap %d: the lanes partition the budget (FR-816, FR-1303)",
					tc.process, tc.retry, first+retries, want)
			}
		})
	}
}

// TestLanesAtTheTwoBoundaries pins the shape newLanes builds where the partition
// degenerates, which is why the clamp is written as a formula rather than a range:
// at C = 2 there are two lanes with one slot each, and at C = 1 - the only way to
// reach R = 0 - there is ONE lane, serving both classes, because a degenerate
// partition MUST NOT starve FR-1201's retry policy.
func TestLanesAtTheTwoBoundaries(t *testing.T) {
	two := newLanes(2, 1)
	if len(two) != 2 {
		t.Fatalf("newLanes(2, 1) built %d lanes, want 2", len(two))
	}
	if two[0].class != store.AttemptClassFirst || two[1].class != store.AttemptClassRetry {
		t.Errorf("newLanes(2, 1) classes are %q, %q; want %q then %q",
			two[0].class, two[1].class, store.AttemptClassFirst, store.AttemptClassRetry)
	}
	if cap(two[0].sem) != 1 || cap(two[1].sem) != 1 {
		t.Errorf("newLanes(2, 1) allowances are %d and %d, want 1 and 1",
			cap(two[0].sem), cap(two[1].sem))
	}

	one := newLanes(1, 1)
	if len(one) != 1 {
		t.Fatalf("newLanes(1, 1) built %d lanes, want the single shared lane of R = 0", len(one))
	}
	if one[0].class != store.AttemptClassAny {
		t.Errorf("newLanes(1, 1) claims class %q, want %q: the one lane serves both classes",
			one[0].class, store.AttemptClassAny)
	}
	if cap(one[0].sem) != 1 {
		t.Errorf("newLanes(1, 1) allowance is %d, want 1 (the process cap)", cap(one[0].sem))
	}
}
