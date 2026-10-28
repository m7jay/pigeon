package identity

import (
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

// TestPasswordCostDefaultIsProductionGrade pins the shipped default, because the
// harness knob (SetPasswordCostForTests) is only safe to hand to a test binary if
// the constant it starts from is the production one. A process that reached this
// test with a moved cost means some init path lowered it, which would be a real
// weakening of stored passwords rather than a slow test suite.
//
// The hash a login actually carries is checked by the bootstrap test in
// identity_test.go (the row EnsureInstanceAdmin writes must be hashed at
// DefaultPasswordCost), so this test does not need a cost-12 hash of its own -
// that alone is ~5 s under -race.
func TestPasswordCostDefaultIsProductionGrade(t *testing.T) {
	if DefaultPasswordCost != 12 {
		t.Fatalf("DefaultPasswordCost = %d, want 12 (bcrypt cost 12 is the production floor)", DefaultPasswordCost)
	}
	if passwordCost != DefaultPasswordCost {
		t.Fatalf("effective bcrypt cost is %d before any test moved it, want the default %d", passwordCost, DefaultPasswordCost)
	}
}

// TestSetPasswordCostForTestsIsBounded covers the harness knob itself: it moves
// the effective cost (so the cheap-fixture harness is not silently a no-op), it
// rejects a cost bcrypt cannot use without applying it, and it is reversible.
func TestSetPasswordCostForTestsIsBounded(t *testing.T) {
	defer func() {
		if err := SetPasswordCostForTests(DefaultPasswordCost); err != nil {
			t.Fatalf("restore the default cost: %v", err)
		}
	}()

	for _, bad := range []int{bcrypt.MinCost - 1, bcrypt.MaxCost + 1, -1} {
		if err := SetPasswordCostForTests(bad); err == nil {
			t.Fatalf("SetPasswordCostForTests(%d) was accepted; bcrypt cannot hash at that cost", bad)
		} else if !strings.Contains(err.Error(), "bcrypt") {
			t.Fatalf("SetPasswordCostForTests(%d) failed with %q, want an error naming bcrypt", bad, err)
		}
	}
	if passwordCost != DefaultPasswordCost {
		t.Fatalf("a refused cost was applied anyway: effective cost is now %d", passwordCost)
	}

	if err := SetPasswordCostForTests(bcrypt.MinCost); err != nil {
		t.Fatalf("lower the cost to bcrypt.MinCost: %v", err)
	}
	hash, err := HashPassword("harness-cost-password")
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	cost, err := bcrypt.Cost([]byte(hash))
	if err != nil {
		t.Fatalf("read cost out of the stored hash: %v", err)
	}
	if cost != bcrypt.MinCost {
		t.Fatalf("stored hash carries bcrypt cost %d after lowering the knob to %d", cost, bcrypt.MinCost)
	}

	// The property the acceptance suites rely on: a password stored at the
	// cheaper cost still verifies, and a wrong one still does not. If a cheaper
	// cost made every verification succeed, trading setup time for it would have
	// gutted the assertions instead of speeding them up.
	if !VerifyPassword(hash, "harness-cost-password") {
		t.Fatal("a password stored at the lowered cost did not verify")
	}
	if VerifyPassword(hash, "harness-cost-password-x") {
		t.Fatal("a wrong password verified at the lowered cost")
	}
}
