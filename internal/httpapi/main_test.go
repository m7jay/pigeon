package httpapi

import (
	"log"
	"os"
	"testing"

	"golang.org/x/crypto/bcrypt"

	"github.com/m7jay/pigeon/internal/identity"
)

// TestMain lowers the bcrypt cost this test binary hashes identities with.
//
// The acceptance harness seeds its fixtures through the real HTTP surface, so
// every `seedTenant` accepts three invitations and logs in four times: seven
// bcrypt calls at the production cost of 12, each ~3.5 s under -race on a 2-vCPU
// runner (measured; see the PR for card t_23c796d3). That is fixture setup, not
// an assertion, and the package seeds ~30 tenants - so it, and not the migration
// set (184 ms for a fresh database) or `buildEnv` (16 s, of which ~14 s is four
// bcrypt calls), was the ~590 s of the 652.880 s the package spent on the S7
// head.
//
// The cost is a *test* knob only: the shipped default is
// identity.DefaultPasswordCost (12), production has no code path that calls the
// setter, and internal/identity's own tests still exercise the production cost.
// Everything the suite asserts about credentials - that an invitation sets a
// password, that login verifies it, that the forced change works, that a
// different password fails - is unchanged, because bcrypt at a lower cost is the
// same algorithm with fewer key expansions.
func TestMain(m *testing.M) {
	if err := identity.SetPasswordCostForTests(bcrypt.MinCost); err != nil {
		log.Fatalf("harness: %v", err)
	}
	os.Exit(m.Run())
}

// TestHarnessSeedsIdentitiesAtACheapCost pins the knob TestMain applies. Without
// the call above this package is still green, just 50x slower (664.799s under
// -race on a 2-vCPU box before this change, 13.208s after), and a slow suite is
// indistinguishable from a fast one until it hits a timeout - which is how this
// card came to exist. It hashes through the same public API the fixtures use, so
// it cannot pass while the knob is a no-op.
func TestHarnessSeedsIdentitiesAtACheapCost(t *testing.T) {
	hash, err := identity.HashPassword("harness-cost-probe-1")
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	cost, err := bcrypt.Cost([]byte(hash))
	if err != nil {
		t.Fatalf("read the bcrypt cost out of the harness hash: %v", err)
	}
	if cost != bcrypt.MinCost {
		t.Fatalf("the harness is hashing identities at bcrypt cost %d, want %d: every acceptance test pays the production cost for each seeded credential, which is what made this package 664.8s of the suite's 666s under -race", cost, bcrypt.MinCost)
	}
}
