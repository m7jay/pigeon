package config

import "testing"

// TestEgressIPsAreListedAndValidated is FR-1007's configuration half. The value
// the route publishes is the allowlist a receiver configures, so a typo is a
// delivery outage on someone else's side; OPS-7 says refuse at boot rather than
// serve it.
func TestEgressIPsAreListedAndValidated(t *testing.T) {
	// defaults() carries every other knob, so a failure here is FR-1007's check
	// and not a missing fixture.
	base := defaults()
	base.DatabaseURL = "postgres://u@h/db"
	base.RedisURL = "redis://h:6379" // D-2's buffered mode requires it
	base.InstanceAdminPassword = "a-long-enough-password"

	t.Run("list form", func(t *testing.T) {
		c := base
		c.EgressIPs = splitList(" 203.0.113.7 , 203.0.113.0/24 ,, ")
		if len(c.EgressIPs) != 2 || c.EgressIPs[0] != "203.0.113.7" || c.EgressIPs[1] != "203.0.113.0/24" {
			t.Fatalf("splitList = %v, want two trimmed entries (an empty entry is a trailing comma, not an error)", c.EgressIPs)
		}
		if err := c.Validate(); err != nil {
			t.Fatalf("Validate rejected a valid allowlist: %v", err)
		}
	})

	t.Run("empty is fine", func(t *testing.T) {
		c := base
		c.EgressIPs = splitList("")
		if c.EgressIPs != nil {
			t.Fatalf("splitList(%q) = %v, want nil: unset means discover, not an empty allowlist", "", c.EgressIPs)
		}
		if err := c.Validate(); err != nil {
			t.Fatalf("Validate rejected an unset allowlist: %v", err)
		}
	})

	t.Run("a typo is refused", func(t *testing.T) {
		for _, bad := range []string{"203.0.113.999", "203.0.113.0/33", "example.com", "10.0.0.0/8/8"} {
			c := base
			c.EgressIPs = []string{bad}
			if err := c.Validate(); err == nil {
				t.Errorf("Validate accepted PIGEON_EGRESS_IPS=%q; it would be published to receivers as an allowlist", bad)
			}
		}
	})
}
