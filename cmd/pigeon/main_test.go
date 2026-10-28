package main

import (
	"testing"

	"github.com/m7jay/pigeon/internal/config"
)

// FR-810: the attempt's User-Agent must identify the platform and the version it
// was built from, and PIGEON_DELIVERY_USER_AGENT must be able to override it -
// config.Load read that knob and main dropped it on the floor, so the setting
// documented in .env.example had no effect at all.
func TestUserAgentIsTheConfiguredHeaderOrTheBuildVersion(t *testing.T) {
	if got := userAgent(config.Config{}, "1.2.3"); got != "pigeon/1.2.3" {
		t.Errorf("unset PIGEON_DELIVERY_USER_AGENT: userAgent = %q, want %q (FR-810)", got, "pigeon/1.2.3")
	}
	cfg := config.Config{DeliveryUserAgent: "acme-hookrelay/2.0"}
	if got := userAgent(cfg, "1.2.3"); got != "acme-hookrelay/2.0" {
		t.Errorf("set PIGEON_DELIVERY_USER_AGENT: userAgent = %q, want the configured %q", got, "acme-hookrelay/2.0")
	}
}
