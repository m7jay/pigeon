package main

import (
	"testing"

	"github.com/m7jay/pigeon/internal/config"
)

// FR-810: an attempt's User-Agent must identify the platform and the version it
// was built from, and PIGEON_DELIVERY_USER_AGENT must be able to override it.
// config.Load read that knob and main dropped it on the floor, so the setting
// documented in .env.example had no effect on the wire.
//
// This covers the config-to-worker value; internal/delivery's integration test
// covers the same value reaching the request header.
func TestUserAgentIsTheConfiguredHeaderOrTheBuildVersion(t *testing.T) {
	if got := userAgent(config.Config{}, "1.2.3"); got != "pigeon/1.2.3" {
		t.Errorf("unset PIGEON_DELIVERY_USER_AGENT: userAgent = %q, want %q (FR-810)", got, "pigeon/1.2.3")
	}
	cfg := config.Config{DeliveryUserAgent: "acme-pigeon/2.0"}
	if got := userAgent(cfg, "1.2.3"); got != "acme-pigeon/2.0" {
		t.Errorf("set PIGEON_DELIVERY_USER_AGENT: userAgent = %q, want the configured %q", got, "acme-pigeon/2.0")
	}
}
