package main

import (
	"fmt"
	"net/http"
	"os"
	"time"
)

// probeLocal is the container healthcheck. It exists as a flag rather than a
// shell script because the runtime image is scratch: there is no curl, no wget,
// and no shell to run one. The binary is the only thing present, so the binary
// must be able to answer "are you alive".
//
// It hits /health (liveness) and deliberately not /health/ready: a container
// healthcheck that fails when Postgres blinks makes Docker restart a healthy
// process, which turns a database hiccup into an outage.
func probeLocal() error {
	addr := os.Getenv("PIGEON_HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	url := "http://127.0.0.1" + addr + "/health"

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("healthcheck %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthcheck %s: status %d", url, resp.StatusCode)
	}
	return nil
}
