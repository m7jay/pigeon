// Command consumer-portal is the consumer portal deployable (M2b, §14.2): a
// server-rendered, htmx-driven thin client of the core API. It holds each
// consumer user's scoped core session in an encrypted httpOnly cookie and makes
// every data call to the core API server-side with that token, so the browser
// never sees the token and the consumer boundary is enforced by the core on
// every request, not by the portal (§14's "authorization is server-side",
// FR-110). "Nothing the portal can do may be impossible from the API": every
// handler below calls a /v1/consumer/* route that a script could call with the
// same scoped token and get the identical answer.
//
// It talks to no database and runs no workers. It is a stateless proxy, so it
// scales horizontally and, per UI-19 and AC-40, may be omitted or removed
// entirely without affecting the core or the producer portal: the consumer
// self-service API lives in the core, and this process only renders it.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/m7jay/pigeon/internal/portal/consumer"
	"github.com/m7jay/pigeon/internal/portalui"
)

// version is stamped by the build: -ldflags "-X main.version=$(git describe)".
var version = "dev"

func main() {
	if err := run(); err != nil {
		// A missing core URL or session secret is the operator's fix, not a bug
		// to debug: one actionable line, no stack trace (as the core does).
		fmt.Fprintf(os.Stderr, "consumer-portal: %v\n", err)
		os.Exit(1)
	}
}

// envOr returns the environment value of key, or def when it is unset or empty.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func run() error {
	addr := flag.String("addr", envOr("PIGEON_PORTAL_ADDR", ":8082"), "listen address")
	healthcheck := flag.Bool("healthcheck", false, "probe this instance's /healthz and exit 0/1 (container healthcheck)")
	flag.Parse()
	if *healthcheck {
		return probeLocal(*addr)
	}

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	cfg := consumer.Config{
		Addr:         *addr,
		CoreURL:      os.Getenv("PIGEON_CORE_API_URL"),
		Brand:        portalui.FromEnv(),
		CookieName:   envOr("PORTAL_SESSION_COOKIE_NAME", "consumer_session"),
		CookieSecret: os.Getenv("PORTAL_SESSION_SECRET"),
		Logger:       log,
	}
	h, err := consumer.New(cfg)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           h.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("consumer portal listening", "addr", cfg.Addr, "core", cfg.CoreURL, "brand", cfg.Brand.Name, "version", version)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		log.Info("shutdown signal received; draining")
	}

	// In-flight page renders drain before exit, like the core (D-3).
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	log.Info("stopped cleanly")
	return nil
}

// probeLocal is the container healthcheck. It is a flag rather than a shell
// script because the runtime image is scratch: there is no curl, wget or shell,
// so the binary is the only thing present to answer "are you alive".
func probeLocal(addr string) error {
	url := "http://127.0.0.1" + addr + "/healthz"
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
