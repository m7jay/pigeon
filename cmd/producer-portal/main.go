// Command producer-portal is the producer portal deployable (M2a, §14.1): a
// server-rendered, htmx-driven thin client of the core API. It holds a producer
// member's FR-112 session token in an encrypted httpOnly cookie and makes every
// data call to the core API server-side with that token, so the browser never sees
// the token and the producer boundary is enforced by the core on every request,
// not by the portal (§14's "authorization is server-side"; the producer and user
// id come from the session claim, never from a request field).
//
// It is the sibling of cmd/consumer-portal and shares its invariants: "nothing the
// portal can do may be impossible from the API" - every handler below calls a
// /v1/producer/* or /v1/session/* route that a script could call with the same
// session bearer and get the identical answer.
//
// It talks to no database and runs no workers. It is a stateless proxy, so it
// scales horizontally and may be omitted or removed entirely without affecting the
// core or the consumer portal (AC-40): the producer API lives in the core, and this
// process only renders it.
//
// FR-106's OIDC login is the one place the portal does something the core cannot:
// it is the relying party (client id/secret, PKCE, the redirect URI, the code
// exchange) because the client secret - the value that lets a process speak for
// this portal at the issuer - is exactly what must not be handed to the API
// server. The core keeps the half that is a trust decision: it verifies the
// id_token and mints the session. With no issuer configured the portal renders no
// OIDC control and the core answers 404 for the route.
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

	"github.com/m7jay/pigeon/internal/portal/producer"
	"github.com/m7jay/pigeon/internal/portalui"
)

// version is stamped by the build: -ldflags "-X main.version=$(git describe)".
var version = "dev"

func main() {
	if err := run(); err != nil {
		// A missing core URL or session secret is the operator's fix, not a bug to
		// debug: one actionable line, no stack trace (as the core does).
		fmt.Fprintf(os.Stderr, "producer-portal: %v\n", err)
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
	addr := flag.String("addr", envOr("PIGEON_PRODUCER_PORTAL_ADDR", ":8083"), "listen address")
	healthcheck := flag.Bool("healthcheck", false, "probe this instance's /healthz and exit 0/1 (container healthcheck)")
	flag.Parse()
	if *healthcheck {
		return probeLocal(*addr)
	}

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	cfg := producer.Config{
		Addr:         *addr,
		CoreURL:      os.Getenv("PIGEON_CORE_API_URL"),
		Brand:        portalui.FromEnv(),
		CookieName:   envOr("PIGEON_PRODUCER_PORTAL_COOKIE_NAME", "producer_session"),
		CookieSecret: os.Getenv("PIGEON_PRODUCER_PORTAL_SESSION_SECRET"),
		Env:          envOr("PIGEON_PRODUCER_PORTAL_ENV", "test"),
		Logger:       log,
		// FR-106: the relying-party half. Every knob is optional together; with no
		// issuer there is no OIDC control and no OIDC route in the core either.
		OIDC: producer.OIDCConfig(
			os.Getenv("PIGEON_OIDC_ISSUER"),
			os.Getenv("PIGEON_OIDC_CLIENT_ID"),
			os.Getenv("PIGEON_OIDC_CLIENT_SECRET"),
			os.Getenv("PIGEON_OIDC_REDIRECT_URL"),
			envOr("PIGEON_OIDC_SCOPES", "openid,email,profile"),
		),
	}
	h, err := producer.New(cfg)
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
		log.Info("producer portal listening", "addr", cfg.Addr, "core", cfg.CoreURL,
			"env", cfg.Env, "brand", cfg.Brand.Name, "oidc", cfg.OIDC.Issuer != "", "version", version)
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

// probeLocal is the container healthcheck. It is a flag rather than a shell script
// because the runtime image is scratch: there is no curl, wget or shell, so the
// binary is the only thing present to answer "are you alive".
func probeLocal(addr string) error {
	u := "http://127.0.0.1" + addr + "/healthz"
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(u)
	if err != nil {
		return fmt.Errorf("healthcheck %s: %w", u, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthcheck %s: status %d", u, resp.StatusCode)
	}
	return nil
}
