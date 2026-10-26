// Command pigeon is the core deployable: API, workers and migrator in one binary.
//
// At M0 it boots, refuses an unsafe configuration (OPS-7), migrates behind an
// advisory lock (OPS-9), bootstraps the instance-admin credential (FR-101), and
// serves liveness, readiness and an authenticated route.
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

	"github.com/pigeon-hq/pigeon/internal/config"
	"github.com/pigeon-hq/pigeon/internal/db"
	"github.com/pigeon-hq/pigeon/internal/httpapi"
	"github.com/pigeon-hq/pigeon/internal/identity"
	"github.com/pigeon-hq/pigeon/internal/store"
)

// version is stamped by the build: -ldflags "-X main.version=$(git describe)".
var version = "dev"

func main() {
	if err := run(); err != nil {
		// Configuration refusals are the operator's fix, not a crash to debug;
		// they get one actionable line rather than a stack trace.
		fmt.Fprintf(os.Stderr, "pigeon: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	printVersion := flag.Bool("version", false, "print version and exit")
	healthcheck := flag.Bool("healthcheck", false, "probe the local instance and exit 0/1 (container healthcheck)")
	flag.Parse()
	if *printVersion {
		fmt.Println(version)
		return nil
	}
	if *healthcheck {
		return probeLocal()
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	log := newLogger(cfg)
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := db.Connect(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		return err
	}
	defer pool.Close()

	if cfg.MigrationsOn {
		if err := db.Migrate(ctx, pool, cfg.MigrateWaitDL); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
		log.Info("migrations up to date")
	}

	created, err := identity.EnsureInstanceAdmin(ctx, pool, cfg.InstanceAdminPassword)
	if err != nil {
		return fmt.Errorf("bootstrap instance admin: %w", err)
	}
	if created {
		log.Warn("instance admin created from PIGEON_INSTANCE_ADMIN_PASSWORD; rotate it after first login (FR-101)")
	}

	api := httpapi.New(httpapi.Deps{
		Lookup: func(ctx context.Context, plaintext string) (store.Principal, error) {
			p, err := store.LookupAPIKey(ctx, pool, plaintext)
			if err != nil {
				return p, err
			}
			// FR-206: last_used_at is evidence, not control state, so it must
			// never delay or fail the request that carries it.
			go func() {
				touchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
				defer cancel()
				if err := store.TouchKey(touchCtx, pool, p.KeyID); err != nil {
					log.Debug("touch api key", "error", err)
				}
			}()
			return p, nil
		},
		Ready:   func(ctx context.Context) error { return pool.Ping(ctx) },
		Version: version,
	}, log)

	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.AccessLog(api.Handler()),
		ReadHeaderTimeout: cfg.ReadHeaderLimit,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("core listening", "addr", cfg.HTTPAddr, "version", version)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		log.Info("shutdown signal received; draining", "grace", cfg.ShutdownGrace.String())
	}

	// D-3: in-flight work drains before exit. Workers arrive in M2, but the
	// shutdown path exists from M0 so it is never retrofitted under pressure.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGrace)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	log.Info("stopped cleanly")
	return nil
}

func newLogger(cfg config.Config) *slog.Logger {
	opts := &slog.HandlerOptions{Level: cfg.LogLevel}
	if cfg.LogJSON {
		return slog.New(slog.NewJSONHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stdout, opts))
}
