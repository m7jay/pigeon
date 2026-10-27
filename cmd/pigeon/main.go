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
	"sync"
	"syscall"

	"github.com/m7jay/pigeon/internal/broker"
	"github.com/m7jay/pigeon/internal/config"
	"github.com/m7jay/pigeon/internal/db"
	"github.com/m7jay/pigeon/internal/delivery"
	"github.com/m7jay/pigeon/internal/httpapi"
	"github.com/m7jay/pigeon/internal/identity"
	"github.com/m7jay/pigeon/internal/metrics"
	"github.com/m7jay/pigeon/internal/ratelimit"
	"github.com/m7jay/pigeon/internal/relay"
	"github.com/m7jay/pigeon/internal/store"
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

	// OPS-8: the budget is arithmetic, and it is checked against the server's real
	// max_connections rather than a documented default. Ten replicas with a
	// ten-connection pool each will take Postgres down in a way no single replica
	// can see.
	var serverMaxConns int32
	if err := pool.QueryRow(ctx, `select current_setting('max_connections')::int`).Scan(&serverMaxConns); err != nil {
		return fmt.Errorf("read max_connections: %w", err)
	}
	if err := cfg.ValidateBudget(serverMaxConns); err != nil {
		return err
	}
	st := store.New(pool)

	if cfg.MigrationsOn {
		// Migrations run as the role that owns the schema, because CREATE TABLE
		// needs it. The API then serves as the unprivileged runtime role:
		// PostgreSQL exempts a table owner from row-level security, so serving on
		// the migrator's connection would make every tenant policy inert.
		migratePool := pool
		if cfg.MigrateDatabaseURL != "" && cfg.MigrateDatabaseURL != cfg.DatabaseURL {
			migratePool, err = db.Connect(ctx, cfg.MigrateDatabaseURL, 5)
			if err != nil {
				return fmt.Errorf("connect migrator: %w", err)
			}
			defer migratePool.Close()
		}
		if err := db.Migrate(ctx, migratePool, cfg.MigrateWaitDL); err != nil {
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

	// nonDeliveryMetrics is FR-508's counter set, shared by the relay (which
	// advances it as events fan out) and the /metrics scrape that reads it.
	nonDeliveryMetrics := metrics.New()
	api := httpapi.New(httpapi.Deps{
		Lookup: func(ctx context.Context, plaintext string) (store.Principal, error) {
			return store.LookupAPIKey(ctx, pool, plaintext)
		},
		Sessions: func(ctx context.Context, token string) (store.Session, error) {
			return st.LookupSession(ctx, token)
		},
		Data: st,
		// FR-508: the fan-out non-delivery counters, exposed at GET /metrics.
		Metrics: nonDeliveryMetrics,
		// FR-206: last_used_at is an evidence write and runs off the request path,
		// inside the middleware that authenticated the key.
		TouchKey:         st.TouchKey,
		Ready:            func(ctx context.Context) error { return pool.Ping(ctx) },
		Version:          version,
		SessionTTL:       cfg.SessionTTL,
		KeyLimiter:       ratelimit.New(cfg.RateLimitPerKeyRPS, cfg.RateLimitPerKeyBurst),
		ProducerLimiter:  ratelimit.New(cfg.RateLimitPerProducerRPS, cfg.RateLimitPerProducerBurst),
		SelfRegistration: cfg.SelfRegistration,
	}, log)

	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.AccessLog(api.Handler()),
		ReadHeaderTimeout: cfg.ReadHeaderLimit,
	}

	// S2: the outbox relay. It runs only when a broker is configured; without one
	// there is nothing to publish to and the API serves the ingest path alone.
	// It lists on Postgres for the fast path and polls as the durable fallback
	// (D-4), leases rows with SKIP LOCKED (FR-702), and marks a row published only
	// after the broker acks (FR-701) - so the durability contract holds even when
	// the relay is killed mid-delivery.
	if cfg.RedisURL != "" {
		br, err := broker.NewRedis(ctx, cfg.RedisURL)
		if err != nil {
			return fmt.Errorf("connect broker: %w", err)
		}
		defer br.Close()
		w := relay.New(st, br, relay.Options{Logger: log, Metrics: nonDeliveryMetrics})
		w.SetNotifier(relay.Notifier(ctx, cfg.DatabaseURL))
		go func() {
			log.Info("relay started", "broker", "redis-streams")
			if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Error("relay stopped unexpectedly", "error", err)
			}
		}()
	}

	errCh := make(chan error, 1)

	// S4: the delivery worker and its sweeper. The worker drains the deliveries
	// table directly, the way the relay drains the outbox: it leases a due
	// delivery, signs the envelope with the endpoint's own secret, POSTs it under
	// a per-attempt timeout with no redirect following, classifies the outcome,
	// and writes ONLY the completion / terminal transition plus one evidence row
	// per failed attempt (a successful first attempt writes no attempt row,
	// FR-811). The sweeper re-queues any lease that has lapsed within the D-6 10 s
	// ceiling under a single advisory-lock leaseholder (D-7). Both stop on ctx
	// cancellation and let in-flight work drain (AC-21).
	workersWG := &sync.WaitGroup{}
	if cfg.DeliveryWorkers > 0 {
		dw := delivery.New(st, delivery.Options{
			Workers:         cfg.DeliveryWorkers,
			Lease:           cfg.DeliveryLease,
			Timeout:         cfg.DeliveryTimeout,
			MaxBodyBytes:    cfg.DeliveryMaxBodyBytes,
			BackoffBase:     cfg.DeliveryBackoffBase,
			BackoffMax:      cfg.DeliveryBackoffMax,
			MaxAttempts:     cfg.DeliveryMaxAttempts,
			MaxConnsPerHost: cfg.DeliveryMaxConnsPerHost,
			PollInterval:    cfg.DeliveryPollInterval,
			UserAgent:       "pigeon/" + version,
			Logger:          log,
		})
		workersWG.Add(1)
		go func() {
			defer workersWG.Done()
			log.Info("delivery worker started", "workers", cfg.DeliveryWorkers)
			if err := dw.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Error("delivery worker stopped unexpectedly", "error", err)
			}
		}()

		sw := delivery.NewSweeper(st, delivery.SweeperOptions{
			URL:      cfg.SweeperLockURL,
			Interval: cfg.SweeperInterval,
			LockWait: cfg.SweeperLockWait,
			Logger:   log,
		})
		workersWG.Add(1)
		go func() {
			defer workersWG.Done()
			log.Info("sweeper started", "interval_s", int(cfg.SweeperInterval.Seconds()))
			if err := sw.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Error("sweeper stopped unexpectedly", "error", err)
			}
		}()
	}

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

	// D-3: the workers drain too. Each returns once its in-flight attempt unwinds
	// (its context was cancelled with the parent, FR-819), so waiting here keeps
	// AC-21 honest — a delivery mid-attempt is left pending under its lease rather
	// than killed into a lost or half-written state. Bounded by the same grace.
	if cfg.DeliveryWorkers > 0 {
		done := make(chan struct{})
		go func() { workersWG.Wait(); close(done) }()
		select {
		case <-done:
		case <-shutdownCtx.Done():
			log.Warn("shutdown grace elapsed before workers drained")
		}
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
