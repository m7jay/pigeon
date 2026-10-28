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

	"github.com/m7jay/pigeon/internal/atrest"
	"github.com/m7jay/pigeon/internal/attemptbuf"
	"github.com/m7jay/pigeon/internal/broker"
	"github.com/m7jay/pigeon/internal/config"
	"github.com/m7jay/pigeon/internal/db"
	"github.com/m7jay/pigeon/internal/delivery"
	"github.com/m7jay/pigeon/internal/httpapi"
	"github.com/m7jay/pigeon/internal/identity"
	"github.com/m7jay/pigeon/internal/limit"
	"github.com/m7jay/pigeon/internal/metrics"
	"github.com/m7jay/pigeon/internal/pause"
	"github.com/m7jay/pigeon/internal/ratelimit"
	"github.com/m7jay/pigeon/internal/relay"
	"github.com/m7jay/pigeon/internal/retention"
	"github.com/m7jay/pigeon/internal/store"
	"github.com/m7jay/pigeon/internal/writeq"
)

// version is stamped by the build: -ldflags "-X main.version=$(git describe)".
var version = "dev"

func main() {
	if err := run(); err != nil {
		var exit exitError
		if errors.As(err, &exit) {
			// The report has already spoken; echo the message only when there is
			// one, then leave with the status OPS-11 assigns.
			if msg := exit.Error(); msg != "" {
				fmt.Fprintf(os.Stderr, "pigeon: %v\n", msg)
			}
			os.Exit(exit.code)
		}
		// Configuration refusals are the operator's fix, not a crash to debug;
		// they get one actionable line rather than a stack trace.
		fmt.Fprintf(os.Stderr, "pigeon: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// The subcommand is dispatched before flag.Parse: doctor has its own flag set,
	// and the default one would refuse its argv.
	if len(os.Args) > 1 && os.Args[1] == "doctor" {
		return runDoctor(os.Args[2:])
	}
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
	// FR-1006: the at-rest key that seals endpoint custom header values. Validate
	// already refused a missing or mis-sized one, so a parse failure here is
	// unreachable; it is still handled rather than ignored, because "the key that
	// protects customer values" is not a place for a panic (or for a Store that
	// silently refuses to store what the API accepted).
	headersKey, err := atrest.ParseKey(cfg.HeadersKey)
	if err != nil {
		return err
	}

	// coreMetrics is the process's counter set: FR-508's non-delivery counters
	// (advanced by the relay as events fan out), S6's delivery, attempt and
	// log-gate counters, and S8's partition-drop and shed counts (SB-6), all
	// exposed at GET /metrics.
	coreMetrics := metrics.New()

	// S12/FR-818: this process's bounded write budget (SPEC.md A-11). ONE budget,
	// shared: the store submits its batched control-state writes to it, and the
	// HTTP layer admits ingest against the same instance, so "full" means this
	// machine's write capacity rather than one subsystem's queue. It replaces the
	// ingest path's unbounded wait on pgxpool.Acquire - the parking S10 measured
	// as zero 429s and a p99 growing from 3,593.4 ms to 4,281.6 ms - with an
	// admission deadline and a shed.
	writes := store.NewWriteQueue(pool, writeq.Options{
		Depth:        cfg.WriteQueueDepth,
		Pool:         int(cfg.DBMaxConns),
		Workers:      cfg.WriteQueueWorkers,
		AdmitTimeout: cfg.IngestAdmitTimeout,
		Metrics:      coreMetrics,
		Logger:       log,
		Parent:       ctx,
	})

	st := store.New(pool, store.WithAttemptBodyCap(cfg.DeliveryMaxBodyBytes), store.WithHeadersKey(headersKey), store.WithWriteQueue(writes))

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

	// S8: the storage budget. The manager is the single place the disk budget is
	// applied: the sweeper drives RunOnce under its leadership lease (D-7), and
	// the API reads Status/IngestGate. The gauge measures PIGEON_DISK_DIR when it
	// is set (core and Postgres sharing a volume), and falls back to pigeon's own
	// database size against PIGEON_DISK_BYTES when the volume is not visible from
	// this process — the honest reading in a split-container deployment. A gauge
	// with neither signal reports "none" rather than a calm zero.
	gauge := &retention.FilesystemGauge{
		Dir:      cfg.DiskDir,
		Bytes:    cfg.DiskBytes,
		Database: st.DatabaseSize,
	}
	rm := retention.NewManager(st, gauge, retention.Options{
		MaxDiskPct:          cfg.RetentionMaxDiskPct,
		DeliveriesRetention: cfg.RetentionDeliveries,
		AttemptsRetention:   cfg.RetentionAttempts,
		EventsRetention:     cfg.RetentionEvents,
		DedupeRetention:     cfg.RetentionDedupe,
		Logger:              log,
	})
	log.Info("retention configured",
		"max_disk_pct", cfg.RetentionMaxDiskPct,
		"deliveries_window", cfg.RetentionDeliveries,
		"attempts_window", cfg.RetentionAttempts,
		"events_window", cfg.RetentionEvents,
		"dedupe_window", cfg.RetentionDedupe,
		"disk_dir", cfg.DiskDir, "disk_bytes", cfg.DiskBytes)
	api := httpapi.New(httpapi.Deps{
		Lookup: func(ctx context.Context, plaintext string) (store.Principal, error) {
			return store.LookupAPIKey(ctx, pool, plaintext)
		},
		Sessions: func(ctx context.Context, token string) (store.Session, error) {
			return st.LookupSession(ctx, token)
		},
		Data: st,
		// FR-508: the fan-out non-delivery counters, exposed at GET /metrics.
		Metrics: coreMetrics,
		// SB-6/SB-7: GET /v1/meta/usage and the publish 429 gate.
		Retention: rm,
		// FR-818: the same write budget the store's control-state writes draw on,
		// so ingest admission and worker writes are one bounded resource rather
		// than two queues that cannot see each other.
		Writes: writes,
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
		w := relay.New(st, br, relay.Options{Logger: log, Metrics: coreMetrics})
		w.SetNotifier(relay.Notifier(ctx, cfg.DatabaseURL))
		go func() {
			log.Info("relay started", "broker", "redis-streams")
			if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Error("relay stopped unexpectedly", "error", err)
			}
		}()
	}

	errCh := make(chan error, 1)

	// S6: the attempt-evidence buffer and its flusher (D-1, D-2, D-3, FR-812/815).
	//
	// D-1 puts the buffer in Redis, never in the worker's own memory, and this is
	// the only place it is created. In `buffered` mode the worker appends each
	// failed attempt to the stream and this process's single flusher drains it
	// into Postgres in batches of at least FR-815's 2,000 rows, under
	// synchronous_commit=off (db.EvidenceTx, FR-812). In `sync` mode there is no
	// buffer at all and the worker writes evidence inline.
	//
	// The flusher gets its OWN context, cancelled only after the workers have
	// stopped pushing, so D-3's "flush before exit" is not raced by an in-flight
	// attempt landing in the stream after the final drain has already run.
	flusherWG := &sync.WaitGroup{}
	var stopFlusher context.CancelFunc
	// attemptBuffer is the process's buffer, shared by the workers that push and
	// the single flusher that drains (D-1). It is nil in sync mode.
	var attemptBuffer *attemptbuf.Buffer
	if cfg.AttemptLogMode == config.AttemptLogBuffered {
		buf, err := attemptbuf.Dial(ctx, cfg.RedisURL, attemptbuf.Options{Logger: log, Metrics: coreMetrics})
		if err != nil {
			return fmt.Errorf("connect attempt buffer: %w", err)
		}
		defer buf.Close()
		attemptBuffer = buf

		fl, err := attemptbuf.NewFlusher(buf, st, attemptbuf.FlusherOptions{
			Batch:      cfg.AttemptFlushBatch,
			Interval:   cfg.AttemptFlushInterval,
			StaleAfter: cfg.AttemptFlushStale,
			DrainGrace: cfg.ShutdownGrace,
			Logger:     log,
			Metrics:    coreMetrics,
		})
		if err != nil {
			return fmt.Errorf("attempt flusher: %w", err)
		}
		flusherCtx, cancelFlusher := context.WithCancel(context.WithoutCancel(ctx))
		stopFlusher = cancelFlusher
		flusherWG.Add(1)
		go func() {
			defer flusherWG.Done()
			log.Info("attempt flusher started",
				"stream", buf.Key(), "batch", cfg.AttemptFlushBatch,
				"interval_ms", cfg.AttemptFlushInterval.Milliseconds())
			if err := fl.Run(flusherCtx); err != nil && !errors.Is(err, context.Canceled) {
				log.Error("attempt flusher stopped unexpectedly", "error", err)
			}
		}()
	}
	// Whatever happens, do not leak the flusher goroutine on an early return.
	defer func() {
		if stopFlusher != nil {
			stopFlusher()
		}
	}()

	// The worker is handed the buffer only in buffered mode. Handing it over in
	// sync mode would leave the attempt number and the evidence stream disagreeing
	// about which mode is in force (D-2).

	// S4: the delivery worker and its sweeper. The worker drains the deliveries
	// table directly, the way the relay drains the outbox: it leases a due
	// delivery, signs the envelope with the endpoint's own secret, POSTs it under
	// a per-attempt timeout with no redirect following, classifies the outcome,
	// and writes ONLY the completion / terminal transition plus one evidence row
	// per failed attempt (a successful first attempt writes no attempt row,
	// FR-811). The sweeper re-queues any lease that has lapsed within the D-6 10 s
	// ceiling under a single advisory-lock leaseholder (D-7). Both stop on ctx
	// cancellation and let in-flight work drain (AC-21).
	//
	// S5 adds the retry window (§9, FR-1201), the failure thresholds that pause a
	// subscription or an endpoint (FR-1202/FR-1203), and the two per-endpoint
	// limits that must hold CLUSTER-WIDE (FR-1302, FR-1306, FR-1307). The limits
	// live in the same Redis the broker uses, keyed by endpoint id alone, so
	// adding a replica does not raise an endpoint's effective cap (AC-41). With
	// no Redis configured the worker falls back to the in-process limiter and
	// says so: that fallback is per-process, which is exactly what FR-1307
	// forbids, so it must never be silent.
	workersWG := &sync.WaitGroup{}
	if cfg.DeliveryWorkers > 0 {
		var (
			limits  limit.Registry
			tracker *pause.Tracker
		)
		if cfg.RedisURL != "" {
			lim, err := limit.NewRedis(ctx, cfg.RedisURL)
			if err != nil {
				return fmt.Errorf("connect delivery limiter: %w", err)
			}
			defer lim.Close()
			limits = lim
			tracker = pause.NewTracker(lim.Client(), pause.DefaultThresholds())
		} else {
			log.Warn("no PIGEON_REDIS_URL: per-endpoint limits and pause thresholds are per-process and thresholds are DISABLED (FR-1302, FR-1307)",
				"requirement", "FR-1302")
			limits = limit.NewLocal()
		}
		dw := delivery.New(st, delivery.Options{
			Workers:     cfg.DeliveryWorkers,
			Concurrency: cfg.DeliveryConcurrency,
			// FR-1303: the retry lane's allowance, partitioned out of
			// Concurrency by the worker (R = clamp(configured, 0, C-1), so the
			// first-attempt lane always keeps at least one slot).
			RetryConcurrency: cfg.DeliveryRetryConcurrency,
			Lease:            cfg.DeliveryLease,
			Timeout:          cfg.DeliveryTimeout,
			SlotTTL:          cfg.DeliverySlotTTL,
			MaxBodyBytes:     cfg.DeliveryMaxBodyBytes,
			BackoffBase:      cfg.DeliveryBackoffBase,
			BackoffMax:       cfg.DeliveryBackoffMax,
			MaxRetries:       cfg.DeliveryMaxRetries,
			RetryWindow:      cfg.DeliveryRetryWindow,
			Jitter:           cfg.DeliveryJitter,
			MaxConnsPerHost:  cfg.DeliveryMaxConnsPerHost,
			PollInterval:     cfg.DeliveryPollInterval,
			UserAgent:        userAgent(cfg, version),
			Limits:           limits,
			Pauses:           tracker,
			Logger:           log,
			// S6: evidence goes to the Redis buffer in buffered mode (D-1) or is
			// written inline in sync mode (D-2). The counters and the log gate
			// carry the volume LOG-2 forbids in the log stream.
			AttemptLog:    cfg.AttemptLogMode,
			AttemptBuffer: attemptBuffer,
			Metrics:       coreMetrics,
			SuccessLogRPS: cfg.SuccessLogRPS,
		})
		workersWG.Add(1)
		go func() {
			defer workersWG.Done()
			log.Info("delivery worker started", "workers", cfg.DeliveryWorkers)
			if err := dw.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Error("delivery worker stopped unexpectedly", "error", err)
			}
		}()
	}

	// The sweeper runs whether or not delivery workers do. It owns two jobs:
	// recovering leases a crashed worker left behind (D-6) and the storage
	// budget's partition drops (SB-3). An ingest-only deployment (DeliveryWorkers
	// = 0) still needs the second, or its disk fills and nothing ever drops a
	// partition. Running it always also gives the deployment one stable leadership
	// holder (D-7), so AC-42's "exactly one partition dropped" holds no matter how
	// many replicas and how many of them run workers. When there are no deliveries
	// the requeue is a cheap no-op; the retention tick still does its work.
	sw := delivery.NewSweeper(st, delivery.SweeperOptions{
		URL:       cfg.SweeperLockURL,
		Interval:  cfg.SweeperInterval,
		LockWait:  cfg.SweeperLockWait,
		Logger:    log,
		Retention: rm,
		Metrics:   coreMetrics,
	})
	workersWG.Add(1)
	go func() {
		defer workersWG.Done()
		log.Info("sweeper started", "interval_s", int(cfg.SweeperInterval.Seconds()))
		if err := sw.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("sweeper stopped unexpectedly", "error", err)
		}
	}()

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

	// D-3: the flusher drains the buffer before exit, and it runs LAST — after
	// the workers have stopped and unwound their in-flight attempts. Cancelling
	// it first would let it find an empty stream and exit while a worker was
	// still about to push, leaving that row in Redis until the next boot. Its
	// Run() flushes what remains on its own bounded drain context.
	if stopFlusher != nil {
		stopFlusher()
		done := make(chan struct{})
		go func() { flusherWG.Wait(); close(done) }()
		select {
		case <-done:
		case <-shutdownCtx.Done():
			log.Warn("shutdown grace elapsed before the attempt buffer drained")
		}
	}

	// FR-818's drain (D-3): the write budget's writer pool writes out what it has
	// already buffered before the process exits, and it runs last for the same
	// reason the flusher does - by now the workers and the HTTP server have
	// stopped, so nothing new is arriving while those control-state rows commit.
	// A completion the worker was told had happened is therefore not lost to the
	// restart.
	st.Close()

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

// userAgent is FR-810's User-Agent header for every attempt: the operator's
// PIGEON_DELIVERY_USER_AGENT when they set one, otherwise the platform and the
// version this binary was built from. config.Load read that knob and main never
// passed it to the worker, so the header was hard-coded and the setting
// documented in .env.example had no effect on the wire; OPS-3's promise that a
// documented setting is a configurable one is what this closes.
func userAgent(cfg config.Config, version string) string {
	if ua := cfg.DeliveryUserAgent; ua != "" {
		return ua
	}
	return "pigeon/" + version
}
