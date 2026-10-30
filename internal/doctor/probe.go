package doctor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/m7jay/pigeon/internal/db"
	"github.com/m7jay/pigeon/internal/redisconn"
)

// The production probes. They are thin on purpose: doctor's behaviour lives in
// the checks, and these only make the syscalls and queries the checks ask for.

// SQLProbe reads Postgres through a pool. Every method is a read; doctor opens the
// pool without the migrator role and never writes, which is what makes it safe to
// point at production.
type SQLProbe struct{ pool *pgxpool.Pool }

// OpenPostgres opens a small pool for the diagnostics. A failure here is not fatal:
// the caller passes a nil probe and the affected checks report a skip.
func OpenPostgres(ctx context.Context, url string) (*SQLProbe, error) {
	if url == "" {
		return nil, fmt.Errorf("PIGEON_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	pool, err := db.Connect(ctx, url, 2)
	if err != nil {
		return nil, err
	}
	return &SQLProbe{pool: pool}, nil
}

func (p *SQLProbe) Ping(ctx context.Context) error { return p.pool.Ping(ctx) }

func (p *SQLProbe) MaxConnections(ctx context.Context) (int32, error) {
	var n int32
	err := p.pool.QueryRow(ctx, `select current_setting('max_connections')::int`).Scan(&n)
	return n, err
}

func (p *SQLProbe) ServerTime(ctx context.Context) (time.Time, error) {
	var t time.Time
	err := p.pool.QueryRow(ctx, `select now()`).Scan(&t)
	return t, err
}

func (p *SQLProbe) DatabaseSize(ctx context.Context) (uint64, error) {
	var n uint64
	err := p.pool.QueryRow(ctx, `select pg_database_size(current_database())`).Scan(&n)
	return n, err
}

// Close releases the pool.
func (p *SQLProbe) Close() { p.pool.Close() }

// RedisProbeImpl reads Redis through a client. CONFIG GET is the only
// administrative call; it is a read and needs no more than the default user.
type RedisProbeImpl struct{ rdb *redis.Client }

// OpenRedis connects for the diagnostics. As with Postgres, a failure means the
// caller passes nil and the Redis checks skip with the reason.
//
// The client is built through internal/redisconn, the one reading of
// PIGEON_REDIS_URL every production caller shares. Doctor has to accept what the
// deployment ships - .env.example and compose.yaml both set
// `redis://redis:6379/0` - and the URL's database index has to reach the client:
// a host-only parse still connects, so the CONFIG GETs would quietly read DB 0
// while the deployment's own database went unexamined.
//
// redis.NewClient(&redis.Options{Addr: url}) was the defect here. go-redis dials
// Addr verbatim, so the documented URL became a *hostname* and doctor reported
//
//	dial tcp: address redis://redis:6379/0: too many colons in address
//
// on a healthy box: a false Redis failure, with D-5's appendonly check skipped
// behind it, on exactly the box the operator ran doctor on because something was
// wrong. (OPS-7, FR-705, FR-1307.)
func OpenRedis(ctx context.Context, url string) (*RedisProbeImpl, error) {
	if url == "" {
		return nil, fmt.Errorf("PIGEON_REDIS_URL is not set")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	rdb, err := redisconn.Client(url)
	if err != nil {
		// The reason is built here, without the value. redisconn's refusal
		// quotes what it refused and net/url quotes it again, so a value
		// carrying a literal quote or backslash comes back escaped - and a
		// value whose shape the parser rejected is exactly the kind that hides
		// a password in a place the parser did not expect. Redacting by
		// substitution only catches a spelling it recognises, so the value is
		// not repeated in any rendering: the check's Observed line already
		// carries Redact(url) and its Fix line already names PIGEON_REDIS_URL.
		// (FR-1008.)
		return nil, errors.New("PIGEON_REDIS_URL could not be parsed as a Redis connection URL (the value is withheld, FR-1008)")
	}
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, err
	}
	return &RedisProbeImpl{rdb: rdb}, nil
}

func (r *RedisProbeImpl) Ping(ctx context.Context) error { return r.rdb.Ping(ctx).Err() }

func (r *RedisProbeImpl) AppendOnly(ctx context.Context) (string, string, error) {
	// Two calls rather than one: go-redis v9's ConfigGet takes a single pattern,
	// and the pair is what matters (an AOF with fsync=no loses the same queue).
	appendonly, err := r.rdb.ConfigGet(ctx, "appendonly").Result()
	if err != nil {
		return "", "", err
	}
	appendfsync, err := r.rdb.ConfigGet(ctx, "appendfsync").Result()
	if err != nil {
		return "", "", err
	}
	return appendonly["appendonly"], appendfsync["appendfsync"], nil
}

func (r *RedisProbeImpl) Time(ctx context.Context) (time.Time, error) {
	return r.rdb.Time(ctx).Result()
}

// Close releases the client.
func (r *RedisProbeImpl) Close() { _ = r.rdb.Close() }

// Filesystem is the real statfs(2). It uses syscall rather than x/sys/unix
// because doctor must not add a dependency to the core binary for two syscalls.
func Filesystem(path string) (Statfs, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return Statfs{}, err
	}
	bsize := uint64(st.Bsize)
	// Bavail, not Bfree: the free blocks available to an unprivileged process are
	// the ones pigeon can actually use, and the reserved-blocks delta is exactly
	// the optimism that turns a full disk into a failed write.
	return Statfs{TotalBytes: st.Blocks * bsize, FreeBytes: st.Bavail * bsize}, nil
}

// FileDescriptors is the real RLIMIT_NOFILE.
func FileDescriptors() (uint64, uint64, error) {
	var lim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
		return 0, 0, err
	}
	return lim.Cur, lim.Max, nil
}

// Env is the raw environment doctor reads for URLs, so a box whose configuration
// was refused can still be diagnosed. It is separate from config.Load on purpose:
// config.Load is the thing that refused, and asking it again would just repeat the
// refusal instead of naming the host that is down.
func Env() (databaseURL, redisURL string) {
	return os.Getenv("PIGEON_DATABASE_URL"), os.Getenv("PIGEON_REDIS_URL")
}
