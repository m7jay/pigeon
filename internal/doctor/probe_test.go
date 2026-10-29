package doctor

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// doctorTestServerAddr is the host:port of the Redis these tests dial: the real
// server PIGEON_TEST_REDIS_URL names when it is set (CI's redis:7 service
// container, the 127.0.0.1:6399 server on a dev box), and a fresh in-process
// miniredis otherwise. Only the address is used - each leg builds the DSN it
// asserts on, so the env value's own database index is dropped here - and a skip
// is never how a run passes.
func doctorTestServerAddr(t *testing.T) string {
	t.Helper()
	raw := os.Getenv("PIGEON_TEST_REDIS_URL")
	if raw == "" {
		return miniredis.RunT(t).Addr()
	}
	if strings.Contains(raw, "://") {
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" {
			t.Fatalf("parse PIGEON_TEST_REDIS_URL %q: %v", raw, err)
		}
		return u.Host
	}
	return raw
}

// TestOpenRedisHonoursTheDSN is OpenRedis's regression test: the probe takes the
// documented PIGEON_REDIS_URL form (redis://host:port/db) and a bare host:port,
// and the database index the URL names reaches the client.
//
// It regresses a false negative, which is the worst thing a diagnostic can
// produce. OpenRedis passed the whole value straight through as
// redis.Options.Addr, and go-redis dials Addr verbatim, so
// redis://redis:6379/0 - the value .env.example and compose.yaml ship - was
// dialled as a *hostname* and the PING failed with
//
//	dial tcp: address redis://redis:6379/0: too many colons in address
//
// so on a healthy box configured the way the shipped compose file configures it,
// doctor reported redis.reachable as a failure, skipped D-5's appendonly check
// behind it and exited non-zero - telling the operator the wrong dependency was
// down. The second leg is the quieter half of the same defect: a client built
// from the URL's host alone still connects and still PINGs, it just lands in DB
// 0, so the CONFIG GETs would read the default database while the deployment's
// own database went unexamined. A passing PING cannot show that, which is why the
// database is asserted directly.
func TestOpenRedisHonoursTheDSN(t *testing.T) {
	addr := doctorTestServerAddr(t)

	for _, tc := range []struct {
		name   string
		dsn    string
		wantDB int
		other  int
	}{
		{name: "url form names a database", dsn: "redis://" + addr + "/9", wantDB: 9, other: 0},
		{name: "the shipped compose form", dsn: "redis://" + addr + "/0", wantDB: 0, other: 9},
		{name: "bare host:port is database 0", dsn: addr, wantDB: 0, other: 9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			p, err := OpenRedis(ctx, tc.dsn)
			if err != nil {
				t.Fatalf("OpenRedis(%q): %v", tc.dsn, err)
			}
			defer p.Close()

			if got := p.rdb.Options().DB; got != tc.wantDB {
				t.Fatalf("OpenRedis(%q): client is on DB %d, want DB %d", tc.dsn, got, tc.wantDB)
			}
			// The probe's own read path works, not only the client it built.
			if err := p.Ping(ctx); err != nil {
				t.Fatalf("OpenRedis(%q): ping: %v", tc.dsn, err)
			}

			// ... and the database is the one the CONFIG GETs run in: a key
			// written through the probe's client is invisible from the other
			// database, which is exactly what a dropped index looks like.
			key := fmt.Sprintf("pigeon:test:openredis:%s:%d",
				strings.ReplaceAll(t.Name(), "/", "-"), time.Now().UnixNano())
			if err := p.rdb.Set(ctx, key, "1", time.Minute).Err(); err != nil {
				t.Fatalf("SET %s through the DB %d client: %v", key, tc.wantDB, err)
			}
			other := redis.NewClient(&redis.Options{Addr: addr, DB: tc.other})
			defer other.Close()
			if n, err := other.Exists(ctx, key).Result(); err != nil || n != 0 {
				t.Fatalf("%s is visible from DB %d (EXISTS=%d, err %v): the probe and the DSN disagree about the database",
					key, tc.other, n, err)
			}
			if err := p.rdb.Del(ctx, key).Err(); err != nil {
				t.Logf("cleanup: DEL %s: %v", key, err)
			}
		})
	}
}

// TestDoctorReportsTheShippedComposeDSNAsReachable is the same defect end to end,
// through the probe the command opens and the checks it feeds: the DSN
// .env.example and compose.yaml ship has to come out of the report as
// `pass redis.reachable`, because a doctor that fails on a healthy box is worse
// than one that refuses to start.
func TestDoctorReportsTheShippedComposeDSNAsReachable(t *testing.T) {
	dsn := "redis://" + doctorTestServerAddr(t) + "/0"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	p, err := OpenRedis(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRedis(%q): %v", dsn, err)
	}
	defer p.Close()

	d := healthy()
	d.RedisURL = dsn
	d.Redis = p

	c := find(t, d.Run(ctx), CheckRedisReachable)
	if c.Status != Pass {
		t.Fatalf("redis.reachable = %s (%s), want pass on a healthy Redis reached by the shipped URL form", c.Status, c.Observed)
	}
	if strings.Contains(c.Observed, "too many colons") {
		t.Fatalf("redis.reachable observed %q: the DSN was dialled as a hostname", c.Observed)
	}
}

// TestOpenRedisWithoutAURLIsNotAnAttempt pins the caller's skip: an unset
// PIGEON_REDIS_URL is "nothing was asked of Redis", which cmd/pigeon turns into
// a nil probe and a skip, and the error has to name the variable for that.
func TestOpenRedisWithoutAURLIsNotAnAttempt(t *testing.T) {
	p, err := OpenRedis(context.Background(), "")
	if err == nil {
		p.Close()
		t.Fatal(`OpenRedis("") returned a probe, want "PIGEON_REDIS_URL is not set"`)
	}
	if !strings.Contains(err.Error(), "PIGEON_REDIS_URL is not set") {
		t.Fatalf(`OpenRedis(""): %v, want it to name PIGEON_REDIS_URL`, err)
	}
}

// TestOpenRedisReportsASilentServerAsAnError keeps doctor's "asked and it did not
// answer" contract: a configured dependency that will not answer is a failure the
// caller records, never a nil probe that reads as "not configured".
func TestOpenRedisReportsASilentServerAsAnError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Nothing listens on port 1, and a connection refused is immediate.
	p, err := OpenRedis(ctx, "redis://127.0.0.1:1/0")
	if err == nil {
		p.Close()
		t.Fatal("OpenRedis against a dead server returned a probe, want the ping error")
	}
}

// TestOpenRedisDoesNotLeakTheDSNInItsError is FR-1008 on the path this change
// adds: the client is built by internal/redisconn, whose refusal quotes the value
// it could not parse - and a Redis URL may carry a password in its userinfo. The
// reason reaches the operator's line, so it must not carry the raw DSN past the
// redaction doctor's own lines apply.
func TestOpenRedisDoesNotLeakTheDSNInItsError(t *testing.T) {
	const password = "sup3rs3cretpw"
	raw := "redis://default:" + password + "@127.0.0.1:6399/%zz"

	p, err := OpenRedis(context.Background(), raw)
	if err == nil {
		p.Close()
		t.Fatalf("OpenRedis(%q) returned a probe, want the parse failure", raw)
	}
	if strings.Contains(err.Error(), password) {
		t.Fatalf("OpenRedis(%q) leaks the password in its error: %v", raw, err)
	}
	if strings.Contains(err.Error(), raw) {
		t.Fatalf("OpenRedis(%q) echoes the raw DSN in its error: %v", raw, err)
	}
}
