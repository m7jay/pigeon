// Package redisconn turns the value PIGEON_REDIS_URL takes into a go-redis
// client, so every process that dials Redis from the configuration reads that
// value the same way.
//
// The documented form is a URL - `redis://host:6379/db`, the shape .env.example
// and compose.yaml ship - and a bare `host:port` is accepted too. The URL's
// database index must reach the client: a client built from the URL's host alone
// still connects, but it lands in DB 0, so a deployment that asked for its own
// database silently shares the default one.
//
// One helper rather than an implementation per caller. Before this package,
// internal/attemptbuf parsed the value correctly while internal/broker and
// internal/limit passed the whole string straight through as
// redis.Options.Addr - so the documented URL form dialled as a *hostname* and the
// API refused to boot with the one value the shipped compose file sets:
//
//	dial tcp: address redis://redis:6379/0: too many colons in address
//
// Three readings of "what is a Redis address" are three chances for one of them
// to disagree; there is one, and it is tested here.
package redisconn

import (
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

// Options parses raw into the go-redis options it denotes.
//
// A value containing "://" is a URL and goes through redis.ParseURL, which is
// what carries the database index in its path - and any username, password or
// TLS the URL names - into the client. Anything else is taken as a bare
// host:port.
//
// ContextTimeoutEnabled is deliberately left at go-redis's default (false): the
// flag decides whether a caller's context deadline bounds a command or whether
// the client rides its own ReadTimeout instead, and turning it on is a decision
// each caller makes for its own reason. internal/attemptbuf sets it (AckTimeout
// must be the bound it reads as, D-9); the broker and the limiter do not need it
// for this fix and are left exactly as they behaved before.
func Options(raw string) (*redis.Options, error) {
	if strings.Contains(raw, "://") {
		o, err := redis.ParseURL(raw)
		if err != nil {
			return nil, fmt.Errorf("parse PIGEON_REDIS_URL %q: %w", raw, err)
		}
		return o, nil
	}
	return &redis.Options{Addr: raw}, nil
}

// Client builds the client raw denotes. It does not dial and does not PING, so a
// caller keeps its own boot gate and its own error text for a wrong address.
func Client(raw string) (*redis.Client, error) {
	o, err := Options(raw)
	if err != nil {
		return nil, err
	}
	return redis.NewClient(o), nil
}
