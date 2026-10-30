package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m7jay/pigeon/internal/apikey"
)

// cmdSeed provisions a producer, one event type, one consumer and N verified
// endpoints subscribed to that type - the minimum fan-out shape the capacity model
// is stated in (§8.1's "× subscriptions"). It exists so a load run is
// reproducible: the doc says exactly which command created the fixtures, and a
// second run re-uses them instead of accumulating new ones.
//
// The endpoints are written with internal = true. That is FR-402's operator escape
// hatch, and it is what makes a load test possible on one box: the sink listens on
// loopback, which FR-1005 refuses at registration, request time and dial time
// unless the endpoint is flagged. Using the flag is honest - the harness is
// provisioning a private-network endpoint deliberately - whereas weakening the SSRF
// check for the sake of the test would not be.
func cmdSeed(args []string) error {
	fs := flag.NewFlagSet("seed", flag.ContinueOnError)
	dsn := fs.String("db", os.Getenv("PIGEON_LOADGEN_DB"), "Postgres DSN")
	slug := fs.String("producer", "loadtest", "producer slug")
	eventType := fs.String("event-type", "load.created", "event type name")
	endpoints := fs.Int("endpoints", 10, "number of endpoints (one subscription each)")
	url := fs.String("url", "http://127.0.0.1:9001/hook", "endpoint URL (the sink)")
	env := fs.String("env", "live", "environment")
	// broadcast is what a probe wants: it is accepted and recorded but writes no
	// delivery rows, so it marks a moment without touching the delivery path.
	// consumer is what a load run wants, because the delivery unit needs fan-out.
	routing := fs.String("routing", "consumer", "event type routing mode: consumer or broadcast")
	quiet := fs.Bool("quiet", false, "do not print the API key")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *routing != "consumer" && *routing != "broadcast" {
		return fmt.Errorf("-routing must be consumer or broadcast, got %q", *routing)
	}
	if *dsn == "" {
		return fmt.Errorf("-db (or PIGEON_LOADGEN_DB) is required")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		return err
	}
	defer pool.Close()

	pid := "prd_" + *slug
	if _, err := pool.Exec(ctx,
		`insert into producers (id, slug, name) values ($1, $2, $3)
		 on conflict (slug) do update set name = excluded.name`,
		pid, *slug, "Load Test "+*slug); err != nil {
		return fmt.Errorf("producer: %w", err)
	}

	key, err := apikey.Generate(*env)
	if err != nil {
		return err
	}
	// One key per producer: re-seeding rotates it rather than leaving a growing
	// pile of valid credentials on a machine that may be shared.
	if _, err := pool.Exec(ctx, `delete from api_keys where producer_id = $1`, pid); err != nil {
		return fmt.Errorf("clear api keys: %w", err)
	}
	if _, err := pool.Exec(ctx,
		`insert into api_keys (id, producer_id, env, key_hash, display_hint) values ($1,$2,$3,$4,$5)`,
		"key_"+*slug+"_load", pid, *env, key.Hash, key.Hint); err != nil {
		return fmt.Errorf("api key: %w", err)
	}

	var consumerID string
	if err := pool.QueryRow(ctx,
		`insert into consumers (id, producer_id, env, ref, name) values ($1,$2,$3,$4,$5)
		 on conflict (producer_id, env, ref) do update set name = excluded.name
		 returning id`,
		"con_"+*slug+"_load", pid, *env, "load-consumer", "Load Consumer").Scan(&consumerID); err != nil {
		return fmt.Errorf("consumer: %w", err)
	}

	if _, err := pool.Exec(ctx,
		// routing_mode = 'consumer': a broadcast event writes no delivery rows
		// (internal/store/fanout.go), because the endpoint model is consumer-owned
		// and a broadcast has no consumer to intersect. A load run that wants a
		// delivery unit must route to a consumer, so the type is declared that way
		// and every publish carries consumer_ref.
		`insert into event_types (id, producer_id, env, name, routing_mode, payload_schema) values ($1,$2,$3,$4,$5,$6)
		 on conflict (producer_id, env, name) do update
		    set payload_schema = excluded.payload_schema, routing_mode = excluded.routing_mode`,
		"evt_"+*slug+"_load", pid, *env, *eventType, *routing,
		// The generator's payload is {"data":"xxxx"}; anything stricter would make
		// the harness measure schema validation rather than ingest.
		`{"type":"object","properties":{"data":{"type":"string"}},"required":["data"]}`,
	); err != nil {
		return fmt.Errorf("event type: %w", err)
	}

	secretBytes := make([]byte, 32)
	_, _ = rand.Read(secretBytes)
	signingSecret := "whsec_" + base64.StdEncoding.EncodeToString(secretBytes)
	tokenHash := sha256.Sum256([]byte("load-verification-token"))
	now := time.Now().UTC()

	for i := 0; i < *endpoints; i++ {
		epID := fmt.Sprintf("epd_%s_%02d", *slug, i)
		if _, err := pool.Exec(ctx, `
			insert into endpoints (id, producer_id, env, consumer_id, name, url, internal, status,
			                       verification_token_hash, verification_hint, verified_at,
			                       signing_secret, signing_secret_hint)
			values ($1,$2,$3,$4,$5,$6,true,'active',$7,'load',$8,$9,$10)
			on conflict (producer_id, id) do update
			   set url = excluded.url, internal = true, status = 'active',
			       verified_at = excluded.verified_at, deleted_at = null, paused_at = null`,
			epID, pid, *env, consumerID, fmt.Sprintf("sink-%02d", i), *url,
			tokenHash[:], now, signingSecret, signingSecret[:12]); err != nil {
			return fmt.Errorf("endpoint %d: %w", i, err)
		}
		if _, err := pool.Exec(ctx, `
			insert into subscriptions (id, producer_id, env, endpoint_id, event_type, status)
			values ($1,$2,$3,$4,$5,'active')
			on conflict (id) do update set status = 'active'`,
			fmt.Sprintf("sub_%s_%02d", *slug, i), pid, *env, epID, *eventType); err != nil {
			return fmt.Errorf("subscription %d: %w", i, err)
		}
	}

	if *routing == "consumer" {
		fmt.Printf("seeded producer %s (%s) with %d endpoint(s) subscribed to %s, routed to consumer_ref load-consumer\n",
			pid, *slug, *endpoints, *eventType)
	} else {
		fmt.Printf("seeded producer %s (%s) with %d endpoint(s) subscribed to %s (broadcast: no consumer_ref, no deliveries)\n",
			pid, *slug, *endpoints, *eventType)
	}
	if *routing == "consumer" {
		fmt.Printf("run with: -consumer-ref load-consumer (a broadcast event creates no deliveries)\n")
	} else {
		fmt.Printf("publish %s directly: a broadcast event is accepted with no consumer_ref and creates no deliveries\n", *eventType)
	}
	if !*quiet {
		fmt.Printf("PIGEON_LOADGEN_KEY=%s\n", key.Plaintext)
		fmt.Println("# This key is a load-test credential for a disposable database. Do not commit it.")
	}
	return nil
}
