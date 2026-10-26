# Pigeon — implementation spec

`REQUIREMENTS.md` says what the system must do. This file says how this tree does
it, and records the places where the requirements are silent, so those gaps are
decisions rather than accidents.

## Layout

```
cmd/pigeon/          the single core binary: API, workers, migrator
internal/config/     environment-only configuration + OPS-7 refusals
internal/db/         pool, migrations (embedded), ControlTx/EvidenceTx
internal/db/migrations/   NN_name.sql, applied in order under an advisory lock
internal/apikey/     key minting, hashing, env parsing
internal/identity/   users, passwords, instance-admin bootstrap
internal/store/      tenant-scoped data access (every method names a producer)
internal/httpapi/    router, middleware, handlers
```

Rules that hold across the tree:

- No package outside `internal/store` writes SQL against tenant tables. RLS makes
  a violation return nothing rather than another producer's rows, but the layering
  is what makes it reviewable.
- No global mutable state, no `init()` that does I/O. Everything is constructed in
  `cmd/pigeon/main.go`, which is the only file that knows the wiring.
- No third-party router, no ORM, no DI framework. `net/http`'s pattern mux plus
  `pgx` is the whole dependency surface at M0, and every added dependency is a
  supply-chain decision a self-hoster inherits.

## Data model decisions

**Environment on every tenant row, not a second schema.** FR-103 wants test and
live fully separated. Two schemas would double the migration set and make every
query pick a schema at runtime; an `env` column plus a composite unique key
(`consumers (producer_id, env, ref)`) keeps one query path and makes a
cross-environment read a constraint violation, not a silent leak.

**RLS plus a scoped repository, not one or the other.** MT-1 asks for both. The
repository is the readable layer; RLS is the layer that survives a bug in it. RLS
reads `current_setting('pigeon.producer_id', true)` — with the variable unset the
policies match nothing, so a request that forgets to scope fails closed.

**One sanctioned bypass.** Authentication happens before the producer is known, so
`api_keys` cannot be readable under a producer-scoped policy. `0002_auth_lookup.sql`
adds a `SECURITY DEFINER` function that returns four columns for one hash and
nothing else. It is the only place in the schema where a query crosses tenants,
and it is a function precisely so that fact is greppable.

**`producers` is unreadable under RLS.** No policy can express "this producer may
see itself" without also letting it enumerate; the admin path needs an explicit
`pigeon.instance_admin` setting and its own policy, which lands with the admin API
in S1. Until then the table is readable only by the migration role.

**API keys are SHA-256, not bcrypt.** The plaintext is 256 bits of uniform
randomness, so stretching defends against nothing while putting ~40 ms on the auth
path of every ingest request. Passwords are bcrypt cost 12, because humans choose
them.

**Deliveries and attempts are not in `0001`.** Their partitioning (SB-1) is a
design decision with a retention consequence, and adding them later is a
migration, not a rewrite. They arrive in S3/S8.

## Durability mapping

§6 is expressed as code in exactly two places, and nothing else may write:

| Write | Helper | `synchronous_commit` |
|---|---|---|
| event + outbox, delivery row, terminal status, secrets | `db.ControlTx` | `on` |
| attempt rows, response bodies, `last_used_at` | `db.EvidenceTx` | `off` |

`set_config(..., true)` is transaction-local, so the setting cannot leak to the
next borrower of a pooled connection — which is the failure mode that makes this
kind of split dangerous when done with `ALTER ROLE`.

## Migrations

- Embedded in the binary (no `migrate` sidecar, no operator step).
- Applied under `pg_advisory_lock` on one dedicated connection, so two replicas
  booting together cannot race (OPS-9). The lock is released in a `defer` even on
  error.
- Each file's SHA-256 is recorded. A migration that already ran and whose file has
  since changed is a startup error, not a silent divergence between environments.
- Forward-only. A bad migration is fixed by a new migration; rollback is
  documented per release, not automated.

## Local toolchain

Go 1.23+ is required. On this build host it is installed at
`/opt/data/toolchain/go/bin` with `GOMODCACHE` under `/opt/data/toolchain`; on a
normal machine the stock `go` on `PATH` is all that is needed. Docker is not
required to build or test the core.

## Gap list — where `REQUIREMENTS.md` is silent or inconsistent

These are the things to settle before the slice that depends on them. None of them
block S0.

1. **AC-14 contradicts FR-1202/FR-1203.** The acceptance criterion pauses a
   subscription after **50** failures for one endpoint in an hour; the requirements
   pause a *subscription* at 10 failures per `(endpoint, event_type)` and an
   *endpoint* at 100. 50 triggers neither. Either AC-14 means "10 failures on one
   subscription, then 90 more on other types to reach the endpoint threshold", or
   one of the numbers is stale. Blocks S5.
2. **NFR-4 has no acceptance criterion on the reference machine.** NFR-4 claims
   1,000 events/s ingest at 2 vCPU / 4 GB. AC-16 measures 1,000 events/s on
   8 vCPU / 16 GB, and AC-31 measures 250 events/s on the reference box. So the
   one number that defines "modest machine" is never tested where it is claimed.
   Blocks S10's load test, not the build.
3. **The envelope has no `consumer_id` field.** §10 carries `consumer_ref` only,
   but FR-505 allows routing by opaque `consumer_id` instead. What does the
   receiver see in that mode — nothing, or a field that means nothing to them?
   Affects S3's envelope struct.
4. **FR-903's 30-day dedupe TTL has no store.** `(endpoint, event_id)` dedupe keys
   outlive delivery retention (14 d) and are read on the support path. Redis with a
   TTL, a Postgres table, or a query over `deliveries` — the choice changes whether
   dedupe survives a Redis wipe, which §6 does not cover. Affects S7.
5. **Consumer-user session format is unspecified.** FR-110 says the session carries
   a `consumer_id` claim and every request is filtered server-side, but not whether
   the portal holds a JWT, an opaque token, or a cookie session, nor how it calls
   the API (scoped token per FR-514). Affects S9.
6. **`pigeon doctor` is referenced by three requirements and specified by none.**
   OPS-4, OPS-10 and AC-22 assume a command that checks Postgres, Redis AOF, clock
   skew, disk headroom and `ulimit -n`. Its output format, exit codes and whether it
   runs in the core binary or beside it are open. Affects S10.
