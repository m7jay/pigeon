# Pigeon — build plan

Derived from `REQUIREMENTS.md` §17 (build order) and §16 (acceptance criteria).
`REQUIREMENTS.md` is the contract; this file is the execution order. Where they
disagree, the requirements win and this file is wrong.

## Rule for every slice

1. The slice names the acceptance criteria it turns green, before any code.
2. Test first: the failing test encodes the criterion, not the implementation.
3. One commit per slice, Conventional Commits, no signing required.
4. A slice is done when its ACs are green **and** `make vet test` passes.
5. Anything learned that changes a requirement goes into `SPEC.md`'s gap list, not
   silently into the code.

## Slices

| # | Slice | Turns green | Depends on | State |
|---|---|---|---|---|
| S0 | Repo, module, config, migrations, RLS, API-key auth, `/health` + `/health/ready`, bootstrap admin, cross-producer harness | AC-15 ✅ (SQL layer), AC-20 ✅ (harness runs in CI) | — | **done** — CI run 40307423507 |
| S1 | Producers, users, environments, instance-admin API, key issue/revoke, `pigeon doctor` skeleton | AC-15, AC-20 | S0 | next |
| S2 | Event types (JSON Schema, name regex, payload version), `POST /events` + outbox in one transaction, relay with `SKIP LOCKED` + `LISTEN/NOTIFY` + 1 s poll | AC-08, AC-09 | S1 | — |
| S3 | Consumers, routing (`consumer_id` / `consumer_ref`), fan-out, delivery rows with `(event_id, endpoint_id)` uniqueness | AC-02, AC-03, AC-23, AC-24, AC-25, AC-27 | S2 | — |
| S4 | Delivery worker: signing, 5 s timeout, status classification, lease at fan-out, sweeper | AC-04, AC-05, AC-06, AC-07, AC-21, AC-28 | S3 | — |
| S5 | Retry policy, `Retry-After`, pause thresholds, per-endpoint leased concurrency + token bucket in Redis | AC-14, AC-16, AC-17, AC-35…AC-37, AC-41 | S4 | — |
| S6 | Attempt buffer + flusher, `synchronous_commit` split, evidence retention | AC-29, AC-33 | S4 | — |
| S7 | Replay (bounded, idempotent), inspect API, event-level rollup | AC-12, AC-13, AC-32 | S4 | — |
| S8 | Partitioning, retention-by-disk, partition drop under the sweeper lease | AC-10, AC-18, AC-42 | S4 | — |
| S9 | Portals — producer (M2a) and consumer (M2b), scoped tokens, branding | AC-01, AC-38, AC-39, AC-40 | S3 | — |
| S10 | Hardening: SSRF at three points, secret rotation, `doctor`, load test, fault-injection matrix in CI | AC-19, AC-22, AC-30, AC-31, AC-34, AC-43 | S5…S9 | — |

Two items are in S2/S3 rather than later, for the reason §17 gives: the outbox
changes the ingest transaction, and per-endpoint throttling changes the queue
topology. Retrofitting either is a rewrite of the paths around it.

## Current state (S0) — closed

Evidence: CI run **40307423507** on head `8b9a74c0adf23c27b8d7fe09ecc4892556fa772e`
(`ci`, success). The test step ran `go test -count=1 -race -v ./...` against the
`postgres:16` service container and logged:

    --- PASS: TestCrossProducerReadsAreEmpty (0.05s)
    --- PASS: TestProducersAreAdminOnly (0.05s)
    ok      github.com/m7jay/pigeon/internal/store  2.315s
    AC-15/AC-20 harness executed and passed

No `--- SKIP` line appeared. Since this commit the CI step fails if one does, or
if either named test did not pass, so "the harness exists" cannot quietly become
"the harness skipped" again. The same suite also ran green locally on Postgres
16.2 on the build host against a scratch cluster, which is how the harness was
debugged before CI.

What that proves:

- **AC-20** — the cross-producer harness executes in CI against real Postgres.
  It skips only when `PIGEON_TEST_DATABASE_URL` is unset, and CI now treats a
  skip as a failure.
- **AC-15 at the SQL layer** — as `pigeon_app` (`login nosuperuser nobypassrls`),
  with the tenant scope set to producer A, counting producer B's consumer by id
  returns 0 and counting A's own returns 1. The positive assertion is what rules
  out a policy that just returns no rows.
- **The instance-admin path for `producers` now exists.**
  `0003_producers_admin.sql` adds a permissive policy gated on
  `pigeon.instance_admin = 'on'`, and `db.SetInstanceAdmin` is the sanctioned way
  to set it. `TestProducersAreAdminOnly` proves both halves: tenant scope and an
  unset marker see 0 producers, the admin marker sees both seeded rows, an insert
  without the marker is refused and one with it succeeds.

Honest limits — S1 owns these:

- The admin gate is a session GUC the application role can set. It defends
  against a forgotten tenant scope in a request path, not against a compromised
  app role. A separate database role for admin operations is the real privilege
  boundary and belongs with the instance-admin API in S1.
- There is no admin HTTP API yet: producers/users/environments CRUD and key
  issue/revoke are S1. S0 ships the bootstrap credential and the DB path only.
- `internal/identity` has no test file; bootstrap idempotency is exercised by the
  binary's start-up path but not asserted in a test. S1 adds it with the API.

Done:

- Go module `github.com/pigeon-hq/pigeon`, Go 1.23, `pgx/v5` only.
- `internal/config` — env-only config, OPS-7 refusals, OPS-8 connection-budget
  arithmetic, all problems reported in one pass.
- `internal/db` — pool, `ControlTx`/`EvidenceTx` (FR-812), embedded migrations
  applied under a Postgres advisory lock (OPS-9) with per-file checksums so an
  edited applied migration fails loudly instead of drifting.
- `0001_init.sql` — producers, users, producer_members, consumers, consumer_users,
  api_keys; RLS on every tenant table (MT-1); `env` on every tenant row (FR-103).
- `0002_auth_lookup.sql` — the single sanctioned RLS bypass, a `SECURITY DEFINER`
  function that reads one key row by hash.
- `0003_producers_admin.sql` — the instance-admin policy for `producers`
  (`producers_admin`, gated on `pigeon.instance_admin = 'on'`), so the admin path
  S1 builds against exists instead of only being named in a comment.
- `internal/apikey` — `pgn_<env>_<43 chars>`, SHA-256 at rest, hint for the UI.
- `internal/httpapi` — liveness/readiness split, bearer auth, structured errors,
  `X-Request-Id`, panic recovery.
- `internal/identity` — idempotent instance-admin bootstrap (FR-101).
- Packaging: `Makefile`, `Dockerfile` (scratch, non-root), `compose.yaml`
  (Postgres + Redis with `appendonly yes` per D-5), `.env.example`.
- Tests: config, api-key, migration loader, HTTP middleware; cross-producer
  harness gated on `PIGEON_TEST_DATABASE_URL`.

Status: **S0 closed.** AC-15 (SQL layer) and AC-20 (harness) are proven against a
live database, in CI and on the build host. The admin policy hole is filled; the
remaining admin surface (HTTP API, separate admin role) is S1's, as listed above.
Nothing in this section is a claim that was not run.

## Definition of done for M0

- `make vet test` green on a clean checkout.
- `PIGEON_TEST_DATABASE_URL` set in CI, harness green, AC-20 in the pipeline.
- `pigeon` refuses to boot with a missing or weak admin password (OPS-7) —
  verified by hand.
- A key issued for producer A cannot read producer B's rows, proven by a test
  that queries with the *wrong* WHERE clause on purpose.
