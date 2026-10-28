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
| S1 | Producers, users, environments, instance-admin API, key issue/revoke, rate limits | AC-15 ✅ (HTTP layer), AC-20 ✅ (every route classified) | S0 | **done** — CI run 40317878772 (after review round 1) |
| S2 | Event types (JSON Schema, name regex, payload version), `POST /events` + outbox in one transaction, relay with `SKIP LOCKED` + `LISTEN/NOTIFY` + 1 s poll | AC-08, AC-09 | S1 | — |
| S3 | Consumers, routing (`consumer_id` / `consumer_ref`), fan-out, delivery rows with the partial `(event_id, endpoint_id)` unique index over first fan-out (I-3) | AC-02, AC-03, AC-23, AC-24, AC-25, AC-27 | S2 | — |
| S4 | Delivery worker: signing, 5 s timeout, status classification, lease at fan-out, sweeper | AC-04, AC-05, AC-06, AC-07, AC-21, AC-28 | S3 | — |
| S5 | Retry policy, `Retry-After`, pause thresholds, per-endpoint leased concurrency + token bucket in Redis | AC-14, AC-16, AC-17, AC-35…AC-37, AC-41 | S4 | — |
| S6 | Attempt buffer + flusher, `synchronous_commit` split, evidence retention | AC-29, AC-33 | S4 | — |
| S7 | Replay (bounded, idempotent), inspect API, event-level rollup | AC-12, AC-13, AC-32 | S4 | — |
| S8 | Partitioning, retention-by-disk, partition drop under the sweeper lease | AC-10, AC-18, AC-42 | S4 | — |
| S9 | Portals — consumer portal (M2b) landed as `t_0eea0b84`; producer portal (M2a) is S9a `t_c0144baa` — scoped tokens, branding; **FR-106 optional OIDC login** and FR-111's password-reset flow (both deferred here — a second login method is a portal-visible feature, and neither exists in S1) | AC-38, AC-39, AC-40, AC-47 (AC-01 → S9a) | S3 | — |
| S10 | Hardening: SSRF at three points, secret rotation, `doctor`, load test, fault-injection matrix in CI; **portal session security (FR-1009)** — `HttpOnly`/`Secure`/`SameSite=Lax` on every portal cookie, a CSRF token on every state-changing POST, cookie lifetime read from the core's `expires_at` | AC-19, AC-22, AC-30, AC-31, AC-34, AC-43, AC-49, AC-50, AC-51 | S5…S9 | — |
| S11 | Delivery fairness (**FR-1303**, **FR-1304**): stop parking the dispatch path on a capped endpoint, split the attempt budget into a first-attempt lane and a retry lane, and make a hold visible | AC-52, AC-53 | S5 | — |

Two items are in S2/S3 rather than later, for the reason §17 gives: the outbox
changes the ingest transaction, and per-endpoint throttling changes the queue
topology. Retrofitting either is a rewrite of the paths around it.

**S9 is split.** `t_0eea0b84` landed the consumer portal (M2b); the producer portal
(M2a) is S9a, card `t_c0144baa`, and AC-01 moved with it. The S9 row above still
reads as the pre-split definition of record (`t_a11b8140`).

**FR-1009 (P0) had no owner and no dashboard implemented it in full.** The consumer
portal shipped with a 12 h hardcoded cookie lifetime (`handler.go:195`), a flash
cookie with no `SameSite` (`handler.go:218`), and `SameSite=Lax` as its only CSRF
control. S10 owns it now (AC-49…AC-51). S9a/M2a inherits the fix by building on
`internal/portal` and MUST NOT reintroduce a hardcoded `MaxAge` or a tokenless form.

**AC-49…AC-51 were numbered past AC-48 on purpose.** `docs/spec-gap-closure` defines
AC-44…AC-48, and its AC-47 already asserts the consumer session cookie's flags and the
CSRF rejection; this amendment adds the shared invariant and the lifetime leg AC-47
lacks. That amendment landed first, so AC-44…AC-51 are one contiguous block in
`REQUIREMENTS.md` §16 and AC-47 keeps its own number on the S9 row above. The 12 h
constant is A-5's *default* session TTL, which is why it looked deliberate; it is wrong
for every other `PIGEON_SESSION_TTL`.

**§7.13's fairness rows had no owner either, and the answer is not one answer.** S5's
requirement line named §9 and FR-1201…FR-1308, and its AC list (AC-14, AC-16, AC-17,
AC-35…AC-37, AC-41) covers the failure-policy half of that range only: the retry policy, the
pause thresholds and the per-endpoint limits. Three rows of the fairness half were implemented,
tested and owned by nothing, which is why the slice was approved without them. `SPEC.md` A-10
disposes of them, and the split is deliberate:

- **FR-1303 (P0) and FR-1304 are S11** (card `t_558a9f59`, `pigeon-dev`), because the shipped
  dispatch path *violates* them. `dispatch` claims a delivery and takes the process slot before
  it asks the endpoint for a slot, then sleeps there (`internal/delivery/delivery.go:439-486`, the
  wait loops at `:502-514` and `:521-534`), so with the default `PIGEON_DELIVERY_WORKERS=1`
  one capped endpoint occupies a
  process's only dispatch loop and nothing reports it. S11 adds no second endpoint limit:
  FR-1308 is untouched, and the retry budget it partitions is the worker's attempt budget.
- **FR-1301's publish half is already shipped** as FR-207's per-key and per-producer rate
  limits; its **delivery**-quota half is deferred to v1.1 with its cost stated
  (`REQUIREMENTS.md` §17.1), because a per-producer value needs a config surface and an API to
  set it. §7.13's row names both halves so it no longer reads as one unowned claim.

**Consequence for S10, recorded rather than acted on.** AC-16's "hanging endpoint capped" leg
is measured in S10, and its NFR-2 leg (normal publish to first attempt ≤ 60 s) cannot pass
while the dispatch path parks behind the first capped endpoint. S11 is a precondition for that
measurement, not a replacement for it: AC-16 and AC-17 stay S10's. Whether that re-gates S10's
`Depends on` is the leader's call, so the dependency is written here instead of by editing the
row under a running card.

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

## Current state (S1) — closed after review round 1

Review round 1 (reviewer `pigeon-rev`, on head `c4248aa`) found one exploitable
defect and three gaps between what the suite proved and what the documents
claimed. The fixes are migration `0005_s1_invite_hardening.sql`, the database role
split, the route classification in the AC-20 suite, and FR-208's issuance path;
each is recorded in `SPEC.md`'s S1 decisions. The claims below were then
re-verified rather than restated.

Evidence: CI run **40317878772** on head `2dbe96cb437b320a50317782d1a1cf1fdae364d7`
(`ci`, success, PR #4), plus the same suite green on Postgres 16.2 on the build
host as `pigeon_app`. The test step ran `go test -count=1 -race -p 1 -v ./...`
against the `postgres:16` service container and required **twenty-one** named
tests (S0's two, S1's eleven acceptance tests, and the eight regression tests this
review round added); a rename or a removal fails the build. No `--- SKIP` line
appeared, and CI still treats one as a failure.

What that proves:

- **AC-15 at the HTTP layer.** Every producer-scoped route, aimed at another
  tenant's identifiers, answers `404` with `code=not_found` (never `403`), and the
  refusal is logged as a `cross-producer attempt` naming the requested identifier
  and the caller's tenant. The two routes that take no identifier —
  `GET /v1/whoami` and `GET /v1/consumer/me` — are aimed with the other tenant's
  key and consumer session and must answer with their own scope only.
- **AC-20 classifies every route.** `TestCrossProducerSuiteCoversEveryRoute` now
  walks the whole table and requires each route to be either producer-scoped with a
  cross-producer case or explicitly excluded with a reason
  (`notProducerScoped`); it reports `30 routes classified, 16 producer-scoped with
  a cross-producer case`. The previous version skipped every route that was neither
  `kindMember` nor `kindOwner`, which is how the two above escaped the suite.
- **No route is reachable without a credential.** Every non-anonymous route answers
  `401` without a bearer token, and the anonymous allow-list is asserted to be
  exactly health, login and invitation acceptance (plus self-registration only when
  the flag is on).
- **FR-103 is enforced in the database, not only in Go.** The policies on
  `consumers`, `consumer_users`, `api_keys` and `invites` require
  `env = current_setting('pigeon.env')`; a transaction with a producer scope and no
  environment scope sees zero rows, asserted in `TestCrossProducerReadsAreEmpty`.
- **Tenant isolation survives the deployed topology.** `TestTenantTablesCannotBeBypassedByTheRuntimeRole`
  asks PostgreSQL whether every tenant table is RLS-enabled and owned by something
  other than the runtime role; `TestComposeSplitsTheMigratorFromTheRuntimeRole`
  reads `compose.yaml` and `deploy/postgres/01-roles.sql` so a one-line edit that
  collapses the two roles fails the build.
- **An invitation cannot take over an existing account.**
  `TestInviteAcceptanceCannotRewriteAnExistingAccount` (store) and
  `TestInvitationAcceptanceRequiresTheInviteesOwnSession` (HTTP, the reproduced
  attack) assert that the old password still works, the attempted new one does not,
  and only the invitee's own session can redeem the token.

Per-requirement map, corrected:

| Requirement | Test |
|---|---|
| FR-101 | `TestInstanceAdminCreatesProducersAndIsAudited` (bootstrap + forced change) |
| FR-102 | `TestInstanceAdminCreatesProducersAndIsAudited` |
| FR-103 | `TestTestAndLivePlanesAreSeparate`, `TestCrossProducerReadsAreEmpty` |
| FR-104 | `TestMembershipSpansProducersAndRolesAreEnforced` |
| FR-105 | `TestMembershipSpansProducersAndRolesAreEnforced` (second membership over the session-bound invitation path) |
| FR-106 | **no test — deferred to S9** (see `SPEC.md`); the earlier claim of one test per FR was wrong |
| FR-107 | `TestSuspensionStopsIngestionAndIsReversible`, `TestAuthLookupReportsSuspension` |
| FR-108 | `TestCrossProducerRequestsAreNotFound`, `TestCrossProducerSuiteCoversEveryRoute` |
| FR-109 | `TestConsumerUserIsBoundToOneConsumerAndRevocable` |
| FR-110 | `TestConsumerUserIsBoundToOneConsumerAndRevocable`, `/v1/consumer/me` case |
| FR-111 | `TestConsumerUserIsBoundToOneConsumerAndRevocable` (no self-registration route) |
| FR-201…FR-206 | `TestAPIKeyLifecycleAndRevocation` |
| FR-207 | `TestRateLimitsArePerKeyAndPerProducer` |
| FR-208 | `TestReplayScopedKeyCanBeIssued` (issuance + reporting; enforcement is S2's publish path) |
| MT-5 | `TestInstanceAdminCreatesProducersAndIsAudited`, `TestAuditLogIsAppendOnly` |
| OPS-8 | `TestValidateBudgetIsTheOPS8Arithmetic`, `docs/topology.md` |

Honest limits — the next slices own these:

- **Rate limits are per process.** FR-207 is satisfied for one replica; N replicas
  allow N × the configured rate. The Redis-backed limiter belongs with the
  per-endpoint throttles in S5 (recorded in `SPEC.md`).
- **The instance-admin DB gate is still a GUC.** `pigeon.instance_admin = 'on'` is
  settable by the runtime role, so it defends against a forgotten tenant scope in a
  request path, not against a compromised application role. The role split means the
  tenant policies now *apply* to that role; a distinct admin role that alone may set
  the marker is still open, and would need its own connection pool.
- **No mailer.** Invitation tokens are returned once in the API response, like API
  keys. An invitation for an address that already has an account is therefore only
  redeemable by that account's own session — the token is a routing hint, not a
  credential. An account that has lost its last binding cannot log in at all, so it
  cannot redeem one either (`SPEC.md` gap list item 7; S9 owns the mailer and the
  password reset FR-111 names).
- **`/v1/producer/*` is not versioned per environment in the URL**, it takes `?env=`
  for member sessions. If the portals prefer a path segment, that is a routing
  change with no data-model consequence.
- **`internal/httpapi` takes ~3.5 min under `-race`** because the acceptance suite
  performs ~50 real bcrypt operations at cost 12. A test-only cost knob is the lever
  if CI gets tight, not dropping `-race`.

## Definition of done for M0

- `make vet test` green on a clean checkout.
- `PIGEON_TEST_DATABASE_URL` set in CI, harness green, AC-20 in the pipeline.
- `pigeon` refuses to boot with a missing or weak admin password (OPS-7) —
  verified by hand.
- A key issued for producer A cannot read producer B's rows, proven by a test
  that queries with the *wrong* WHERE clause on purpose.
