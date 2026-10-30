# Pigeon — implementation spec

`REQUIREMENTS.md` says what the system must do. This file says how this tree does
it, and records the places where the requirements are silent, so those gaps are
decisions rather than accidents.

## Layout

```
cmd/pigeon/          the single core binary: API, workers, migrator
internal/config/     environment-only configuration + OPS-7 refusals + OPS-8 budget
internal/db/         pool, migrations (embedded), ControlTx/EvidenceTx, scope setters
internal/db/migrations/   NN_name.sql, applied in order under an advisory lock
internal/apikey/     key minting, hashing, env parsing
internal/id/         prefixed identifiers (prd_, usr_, key_, ses_, inv_, aud_)
internal/identity/   users, passwords, instance-admin bootstrap
internal/ratelimit/  in-process token buckets behind FR-207
internal/store/      tenant-scoped data access (every method names a producer+env)
internal/httpapi/    route table, credential kinds, handlers
```

**Module path.** `github.com/m7jay/pigeon`. `REQUIREMENTS.md` §19 chose
`github.com/pigeon-hq/pigeon` on the assumption of an organisation; no such org
exists and a fine-grained PAT cannot create one, so the module follows the actual
repository. `ghcr.io/pigeon-hq/pigeon` in §19 stays as the intended image path
until an org exists — being wrong about an unpublished image prefix costs nothing,
being wrong about the import path costs every consumer a module rename. Amend §19
when the org is created.

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

## S1 decisions — identity, environments, admin surface

These are the places S1 had to choose, recorded so the next slice inherits
decisions rather than accidents.

**`env` is part of the RLS predicate, not just a column.** FR-103 wants `test` and
`live` fully separated. A column that a handler must remember to filter separates
nothing, so 0004 adds `pigeon.env` and rewrites the policies on `consumers`,
`consumer_users`, `api_keys` and `invites` to require
`env = current_setting('pigeon.env', true)`. A transaction that sets the producer
but not the environment sees **zero** rows on every environment-bearing table
(fail closed). The store also filters explicitly, so the property is enforced twice
(MT-1). Consequence: `store.scoped` accepts an empty `Env`, which means
"producer-wide" and can only reach `producers`, `producer_members` and
environment-less (member) invitations.

**A producer-wide row exists only where the concept is producer-wide.** A producer
membership (FR-104) is producer-wide, so its invitation carries `env = null` rather
than a misleading environment; a consumer invitation (FR-109) always carries the
consumer's environment. The RLS policy encodes both cases in one predicate.

**Sessions carry claims that are re-read per request.** FR-110 needs a
`consumer_id` claim on a consumer user's session; the instance admin and producer
members need a credential too. One `sessions` table holds an optional producer /
consumer claim, and the token is opaque (256-bit, SHA-256 at rest, like an API
key). Claims are resolved by `pigeon_session_lookup` on **every** request instead of
being trusted from the token: removing a membership, revoking a consumer binding,
or revoking a session all take effect on the next request, which is what FR-109's
"independently" and FR-205's 60 s budget actually require. A session token is
`pgns_…` and an API key is `pgn_…`; each is rejected where the other belongs.

**`sessions` has RLS enabled and no policy.** It is not tenant data a request may
query — it is read only through definer functions, before the tenant is known. The
same shape as 0002's key lookup, and for the same reason.

**`producers` has no ownerless state.** `POST /v1/admin/producers` requires
`owner_email` and returns the owner's invitation token once. Without that, an
admin-created tenant has nobody who can invite, so it would be unmanageable until
someone added a second admin endpoint. Creating tenant + first-owner invitation in
one transaction is what keeps the invariant true from the first second. (FR-102's
optional self-registration does the same thing by creating the owner user
directly.)

**FR-108 is a routing property, not a handler property.** `routes()` declares each
route with the credential kind it needs; `authorize()` is the only place that
decides. A caller who is authenticated but out of scope gets **404**: an admin
session has no producer route, a producer owner has no admin route, a consumer user
has neither. Two failures are honest 403s, and both concern a caller who already
is who they say they are: FR-101's pending forced password change, and FR-107's
suspended producer (whose key still authenticates, and must be told why it is
refused rather than sent to re-issue a valid key). Refusals are logged as
`cross-producer attempt` with the caller's tenant, which is AC-15's "logged" half.

**Rate limiting is in-process for S1.** FR-207 asks for per-key and per-producer
limits with `429` + `Retry-After`; the token buckets live in the core process, so N
replicas allow N times the configured rate. That is honest for S1 (there is one
publishing route and no ingest path yet) and wrong for production multi-replica
deployments; the distributed limiter belongs with the per-endpoint throttles in S5,
where Redis is already required for the queue. Recorded in PLAN.md as a limit, not
as a finished requirement.

**Revocation is a read, not a cache invalidation.** `pigeon_auth_lookup` filters
`revoked_at is null`, so a revoked key or session is refused on the very next
request from any replica. FR-205's 60 s budget is met by construction; there is no
cache that could go stale.

**A consumer user is bound to exactly one consumer by the schema, not by code.**
`consumer_users_one_consumer_key` (0001) makes the cardinality a constraint, so the
second acceptance for one email fails as a conflict (409) instead of silently
rebinding. Revoking the binding deletes only `consumer_users`; the consumer row,
its endpoints and its history stay, and the session stops resolving.

**An invitation never changes an existing account.** 0004's acceptance function
overwrote the password of whatever user already owned the invited address. Because
there is no mailer yet and the invitation token is returned to the *inviter*, that
was an account takeover: any producer owner could invite `admin@pigeon.local`,
take the token out of the API response, accept it with a chosen password and log in
as the instance admin. 0005 replaces the function with two rules: (a) acceptance
never writes `password_hash`, `must_change_password` or `password_changed_at` for an
existing user, and (b) an invitation addressed to an address that already has an
account is only redeemable by that account's own session (`POST /v1/invites/accept`
takes an optional session, and the store answers `403 session_required` without
one). The token is then a routing hint, not a credential.

The alternative the review suggested — refuse to mint invitations for addresses
that already have an account — was not taken because FR-105 (a user in more than
one producer) has no other path in the API: every second membership is an
invitation for an existing account. Requiring the invitee's own session closes the
same hole without deleting an in-scope requirement. A regression test
(`TestInviteAcceptanceCannotRewriteAnExistingAccount`) asserts both halves: the old
password still works, the attempted new one does not, and the membership is added.

**`pigeon_invite_lookup` runs before bcrypt.** `POST /v1/invites/accept` is the only
anonymous write in the API. Hashing the supplied password before looking the token
up turned a random well-formed token into ~250 ms of CPU per request, which is a
cheap exhaustion vector. The store now resolves the invitation first (one indexed
read through a `SECURITY DEFINER` function) and hashes only when it is about to
create a new account.

**Two database roles.** PostgreSQL exempts a table's owner from row-level security,
so a deployment that migrates and serves as one role has no tenant isolation — the
policies run for nobody, and the suite would still be green because it connects as
a non-owner. `compose.yaml` therefore runs migrations as `pigeon_migrator` (the
owner) and serves as `pigeon_app` (no `SUPERUSER`, no `BYPASSRLS`, no ownership),
`deploy/postgres/01-roles.sql` creates the runtime role and its default privileges,
`config.Validate` refuses a configuration that points `PIGEON_MIGRATE_DATABASE_URL`
at the same role as `PIGEON_DATABASE_URL`, and two tests assert it: one reads the
shipped artifacts, one asks PostgreSQL whether every tenant table is protected from
the runtime role. `FORCE ROW LEVEL SECURITY` is deliberately *not* used: the
`SECURITY DEFINER` authentication functions run as the table owner, so forcing the
policies would break every pre-authentication lookup.

**FR-208's replay scope is issued and reported; enforcement waits for publish.**
`POST /v1/producer/keys` accepts `{"replay_scope": true}`, the column is written,
every read reports it, and the auth path carries it into the request as
`Principal.ReplayOnly` / `whoami.replay_only`. What a replay-scoped key may *not*
do cannot be expressed until there is a route to forbid it on: the publish path
(`POST /events`) arrives in S2, and that is where the scope becomes a rule.

**FR-106 (optional OIDC) is deferred to S9, with password auth staying default.**
No config key, route or dependency exists for it in S1, and inventing one here would
mean shipping an unreviewed OIDC integration to satisfy a table row. The portals
(S9) are where a second login method becomes user-visible. Recorded in PLAN.md's
slice table, and the earlier "one acceptance test per FR-101…FR-111" claim is
corrected there: FR-106 has no test because it has no code.

**FR-101's deviation from "printed once to stdout".** `REQUIREMENTS.md` asks for the
bootstrap credential to be printed once. The implementation reads it from
`PIGEON_INSTANCE_ADMIN_PASSWORD` and never prints it: a secret on stdout ends up in
container logs and CI transcripts, which is a worse failure than a documented env
var. It is still single-use in effect — the first login must change it, and an
existing admin is never overwritten. This is a deliberate, recorded deviation, not
an oversight.

**The audit log is append-only by policy.** `admin_audit_log` has insert and select
policies gated on `pigeon.instance_admin = 'on'` and no update or delete policy, so
the application role can record and read admin actions but not rewrite them. Rows
reference the actor with `NO ACTION`, so a record of who suspended what outlives the
account.

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

## S8 decisions — partitioning, retention and the sweeper lease

These are the places S8 (SB-1…SB-8, D-7) had to choose. They are recorded because
each one changes observable behaviour or closes a destructive failure mode.

**Partitioning is a swap migration, not an in-place `ALTER`.** PostgreSQL cannot
convert a plain table into a partitioned one. `0012_partitioning.sql` therefore
renames `deliveries`/`attempts` to `*_legacy`, creates partitioned replacements
with identical columns and daily range bounds on the UTC day of
`deliveries.created_at` / `attempts.occurred_at`, copies the rows into per-day
partitions, and drops the legacy tables — all inside `db.Migrate`'s single
transaction, so a failure leaves the plain tables untouched. Day boundaries are
UTC so a partition's contents do not depend on the server's `TimeZone`.

**A row's day is a prerequisite, not a column value.** A write, or an update that
moves a row to a day with no partition, fails with SQLSTATE 23514 (`no partition of
relation "deliveries" found for row`), so the writer has to create the day first.
Fan-out does that for the **event's** day before it inserts
(`internal/store/fanout.go`, `EnsurePartitionCached`) — which is what keeps an event
that waited in the outbox for a week fannable — and the sweeper creates days ahead of
today. Nothing in the product moves a row backwards in time (retention only ever
removes whole partitions), but a fixture or an operator backfill has to create its day
explicitly; two of S7's inspection-test fixtures that back-dated deliveries now do.

**The dedupe key now includes the partition key.** The first-fan-out arbiter that
0011 created — `UNIQUE (event_id, endpoint_id) WHERE replay_of_delivery_id IS NULL`
(A-4) — becomes `UNIQUE (event_id, endpoint_id, created_at) WHERE
replay_of_delivery_id IS NULL`: a partitioned table's unique index must contain the
partition key, and the predicate, which *is* the invariant, is kept.
`attempts` keeps per-partition uniqueness on `(delivery_id, attempt)`. This is a
behavioural change: FR-704's idempotency now holds *within a day*. It is safe
because fan-out stamps `deliveries.created_at` from the **event's** `created_at`
(not `now()`), so re-fanning the same event lands in the same partition and still
conflicts; a replay row is outside the partial index and may add a second delivery
for the pair (FR-1106). Fan-out's `ON CONFLICT` target
(`internal/store/fanout.go`) names the same key and predicate, so the arbiter
inference matches the index. A defaulted `now()` key would have defeated the
conflict check — this was verified against real Postgres before the migration was
written.

**The swap carries what S6 and S7 added, and it runs last on purpose.** The
partitioning is `0012_partitioning.sql`, after `0010_attempt_buffer.sql` (S6) and
`0011_s7_inspect_replay.sql` (S7): migrations apply in ascending order, so those two
see the plain tables and 0012 reconciles the objects the swap would otherwise lose.
`deliveries.replay_of_delivery_id` loses its self-referencing foreign key and becomes
a plain provenance column (a partitioned table's referenced key must contain the
partition key — the same reason `attempts` lost its FK, and dropping the original
partition still cannot take the replay with it). `deliveries_first_fanout_key` is
recreated with the partition key and A-4's predicate; `deliveries_event_idx`, the
`attempts` read policy and the `delivery_dedupe_all` view are recreated.
`pigeon_record_attempts` (S6's batched evidence insert) is restated with the
targetless `ON CONFLICT` — a partitioned parent cannot infer a per-partition unique
index — and creates the day's partition itself, because evidence is stamped `now()`
and a first attempt just after midnight must not fail on a partition the sweeper has
not reached. Renumbering rather than editing an applied migration is deliberate:
`db.Migrate` records a checksum per file, so editing 0010/0011 in place would break
every database that had already applied them.

**Every partition carries its parent's row level security.** PostgreSQL does not
propagate RLS to partitions: the flag and the policies belong to each relation
(measured on 16.2, `deliveries.relrowsecurity` true and `deliveries_YYYYMMDD`
false). A swap that turns one protected relation into a parent plus children would
therefore have handed the runtime role every tenant's rows to any query that names
the day — the parent filters, a leaf would not. `pigeon_ensure_partition` enables
RLS on every leaf it creates and copies the parent's policy onto it (the same policy
name; the `attempts` leaf's subquery names the leaf, since `attempts` is not in scope
there), because it is the single place a partition is created — the cutover loop,
fan-out, the sweeper and `pigeon_record_attempts` all go through it. It re-checks
both on an existing leaf, so a leaf that predates this file is repaired rather than
trusted. Reads through the parent, reads that name a leaf, and writes against either
are all filtered, and a cross-tenant write is refused with SQLSTATE 42501
(`TestPartitionLeavesEnforceTenantIsolation`).

**Retention windows are configuration, once.** The merged base carried a second,
day-granularity set of retention knobs (`PIGEON_RETENTION_DELIVERIES_DAYS`,
`…_ATTEMPTS_DAYS`, `…_EVENTS_DAYS`) with no consumer,
while `retention.Options` takes durations and S6's
`PIGEON_RETENTION_DELIVERIES/ATTEMPTS/EVENTS` parse them. The duplicates were removed
rather than kept: one window, one setting. `PIGEON_RETENTION_MAX_DISK_PCT`,
`PIGEON_DISK_DIR` and `PIGEON_DISK_BYTES` are S8's and unchanged.

**Retention removes whole partitions, never rows.** `DROP TABLE` on a leaf
partition is the only removal path (SB-2); there is no `DELETE` on either table in
the retention code. The drop primitives are `SECURITY DEFINER` functions because
the runtime role owns nothing and must not hold `CREATE` on the schema; they
validate the table against a closed set, the name against
`^<table>_[0-9]{8}$`, that the relation really is a leaf of that parent, and that
the day has fully elapsed. Grants on the partitioned parent are sufficient for the
runtime role in deployments (default privileges cover partitions created later);
the test harness's blanket grant does the same for existing partitions.

**Disk pressure is checked before age, and the oldest partition goes first.** When
the volume is at or above `PIGEON_RETENTION_MAX_DISK_PCT` (default 80), `RunOnce`
drops exactly one droppable partition per tick — the oldest across both tables —
and logs at warning level (SB-3). Age windows (`deliveries` 14 d, `attempts` 7 d)
are enforced only when the volume is under budget. Today's and future partitions
are never candidates. If the budget is exceeded and nothing is droppable, the
report sets `KeptOverBudget` and the ingest path sheds; the next tick retries.

**The gauge prefers the filesystem, falls back to database size.** `PIGEON_DISK_DIR`
(set when core and Postgres share a volume) is read with `statfs`; otherwise usage
is pigeon's own `pg_database_size` against `PIGEON_DISK_BYTES`. With neither, the
reading source is `none`, `GET /v1/meta/usage` answers 503, and the ingest gate
never sheds — a documented degraded mode rather than a fabricated 0%. A failed
read keeps the last good reading and surfaces `read_error` in the payload, so an
operator is never shown a calm figure that stopped updating.

**Over budget means shed ingest, not queue it.** While over budget the publish
path refuses with `429`, `Retry-After: 30`, error code `over_budget`, and increments
`pigeon_publishes_shed_total` (SB-7); nothing is written for a shed publish. The
usage payload and the drop/shed counters are admin-only infrastructure telemetry
(SB-6) — `/v1/meta/usage` and `/metrics` are `kindAdmin`, not tenant routes.

**One sweeper per deployment, and it is also the dropper.** Leadership is the
session-level advisory lock on a dedicated connection (`sweeperLockID`), taken with
`pg_try_advisory_lock`; the lock dies with the session, and low TCP keepalives
bound Postgres' detection of a dead holder to ~12 s, under D-7's 30 s ceiling. A
replica that does not win stays a **standby that keeps competing** (it does not
give up after a lock wait — AC-42 needs it alive to take over). The leader
re-verifies each tick, via `pg_locks` on its own locked connection, that it still
holds the lock and stands down if it does not, so a dead lease cannot keep
sweeping. Retention runs on the same lease, so exactly one replica drops
partitions. As defence in depth the SQL drop primitive takes its own
transaction-scoped advisory lock with `lock_timeout = 5s`, so no partition drop can
run concurrently with itself even across processes; a contended drop returns false
and is retried on the next tick.

**Events retention is reported, not enforced.** `events` is not partitioned yet, so
the configured `PIGEON_RETENTION_EVENTS` window appears in the usage payload
but no partitions are dropped for it. This is a known gap, not a silent promise.

**SB-6's dashboard half has no operator dashboard to put it in.** SB-6 asks for the disk
figure on "the dashboard and `GET /v1/meta/usage`". There is no operator dashboard in
the tree, so the pressure signal ships as the admin-only JSON endpoint plus
`pigeon_partition_drops_total` and `pigeon_publishes_shed_total` on `/metrics`, which an
external dashboard or alert scrapes. The producer portal's UI-8 panel is the one in-tree
place a human sees it: it reads `GET /v1/producer/usage` (S9a), the member-visible half
of the same reading - the pressure and the windows, never the partition catalogue, which
stays on the admin route - so a producer sees the platform's disk pressure without being
handed the operator's table listing. If an operator UI lands later, it reads the same
admin payload.

## Local toolchain

Go 1.23+ is required. On this build host it is installed at
`/opt/data/toolchain/go/bin` with `GOMODCACHE` under `/opt/data/toolchain`; on a
normal machine the stock `go` on `PATH` is all that is needed. Docker is not
required to build or test the core.

## Amendments

Decisions taken in place of gaps, each with the mechanism that makes it
implementable and the trade-off it accepts, because a decision without its mechanism
gets relitigated the moment somebody implements the other one.

### A-1 — Pause thresholds: depth per subscription, breadth per endpoint

**Decision.** Two levels stay, and AC-14's 50 is the stale number: 50 is a threshold
nowhere - it sits between the subscription depth (10) and the endpoint depth the earlier
draft of this amendment had (100) - which is the only reading that explains AC-14 as
written: it is the residue of a single-level draft.

A depth trigger of 100 failed attempts cannot do the job it was written for. A subscription
stops producing failures the moment it pauses (§7.12), so an endpoint accumulates at
most 10 failures per subscription: 100 is unreachable below ten subscriptions, and at
ten it fires only after every one has already tripped and sent its own notification,
which is the opposite of the mail storm it was supposed to prevent. The endpoint level
therefore accuses **breadth** instead: it trips when enough *distinct* subscriptions
are failing, each having accumulated 3 failed attempts inside the same sliding hour.
The 100-attempt depth trigger is deleted rather than kept beside it, and the reason is
not that breadth always fires first. Breadth trips at `3 × ceil(active/4)` attempts -
99 at 132 live subscriptions, 102 at 133, 150 at 200 - so above 132 subscriptions a
depth trigger would fire earlier. The reason is what each one measures at that size.
At 133 subscriptions, 100 failures is under 0.76 attempts per subscription: depth
would pause the endpoint on 10 failing subscriptions, a breadth of 7.5%, which is
exactly the small-endpoint over-reach the 3-attempt floor exists to prevent, while
breadth pauses at 34 of 133 (26%). Below 133 subscriptions breadth also fires first,
so depth only ever wins where it is the wrong rule; keeping it beside breadth would
mean implementing, testing and documenting a trigger whose sole distinct outcome is a
worse one.

**Mechanism.**

- A *failure* is one attempt that did not return `2xx`: transport error, timeout, or
  a non-2xx response, retriable or terminal. Successes never touch a counter.
  Counting attempts rather than terminal deliveries is the point - the pause exists
  to stop hammering a receiver, and at §9's default backoff a single dead delivery
  produces 7 failing attempts in its first hour, so a subscription with steady
  traffic pauses within minutes while a quiet one pauses only once it has genuinely
  accumulated 10.
- Two Redis sorted sets per failure, in one pipeline: `pause:sub:{endpoint_id}:{event_type}`,
  one member per failed attempt scored by attempt time (`ZADD`, `ZREMRANGEBYSCORE` to
  trim the hour, `ZCARD` to read the subscription depth), and
  `pause:subs:{endpoint_id}`, one member per *subscription* scored by its latest
  failure, trimmed to the hour, so `ZCARD` is the breadth count and a subscription
  that fails fifty times still costs one member.
- A subscription is in `pause:subs:{endpoint_id}` exactly while its own attempt window
  holds 3 or more failures. The set is a projection of those windows, not a separate
  state machine: a subscription is added when its window reaches 3, and removed when
  the hour trims it below 3 or when a resume clears the window. Writing it as a set
  maintained independently is how FR-1203's state predicate ("has accumulated 3 within
  the hour") and the set's membership drift apart. The 3-attempt floor is what keeps a
  single bad event - one terminal `4xx`, one timeout - from counting a subscription as
  failing. A member's window therefore holds 3 or more failures: 3 to 9 while the
  subscription is unpaused, and 10 or more once the tenth has paused it and the hour has
  not yet trimmed the window - which is why FR-1203's breadth count includes paused
  subscriptions, and why AC-44's second run trips only because it does.
- **Breadth threshold.** The endpoint pauses when `pause:subs:{endpoint_id}` reaches
  `N = min(active, max(3, ceil(active/4)))`, where `active` is the number of unpaused
  subscriptions on the endpoint (and `active ≥ 2`). Ten subscriptions give 3, two give
  2, fifty give 13: a quarter of the endpoint's live subscription surface, floored at
  3 so that a small endpoint has a reachable trigger at all and one broken event type
  cannot pause everything it touches. `active` is read from Postgres when the breadth
  set grows - at most once per newly failing subscription per hour, never per attempt.
- The **endpoint threshold is evaluated first**, and it is the only threshold whose
  notification is aggregated. When the endpoint trips, the endpoint pause is written,
  the endpoint sends exactly one FR-1204 notification naming the endpoint, the number
  of failing subscriptions and the first and last failing response, and **no further**
  per-subscription pause notification is emitted for that endpoint for the duration of
  the pause. A notification already sent before the endpoint tripped is not repeated
  and not recalled: suppressing mail that has been delivered is not something a pause
  can do, and this amendment does not claim it.
- The pause itself is a column, not a Redis key: `endpoints.pause_reason`/`paused_at`
  and `subscriptions.pause_reason`/`paused_at`, written in a `ControlTx`
  (`synchronous_commit=on`). Fan-out requires the endpoint *and* the subscription to
  be unpaused. The endpoint pause does **not** write per-subscription pause rows -
  they tripped for their own reason or they did not - so resuming an endpoint is one
  flag clear that restores every subscription which had not tripped its own threshold,
  and leaves those that did paused, with the email that said which.
- Resume clears the flag and that key's counter state in one action, and the branch
  matters. Resuming a *subscription* deletes its own window and its membership in
  `pause:subs:{endpoint_id}`. Resuming an *endpoint* deletes the breadth set **and the
  attempt window of every subscription that was in it**, so each former member
  re-qualifies only after accumulating 3 fresh failures; subscriptions that were not
  members are untouched. Clearing the set alone would leave those windows at or above
  the 3-attempt floor, FR-1203's predicate would still hold, and the endpoint would
  re-pause after one further failure per subscription - exactly the recurrence FR-1211
  says resume prevents. The cost is that an endpoint resume also forgives up to 9 depth
  failures per former member, which is what an operator pressing "resume" is asking
  for: a clean hour for the receivers that were being hammered. Without any delete the
  failures are still inside the hour and the next one re-pauses immediately, which
  would make AC-14's "resume works" false.
- Redis loss is safe in exactly one direction: the counters vanish, so a pause can be
  delayed by up to an hour but can never be invented, and the pause flags survive, so
  AC-10's `FLUSHALL` cannot silently unpause a dead endpoint and resume the hammering
  the pause stopped.

**Trade-off.** Three sorted-set operations per failure (two windows plus a breadth
membership upsert) and none per success, so the cost tracks the failure rate rather
than throughput. A tumbling `INCR`+`EXPIRE` window would cost one operation and would
also let a window boundary deliver twice the threshold inside a few seconds, which is
precisely what the threshold exists to prevent.

Breadth is a heuristic, and its cost is over-reach: on a two-subscription endpoint both
subscriptions failing three times pauses the whole endpoint, including a subscription
whose receiver was healthy but whose event type the producer broke. That is the price
of the endpoint level existing at all on a small endpoint, and it is bounded - above
twelve live subscriptions `N` is a quarter of them, up to twelve it is 3 or every
subscription if there are fewer than three - and resume is one action.

The alternative, a depth threshold, was not a stricter version of the same rule; it was
a rule that fires either too late to matter or on the wrong evidence. Up to 132
subscriptions breadth fires first (99 attempts at 132), so depth could only fire after
the pause it was supposed to cause; above 132 it can fire first, but then it pauses on
roughly 10 of 133 subscriptions failing ten times each, which is volume rather than
breadth. The previous draft of this amendment proved the trigger unreachable at the
sizes that mattered and kept it anyway.

The email count is what the mechanism yields, not what a marketing sentence would
prefer: in the ordinary total-outage case the endpoint trips after `N` subscriptions
have failed 3 times each - 9 attempts on a ten-subscription endpoint - so nobody had
tripped individually and exactly one email is sent. When traffic is skewed - one
subscription fails hard to 10 and pauses, then `N` others start failing - two emails
are sent, and that is right: the first one named a specific event type and was true
when it was sent. The bound is `N-1` subscription emails plus one endpoint email, and
only in the case where every one of them is genuinely dying.

**Encoded by.** FR-1202 (unchanged), FR-1203 (rewritten), FR-1209, FR-1210, FR-1211,
D-8, AC-14 (rewritten), AC-44.

### A-2 — NFR-4 was two claims wearing one number

**Decision.** Restate NFR-4 as three claims, each naming its machine and its
workload, and measure each where it is claimed:

| Claim | Machine | Workload | Pass |
|---|---|---|---|
| ingest write path | 2 vCPU / 4 GB (reference) | 1,000 events/s with the relay stopped, so nothing fans out | all `202`, p99 ≤ 100 ms, zero `429`/`5xx`, RSS ≤ 1.5 GB per service, commit rate ≤ 100/s |
| end-to-end rated load | 2 vCPU / 4 GB (reference) | 250 events/s × 10 subscriptions = 2,500 deliveries/s | NFR-1/NFR-2 hold, queue drains per NFR-6 |
| end-to-end rated load | 8 vCPU / 16 GB | 1,000 events/s × 10 subscriptions = 10,000 deliveries/s | NFR-1/NFR-2 hold (AC-16) |

The original sentence asserted the third row's workload on the first row's machine.
That is not an unmeasured claim, it is an impossible one: 10,000 delivery inserts
plus 10,000 completion updates per second is four times what §8.1 rates the
reference box for, and §8.1's own table says so a page later.

**Mechanism.** AC-45 is the new test of claim 1, and the load protocol now written
into §12 is what makes any of the three numbers mean something: 5 minutes of warm-up
excluded, 30 minutes measured, pass criteria evaluated over the last 10 minutes,
`attempt_log.mode=buffered`, accepted events reconciled against `events` and `outbox`
rows rather than against the generator's own counter, and `pg_stat_database`
sampled to prove FR-815's batching. Claim 1 stops the relay on purpose, because
delivery amplification is the only thing that separates it from claim 2; the
end-to-end claims follow in AC-31 and AC-16.

**Trade-off.** Claim 1 is a stronger statement about ingest than this tree has ever
demonstrated, and if S10 measures below 1,000 events/s the answer is either a defect
to fix or an amendment to §8.1 recorded as one - never a tolerance quietly added to
the test. Claim 2 is a smaller headline number than the old NFR-4 advertised (250
events/s, not 1,000), and it is the number an operator can actually size capacity
against, which the inflated one was not: §8.1.1 already says so, and an operator who
plans against 1,000 events/s on a 2 vCPU box discovers the truth at 3 a.m., which is
the failure mode §8 exists to prevent. Neither claim addresses *sustained* weeks-long
throughput, which §8.1.1 shows is a disk and retention question, not a CPU one.

### A-3 — The envelope carries the resolved `consumer_ref` and never `consumer_id`

**Decision.** Routing input and envelope output are decoupled. A producer may publish
with `consumer_id` or `consumer_ref` (FR-505), and every `consumer`-routed event
leaves the platform carrying the **resolved `consumer_ref`**. `consumer_id` does not
appear in the envelope at all.

**Mechanism.** `consumers.ref` is `not null` and unique per `(producer, env)`
(FR-506, `0001_init.sql`), so the case the gap feared - a producer publishes by
opaque id and the receiver is handed nothing - cannot arise: id to consumer row to
ref always resolves at fan-out, and the resolved ref is written into the envelope
once, from the same lookup FR-511 already caches. For `broadcast` events neither
field is present, as §10 already said. S3's struct is therefore
`ConsumerRef *string`, nil only for broadcast, with no `ConsumerID` field: a field
that is always absent, or always ignored, is worse than no field.

The resolution rule for the id form is FR-514, and it is deliberately the same rule
as the ref form: an unknown, deleted, or other-producer's `consumer_id` is
`unroutable:consumer_not_registered` in live (`202`) and `422` in test, exactly as
FR-508 treats an unknown ref. Two different outcomes for "the reference did not
resolve" would be a silent data-loss asymmetry, and answering a cross-tenant id probe
with a distinct response would hand out an id-enumeration oracle.

**Trade-off.** The receiver sees only an identifier it already understands: the
producer's own name for it, which a shared endpoint can route on without a lookup -
which is why §10 carries it. The cost is that `consumer_id` is the one identifier
that survives FR-506's re-key, so a consumer that must survive a re-key cannot key on
the ref; it keys on `id` per event, or on a field in `data` its producer controls -
the same guidance §10 already gives for replays. Emitting both would have handed
receivers an internal identifier whose stability we would then owe them, and a spec
gap is a poor reason to take on a compatibility promise.

**Encoded by.** FR-513, FR-514, §10's field table, AC-23 (extended to id mode).

### A-4 — Dedupe keys are a Postgres archive, not a Redis TTL and not a hot-path row

**Decision.** FR-903's 30-day dedupe answer is served from Postgres twice over: from
the `deliveries` row while its partition exists (0 to 14 days), and from an archive
table `delivery_dedupe` copied out of each partition as the sweeper drops it (14 to
30 days). Nothing about dedupe lives in Redis.

**Mechanism.**

- `delivery_dedupe(endpoint_id, event_id, first_delivery_id, first_seen_at)`,
  primary key `(endpoint_id, event_id)` and no surrogate id, so the support question
  "was this a duplicate for this endpoint?" is an index-only scan on an exact key.
- The archive is filled by the sweeper (single leaseholder, D-7), which copies the
  partition's `(endpoint_id, event_id, id, created_at)` into it **before** dropping
  that partition, `ON CONFLICT DO NOTHING`, **in the same transaction that drops it**,
  so the copy and the drop commit together or neither does: there is no instant at
  which neither the `deliveries` row nor the archive answers a key, and an age-driven
  drop therefore loses no answer at all. A failed copy aborts that transaction, and
  with it the drop. SB-3 disk pressure is the one thing that overrides
  this, in two ways, both of which lose support answers and neither of which loses a
  delivery: a `deliveries` partition dropped before its copy ran loses every key it
  held - and because SB-3 drops on pressure *before* age, such a partition can still be
  inside its first 14 days, so the loss is not only the 14 to 30 d portion; and a
  `delivery_dedupe` partition dropped ahead of its retention removes the archive's
  oldest days whole. Both are logged at warn naming the partition and the days it
  covered (FR-903, FR-905).
- `delivery_dedupe` is itself range-partitioned by day and dropped by partition at
  `dedupe_retention` (default 30 d), so SB-2 holds here too and there is no `DELETE`
  and no vacuum pressure. Under disk pressure these partitions are dropped *before*
  delivery partitions, because they are derived.
- One view, `delivery_dedupe_all`, unions live `deliveries` with the archive (a `UNION`,
  not a `UNION ALL`, so a key answerable from both sides counts once), so the
  support path - and the API that answers it - reads one name and does not care which
  side answered.
- Fan-out idempotency does **not** depend on the archive. It is the `deliveries`
  unique index, whose coverage is exactly as long as an outbox row can exist, which
  is the only window in which a re-fan-out is possible.

**Trade-off.** Two mechanisms and a union view instead of one insert, and the archive is
a copy taken at the drop rather than a live second copy - a day's pairs enter it when
its partition is dropped, not when they are written, so nothing answers from it until
then. In exchange the hot path pays nothing:
an inline row per delivery would add 10,000 control writes per second to the
reference load, doubling the write amplification §8.1 spends its whole argument
minimising - and that is what a Redis `SETNX` would have done to Redis, plus it would
have been lost on the wipe AC-10 exercises, breaking P3 for the one question FR-903
exists to answer.

The price of that atomicity is a wide lock and a longer sweeper step: one transaction
copies one day's partition - about 3 M pairs at the reference 35 deliveries/s, ~600 MB -
and drops it, so the `DROP` waits behind the insert and the transaction holds the archive
partition it writes for that whole span. The alternative is two transactions with the
copy committed first: the archive query then also has to be duplicate-insensitive, because
the window between the two commits answers a key from both sides at once (`delivery_dedupe_all`
is a `UNION`, so that is one keyword rather than a mechanism), and the window has to be
driven to zero by retrying the drop. Both orders satisfy FR-903 at every instant; atomic
is chosen because it is one thing to reason about and leaves no state in which the same
key has two answers, and the sweeper is a background leaseholder on a nightly slice, so
it can afford the longer span.

The honest cost is disk, and the first draft of this amendment understated it by
roughly three times by counting payload only. Every identifier in `0001_init.sql` is
`text` holding a 26-character ULID, so an archive row is a 24 B tuple header plus
three 27 B text values plus an 8 B `timestamptz`, aligned and line-pointed: about
124 B in the heap. The `(endpoint_id, event_id)` primary key adds a 62 B btree entry,
about 73 B once the default `fillfactor = 0.9` is counted and the 4 B item pointer is
added to the 69 B it yields (62 / 0.9), so a pair costs roughly 200 B all in. At the
reference box's sustained 35 deliveries/s the archive holds
48.4 M pairs - the 16 days between the two retentions - which is **about 10 GB**, a
quarter of SB-8's 40 GB disk rather than the 3 GB the payload count suggested.

Storing the ids as `uuid` would cut that by about a third (16 B keys instead of 26 B
text plus a varlena header) and is not available: the ids are ULID text at every layer,
so the archive cannot change their encoding without introducing a second representation
of the same key at the exact seam where a support answer is already hard to get. The
number is therefore managed rather than optimised away - retention is a knob, the
archive's partitions are dropped first under pressure (SB-3), and FR-905 requires the
size to be published from `pg_total_relation_size`, so an operator reads the real
figure instead of trusting this estimate. Every figure in this paragraph is an
assumption an operator can check with one query, which is the only kind of disk number
worth writing down.

**Adjacent, found here and decided with it: I-3's uniqueness is partial.** I-3 says
`(event_id, endpoint_id)` is unique "so fan-out and replay are idempotent", but
FR-1106 requires a replay to *create* a second delivery for that same pair. Both
cannot be a table constraint. The delivery index is therefore
`UNIQUE (event_id, endpoint_id) WHERE replay_of_delivery_id IS NULL` - first fan-out
only - and replay idempotency comes from its own request key (FR-1107). The archive
records the pair's first delivery and a replay does not touch it. Cost: one invariant
in two places, which is one more thing a reviewer holds in their head; the
alternatives were an index that makes FR-1106 unimplementable or dropping the index
and letting a retried fan-out batch create duplicates.

**Encoded by.** FR-903 (rewritten), FR-905, §6's two dedupe rows, I-3 (amended),
AC-46.

**As implemented (0014_dedupe_archive.sql, 0014 because 0012's checksum is applied).**
Four points where the code had to say something this section left open, so the next
reader does not have to rediscover them:

- **The primary key carries the partition key.** It is
  `(endpoint_id, event_id, first_seen_at)`, not the pair alone: PostgreSQL requires a
  partitioned table's unique key to contain the partition key, which is the same rule
  that made S8's swap key `deliveries` on `(event_id, endpoint_id, created_at)`. The
  pair is still the identity - the copy is `ON CONFLICT DO NOTHING` with no arbiter,
  and a pair that appears in two days (a replay the next day, FR-1106) is folded back
  into one answer by `delivery_dedupe_all`'s `min()` + `GROUP BY`.
- **The gauge sums the partitions.** `pigeon_dedupe_archive_bytes` (gauge, not a
  counter - it is a level, per OBS-9/OBS-11) is `pg_total_relation_size` summed over
  the archive's partitions, because a partitioned table has no storage of its own and
  `pg_total_relation_size('delivery_dedupe')` answers **0**. Measured on PostgreSQL
  16.2 before the fix read 22 MB as 0.
- **The knob is `PIGEON_RETENTION_DEDUPE`** (Go duration, default `720h` = 30 d,
  `retention.dedupe_retention`), alongside the three existing windows, and the
  archive's window is published as `retention_days.delivery_dedupe` in
  `GET /v1/meta/usage`.
- **The published figure holds.** AC-46's 100,000-pair probe measured **217 B per
  pair** (21,757,952 B over 100,000 pairs, heap + primary-key index, `endpoint_id`
  and `event_id` the 26-character text ids A-4 sizes for), inside the criterion's 50%
  band around ~200 B and inside A-4's 10 GB estimate at the reference load.

### A-5 — Consumer sessions are opaque Postgres-backed tokens in an `HttpOnly` cookie

**Decision.** A consumer user's session is an opaque 256-bit random token, stored
hashed in a `sessions` row that carries the `consumer_id` binding, delivered to the
browser as `__Host-pigeon_session` and presented to the API as a bearer. No JWT, no
client-held claim, no signed cookie.

**Mechanism.**

- Login mints the token and stores `sha256(token)` with `user_id`, `producer_id`,
  `env`, `consumer_id`, `expires_at` (absolute TTL, default 12 h) and
  `last_seen_at` (idle TTL default 30 min, written at most once a minute through the
  `EvidenceTx` class so it costs no fsync). There is no per-session CSRF secret: the
  browser-to-portal token is FR-1009's own (§14.2), minted by the portal from the
  secret it already holds, so nothing about it belongs on the core's session record.
  The cookie is `HttpOnly; Secure; SameSite=Lax; Path=/` per FR-1009 and carries no
  data beyond the token.
- The portal is a server-side API client: its handlers call the core over the network
  with `Authorization: Bearer <that token>` and `X-Request-Id` only. No CSRF token
  travels on that hop - it is not a hop a CSRF attack traverses, and the token the
  portal's own forms carry is never forwarded (FR-113). The browser never holds a
  credential it can replay at another host, and the API performs the filtering, so a
  compromised or buggy portal cannot widen a result set it was handed.
- The same token is a plain bearer, so AC-39's "a consumer user calls the API
  directly" is testable with the value from the cookie and without inventing a second
  credential type. A copyable long-lived consumer API token is not in v1.0; if it is
  ever wanted it is another row in the same table with the same binding columns and
  no schema change.
- Revocation is `UPDATE sessions SET revoked_at = now()`: immediate, one row,
  independent of the consumer record (FR-109). Each instance caches a binding for at
  most 5 s, so revocation takes effect inside 5 s, comfortably inside FR-205's 60 s.
- Producer-portal sessions reuse the same table with `consumer_id` null and a producer
  role (FR-112). §8.4's compose table is edited to match: its "signed cookie session"
  and "scoped token" cells now read `opaque session token (FR-112)`, and the two
  sentences in §14 that called the portal credential a "scoped token" now name the
  session bearer. Signed means stateless, stateless means a revocation list, and the
  revocation list is this table - so a signed cookie here would have been a second
  mechanism for the row that already exists. The producer portal's data paths - which
  `/v1/producer/*` and `/v1/session/*` route each panel calls, and with which
  credential (a member session bearer, an owner one, or none) - are listed in
  `REQUIREMENTS.md` §14.3.

**Trade-off.** One indexed read per authenticated request where a JWT needs none,
mitigated by the 5 s cache; a session table on the auth path where a JWT needs
nothing. In exchange: revocation is a row update rather than a deny-list with its own
expiry, there is no signing key to rotate or leak, no claim that disagrees with the
database after a role change, no clock-skew window, and no way for a stale token to
keep a revoked consumer user alive for the life of its `exp`. A JWT earns its place
when a stateless edge validates tokens at a scale where the lookup hurts; v1.0 has one
core binary, and its authorization question is a row lookup either way.

**Also corrected here:** SPEC previously cited "a scoped token per FR-514", and no
FR-514 existed - the scoped-token sentence was §14's prose only. The requirement is
now FR-112 and FR-113; FR-514 is assigned to A-3's id-resolution rule, and this
amendment no longer refers to it.

**Encoded by.** FR-110 (rewritten), FR-112, FR-113, AC-47.

### A-6 — `pigeon doctor` is a subcommand of the core binary with a scripted contract

**Decision.** `doctor` is a subcommand of `cmd/pigeon`: no second image, no cobra, no
shell script. It is read-only, prints one line per check under a stable id, offers
`--json`, and exits 0/1/2/3 by outcome.

**Mechanism.**

- `main` dispatches on `os.Args[1] == "doctor"` before `flag.Parse`, the way
  `-healthcheck` and `-version` already live in this binary. The runtime image is
  `scratch`: there is no shell, no `curl`, no `ulimit` binary, so anything an operator
  must run to diagnose the box has to be in the image that is already on the box.
- Checks, each with a stable id and a fix string: `config.valid` (the OPS-7 refusals,
  reported rather than thrown), `postgres.reachable`, `postgres.migrations_current`,
  `redis.reachable`, `redis.appendonly` (`appendonly yes` **and** `appendfsync
  everysec`, D-5), `clock.skew`, `disk.headroom`, `ulimit.nofile` (its own
  `RLIMIT_NOFILE` soft limit against the configured per-process cap plus pool,
  FR-816), and `db.connection_budget` (OPS-10's arithmetic, printed with the numbers
  substituted: `replicas × pool_max + reserved` against `SHOW max_connections`).
- `doctor` is the one path allowed to run with a broken config, because a broken
  config is the most common reason to run it. It parses what it can and reports
  `config.valid` as a failing check instead of exiting early with a single message.
- `ulimit.nofile` reads `getrlimit(RLIMIT_NOFILE)` in-process. There is no other
  option in a `scratch` image, and it is also the more correct one: the limit that
  matters is the limit of the process making the connections, not of the operator's
  shell.
- Format: `STATUS  check-id  observed  required  fix` on stdout, both machine fields
  always printed. `--json` emits
  `{"ok":bool,"checks":[{"id","status","observed","required","fix"}]}` with
  `status ∈ pass|warn|fail|skip`. Exit: `0` nothing failed, `1` at least one failure,
  `2` warnings only, `3` doctor could not run (unknown flag, unreadable env).
  `--strict` promotes a warning to exit 1 for CI. Every check is bounded at 5 s and
  the run at 20 s so it fits a healthcheck; no secret is ever printed (FR-1008); there
  is no `--fix` in v1.0.
- `clock.skew` measures against Postgres `now()` when the two are co-located and
  against `PIGEON_DOCTOR_CLOCK_REF_URL`'s HTTPS `Date` header when that is configured.
  With neither available it reports `skip` with the reason instead of passing, because
  a check that cannot measure must not claim to have.

**Trade-off.** Exit 2 for warnings-only is a Nagios-style reading some operators will
have to look up, and the alternative - anything nonzero is 1 - leaves a CI gate unable
to distinguish "disk at 81%" from "appendonly off", which is the distinction AC-22 is
about. Shipping doctor inside the core binary means diagnosing a binary that will not
start still needs images and logs; that is acceptable because doctor's scope (OPS-4,
OPS-10, AC-22) is the box, not the process. And there is no automatic repair, on
purpose: fixing `appendonly` means rewriting `redis.conf` and restarting Redis, which
is an operator decision about data the app does not own.

**Encoded by.** OPS-4 (amended), OPS-10 (amended), OPS-11, AC-48.

### A-7 — The attempt counter is control state on the delivery row, not the buffer's pending-entry count

**Decision.** The attempt counter is `deliveries.attempts`: control state, advanced by the
same write that schedules a retry, and an attempt's number is
`max(deliveries.attempts, the attempt rows already written) + 1`. The Redis Stream's
pending-entry list is the *buffer's* recovery accounting and is never the attempt counter.
D-2 said the opposite ("the attempt counter comes from the Redis Streams pending-entry
delivery count"), which was the design intent behind §19's `Queue: Redis Streams` row
rather than a measurement. S6 shipped the mechanism that works and documented the
deviation in `0010_attempt_buffer.sql`'s header instead of in this list, which is the
process defect PLAN.md rule 5 exists to catch; this amendment is that record, made after
the fact.

**Mechanism.**

- `0010_attempt_buffer.sql` adds `pigeon_requeue_delivery(p_delivery_id, p_backoff,
  p_attempt)`, which sets `lease_expires_at = now() + p_backoff` and
  `attempts = greatest(attempts, p_attempt)`. It runs in the same control transaction the
  retry already needed (`db.ControlTx`, `synchronous_commit=on`, FR-812), so the counter
  costs no extra round trip and no extra fsync.
- `greatest()`, never assignment, so a re-delivered requeue cannot walk the counter
  backwards; `0008`'s completion and terminal writes still set `attempts` exactly to the
  attempt that happened.
- `pigeon_claim_delivery` returns the counter as `row_attempts` beside the evidence count
  `prior_attempts`; `store.LeasedDelivery.NextAttemptNumber()` returns their maximum plus
  one for the worker's own claim, and `pigeon_next_attempt()` takes the same maximum for
  callers that hold only a delivery id (S7's inspect API).
- `sync` mode is the reason this cannot live in the stream: `attempt_log.mode=sync` writes
  every attempt row inline and has no stream at all. In `buffered` mode the stream's
  pending-entry count moves with the flusher's bookkeeping - every reclaim of a dead
  flusher's batch - rather than with delivery attempts.
- The maximum is what makes the two modes agree. In `sync` both counters advance together;
  on a database written before 0010 the evidence rows are the only counter there is.
- The sweeper (D-6) does not touch the counter: a lapsed lease is not a new attempt.

**Trade-off.** Two records of one fact - the counter on the delivery row and the rows in
`attempts` - legitimately disagree while evidence is buffered: the counter leads by up to
§6's 1 s flush window. The number handed out is their maximum, which can skip a number in
exactly one shape: a retry whose schedule write committed and whose evidence row was then
lost (a worker killed between the control commit and the buffer push, or an unclean Redis
crash). A skipped number is a gap in a trail; the alternative, reusing a number, silently
drops an evidence row against `attempts_delivery_attempt_key`'s unique
`(delivery_id, attempt)` - which is the loss this change exists to prevent. The counter is
also readable when Redis is not, which matters because a Redis outage sends attempts
inline.

**Encoded by.** D-2 (rewritten), §6's attempt-evidence row (now names the Redis Stream and
its consumer group, because "Redis list" cannot keep the row's own "0 on worker crash"
promise), FR-811, FR-902, AC-07, AC-28; `0010_attempt_buffer.sql`,
`store.LeasedDelivery.NextAttemptNumber`, `store.NextAttempt`.

### A-8 — LOG-2's bound is the gate's rate, sustained, with a one-second allowance

**Decision.** LOG-2's normative bound is `PIGEON_SUCCESS_LOG_RPS` (100 lines/s by default)
sustained per process, and at most twice that rate in any one-second window. §13.1 states
it and AC-33 asserts it. The gate keeps the shape it shipped with - a token bucket, full at
start, capacity equal to the rate - rather than being clamped to `rps` per window.

**Mechanism.** `internal/delivery/successlog.go`: the bucket starts holding one second's
allowance, refills at the configured rate on each call and never above it, and emits while
it holds a whole token. Emitted and suppressed successes increment separate counters, so the
volume the line would have carried is never lost. Below the rate every success still logs
(`TestSuccessLogGateIsAFullBucketOnStart`), which is the property a small deployment wants
and the reason the bucket starts full rather than empty. Measured by replaying the gate's
own arithmetic: a 10,000-deliveries-in-one-second burst emits **199** lines in its first
second at the default (the ceiling `2 × rps` is not reached, because the refill that would
complete the 200th token lands exactly at the window's edge), and a load that stays above
the rate emits at the rate from the second second on.
`TestSuccessLogGateHoldsTheBoundAtLoad` asserts `allowed <= 2*rps`; S6's
review measured the real worker path at `SuccessLogRPS=1` over 12 successes and got exactly
one line. `PIGEON_SUCCESS_LOG_RPS` is floored at one line per second by `config.Load` before
`Validate` runs, so `0` and a negative value both become 1 and no environment value turns the
gate off; the "log every success" path is a `delivery.New` caller passing a negative
`Options.SuccessLogRPS` (`internal/delivery/delivery.go:165`), which the environment cannot
reach. Failures and state transitions are never gated.

**Trade-off.** The alternative bound - "at most `rps` in any one-second window" - needs the
bucket to start near-empty or a per-window clamp, and either shape costs the visible first
successes after a quiet period: the liveness signal §13.1's own table row is about
("what was the process doing at 14:02?"). What it buys is one second of a burst dropping
from 199 lines to 100 at the default, in a window where the requirement's purpose - a
service not answering its own log I/O problem - is already met by two orders of magnitude,
and a per-window clamp puts new state on the hot success path to get it. Chose the bound the
code already holds, so LOG-2, §13.1 and AC-33 name the number the gate enforces. Two scopes
ship with it, both deliberate and both now stated in §13.1: the gate is **per process**, so
a deployment's ceiling is `replicas × PIGEON_SUCCESS_LOG_RPS` sustained (a cross-replica
bound would need Redis on the success path, the very I/O the gate exists to avoid, and
FR-207's rate limits are per process for the same reason), and the bound covers successes
only.

**Encoded by.** LOG-2 (rewritten), §13.1 (states the bound, the 199-line measurement and the
per-process scope), AC-33 (rewritten to point at that bound); `internal/delivery/successlog.go`,
`PIGEON_SUCCESS_LOG_RPS` in `internal/config`; S10's AC-33 load test.

### A-9 — The attempt buffer's acknowledgement is one transaction, and it outlives the pass that made it

**Decision.** `Buffer.Ack` is one atomic operation - the `XACK` and the entry's removal in a single
`MULTI`/`EXEC` - and `Flusher.insert` runs it on a deadline of its own that the flush pass's
cancellation cannot revoke. A start additionally sweeps the entries a peer acknowledged but never
removed that lie below the oldest pending entry when it sweeps, and keeps sweeping them on a
cadence while it runs. The batch still leaves the retry slot before the retirement is attempted,
`StaleAfter`'s reclaim stays the only recovery path for a batch whose insert failed, and OBS-9's
depth gauge stays `XLEN`.

**The two windows.** Both were found while root-causing `TestRunRetriesAStuckBatch` (`t_8ba5309b`,
fixed as a test defect in `t_07bc844a`), and both are production behaviour rather than test
behaviour: `Flusher.insert` acknowledged on the pass's own context - the one `Run` was given, which
`cmd/pigeon`'s shutdown cancels through `stopFlusher()` - after clearing the retry slot, and
`Buffer.Ack` issued `XACK` then `XDEL` as two commands on that context.

- **Half an acknowledgement, one way.** A cancellation landing before the `XACK` leaves the entries
  pending, and nothing in `Run` hands the batch back: `readBatch` finds the retry slot empty,
  `Reclaim` is filtered by `StaleAfter` (30 s by default), and `Read(">")` returns only new entries.
  Measured in an in-package copy at `f3af323`, with a sink that fails the first insert and cancels
  before returning from the retry's `RecordAttempts`: after `Run` returns, `depth = 1`, `pending =
  1`, `sink calls = 2`, `rows = 1`; the next start's `reclaim(0)` then recovers it (`reclaimed 1`
  row, the same delivery re-inserted, `depth = 0`). Nothing is lost - the row was committed before
  the ack - so this is a delay, not a defect in the insert path. It is not free either: the
  re-insert meets `pigeon_record_attempts`'s `on conflict (delivery_id, attempt) do nothing`,
  returns 0 inserted rows, and lands as an `AttemptsConflicted` bump and a warn line at the *next
  boot*.
- **Half an acknowledgement, the other way.** A cancellation, or a dropped connection, between the
  two commands leaves the entry out of the pending list and in the stream, where nothing can reach
  it: measured `Reclaim(min-idle 0) = 0`, `Read(">") = 0`, `Depth() = 2` for two acked-but-untrimmed
  entries, and the package has no `XTRIM` at all. That one is permanent. `XLEN` counts those entries
  for the life of the stream, so `AttemptBufferPending` - the gauge OBS-9's `queue_depth` is read
  from - over-counts the backlog forever, and NFR-6 ("queue depth MUST return to baseline within 10
  minutes") cannot hold again without an operator running `XTRIM` by hand.

**Mechanism.**

- `Buffer.Ack` issues `XAck` and `XDel` inside one `TxPipelined` (go-redis's `MULTI`/`EXEC`), in
  that order. The ordering rule is untouched - the row is committed before either command is sent -
  and the transaction makes the two halves indivisible in both directions: a context cancelled
  before the flush applies neither (measured: the entry stays pending *and* in the stream), and a
  rejected transaction leaves nothing behind. One round trip, one `EXEC`.
- `Flusher.insert` retires on `context.WithTimeout(context.WithoutCancel(ctx), AckTimeout)`, 2 s by
  default. By the time `Ack` runs the transaction has committed, so the pass's cancellation can only
  revoke the retirement; `Run`'s own D-3 drain already uses `context.WithoutCancel` on the same
  reasoning (`flusher.go:172` at `f4f20dc`), and the retirement was the last write on that path a
  SIGTERM could still cancel. `AckTimeout` is a `FlusherOptions` field and deliberately not a
  `PIGEON_` knob, the same status as `FailBackoff`: a Redis that has stopped answering must not hold
  a pass open, and the value is not an operator's to tune. The gauge read that follows the
  retirement uses the same bounded context, so a shutdown still records the depth it retired.
- `clientFromURL` enables `ContextTimeoutEnabled` on the buffer's client, because without it
  `AckTimeout` is not the bound it reads as. Measured against a listener that accepts and never
  answers: `XACK` with a 200 ms deadline returns in **3 s** with the options the package builds
  today (the client's `ReadTimeout`; context deadlines are off by default) and in **200 ms** with
  `ContextTimeoutEnabled`. The flag's wider effect is on the delivery path's `Push`, which from then
  on honours the request context it is handed: the command whose reply is lost mid-flight fails
  instead of holding a worker for the client's 3 s. `internal/delivery` already answers for that
  error class, by branch. A `Push` that fails on a live context falls back to writing the row inline
  (`internal/delivery`'s `recordAttempt`, the `default` arm that does not return -
  `delivery.go:423-430` at `15faf6c`), where the unique `(delivery_id, attempt)` key makes the
  duplicate a no-op. A `Push` that fails on a revoked context returns before the fallback on purpose
  (`delivery.go:415-421` at `15faf6c` - the attempt was aborted, so there is nothing to record), and
  an `XADD` applied before the revoke is an unread entry the next read flushes. So the delta the
  flag buys is the mid-flight reply, not the cancelled-before-send case: a context already cancelled
  at call time fails `Push` today as well, flag off and on (`buffer attempt 1/1: context canceled`,
  `XLEN 0`).
- `Buffer.SweepStranded` runs at `Run`'s startup, after the startup `reclaim(0)`, and again from
  `Run`'s loop at most once per `StaleAfter` of wall time, tracked from the last sweep (`sweepIfDue`
  in `internal/attemptbuf`).
  It compares `XLEN` with the group's pending count (`XINFO GROUPS`) and, only when the former is
  larger, issues `XTRIM <key> MINID <bound>`, where the bound is the smallest pending id
  (`XPENDING`'s summary `Lower`) if anything is pending, else the group's `LastDeliveredID`
  incremented by one. The bound is the entire safety argument, because `XTRIM` does not consult the
  pending list: redis.io's `XTRIM` reference has the default `KEEPREF` removing entries "regardless
  of whether they are referenced by any consumer groups" while preserving "existing references to
  these entries in all consumer groups' PEL", and the `ACKED` option that would spare a referenced
  entry exists only since Redis 8.2, while `compose.yaml` pins `redis:7-alpine`. Measured on the CI
  `redis:7` service container (7.4.12), against a stream holding one pending entry and one
  acked-but-untrimmed strand, a bound one id above the pending entry - the smallest off-by-one the
  sweep can make - removed it: `removed = 1`, `XLEN` 4 → 3, `XRANGE` at its id empty, and
  `Reclaim(min-idle 0)` then returned nothing to re-insert. A bound too high is therefore not a
  larger trim but silent evidence loss, and the id it takes stays in the PEL as a dangling reference
  (`XPENDING` counts an entry `XRANGE` can no longer return) until a reclaim scan clears the
  reference without recovering a record. Every entry below the bound has been delivered
  (`last-delivered-id` only moves forward, and only a read or a claim moves it) and is not pending,
  so it has already been acknowledged. Above the bound sit three populations: pending entries,
  entries nobody has read, and the case the sweep exists for - a strand an earlier retirement tore
  off above the bound. The first two are kept, and the third is reached by a sweep whose bound has
  nothing pending below it: either a sweep at which nothing is pending, or a later one in the same
  process, once the entries that held the bound down have themselves been retired. Repeating the
  sweep changes none of that, which is what makes the cadence safe: the bound is read from the
  group's own bookkeeping on the same live connection at each sweep, an entry still in flight is
  pending and so anchors the bound below itself, and every sweep keeps everything at or above the
  bound it computed. A strand is an anomaly - it means a retirement was torn - so what the sweep
  removes is logged at warn, with the count, and a sweep that removed anything also records the
  depth it left (`Flusher.depth`). That second half is not incidental: the trim moves `XLEN` with no
  insert behind it, so without it the gauge OBS-9 reads keeps the skewed depth until some later
  batch commits - on an idle process, forever.
- The three failure shapes are each closed: a graceful SIGTERM can no longer revoke a retirement
  (the bounded context); a hard kill or a dropped connection mid-retirement cannot leave half of one
  (the transaction); and a Redis-side tear that splits the transaction anyway - a SIGKILL between
  the two commands inside `EXEC`, or a crash whose AOF replay stops after the `XACK` - is repaired
  by the sweep, which is also when the gauge it skewed is corrected - the sweep records the depth it
  left behind, because a trim moves `XLEN` with no insert behind it and the gauge would otherwise
  keep the skew until the next batch. The cadence is what makes that
  repair unconditional: the bound is wherever the pending list puts it at each sweep, so a strand a
  start's own sweep had to leave - because an older entry was still pending and held the bound below
  it - is retired by a later sweep in the same process, once the retry has retired that entry.
  `cmd/pigeon`'s shutdown grace can still cut the drain short, which is the same class and the same
  answer.

**Trade-off.** The atomic retire was chosen over a compensation path - a Redis-side list of ids
awaiting removal, or a scan that deletes entries that are neither pending nor newer than the group's
last delivery. Both add a second piece of state, or a destructive command whose inputs are inferred;
a transaction is what `MULTI`/`EXEC` exists for, and it closes both halves of the window with one
round trip rather than two. It does not close a tear inside the transaction itself, which is why the
sweep exists rather than being folded into the retire: the sweep derives its bound from the group's
own bookkeeping, so it needs no record of what a dead process was doing. Leaving the retirement on
the pass's context was the other candidate, with `StaleAfter`'s reclaim as the repair at the next
start: measured to work, and still costs up to 30 s in which the depth gauge is wrong, plus a
duplicate insert, a conflict bump and a warn line at the next boot for a batch whose rows are
already durable.

Two alternatives are rejected outright. Retaining the batch in `f.stuck` after a failed
acknowledgement would let one unackable batch stall the whole evidence path: `readBatch` returns the
retry slot first, so nothing new is read while Redis is unhappy, every retry re-inserts a committed
batch and bumps the conflict counter, and the stream grows behind it. `XTRIM`/`MAXLEN` on `Push` as
a bound on the stream is worse: trimming does not consult the pending list, so it drops entries
nobody has read - the evidence loss that `XADD` → read → commit exists to prevent. What the sweep
accepts is one cadence of latency, for two strand shapes. A strand created after this process swept
is seen by the next sweep in the same process - at most `StaleAfter` later - and logged at warn
there, rather than never until the next start. That sweep retires it when nothing older is pending;
when something older is pending it holds the bound down, so the strand waits for that entry's own
retirement - up to `StaleAfter` of it when the entry is a peer's untouched batch that only the stale
reclaim takes over - plus the sweep that follows: the two-interval worst case D-9 scopes. A strand
already in the stream, newer than the oldest
pending entry at that sweep - the shape a single sweep cannot reach at all - needs a sweep whose
bound has moved up: the startup reclaim's insert or its retirement can fail (a Redis or Postgres
hiccup, an `AckTimeout` expiry), which leaves the entry it holds pending while that start's sweep
runs, and the retry retires it a moment after the sweep has gone. Measured against a one-shot sweep
(throwaway probe, miniredis, `fbebd60`): in that state the live process reads `XLEN = 1` with
`pending = 0`, `Reclaim(min-idle 0)` and `Read(">")` both return 0 entries, and a second sweep -
which that `Run` never issued - retires it (`removed = 1`). With the cadence that second sweep is
the loop's, so the strand costs one pending-batch interval of an inflated `AttemptBufferPending` in
this state - the retry retires the entry holding the bound down within `FailBackoff`, and the sweep
that follows is at most one `StaleAfter` away - instead of the process's life, which is what NFR-6's
ten-minute return to baseline needs. It is two intervals in the worst case, when the bound-holder is
a peer's untouched batch and only the stale reclaim takes it over; either way the retire is logged
rather than silent.
`internal/attemptbuf`'s `TestStrandAboveTheBoundIsRetiredByALaterSweep` pins it end to end through
`Run`: while the run is live and after it settles, `Depth() == 0` with an empty pending set and
`AttemptBufferPending` back at 0 - read at each `XTRIM` the run issues, so the trim that retired the
strand is what returned the gauge to baseline rather than a later insert - and every record inserted
exactly once.

What the cadence does not do is reach a strand directly, and that is deliberate: the bound stays
what keeps the trim off an entry nobody has acknowledged, so an entry above it is retired only once
the bookkeeping puts it below. The alternative - `XDEL` by id, or a scan for ids that are below
`last-delivered-id` and absent from the PEL - is the inferred-destructive-command mechanism this
trade-off rejects, and it would trade a bounded gauge skew for the risk of deleting evidence on a
guess.

**Encoded by.** D-9 (added), §6's attempt-evidence row (the acknowledgement after the commit is one
transaction and is not revocable by the cancellation that ends a pass), §19's decision row;
`internal/attemptbuf`'s `Buffer.Ack`, `Buffer.SweepStranded`, `Flusher.insert`/`Run` (including
`Run`'s `sweep`/`sweepIfDue` cadence), `FlusherOptions.AckTimeout`, `clientFromURL`;
`TestStrandAboveTheBoundIsRetiredByALaterSweep`, `TestCadencedSweepIsNotDestructiveInSteadyState`.
### A-10 — The retry budget is the worker's attempt budget, not a second endpoint limit

**Decision.** FR-1303's "separate budget" is the platform's attempt-execution capacity, not the
receiver's: a delivery worker MUST partition its attempt budget into a **first-attempt lane**
(`deliveries.attempts = 0`) and a **retry lane** (`attempts > 0`) with independent allowances, and
every attempt class MUST keep sharing the endpoint's concurrency slots and RPS bucket (FR-1302,
FR-1306, FR-1308, unchanged). FR-1308 is a promise to the receiver, and a second slot set for
retries would break it exactly when the receiver is least able to absorb it. FR-1303 is a promise
to the publisher: the ability to make a first attempt MUST NOT be consumed by the platform's own
retry work.

**The gap.** §7.13's fairness half had three rows with no slice, no acceptance criterion and no
implementation - FR-1301, FR-1303, FR-1304 - and FR-1303's own text could not be made to agree with
FR-1308: read as an endpoint limit it gives retries a second slot set, and the receiver sees twice
the concurrency it was promised; read as a producer quota it needs FR-1301, which does not exist.
Neither is the mechanism. The budget that separates is the worker's.

**Mechanism.**

- **Class.** `deliveries.attempts = 0` is a first attempt and `> 0` is a retry; a replay creates a
  new delivery row (FR-1106), so a replayed delivery's first attempt is a first attempt on the
  first-attempt lane, and FR-1308 keeps it on the endpoint's shared limits.
- **Claim side.** `pigeon_claim_delivery` gains an attempt-class predicate, so a lane selects only
  its own class. Today it cannot: it selects `order by d.id limit 1` over every pending delivery
  (`0013_s5_retry_pause_limits.sql:308`), so the oldest row wins and a retry backlog is claimed
  ahead of every newer first attempt.
- **Lane side.** The two allowances are a **partition** of the process's existing attempt budget,
  not an addition to it: the process cap `C` is
  `PIGEON_DELIVERY_CONCURRENCY` (FR-816's per-process cap, default 64), the retry lane's
  configurable allowance `R` is `PIGEON_DELIVERY_RETRY_CONCURRENCY` (default 16), and the lanes hold
  `R` and `C - R` slots respectively (48 first-attempt slots at those defaults). Attempts in flight
  for one process therefore still total at most `C`, the ceiling FR-816 already shipped; the
  trade-off below is the price of the split, not a raised budget.
  `R = clamp(configured, 0, C - 1)`, with the knob validated `>= 1` at config load
  (`internal/config/config.go:442-443`), so `C - R >= 1` holds at every accepted `C` and `R >= 1` at
  every `C >= 2`; a configured value above `C - 1` is clamped rather than refused, because the
  result still leaves the first-attempt lane one slot.
  `R = 0` is a defined state, not an error, and the clamp reaches it only at `C = 1`: it means **no
  separate retry lane** - one lane serves both classes and the isolation this bullet opens with is
  not claimed for that process - rather than a lane that can never run, because a degenerate
  partition MUST NOT starve FR-1201's retry policy. A *configured* `0` never reaches it: a retry
  allowance of zero reads as "stop retrying", which is the reading FR-1201 forbids, so the value is
  refused at startup instead of silently collapsing the two lanes.
  Both allowances are per process, the
  scope FR-816's cap and FR-207's rate limits already have (§13.1): the property is that this
  worker can always start a first attempt, and a cross-replica budget would need Redis on the
  dispatch path.
- **A hold is not an attempt.** When the endpoint cannot admit a request (no free slot, empty
  bucket), the worker MUST NOT sleep holding the claim and the lane slot. `limit.Acquire` and
  `limit.Allow` already return the wait; the worker reschedules the delivery at `now + wait`,
  consumes no attempt, leaves `retry_deadline` untouched, and records the hold.
- **Why the hold is load-bearing, and what it replaces.** The pre-S11 `dispatch` claimed a delivery
  and took the process slot *before* `acquireEndpointLimits` (`internal/delivery/delivery.go` at
  `a9b3a67:439-486`), then slept inside that path (the two wait loops at `:502-514` and
  `:521-534`). With `PIGEON_DELIVERY_WORKERS=1`, the shipped default
  (`internal/config/config.go:171`), one capped endpoint therefore occupied the process's only
  dispatch loop. An endpoint at its default cap of 10 with 5 s hangs drained at 2 attempts/s, so a
  due backlog of `D` rows parked that process for `D/2` seconds - 1,000 rows for about 8 minutes -
  while every other endpoint and producer waited, and nothing said why. That is FR-1304's "silent
  latency", and it is also the precondition of AC-16's NFR-2 leg: S10 measures that leg, and it
  cannot pass while the dispatch path parks.
- **Visibility.** The hold is control state, not evidence: `held_back_since` and `held_back_reason`
  on the delivery row, returned by the delivery view (FR-1102, FR-1103), plus
  `endpoint_held_back_total` and `endpoint_held_back_seconds` (OBS-13). The producer portal renders
  the field because it is a thin API client (M2a, `t_c0144baa`).

**The lanes are served by a rotation, and the number of loops is not what A-10 fixes.** A-10 first
read "the worker runs one claim loop per lane"; the shipped shape is one claim loop per worker -
`PIGEON_DELIVERY_WORKERS` (default 1), the count FR-816 already shipped - rotating over the lanes.
Nothing A-10 fixes distinguishes the two shapes: the rotation advances its cursor on every dispatch
and a lane at its allowance or with nothing due hands the pass to the next one, so neither lane
starves the other; each claim carries the lane's own class, so a retry backlog cannot enter the
first-attempt allowance; and the allowances are semaphores on the process rather than on a loop, so
the number of claim loops does not multiply them (`R` and `C - R` whatever the loop count). Claim
refill is the one dimension in which they differ: with `PIGEON_DELIVERY_WORKERS = 1`, a claim loop
per lane would claim up to two deliveries per pass where the rotation claims one. That dimension is
untested - AC-52 and AC-53 bound *attempts* in flight by the lane allowances, not claims in flight,
and the literal shape would not move those bounds - so a claim loop per lane would widen
`PIGEON_DELIVERY_WORKERS` from claim loops to lanes for nothing those criteria observe. The wording
this replaces also survives in `internal/db/migrations/0016_s11_delivery_lanes_and_holds.sql:20-21`,
whose header still says "The worker runs one claim loop per lane against this predicate"; that file
is checksum-frozen - `db.Migrate` refuses a migration whose content changed after it ran
(`internal/db/migrate.go:128-129`), the same checksum rule that had 0010/0011 renumbered rather than
edited in place above - so the header is not edited and this paragraph is the record that supersedes
it.

**Composition with FR-1308, stated once.** An attempt passes two independent admissions: its lane's
allowance (FR-1303, platform capacity, per process) and the endpoint's shared limits (FR-1302,
FR-1306, FR-1308, receiver capacity, cluster-wide). Neither implies the other: the lane decides who
may start, the endpoint limit decides how many reach the receiver. A retry storm saturates the retry
lane and the endpoint's shared slots and cannot reach the first-attempt lane; a replay burst
(FR-1107's 50/s per endpoint) is admitted as first attempts and bounded by the same endpoint limits
it always was. FR-1308 is neither amended nor weakened here.

**One overloaded phrase, disambiguated.** FR-406 and FR-503 already say that a paused endpoint
"holds in-flight ones without consuming retry budget" and that an event with no matching
subscription "consume[s] no retry budget". There, *retry budget* is the **per-delivery** allowance -
§9's 15 retries inside the 72 h window - and those rows mean the delivery advances no attempt
counter toward FR-1201's bound. Nothing here changes them, and the two readings agree on every case
those rows name, because a delivery that is held or paused is not an attempt either way. The phrase
is now doing two jobs, so this paragraph is where a reader is sent, rather than FR-406 and FR-503
being rewritten for a distinction their own cases do not turn on.

**FR-1301's split.** Its publish half is met and shipped: FR-207's per-key and per-producer rate
limits (`internal/ratelimit`, `429` with `Retry-After`, per process). Its delivery-quota half is
deferred to v1.1 (§17.1) with the cost stated - a producer with many endpoints can occupy the shared
lanes up to the sum of its endpoints' caps - because the enforcement is a producer-keyed lease
beside `internal/limit` (the claim row already carries `producer_id`), while the *configurable*
value is a config surface and an API that v1.0 does not otherwise need. §7.13's row names both
halves so it stops reading as one unowned claim.

**Trade-off.** A retry lane caps how fast a failing endpoint's backlog drains, and every slot given
to the retry lane is a slot the first-attempt lane does not have - the per-process budget itself
stays `C` (FR-816), and the split only decides who may use it - so a deployment that wants one
endpoint's 20,000-row backlog drained as fast as its cap allows must now raise a knob and pay for it
in first-attempt capacity. A hold delays a delivery without extending its window, so an endpoint
saturated for hours can push deliveries past their 72 h deadline that a pause would have extended
(FR-1207). That is deliberate - the window is the consumer's promise about staleness, and a cap is a
rate that drains - but it is a real difference and it is stated rather than incidental.

**Rejected.** A second endpoint slot set or bucket for retries (breaks FR-1302 and FR-1308, and
doubles the burst a capped receiver sees); a per-producer delivery quota as FR-1303's mechanism (a
different axis, deferred as FR-1301 rather than smuggled in here); lowering
`PIGEON_DELIVERY_CONCURRENCY` (throttles first attempts, which is the property being protected);
leaving FR-1303 as prose over one shared pool (what shipped, and the state in which AC-16 cannot
pass).

**Encoded by.** FR-1303 (rewritten), FR-1304 (rewritten with the hold and its surface), FR-1301
(halves named), §7.13's mechanism note, AC-52 and AC-53, OBS-13, §17's M3 exit, §17.1's deferral
row, §19's decision row; `internal/store`'s claim, `internal/delivery`'s dispatch and limiter wait,
`PIGEON_DELIVERY_RETRY_CONCURRENCY` in `internal/config`; slice S11, card `t_558a9f59`.

### A-11 — FR-818 is a bounded write budget, not a batched publish

**Found** by S10's load test (card `t_b5272d2d`, `docs/load-test.md`, artifact `run3-250x10.json`): FR-818 was a P0 with no owning slice in `PLAN.md` and no implementation in the tree. `handlePublish` (`internal/httpapi/events.go`) calls `store.PublishEvent` synchronously, one transaction per request, and takes its pool connection with no deadline - `pgxpool.Acquire` waits until one frees. There is therefore nothing that can be "full" and nothing that can shed. Measured on the 2 vCPU reference box: 250 events/s offered for 30 minutes produced **zero** `429`s on the publish path, p99 grew 3,593.4 → 4,281.6 ms across the graded window, a single publish took **10.2 s** with the shipped `PIGEON_DB_MAX_CONNS=10` at 40 concurrent publishes and **86 ms** at a pool of 40, and the only publish-path `429` that reports a capacity limit, apart from FR-207's `rate_limited` (`internal/httpapi/auth.go`), is SB-7's disk gate (`over_budget`). The worker write paths are unbatched the same way: every delivery completion is its own `ControlTx`, and the relay leases one outbox row per iteration.

**Decision.** FR-818 is *owned*, not descoped, and its text is narrowed to the mechanism that is implementable and testable:

- **one bounded write budget per process**, shared by every control-state writer, with a fixed worker pool (FR-816). A slot is held for one database write - one batch transaction, or one publish transaction - and released on completion.
- **`Submit` blocks** for the relay and delivery workers, which batch what §8.1's table binds on (fan-out delivery rows, delivery completion and terminal updates, outbox marks) into one transaction per batch (FR-815).
- **`Admit` waits at most `PIGEON_INGEST_ADMIT_TIMEOUT` (default 100 ms, the rated publish p99) for ingest**, which then sheds `429` with `code=over_capacity`, `Retry-After` and the shed counter. Saturation becomes visible as shedding instead of as latency.
- **the publish transaction stays one transaction per publish.** FR-604 makes the `202` the commit; FR-815's 2,000-row floor on that path is a 4 s window at the reference rate (250 events/s) against NFR-1's 100 ms, and batching it further needs a savepoint per job to preserve FR-303's failing pointer, FR-508's test-mode refusal and FR-603's idempotency - for two rows per event against the fan-out's ten.
- **`PIGEON_DB_MAX_CONNS` moves 10 → 40** (the measured-good value; `1 × 40 + 5 = 45 ≤ 100` at one replica and 85 at two), because AC-45 requires **zero** `429` at the reference class's rated ingest load: a shipped default that sheds at the rated load is a defect, not a knob an operator is expected to find.
- **evidence writes are untouched**: D-1's Redis buffer and the flusher already bound and batch them (FR-815).
- **FR-703's transaction boundary is the batch, not the event** (the follow-up card `t_194e6caf`, raised by the review of `t_b9850e1b` once the batched drain had landed): the per-event entry points (`FanOutEvent`, `FanOutEventAndMark`) are unchanged, and the relay's batch path writes every delivery row of every event in a pack in the one transaction the budget opened for it. What FR-703 keeps is everything it exists for - one delivery per match, before any attempt, and no event's fan-out set split across transactions, because `PackFanOut` never splits a plan and the outbox mark that claims an event's deliveries commits with them. §8.1's L1 lever already says transaction boundaries are an implementation detail, not a contract; `internal/store/fanout.go`'s header no longer states the per-event boundary as fan-out's general property, and the frozen `0007_routing.sql` header keeps the pre-amendment wording (migration checksums leave an applied migration unedited), so this record supersedes it.

**Trade-off, stated.** The amendment gives up "every database write is batched through one queue" as a literal reading of FR-818. What it keeps is every clause the requirement exists for and AC-30 grades: publishes shed `429` past the ceiling, latency is bounded rather than silently growing, goroutines and buffers are bounded by configuration, and an accepted event is never dropped. It also reads FR-815's floor where it binds - §8.1's aggregate insert rate, not a per-request-path one - and names the ingest path as the place that floor cannot hold.

**Rejected.** Batching the publish transaction through the same queue (FR-818 read literally): rejected for the latency and error-semantics reasons above, and because the two rows per event it would amortise are an order of magnitude less than the fan-out rows the batch path already reaches. Descoping FR-818 with the AC-30 consequence recorded: rejected because AC-30's first two clauses have no other mechanism, and §12 forbids a tolerance in their place. Raising `PIGEON_DB_MAX_CONNS` alone: rejected because a bigger pool moves the parking further out rather than shedding, and leaves the fan-out and completion writes single-row.

**AC-30's third clause.** "The ceiling matches the §8.1 table within 20%" is implemented nowhere: `cmd/loadgen compare` is AC-43's replica-scaling comparison, there is no `-class` flag and no §8.1 table in the harness, and `docs/load-test.md` records the gap rather than implying otherwise. A-11 records it as owned by card `t_0bf3effa` - the class comparison in the harness plus the graded run on a host that carries a rating - gated on S12 (`t_c6afa452`) and `t_b9850e1b`, because a relay that drains at ~1.4 events/s and a write path with no ceiling leave nothing to compare. **Discharged for the relay leg** by card `t_b9850e1b`: the drain is a batch drain now (a batch lease, then the whole fan-out in one write-budget transaction per pack that fills FR-815's row bound), measured at 496.7 events/s / 4,966.8 deliveries/s against 41.5 / 414.9 before it, on the same probe and the same 2 vCPU box - so the graded run no longer waits on a relay that cannot drain. The number and its measurement caveats are in `docs/load-test.md`. **Discharged for the class comparison** by card `t_0bf3effa`: the harness has the comparison now - `loadgen grade` re-decides the clause from the artifact against a §8.1 table pinned to `REQUIREMENTS.md` §8.1 by a test, and `run -class` records the claim beside the machine it is claimed for. That machine is the **machine under test**, recorded as `machine_under_test` and not the machine block: §12 puts the generator outside the machine under test or in its own container, and `cmd/loadgen` *is* the generator, so on every gradeable run the block holds the generator's box - the machine under test is the observed machine when the generator shares it and `-sut-cpus`/`-sut-mem-gib` (the operator's statement about a box the generator cannot see) when it does not. A claim that machine does not carry, and an artifact that records no machine under test at all, are refused before the run and at the verdict rather than measured and explained afterwards, which is what makes a graded number on an unrated box unreachable; the class's evidence (`observed` / `attested`) is derived from how the run started, so an artifact cannot claim observation while recording that the generator ran elsewhere. The graded run on a host that carries the class remains the operator's, and no clause number is claimed from the harness alone.

**Encoded by.** FR-818 (rewritten), §19's decision row; `internal/config` (`PIGEON_DB_MAX_CONNS`, `PIGEON_WRITE_QUEUE_DEPTH`, `PIGEON_WRITE_QUEUE_WORKERS`, `PIGEON_INGEST_ADMIT_TIMEOUT`), `internal/httpapi`'s publish admission, `internal/metrics`'s shed counter, `.env.example`, `docs/topology.md`; `cmd/loadgen`'s `run -class`/`-sut-cpus`/`-sut-mem-gib` and `grade` (AC-30's third clause); slice S12, cards `t_c6afa452` (mechanism) and `t_0bf3effa` (the §8.1 class comparison in `cmd/loadgen`; its graded run is the operator's, on a host that carries the class).

### A-12 — The ownership challenge is re-issuable, and "shown once" survives it

**Found** by Jay while reviewing the getting-started flow (card `t_30fac425`): "after
registering the consumer endpoint, there is a verification step, how does the consumer /
the onboarding team get this verification token?" The answer is that they get it exactly
once, in the creation response (`id.New("vrf")` at `internal/store/endpoints.go:155` and
`internal/store/consumerportal.go:133`; `:169` stores only the SHA-256 and `token[:8]`, and
no read path carries either), and that nothing in the store, the API or the two portals can
mint another. An endpoint whose token is lost between registration and the answer can never
be verified and never receives a live event: at fan-out it becomes a `skipped` row with
reason `not_delivered:endpoint_unverified` (`internal/store/fanout.go:199-200`, `:293-320`),
and the only recovery in the tree is a replacement endpoint, which orphans the first one's
subscriptions and history. Two measurements sharpen the gap. The 8-character
`verification_hint` is readable nowhere after creation (`endpointCreatedJSON` is its only
carrier and no portal template renders it), so the loss is total rather than partial. And the
events an unverified endpoint skipped are `skipped` delivery rows, which FR-1106 deliberately
refuses to replay (`internal/store/inspect.go:697`, `selectionWhere` excludes `skipped`), so
the shipped recovery costs the endpoint, its subscriptions, its history, a receiver-side
signing-secret change **and** every event published while it was unverified.

**Decision.** The challenge is re-issuable until it is answered, at parity with FR-404's
rotation of the signing secret:

- **who may mint**: exactly the actors who may answer the challenge - a member of the owning
  producer (`POST /v1/producer/consumers/{consumerID}/endpoints/{endpointID}/challenge`) and
  a user bound to the owning consumer (`POST /v1/consumer/endpoints/{endpointID}/challenge`),
  each answering `201` with the creation response's own field names (`verification_token`,
  `verification_hint`), because each call mints a new value and is therefore not idempotent.
  The store predicate carries `producer_id`, `env`, `consumer_id` and `status <> 'deleted'`,
  so a foreign, unknown or deleted id is `404` and never `403` (AC-39), exactly as `/rotate`
  and `/verify` answer.
- **the instance admin may not**: no admin route and no admin store path. An operator-minted
  challenge would be a credential the operator holds and can answer, which is the one thing
  "shown once, never read back" exists to prevent.
- **one write replaces both columns**: `verification_token_hash` and `verification_hint` move
  in the same statement and `verified_at` is untouched, so the previous token stops verifying
  when that write commits. There is **no overlap window**, unlike FR-1004's 24 h secret
  overlap: the overlap exists because a receiver holds the outgoing secret and verifies
  signatures with it, and no counterparty holds a challenge token - only the registrant, who
  is the one asking for a new one.
- **an already verified endpoint is refused** (`409`, no state change). Verified is one-way in
  v1.0: FR-403 has no de-verification clause. Clearing `verified_at` would add an un-verify
  lever no requirement asks for, and minting a token that cannot change anything would be a
  dead credential that a support conversation would read as meaningful. Pause and delete
  remain the ways to take an endpoint out of service.
- **no schema change and no new credential type**: both columns exist
  (`0007_routing.sql:45-46`), the token is minted by the helper creation already uses
  (`id.New("vrf")`, `vrf_` plus 24 hex), and the answer path is untouched: constant-time
  compare against the stored digest plus FR-1005's URL re-check.
- **log-only record**: the action is logged with actor and request id (LOG-1…LOG-4) like every
  other mutation. It writes no `admin_audit_log` row, which is MT-5's instance-admin log rather
  than a tenant table, and per-tenant audit history stays deferred to v1.1 (§17.1, Q5). When it
  lands, re-issue joins it as rotation will.
- **it arms an endpoint, not its events**: a re-issue re-fans-out nothing and touches no
  delivery row. Events published during the unverified window stay `skipped` and
  un-replayable, so they are recovered by re-publishing. Stated so "recoverable" is not read
  as "nothing was lost".

**Trade-off, stated.** A challenge token can now be replaced by exactly the people who could
already arm the endpoint another way: they may register a replacement endpoint and verify it
themselves (`internal/httpapi/routes.go:150` for a member, `:186` for a consumer user), rotate
its secret, change its URL, pause it and delete it - and a re-issue preserves the endpoint id,
its subscriptions and its history where the replacement does not. What the ruling gives up is
the claim that the token the registrant first held is the only token that ever verifies the
endpoint: a claim FR-403 never made and nothing observes. What it keeps is the property that
matters - the challenge is still never read back, and it is still answered by its own
registrant rather than by an operator. The cost that remains is the event window: a re-issue
does not bring back what was published while the endpoint was unverified.

**Rejected.** Ruling the loss final, on the ground that "shown once" is a security property: it
protects no capability, it contradicts the shape of every other one-time credential in this
tree (an API key is revoked and re-issued, FR-201; a signing secret is rotated, FR-404; an
invitation is revoked and re-sent, FR-104/FR-105), and it would make the README teach a closed
tab as an endpoint loss. An instance-admin re-issue action: excluded for the reason above.
Restricting re-issue to producer owners: registration, verification and rotation are all
`kindMember`, so an owner-only recovery would strand the onboarding case this ruling exists for.
A re-issue that clears `verified_at`: an un-verify lever with no requirement behind it. A
re-issue that leaves it set: a dead credential. A 24 h overlap for the outgoing challenge:
nothing holds the old token, so an overlap would only leave a known-lost token valid for a day.
Exposing the stored hash or the hint on a read: the hash is not a credential and the hint is
not a recovery path, and either would make "shown once" ambiguous without adding a way to
answer the challenge.

**Priority.** FR-409 is P1, and that is a statement about its class rather than a deferral: it is
a recovery affordance on a P0 flow, not a delivery guarantee, and the shipped flow - the token
shown once at creation - is correct without it. The ruling does not defer it: card `t_af2fb85c`
implements it, and the slice exists because the alternative recovery in the tree today destroys an
endpoint.

**Encoded by.** FR-409 and AC-54; §6's `not_delivered:endpoint_unverified` row; §11's API
surface; UI-13 (§14.2) and §14.3's producer data paths; §19's decision row; slice S13, card
`t_af2fb85c` (store method, both routes and handlers, both portals' controls, AC-54's tests and
their registration in `ci.yml`'s required-names list). No code changes land with this
amendment, and no new UI id is added: the control extends two surfaces that already exist - the
producer portal's endpoint page (UI-1 step 4, UI-11) and the consumer portal's, which UI-13
this amendment extends.

## Gap list — thirteen decided, one open

Six gaps, each now a decision with a mechanism and a named trade-off. Row 4b is a
seventh contradiction found while resolving the fourth, recorded here instead of being
left for S3 to guess at. The seventh numbered item below was found after the
amendment and is not one of them; it stays open. Rows 8 and 9 came after S6 landed:
both are places where the shipped mechanism, not a silence, changed the answer, which
is the case PLAN.md's rule 5 sends to this list rather than into a code comment. Row 10
is a decision taken *before* its mechanism: §6's attempt-evidence row promised an
acknowledgement after the commit and said nothing about a cancellation revoking it, and
the two windows in `Flusher.insert` (A-9) show what that silence cost - a retirement
delayed by `StaleAfter`, and an entry the stream can never retire.

Row 11 is A-10, found by the review of S5: three rows of §7.13's fairness half had no owner, no
slice and no implementation, and the one that names a budget could not be read without
contradicting FR-1308. Row 10 was held for A-9 (card `t_af54d465`), the attempt-buffer
acknowledgement, while that branch was unlanded, so each row keeps the number it was written with;
A-9 has since landed into row 10, and the heading's count is now eleven.
Row 12 is A-11, found by S10's load test: FR-818 was a P0 with no slice, no
implementation and no shed path - the instance queued on the pgx pool - and AC-30's third clause
had no harness behind it at all, so the heading's count was twelve at that point - row 13 below takes it to thirteen. A-11 was extended afterwards, by the review of `t_b9850e1b` (card `t_194e6caf`): FR-703 still read as a per-event transaction boundary for the mechanism A-11 had just given a batched one, so row 12's contract-change cell carries that reading too, and no new row or count is claimed for it. Row 13 is A-12, found by Jay while reviewing the
getting-started flow (card `t_30fac425`): FR-403's challenge is shown once and had no way to mint
another, so an endpoint whose token was lost before the challenge was answered could never be
verified - and the events it skipped were `skipped` rows FR-1106 refuses to replay, so the only
recovery in the tree cost the endpoint, its subscriptions and its history. A-12 rules the challenge
re-issuable at parity with FR-404's rotation, so the heading's count is now thirteen.

| # | Gap | Decision | Contract changed | Unblocks |
|---|---|---|---|---|
| 1 | AC-14 contradicted FR-1202/FR-1203 | A-1 | AC-14 rewritten; FR-1203 rewritten as a breadth trigger; FR-1209…FR-1211, D-8 added | S5 |
| 2 | NFR-4 untestable on the reference machine | A-2 | NFR-1, NFR-4 restated; load protocol in §12; AC-45 added | S10 |
| 3 | Envelope had no `consumer_id` | A-3 | FR-513, FR-514 added; §10 field table | S3 |
| 4 | FR-903's 30-day TTL had no store | A-4 | FR-903 rewritten, FR-905 added; two §6 rows; AC-46 | S7 |
| 4b | I-3 said replay was unique with fan-out (found during 4) | A-4 | I-3 amended to a partial unique index | S3, S7 |
| 5 | Consumer session format unspecified | A-5 | FR-110 rewritten; FR-112, FR-113 added; AC-47 | S9 |
| 6 | `pigeon doctor` unspecified | A-6 | OPS-4 and OPS-10 amended; OPS-11 added; AC-48 | S10 |
| 8 | D-2 named the Redis Streams pending-entry count as the attempt counter | A-7 | D-2 rewritten to name `deliveries.attempts`; §6's attempt-evidence row names the Redis Stream and its consumer group | S6 (landed), S7's inspect API |
| 9 | LOG-2's "above 100/s" fixed neither a burst allowance nor a replica count | A-8 | LOG-2, §13.1 and AC-33 state `PIGEON_SUCCESS_LOG_RPS` sustained per process and at most 2 × that in any one-second window | S6 (landed), S10's AC-33 load test |
| 10 | The acknowledgement could be revoked midway (a batch the flusher cannot hand back), and nothing retired an entry that lost its removal | A-9 | D-9 added; §6's attempt-evidence row states an acknowledgement after the commit that is one transaction and is not revocable by the cancellation ending a pass | S6's flusher (implementation routed separately) |
| 11 | §7.13's FR-1301, FR-1303 and FR-1304 had no slice, no AC and no implementation, and FR-1303's "separate budget" contradicted FR-1308 read as an endpoint limit | A-10 | FR-1303 and FR-1304 rewritten with the lane and hold mechanisms; FR-1301's halves named (publish = FR-207, delivery deferred to v1.1); AC-52, AC-53 and OBS-13 added; §17's M3 exit and §17.1 amended | S11 (card `t_558a9f59`), and AC-16's NFR-2 leg in S10 |
| 12 | FR-818 (P0) had no owning slice, no implementation and no mechanism that could shed - the instance queues on the pgx pool - and its literal text (every database write batched through one queue) cannot hold for the publish transaction, which is `202`-on-commit (FR-604) inside NFR-1's 100 ms budget. AC-30's third clause is implemented nowhere in the harness | A-11 | FR-818 rewritten as a bounded write budget with an ingest admission deadline and an `over_capacity` shed; FR-815's floor read as §8.1's binding rows; `PIGEON_DB_MAX_CONNS` 10 → 40; `PLAN.md` slice S12 added; FR-703's transaction boundary read at the batch rather than the event - an event's fan-out set is never split across transactions (follow-up card `t_194e6caf`) | S12 (card `t_c6afa452`) and the graded AC-30 run (card `t_0bf3effa`) |
| 13 | FR-403's challenge is shown once and cannot be re-issued, so an endpoint whose token is lost before the challenge is answered can never be verified and never receives a live event; the only in-tree recovery is a replacement endpoint, and the events the unverified endpoint skipped are `skipped` rows FR-1106 refuses to replay (found by Jay reviewing the getting-started flow, card `t_30fac425`) | A-12 | FR-409 and AC-54 added; §6's unverified-state row, §11's API surface, UI-13, §14.3's producer data paths and §19's decision row amended; `PLAN.md` slice S13 added | S13 (card `t_af2fb85c`), and the README's verification step (card `t_5a78686f`) |

**They do not contradict each other**, checked pairwise where they touch: A-1's pause
flags and A-4's archive both ride the control/evidence split in §6 (D-8, FR-812); A-2's
numbers are §8.1's own table, not new ones; A-3 decides what the envelope shows while
A-4 decides what Postgres remembers, and dedupe keys on ids so neither reaches the
other; A-5's sessions are control-state rows read
most of the time, written at most once a minute; A-6 reads the same environment config
that A-5's session TTL and A-1's thresholds come from, and its `db.connection_budget`
check is OPS-8's formula, not a second one. A-7 is D-1's other half - the buffer keeps the
evidence path and the counter moves to control state - so nothing it says crosses the
write-class split §6 and FR-812 define; A-8 quantifies the bound LOG-2 already had instead
of adding a mechanism, and takes FR-207's per-process scope so a deployment's ceiling is
`replicas × PIGEON_SUCCESS_LOG_RPS`, stated rather than left to be discovered by a load
test. Neither reaches the other: A-7 touches the delivery row's counter, A-8 touches log
emission, and no requirement names both. A-9 is D-1's buffer again: it moves no counter and
no write class, it makes one retirement indivisible and bounded, so A-7's counter, A-8's gate
and A-2's numbers stay where they were; the one requirement it touches is NFR-6, and it
touches it by making the gauge that requirement is read from honest rather than by moving its
bound. A-10 reaches the delivery worker's dispatch path: its
class predicate reads `deliveries.attempts`, which is A-7's counter and not A-9's pending-entry
list, so A-9's decision does not reach it, and its added delivery columns are read by the
delivery view A-7 left alone. The check that mattered most is against the rows A-10 amends:
FR-1308, FR-1302 and FR-1306 are untouched, because no decision in this list adds a second endpoint
limit, and A-10's lanes are per process exactly as A-8's log gate and FR-207's rate limits are -
which is also why neither A-10 nor A-8 raises a deployment's ceiling with its replica count without
saying so. A-11 reaches the ingest transaction and the worker write paths, and it narrows FR-818's
literal text without moving what the surrounding decisions fix: FR-604 still makes the `202` the
commit, FR-812's class split and D-1's buffer are untouched, FR-815's floor is read as §8.1 reads it
(the rows that bind) rather than as a floor on every path, and FR-816's cap is respected because the
write budget's worker pool is fixed at boot. FR-703 is the second row A-11 changes, and only in its transaction boundary: one event's fan-out set is never split, and the batch the write budget forms is what carries it, which `PackFanOut` already enforces by never splitting a plan. A-11's one configuration change, `PIGEON_DB_MAX_CONNS` 10 →
40, is a value OPS-8's arithmetic already constrains (`1 × 40 + 5 ≤ 100` at one replica, 85 at two),
so it moves no topology decision and no requirement's number. A-12 reaches the endpoints row and
nothing else, and its contract change is a new affordance (FR-409) rather
than a reading of an existing row: it moves no counter, no write class and no delivery state, so
A-7's counter, A-8's gate, A-9's acknowledgement, A-10's lanes and A-11's write budget are
untouched, and §6's `not_delivered:endpoint_unverified` row keeps its meaning - its fix cell gains
a pointer to FR-409 and nothing else. It deliberately does not amend FR-404: the signing secret's
24 h overlap exists because a receiver holds the outgoing secret and verifies signatures with it,
while no counterparty holds a challenge token, which is why re-issue has no overlap window. It also
does not amend FR-1106, and that is the boundary the ruling states rather than leaves to be
discovered: `skipped` rows stay un-replayable (`internal/store/inspect.go:697`), so a re-issue arms
an endpoint and never resurrects an event, and the recovery path for the events published before
verification stays re-publish. FR-403 itself is unchanged: verification is still a precondition of
live delivery, and FR-409 says only who may mint the challenge that answers it.

One thing this list no longer leaves open: whether the endpoint pause threshold scales
with subscription count. It does - `N = min(active, max(3, ceil(active/4)))` in FR-1203
- because the review of this amendment showed a fixed threshold fails in both
directions: below ten subscriptions it cannot fire at all, since a paused subscription
stops producing failures and caps an endpoint at ten failed attempts per subscription;
and above ten it fires only after ten subscriptions have each exhausted their own 10
failures, so the ten mails it was meant to replace have already gone out and the only
mail it still suppresses is the notifications of subscriptions that fail afterwards.
One thing it still does
not decide, stated so it is not mistaken for decided: whether consumer users get a
long-lived API token (A-5 defers it; the schema does not need to change if they do).

**Item 7 was found after this amendment and is not one of the six, so it stays
open.** It keeps its number so `PLAN.md`'s citation of it still resolves.

7. **A user with no claims cannot log in, so cannot redeem an invitation.**
   `CreateSession` refuses a user who has no producer membership, no admin flag and
   no consumer binding ("an authenticated identity that can do nothing is a support
   ticket"). That is reasonable, but it means an account that lost its last binding
   — a consumer user whose access was revoked, or a member removed from a producer —
   cannot sign in, and since 0005 an invitation for an existing account must be
   redeemed with that account's own session. Re-inviting such a user is therefore a
   dead end until there is a mailer (S9) or a password-reset flow (FR-111 names one;
   no slice owns it). Affects S9, not the S1 acceptance criteria: every S1 flow
   starts from an account that has a claim.
