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

A depth trigger of 100 failed attempts cannot reach an endpoint at all. A subscription
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
At 133 subscriptions, 100 failures is at most 0.75 attempts per subscription: depth
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
  failing, and the floor is why a breadth member may hold anywhere from 3 to 9 failures
  in its window (the tenth pauses the subscription itself).
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
  that partition, `ON CONFLICT DO NOTHING`, in the same loop iteration as the drop.
  A failed copy aborts that drop. The one exception is SB-3's disk-pressure drop,
  which is logged at warn and leaves that partition's tail unarchived - a support
  answer lost, never a delivery.
- `delivery_dedupe` is itself range-partitioned by day and dropped by partition at
  `dedupe_retention` (default 30 d), so SB-2 holds here too and there is no `DELETE`
  and no vacuum pressure. Under disk pressure these partitions are dropped *before*
  delivery partitions, because they are derived.
- One view, `delivery_dedupe_all`, unions live `deliveries` with the archive, so the
  support path - and the API that answers it - reads one name and does not care which
  side answered.
- Fan-out idempotency does **not** depend on the archive. It is the `deliveries`
  unique index, whose coverage is exactly as long as an outbox row can exist, which
  is the only window in which a re-fan-out is possible.

**Trade-off.** Two mechanisms and a union view instead of one insert, and the archive
lags a partition by at most one sweeper cycle. In exchange the hot path pays nothing:
an inline row per delivery would add 10,000 control writes per second to the
reference load, doubling the write amplification §8.1 spends its whole argument
minimising - and that is what a Redis `SETNX` would have done to Redis, plus it would
have been lost on the wipe AC-10 exercises, breaking P3 for the one question FR-903
exists to answer.

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

### A-5 — Consumer sessions are opaque Postgres-backed tokens in an `HttpOnly` cookie

**Decision.** A consumer user's session is an opaque 256-bit random token, stored
hashed in a `sessions` row that carries the `consumer_id` binding, delivered to the
browser as `__Host-pigeon_session` and presented to the API as a bearer. No JWT, no
client-held claim, no signed cookie.

**Mechanism.**

- Login mints the token and stores `sha256(token)` with `user_id`, `producer_id`,
  `env`, `consumer_id`, `csrf_secret`, `expires_at` (absolute TTL, default 12 h) and
  `last_seen_at` (idle TTL default 30 min, written at most once a minute through the
  `EvidenceTx` class so it costs no fsync). The cookie is `HttpOnly; Secure;
  SameSite=Lax; Path=/` per FR-1009 and carries no data beyond the token.
- The portal is a server-side API client: its handlers call the core over the network
  with `Authorization: Bearer <that token>`, `X-Request-Id`, and a double-submit CSRF
  token on mutations. The browser never holds a credential it can replay at another
  host, and the API performs the filtering, so a compromised or buggy portal cannot
  widen a result set it was handed.
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
  mechanism for the row that already exists.

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

## Gap list — six decided, one open

Six gaps, each now a decision with a mechanism and a named trade-off. Row 4b is a
seventh contradiction found while resolving the fourth, recorded here instead of being
left for S3 to guess at. The seventh numbered item below was found after the
amendment and is not one of them; it stays open.

| # | Gap | Decision | Contract changed | Unblocks |
|---|---|---|---|---|
| 1 | AC-14 contradicted FR-1202/FR-1203 | A-1 | AC-14 rewritten; FR-1203 rewritten as a breadth trigger; FR-1209…FR-1211, D-8 added | S5 |
| 2 | NFR-4 untestable on the reference machine | A-2 | NFR-1, NFR-4 restated; load protocol in §12; AC-45 added | S10 |
| 3 | Envelope had no `consumer_id` | A-3 | FR-513, FR-514 added; §10 field table | S3 |
| 4 | FR-903's 30-day TTL had no store | A-4 | FR-903 rewritten, FR-905 added; two §6 rows; AC-46 | S7 |
| 4b | I-3 said replay was unique with fan-out (found during 4) | A-4 | I-3 amended to a partial unique index | S3, S7 |
| 5 | Consumer session format unspecified | A-5 | FR-110 rewritten; FR-112, FR-113 added; AC-47 | S9 |
| 6 | `pigeon doctor` unspecified | A-6 | OPS-4 and OPS-10 amended; OPS-11 added; AC-48 | S10 |

**They do not contradict each other**, checked pairwise where they touch: A-1's pause
flags and A-4's archive both ride the control/evidence split in §6 (D-8, FR-812); A-2's
numbers are §8.1's own table, not new ones; A-3 decides what the envelope shows while
A-4 decides what Postgres remembers, and dedupe keys on ids so neither reaches the
other; A-5's sessions are control-state rows read
most of the time, written at most once a minute; A-6 reads the same environment config
that A-5's session TTL and A-1's thresholds come from, and its `db.connection_budget`
check is OPS-8's formula, not a second one.

One thing this list no longer leaves open: whether the endpoint pause threshold scales
with subscription count. It does - `N = min(active, max(3, ceil(active/4)))` in FR-1203
- because the review of this amendment showed that a fixed threshold either cannot fire
(below the subscription count) or suppresses nothing (above it). One thing it still does
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
