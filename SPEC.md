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
