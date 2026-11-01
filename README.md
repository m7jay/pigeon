# Pigeon

Self-hosted, open-source webhook delivery. A producer publishes an event to the
API, and pigeon fans it out to that consumer's endpoints as a signed HTTP
delivery - Standard Webhooks headers (`webhook-id`, `webhook-timestamp`,
`webhook-signature`, FR-1001) - with at-least-once semantics, a retry schedule
with a per-attempt trail, and a 5 s per-attempt cap that decides how a slow
receiver is classified (FR-802). Go + Postgres + Redis Streams, on one modest
host.

## Quickstart

`compose.yaml` is the reference deployment (OPS-1): five containers - the core,
both portals, Postgres and Redis. Go 1.26 is what `go.mod` asks for. The
walkthrough below is the flow a person actually performs in a browser at the two
portals; `curl` appears only for the five steps no portal has a page for, each one
named where it comes up, and those need `curl` and `jq`.

```sh
git clone https://github.com/m7jay/pigeon.git && cd pigeon

# .env.example is the complete list of settings (OPS-3, enforced by
# `go test ./internal/config`). Copy it and work through the ENV checklist below
# before you start anything.
cp .env.example .env
# The headers key is the one value that refuses to boot at the shipped
# placeholder, and the one you should not type by hand (FR-1006):
sed -i "s|^PIGEON_HEADERS_KEY=.*|PIGEON_HEADERS_KEY=$(openssl rand -base64 32)|" .env
$EDITOR .env

make compose-up            # == docker compose up -d --build
```

A `PIGEON_HEADERS_KEY` that is not 32 bytes of base64 is refused at boot by name;
`pigeon doctor` reports a configuration it cannot load instead of exiting on it.

```sh
curl -s localhost:8080/health        # {"status":"ok","version":"..."}  liveness
curl -s localhost:8080/health/ready  # {"status":"ready"}              readiness
curl -s localhost:8082/healthz       # consumer portal
curl -s localhost:8083/healthz       # producer portal
```

Start a tenant and watch one event land. The walkthrough registers a public echo
service as the receiver, because a live endpoint must be a public `https` URL
(FR-402) - point it at a receiver of your own for real traffic.

### 1. Create the first producer and its owner (curl)

Neither portal has an admin surface: the producer portal signs producer members in
and is a thin client of `/v1/producer/*`, the consumer portal is a thin client of
`/v1/consumer/*`, and `POST /v1/admin/producers` is the instance admin's. So the
bootstrap is the API.

```sh
B=http://localhost:8080

# The bootstrap credential is the instance admin (FR-101): admin@pigeon.local
# with PIGEON_INSTANCE_ADMIN_PASSWORD. Its first login must change it.
ADMIN=$(curl -s -X POST $B/v1/session -H 'content-type: application/json' \
  -d '{"email":"admin@pigeon.local","password":"<PIGEON_INSTANCE_ADMIN_PASSWORD>"}' | jq -r .token)
curl -s -o /dev/null -w '%{http_code}\n' -X POST $B/v1/session/password \
  -H "authorization: Bearer $ADMIN" -H 'content-type: application/json' \
  -d '{"old_password":"<PIGEON_INSTANCE_ADMIN_PASSWORD>","new_password":"<a new one>"}'   # 204
ADMIN=$(curl -s -X POST $B/v1/session -H 'content-type: application/json' \
  -d '{"email":"admin@pigeon.local","password":"<a new one>"}' | jq -r .token)

# A producer, plus the invitation for its first owner, returned once (FR-102).
PROD=$(curl -s -X POST $B/v1/admin/producers -H "authorization: Bearer $ADMIN" \
  -H 'content-type: application/json' \
  -d '{"slug":"acme","name":"Acme","owner_email":"owner@acme.test"}')
INVITE=$(echo "$PROD" | jq -r .owner_invite.token)

# Redeem it as that owner. Still curl: no route of the producer portal redeems an
# invitation - the only redeem form in the tree is the consumer portal's /invite,
# and that one binds the session it mints to a consumer.
curl -s -X POST $B/v1/invites/accept -H 'content-type: application/json' \
  -d "{\"token\":\"$INVITE\",\"password\":\"<owner password>\"}"
```

### 2. Sign in at the producer portal, and switch to Live

Open `http://localhost:8083` and sign in as `owner@acme.test`. The portal opens in
the plane `PIGEON_PRODUCER_PORTAL_ENV` names (`test` at the shipped default), and
the nav carries the plane buttons - click **Live**, because that is where the rest
of this walkthrough creates data. The planes are separate data, not a filter
(FR-103), and every producer call carries the plane explicitly.

The **Get started** page (`/onboarding`) is UI-1's checklist. Each step's state is
read back from the API, so the checklist cannot disagree with the platform.

### 3. Register the event type

On **Get started**, "Step 2: register an event type": name `acme.created`, routing
mode `consumer`, payload schema `{"type":"object"}` (an empty schema box registers
exactly that). A new type is publicly subscribable by default, which is what lets
a consumer's user subscribe to it (UI-14).

### 4. Register the consumer and invite its owner

**Consumers** -> "Create the consumer": reference `acme`, name `Acme Signals`.
Open the consumer and use **Invite someone by email** for `ops@acme-consumer.test`.
The invitation token is rendered once on that page - v1.0 ships no mailer, so pass
it on yourself - and an invitation is the only way in (FR-111: no
self-registration).

### 5. The consumer owner redeems the invitation

Open `http://localhost:8082/invite`, paste the token and the address it was
addressed to, and choose a password. That creates the account, binds it to the
consumer and signs it in.

### 6. Register the endpoint, and copy both secrets now

Consumer portal, **Endpoints**, "Register an endpoint": name `httpbin`, URL
`https://httpbin.org/post`. It is a public `https` URL because a live endpoint
must be (FR-402); this form has no internal-endpoint checkbox, so a private or
loopback receiver needs the producer portal's register form, which has one.

On submit the page shows the endpoint's **signing secret** and its **verification
token**, and says the secret is shown exactly once and cannot be retrieved again -
copy both before you navigate away. The signing secret is what a verifier checks
the `webhook-signature` header with, and this endpoint's page links **Webhook
snippet**, which renders a verifier in curl, Go, Node or Python (UI-7) - the
producer portal's endpoint page offers the same thing as "Build a verification
snippet for this secret".

### 7. Answer the ownership challenge (FR-403)

The same page offers **Verify now**, with the token already in the form: press it
and the endpoint is verified. If you lose that moment, the endpoint's own page
carries the standing path - "Verify ownership - paste the verification token that
was shown once when you registered this endpoint. Until it is answered the
endpoint receives no live events (FR-403)" - and the producer portal's endpoint
page carries the same ownership form for an endpoint it can see. Both endpoint
pages also carry the signing-secret **Rotate** action, behind a confirm.

Lost the token before you answered it? The endpoint is inert rather than broken: it
keeps its subscriptions and its history, and each delivery it was skipped for is
recorded with the reason `not_delivered:endpoint_unverified`, but it receives
nothing. The endpoint's own page - in both the consumer and the producer portal -
carries a **Re-issue the verification token** control: it mints a fresh token, shown
once like the original, and the old one stops verifying immediately (FR-409).
Re-issuing arms the endpoint; it does not re-deliver what was published while the
endpoint was unverified, and a `skipped` row is refused on purpose (FR-1106), so the
missed window is recovered by publishing the events again - the producer portal's
Deliveries page has the replay panel (UI-5).

### 8. Subscribe the endpoint to the event type

Endpoint page -> **Subscriptions** -> choose `acme.created` -> **Subscribe**. The
choices are the types the producer has left publicly subscribable (UI-14).

### 9. Issue a producer API key and publish with it (curl)

No page of the producer portal issues a key, and none publishes with one. Its
`/test-event` page is the no-key way to send an event from the UI: it publishes
with your session and is marked as a test everywhere it appears (UI-4).

```sh
B=http://localhost:8080

OWNER=$(curl -s -X POST $B/v1/session -H 'content-type: application/json' \
  -d '{"email":"owner@acme.test","password":"<owner password>"}' | jq -r .token)

# A producer API key (FR-201): returned once, hashed at rest, revocable.
KEY=$(curl -s -X POST "$B/v1/producer/keys?env=live" -H "authorization: Bearer $OWNER" \
  -H 'content-type: application/json' -d '{}' | jq -r .key)

# Publish. 202 is the commit: the event and its outbox row are one transaction
# (FR-604), and the relay fans out from there.
curl -s -X POST $B/v1/events -H "authorization: Bearer $KEY" \
  -H 'content-type: application/json' \
  -d '{"event_type":"acme.created","consumer_ref":"acme","payload":{"id":"evt_1"}}'
```

### 10. Watch the delivery land

Back in the browser: the producer portal's **Deliveries** (`/deliveries`, in the
Live plane) and the consumer portal's **Deliveries** both show the new delivery
with its status and attempt count - each consumer user sees only its own consumer's
deliveries (FR-1105). The relay drains the outbox on a cadence and the worker polls
for due deliveries, so reload after a moment.

The curls above are the five steps with no page to submit: the instance-admin
bootstrap and `POST /v1/admin/producers` (no admin surface exists in either
portal), the producer owner's invitation (`POST /v1/invites/accept`), the API key
(`POST /v1/producer/keys`), and the key-authenticated publish (`POST /v1/events`,
where the UI's only publish sends a test event with your session). Every other
step above is a form you submitted in the browser.

## The shape of the system

| Piece | What it does |
|---|---|
| `cmd/pigeon` | the core: ingest API, relay, delivery workers, sweeper, migrator, `doctor` |
| `cmd/consumer-portal` / `cmd/producer-portal` | the two UIs (`:8082`, `:8083`), stateless clients of the core API |
| Postgres | the queue of record: events, outbox, deliveries, attempts, tenant-scoped by RLS |
| Redis Streams | the event bus and the attempt-evidence buffer (D-1) |

Either portal may be omitted without the core losing anything: it serves the same
API with the same server-side scoping (AC-40). An event's path: `POST /v1/events`
writes the event and its outbox row in one transaction and answers `202`; the
relay claims outbox rows in batches, and fan-out writes one delivery row per
matching endpoint and subscription; a delivery worker leases a due delivery,
signs the envelope with that endpoint's secret, POSTs it under the 5 s cap, and
writes the outcome - a retry scheduling the next attempt, or a terminal status.
The sweeper re-queues deliveries whose lease lapsed, and drops whole day
partitions under the retention windows. The connection budget, replica counts and
what each process owns are in [`docs/topology.md`](docs/topology.md).

## Configuration

[`.env.example`](.env.example) is the complete list of settings - a test fails the
build if any config-reading file of this module, or `compose.yaml`'s own `${...}`
inputs, adds one that is not documented there (OPS-3) - with each one's default and
clamps documented. The handful an operator actually turns:

- `PIGEON_DATABASE_URL` / `PIGEON_MIGRATE_DATABASE_URL` - two roles on purpose: a
  runtime role that owns the tables is exempt from every RLS policy (OPS-7
  refuses both URLs naming one role).
- `PIGEON_DB_MAX_CONNS`, `PIGEON_CORE_REPLICAS`, `PIGEON_WRITE_QUEUE_DEPTH` - one
  write budget, checked against the server's `max_connections` at boot (OPS-8).
- `PIGEON_DELIVERY_WORKERS`, `PIGEON_DELIVERY_CONCURRENCY`,
  `PIGEON_DELIVERY_RETRY_CONCURRENCY` - claim loops, and the attempt budget the
  delivery lanes partition (FR-816, FR-1303).
- `PIGEON_RATE_LIMIT_PER_KEY_{RPS,BURST}`,
  `PIGEON_RATE_LIMIT_PER_PRODUCER_{RPS,BURST}` - per process, so N replicas allow
  N times the rate (FR-207).
- `PIGEON_RETENTION_{DELIVERIES,ATTEMPTS,EVENTS,DEDUPE}`,
  `PIGEON_RETENTION_MAX_DISK_PCT`, `PIGEON_DISK_DIR` / `PIGEON_DISK_BYTES` -
  history leaves by whole partition, and under disk pressure a partition goes
  before its window (SB-2, SB-3); [`docs/retention.md`](docs/retention.md) has the
  per-row numbers to size them with.
- `PIGEON_HEADERS_KEY` - the at-rest key for endpoint custom header values
  (FR-1006); required, with no rotation yet
  ([`docs/at-rest-encryption.md`](docs/at-rest-encryption.md)).
- `PIGEON_EGRESS_IPS` - the allowlist `GET /v1/meta/egress-ips` publishes
  (FR-1007), so a consumer can allowlist you before it starts refusing traffic.

`pigeon doctor` reports the box rather than the process - config validity,
Postgres and Redis reachability, Redis `appendonly`, clock skew, disk headroom,
`ulimit -n`, the OPS-8 budget - one line per check under a stable id, with
`--json` and an exit code (OPS-11).

## Configuration checklist

[`.env.example`](.env.example) is the complete list of settings, and `go test
./internal/config` enforces that (OPS-3): `TestEnvExampleDocumentsEveryKnob` fails
the build when a knob this module reads, or one `compose.yaml` interpolates
itself, has no entry there, and `TestComposeRunsEachDeployableWithItsOwnKnobs`
fails when a portal's knob is not forwarded to its container. The file carries
every knob's default, clamp and ceiling; this checklist carries the decisions.

Work through it once per environment. "Shipped" is the literal value in
`.env.example`; **refused at boot** means the value is checked where it is read, so
the process exits non-zero with one line naming the variable instead of starting
misconfigured (OPS-7) - `pigeon doctor` reports the same validation without
exiting (OPS-11).

| Variable | What it is | Dev | Prod | Refused at boot at the shipped value? |
|---|---|---|---|---|
| `PIGEON_HEADERS_KEY` | at-rest key for endpoint custom header values: 32 bytes of base64 (FR-1006) | generated once: `openssl rand -base64 32` | same, generated once and kept - there is no rotation yet | **Yes** - the shipped placeholder is not base64 |
| `PIGEON_INSTANCE_ADMIN_PASSWORD` | the first-run instance-admin credential (FR-101) | any value of 8+ characters that is not one of the six weak words | a long random value; used only on the first run, but validated on every boot | No - the shipped placeholder boots; empty, under 8 characters, or one of `password`, `changeme`, `admin`, `pigeon`, `secret`, `test` is refused |
| `PIGEON_DATABASE_URL` | the runtime role's DSN: serves requests, must not own the tables (a table owner is exempt from RLS) | your local Postgres - the shipped value names the compose service host, so a `make run` outside compose needs your own | the `pigeon_app` role | No at the shipped value; **yes if empty** |
| `PIGEON_MIGRATE_DATABASE_URL` | the owning role's DSN, used only by the migration path | empty is accepted: migrations then run on the runtime connection, which for a single-role dev database means the runtime role owns the tables and RLS is inert for it | `pigeon_migrator`, a second role | **Yes if it names the same role as `PIGEON_DATABASE_URL`** |
| `PIGEON_REDIS_URL` | the event bus and the attempt-evidence buffer (D-1) | your local Redis | required | No at the shipped value; **yes if empty while `PIGEON_ATTEMPT_LOG_MODE=buffered`** |
| `PIGEON_MIGRATIONS_ON_BOOT` | OPS-9: exactly one migrator per deployment | `true` | `true` on replica 1, `false` on replicas 2..N | No |
| `PIGEON_EGRESS_IPS` | the delivery-source allowlist `GET /v1/meta/egress-ips` publishes (FR-1007) | empty - a flat network reads its own interfaces | **must be set** behind NAT, a load balancer, a proxy or a container network: the address a receiver sees is not the one the process can discover | An entry that is neither an IP nor a CIDR block |
| `PIGEON_DISK_DIR` / `PIGEON_DISK_BYTES` | where SB-3's disk budget is measured from | empty: `GET /v1/meta/usage` reports source `none`, and ingest never sheds | one of them - `PIGEON_DISK_DIR` when core and Postgres share a volume, `PIGEON_DISK_BYTES` when core cannot see that filesystem | No - empty is a documented degraded mode |
| `PIGEON_SELF_REGISTRATION` | lets `POST /v1/producers` create a tenant and its first owner with no instance admin (FR-102) | `false` | `false`, and never on an internet-facing instance | No |
| `PIGEON_PASSWORD_RESET_LOG_TOKENS` | writes the password-reset token to the core's own log (FR-111) | `false` | `false` anywhere the logs leave the host: it is a reset link in a shared log | No |
| `POSTGRES_PASSWORD` | the migrator role's password; compose builds both DSNs from it | any non-empty value | a real secret, different from the others | `compose.yaml` declares it `${POSTGRES_PASSWORD:?}`, so compose refuses to resolve while it is absent or empty |
| `PIGEON_APP_PASSWORD` | the `pigeon_app` runtime role's password; `deploy/postgres/01-roles.sql` creates the role with it | any non-empty value | a real secret, different from the others | Same, `${PIGEON_APP_PASSWORD:?}` in `compose.yaml` |
| `PIGEON_CORE_API_URL` | the core's base URL as a portal sees it | `http://localhost:8080` (`make run-portal` and `make run-producer-portal` default it) | `http://core:8080` on the compose network - `compose.yaml` hardcodes it, so it is not an operator input there | **Yes if empty** - both portals refuse to start without it |
| `PORTAL_SESSION_SECRET` | encrypts the consumer session cookie and keys its CSRF token | `make run-portal` falls back to `dev-insecure-secret` | generated, and kept stable: changing it logs every portal user out | `compose.yaml` declares it `${...:?}`; the portal process itself refuses to start without it |
| `PIGEON_PRODUCER_PORTAL_SESSION_SECRET` | the same for the producer portal | `make run-producer-portal` falls back to `dev-insecure-secret` | generated, and kept stable | Same, `${...:?}` in `compose.yaml` |
| `PIGEON_PRODUCER_PORTAL_ENV` | the plane a fresh producer-portal visitor starts in (the signed-in human switches, and every call carries the plane, FR-103) | `test` | whatever you want as the landing plane | Anything other than `test` or `live` refuses the portal |
| `PIGEON_OIDC_ISSUER` / `PIGEON_OIDC_CLIENT_ID` | FR-106's optional OIDC login, the core's half (it verifies the id_token) | leave both empty: the feature is off and `POST /v1/session/oidc` answers 404 | set together, with the portal's half below | **Yes when only one of the two is set** |
| `PIGEON_OIDC_CLIENT_SECRET` / `PIGEON_OIDC_REDIRECT_URL` / `PIGEON_OIDC_SCOPES` | the relying-party half, read by the producer portal only - the core never holds the secret (FR-106) | leave empty while OIDC is off | the value registered at your issuer, and the portal's callback URL | No |
| `PIGEON_LOADGEN_DB` / `PIGEON_LOADGEN_KEY` | the load harness's DSN and API key; `cmd/loadgen` is not a deployment component (§12) | empty | empty - the grader sets them per run | No |

Everything else in the file is a knob whose shipped default the reference
deployment runs with, and `.env.example` documents each one's default, clamp and
ceiling next to it. The groups, so you can find the one you want:

| Group | Names | Verdict |
|---|---|---|
| HTTP | `PIGEON_HTTP_ADDR`, `PIGEON_SHUTDOWN_GRACE`, `PIGEON_SESSION_TTL` | leave as shipped |
| Connection budget (OPS-8) | `PIGEON_DB_MAX_CONNS`, `PIGEON_DB_RESERVED_CONNS`, `PIGEON_CORE_REPLICAS` | one arithmetic, checked against the server's `max_connections` at boot: replicas x pool + reserved <= `max_connections` |
| Rate limits (FR-207) | `PIGEON_RATE_LIMIT_PER_KEY_RPS`, `PIGEON_RATE_LIMIT_PER_KEY_BURST`, `PIGEON_RATE_LIMIT_PER_PRODUCER_RPS`, `PIGEON_RATE_LIMIT_PER_PRODUCER_BURST` | leave as shipped; per process, so N replicas allow N times the rate |
| Delivery worker | `PIGEON_DELIVERY_WORKERS`, `PIGEON_DELIVERY_LEASE`, `PIGEON_DELIVERY_TIMEOUT`, `PIGEON_DELIVERY_MAX_BODY_BYTES`, `PIGEON_DELIVERY_BACKOFF_BASE`, `PIGEON_DELIVERY_BACKOFF_MAX`, `PIGEON_DELIVERY_MAX_RETRIES`, `PIGEON_DELIVERY_RETRY_WINDOW`, `PIGEON_DELIVERY_JITTER`, `PIGEON_DELIVERY_CONCURRENCY`, `PIGEON_DELIVERY_RETRY_CONCURRENCY`, `PIGEON_DELIVERY_SLOT_TTL`, `PIGEON_DELIVERY_MAX_CONNS_PER_HOST`, `PIGEON_DELIVERY_POLL_INTERVAL`, `PIGEON_DELIVERY_USER_AGENT` | leave as shipped; the 5 s timeout is FR-802's ceiling and a larger value is clamped to it |
| Sweeper | `PIGEON_SWEEPER_INTERVAL`, `PIGEON_SWEEPER_LOCK_URL`, `PIGEON_SWEEPER_LOCK_WAIT` | leave as shipped |
| Write budget (S12) | `PIGEON_WRITE_QUEUE_DEPTH`, `PIGEON_WRITE_QUEUE_WORKERS`, `PIGEON_INGEST_ADMIT_TIMEOUT` | leave as shipped |
| Attempt evidence | `PIGEON_ATTEMPT_LOG_MODE`, `PIGEON_ATTEMPT_FLUSH_BATCH`, `PIGEON_ATTEMPT_FLUSH_INTERVAL`, `PIGEON_ATTEMPT_FLUSH_STALE`, `PIGEON_SUCCESS_LOG_RPS` | leave as shipped |
| Retention (SB-3, SB-5) | `PIGEON_RETENTION_DELIVERIES`, `PIGEON_RETENTION_ATTEMPTS`, `PIGEON_RETENTION_EVENTS`, `PIGEON_RETENTION_DEDUPE`, `PIGEON_RETENTION_MAX_DISK_PCT` | size them for your own disk; [`docs/retention.md`](docs/retention.md) has the per-row numbers |
| Observability | `PIGEON_LOG_LEVEL`, `PIGEON_LOG_JSON` | leave as shipped |
| Portal cosmetics | `PIGEON_PORTAL_ADDR`, `PORTAL_SESSION_COOKIE_NAME`, `PORTAL_BRAND_NAME`, `PORTAL_BRAND_ACCENT`, `PORTAL_BRAND_LOGO_URL`, `PORTAL_BRAND_COMPANY`, `PIGEON_PRODUCER_PORTAL_ADDR`, `PIGEON_PRODUCER_PORTAL_COOKIE_NAME`, `PIGEON_PASSWORD_RESET_TTL` | leave as shipped; a portal's published port in `compose.yaml` and its listen address move together |

**Secrets** - never committed, and different in every environment:
`PIGEON_HEADERS_KEY`, `PIGEON_INSTANCE_ADMIN_PASSWORD`, `POSTGRES_PASSWORD`,
`PIGEON_APP_PASSWORD`, `PORTAL_SESSION_SECRET`,
`PIGEON_PRODUCER_PORTAL_SESSION_SECRET`, `PIGEON_OIDC_CLIENT_SECRET`,
`PIGEON_LOADGEN_KEY`, and the two database URLs, which carry a password each.
`.env` is in `.gitignore`; `.env.example` holds published placeholders, which are
not secrets and must not be shipped as if they were.

**Running the suite locally** has two variables of its own, neither of which is
in `.env.example` (no deployment process reads them):

- `PIGEON_TEST_DATABASE_URL` - the disposable Postgres the DB-backed packages use.
  It never has to be exported: left unset, `make test` points the suite at
  compose.yaml's `test` profile (`127.0.0.1:55432`) and creates the database if it
  is missing; set it to use a Postgres you already have. Compose itself does need
  the five values the checklist's first item puts in `.env` (it interpolates the
  whole file), and on a box with no usable `docker compose`, or without that
  `.env`, `make test` stops at `test-up` instead of running anything. **Its
  database name must contain `test` by design**: `internal/httpapi` refuses to
  truncate any other name and `internal/relay` refuses any other DSN, while
  `internal/store`, `internal/delivery`, `internal/identity`, `internal/db` and
  `internal/faults` migrate and truncate whatever database the DSN names. Point it
  at a database you are willing to lose. Whatever it names, `internal/httpapi`'s
  harness refuses to skip: a database that is not reachable reds its sixty
  acceptance tests at 0.00 s and reads like a regression.
- `PIGEON_TEST_REDIS_URL` - optional locally: the Redis-backed packages fall back
  to an in-process miniredis. CI sets it from a `redis:7` service container, and
  the job fails on any `--- SKIP`, so a real Redis leg is what CI measures.

Checklist:

- [ ] `cp .env.example .env`, and `.env` is not committed.
- [ ] `PIGEON_HEADERS_KEY=$(openssl rand -base64 32)` - the core refuses any other shape.
- [ ] `PIGEON_INSTANCE_ADMIN_PASSWORD` is 8+ characters and not one of the six weak words.
- [ ] The five values `compose.yaml` requires are non-empty: `POSTGRES_PASSWORD`, `PIGEON_APP_PASSWORD`, `PIGEON_HEADERS_KEY`, `PORTAL_SESSION_SECRET`, `PIGEON_PRODUCER_PORTAL_SESSION_SECRET`.
- [ ] `PIGEON_DATABASE_URL` and `PIGEON_MIGRATE_DATABASE_URL` name different roles.
- [ ] `PIGEON_REDIS_URL` is set, or `PIGEON_ATTEMPT_LOG_MODE=sync` if you have no Redis.
- [ ] Behind NAT, a load balancer, a proxy or a container network: `PIGEON_EGRESS_IPS` is set.
- [ ] `PIGEON_DISK_DIR` or `PIGEON_DISK_BYTES` is set, or you accept that usage reports `none` and ingest never sheds.
- [ ] `PIGEON_CORE_REPLICAS` x `PIGEON_DB_MAX_CONNS` + `PIGEON_DB_RESERVED_CONNS` <= the server's `max_connections` (OPS-8).
- [ ] On replicas 2..N: `PIGEON_MIGRATIONS_ON_BOOT=false`.
- [ ] `PIGEON_SELF_REGISTRATION=false` and `PIGEON_PASSWORD_RESET_LOG_TOKENS=false`.
- [ ] OIDC is either entirely off or entirely configured (issuer and client id together).
- [ ] Every secret above differs between dev and prod, and none is in git.
- [ ] On the machine that runs the suite: `PIGEON_TEST_DATABASE_URL`, if it is set, names a database you can lose, and its name contains `test`.

## Where the rules live

| Document | What it is |
|---|---|
| [`REQUIREMENTS.md`](REQUIREMENTS.md) | the contract: numbered FR/NFR/OPS/SB/D requirements, and §16's acceptance criteria |
| [`SPEC.md`](SPEC.md) | how this tree implements them, the decisions taken where the requirements were silent (A-1…A-12), and the gap list |
| [`PLAN.md`](PLAN.md) | the slice order, and what each slice turns green |
| [`docs/worker-git.md`](docs/worker-git.md) | the git, PR, review and merge protocol |
| [`docs/load-test.md`](docs/load-test.md) | §16's measurement protocol, and what the last run does not prove |
| [`docs/topology.md`](docs/topology.md) | the connection budget, replica counts, database roles, egress addresses |
| [`docs/retention.md`](docs/retention.md) | retention windows, disk budget, per-row sizing |
| [`docs/at-rest-encryption.md`](docs/at-rest-encryption.md) | the headers key, and what losing it costs |

## Status

Landed on `feat/m0-skeleton`: the core API (tenants, identity, keys, rate limits),
event types and the transactional outbox, routing and fan-out, the delivery worker
with signing, leases, retries and the sweeper, the attempt buffer and flusher,
replay and the inspect API, partitioning and retention by disk, both portals, the
hardening work (SSRF at three points, secret rotation, `doctor`), the bounded write
budget, and the delivery lanes and holds. The slice table in `PLAN.md` runs S0 to
S13 (with S9a for the producer portal): S0 to S12 have landed, and S13 - the
challenge re-issue (FR-409, `SPEC.md` A-12) - has not. Its State column is stale
for the slices that have landed, though - it still shows a dash for S2 through
S12, while S13's dash is accurate - so read the tree, not the column.

Not landed, and not to be read as green:

- **The rated-class scale measurements are deferred.** AC-16, AC-17 and AC-43 are
  measured on the 8 vCPU / 16 GB class by the maintainer on a rated host; this
  project has no host that carries the rating, so they run outside CI, and nothing
  has been measured on the reference box.
- **No throughput figure is a ceiling.** The numbers in
  [`docs/load-test.md`](docs/load-test.md) were taken on a tree that predates the
  write budget (S12) and the dispatch fix (S11); its "what this run does not prove"
  section says what they are and are not, and the graded re-run is the
  maintainer's, on a rated host.
- **Gaps the tree records stay gaps.** `SPEC.md`'s gap list has one open item (item
  7: a user with no claims cannot sign in, and since migration 0005 an invitation
  for an existing account must be redeemed with that account's own session, so
  re-inviting such a user is a dead end). FR-111's password-reset flow has landed
  since (S9a) and is not a way out either: it grants a password and never a claim,
  so the same account still cannot sign in, and v1.0 ships no mailer - the reset
  token goes to the core's own notifier seam. `PIGEON_RETENTION_EVENTS` is reported
  but not enforced, because `events` is not partitioned yet; the instance-admin
  marker is a bug-and-misrouting gate, not a privilege boundary; rate limits and the
  success-log bound are per process.
- **No licence file.** The repository carries no `LICENSE`; `REQUIREMENTS.md` §19
  records Apache-2.0 as the intended licence, which is a decision to make in a file
  rather than in a README.

## Working in the repository

- Conventional Commits, one slice per PR, branched from the default branch
  (`feat/m0-skeleton`) - never from another slice's branch.
- `make vet test` before pushing. `make build` and Docker image builds run the
  self-contained Go suite first. `make test` / `make unit-test` runs that same
  suite without Docker. Database-backed acceptance tests skip
  when no test DSN is provided. Run those separately with `make test-integration`,
  which starts the isolated Postgres and Redis test services and runs the full suite.

  ```sh
  make test                # unit tests; no containers required
  make test-integration    # full suite, including database-backed integration tests
  make loadgen-integration-test # opt-in short fake-server check of the loadgen loop
  make load-test            # manual capacity ramp against isolated Docker services
  make test-down           # stop the isolated integration-test containers
  ```

  With a Postgres and a Redis of your own - service containers, another compose
  project, a server in the next room - set both DSNs instead and nothing needs
  docker: `PIGEON_TEST_DATABASE_URL=postgres://user@host:5432/pigeon_test?sslmode=disable
  PIGEON_TEST_REDIS_URL=redis://host:6379/0 make test-integration`. The database's name must
  contain `test` (`internal/httpapi` and `internal/relay` refuse any other name).
  Load testing is separate and manual; `make load-test` starts `core-test` against
  the disposable Postgres and Redis in `test.compose.yaml`, seeds a load producer,
  then ramps traffic from the local terminal. It never targets the product `core`,
  `postgres`, or `redis` services in `compose.yaml`. The ramp uses available CPUs
  for its starting rate and concurrency, records CPU and memory, and refines the
  highest safe rate before the first pressure point. Run `make test-down` afterward
  to stop the isolated stack. No hardware class matrix runs automatically.
- CI (`.github/workflows/ci.yml`) runs the `test` job on `ubuntu-24.04` with
  Postgres 16 and Redis 7 service containers:
  `go test -count=1 -race -p 1 -v -timeout 25m ./...`. A `--- SKIP` fails the job,
  and a fixed list of acceptance-criterion test names must each print `--- PASS`,
  so renaming or removing one fails the build. The review loop itself is in
  [`docs/worker-git.md`](docs/worker-git.md): a card is done when the reviewer
  merges the PR and completes the card, not when the branch is pushed.
