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
both portals, Postgres and Redis. Go 1.26 is what `go.mod` asks for; the
walkthrough below needs `curl` and `jq`.

```sh
git clone https://github.com/m7jay/pigeon.git && cd pigeon

# .env.example is the complete list of settings (OPS-3, enforced by
# `go test ./internal/config`). Copy it and replace the published placeholders -
# they are published, not secret.
cp .env.example .env
# The shipped headers key is the one value refused outright at boot, which is why
# it is generated here rather than by hand (FR-1006):
sed -i "s|^PIGEON_HEADERS_KEY=.*|PIGEON_HEADERS_KEY=$(openssl rand -base64 32)|" .env
$EDITOR .env   # POSTGRES_PASSWORD, PIGEON_APP_PASSWORD, PORTAL_SESSION_SECRET,
               # PIGEON_PRODUCER_PORTAL_SESSION_SECRET - compose has no defaults
               # for these four and refuses to resolve while any is empty - and
               # PIGEON_INSTANCE_ADMIN_PASSWORD, which the core refuses at boot
               # when it is empty or a known-weak default (FR-101, OPS-7).

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

Bootstrap a tenant and deliver one event. The walkthrough registers a public echo
service as the receiver, because a live endpoint must be a public `https` URL
(FR-402) - point it at a receiver of your own for real traffic.

```sh
B=http://localhost:8080

# 1. The bootstrap credential is the instance admin (FR-101): admin@pigeon.local
#    with PIGEON_INSTANCE_ADMIN_PASSWORD. Its first login must change it.
ADMIN=$(curl -s -X POST $B/v1/session -H 'content-type: application/json' \
  -d '{"email":"admin@pigeon.local","password":"<PIGEON_INSTANCE_ADMIN_PASSWORD>"}' | jq -r .token)
curl -s -o /dev/null -w '%{http_code}\n' -X POST $B/v1/session/password \
  -H "authorization: Bearer $ADMIN" -H 'content-type: application/json' \
  -d '{"old_password":"<PIGEON_INSTANCE_ADMIN_PASSWORD>","new_password":"<a new one>"}'   # 204

# 2. A producer, plus the invitation for its first owner, returned once (FR-102).
PROD=$(curl -s -X POST $B/v1/admin/producers -H "authorization: Bearer $ADMIN" \
  -H 'content-type: application/json' \
  -d '{"slug":"acme","name":"Acme","owner_email":"owner@acme.test"}')
INVITE=$(echo "$PROD" | jq -r .owner_invite.token)
curl -s -X POST $B/v1/invites/accept -H 'content-type: application/json' \
  -d "{\"token\":\"$INVITE\",\"password\":\"<owner password>\"}"

# 3. Sign in as that owner. Producer routes carry the plane in ?env=test|live:
#    the planes are separate data, not a filter (FR-103).
OWNER=$(curl -s -X POST $B/v1/session -H 'content-type: application/json' \
  -d '{"email":"owner@acme.test","password":"<owner password>"}' | jq -r .token)

# 4. An event type, a consumer, and its endpoint, which must answer the ownership
#    challenge before it may receive live deliveries (FR-403; test is not gated).
curl -s -X POST "$B/v1/producer/event-types?env=live" -H "authorization: Bearer $OWNER" \
  -H 'content-type: application/json' \
  -d '{"name":"acme.created","routing_mode":"consumer","payload_schema":{"type":"object"}}'
CONSUMER=$(curl -s -X POST "$B/v1/producer/consumers?env=live" -H "authorization: Bearer $OWNER" \
  -H 'content-type: application/json' -d '{"ref":"acme","name":"Acme"}' | jq -r .id)
EP=$(curl -s -X POST "$B/v1/producer/consumers/$CONSUMER/endpoints?env=live" \
  -H "authorization: Bearer $OWNER" -H 'content-type: application/json' \
  -d '{"name":"httpbin","url":"https://httpbin.org/post"}')
ENDPOINT=$(echo "$EP" | jq -r .id)
curl -s -o /dev/null -w '%{http_code}\n' -X POST \
  "$B/v1/producer/consumers/$CONSUMER/endpoints/$ENDPOINT/verify?env=live" \
  -H "authorization: Bearer $OWNER" -H 'content-type: application/json' \
  -d "{\"token\":\"$(echo "$EP" | jq -r .verification_token)\"}"          # 204
curl -s -X POST "$B/v1/producer/endpoints/$ENDPOINT/subscriptions?env=live" \
  -H "authorization: Bearer $OWNER" -H 'content-type: application/json' \
  -d '{"event_type":"acme.created"}'

# 5. A producer API key (FR-201): returned once, hashed at rest, revocable.
KEY=$(curl -s -X POST "$B/v1/producer/keys?env=live" -H "authorization: Bearer $OWNER" \
  -H 'content-type: application/json' -d '{}' | jq -r .key)

# 6. Publish. 202 is the commit: the event and its outbox row are one transaction
#    (FR-604), and the relay fans out from there.
curl -s -X POST $B/v1/events -H "authorization: Bearer $KEY" \
  -H 'content-type: application/json' \
  -d '{"event_type":"acme.created","consumer_ref":"acme","payload":{"id":"evt_1"}}'

# 7. What happened to it. The relay drains the outbox on a cadence and the worker
#    polls for due deliveries, so give them a moment.
sleep 5
curl -s "$B/v1/producer/deliveries?env=live" -H "authorization: Bearer $OWNER" | jq
```

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

## Where the rules live

| Document | What it is |
|---|---|
| [`REQUIREMENTS.md`](REQUIREMENTS.md) | the contract: numbered FR/NFR/OPS/SB/D requirements, and §16's acceptance criteria |
| [`SPEC.md`](SPEC.md) | how this tree implements them, the decisions taken where the requirements were silent (A-1…A-11), and the gap list |
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
S12 (with S9a for the producer portal) and every one of them has landed; its State
column is stale, though - it still shows a dash for S2 and later - so read the
tree, not the column.

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
- `make vet test` before pushing, with `PIGEON_TEST_DATABASE_URL` pointing at a
  disposable Postgres: `make test` runs the DB-backed suite and refuses to run
  without that DSN (its `-p 1` is CI's, for the same reason). `make test-integration`
  is the same target under its older name.
- CI (`.github/workflows/ci.yml`) runs the `test` job on `ubuntu-24.04` with
  Postgres 16 and Redis 7 service containers:
  `go test -count=1 -race -p 1 -v -timeout 25m ./...`. A `--- SKIP` fails the job,
  and a fixed list of acceptance-criterion test names must each print `--- PASS`,
  so renaming or removing one fails the build. The review loop itself is in
  [`docs/worker-git.md`](docs/worker-git.md): a card is done when the reviewer
  merges the PR and completes the card, not when the branch is pushed.
