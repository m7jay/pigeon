# Deployment topology and the connection budget (OPS-8, OPS-9, OPS-10)

The core opens its own Postgres pool per replica. That is the whole story of
OPS-8: connections are consumed by replica count before CPU ever becomes the
limit, and Postgres refuses new connections rather than slowing down, so the
first symptom of an oversized deployment is an outage with no single culprit.

## The arithmetic

```
core_replicas × PIGEON_DB_MAX_CONNS + PIGEON_DB_RESERVED_CONNS ≤ max_connections
```

`PIGEON_DB_RESERVED_CONNS` covers the connections the deployment needs outside the
core's pools: migrations (one dedicated connection while the advisory lock is
held), `pg_dump` for backups (OPS-5), and an operator's `psql` during an incident.

The core **checks this at boot** against the server's real `max_connections` and
refuses to start when it does not fit:

```
pigeon: unsafe configuration: OPS-8 connection budget: 4 replicas x 40 pool + 5
reserved = 165 > max_connections 100. Lower PIGEON_DB_MAX_CONNS, lower
PIGEON_CORE_REPLICAS, or put pgbouncer in front of Postgres
```

A replica that starts anyway and then steals another replica's connections is an
outage nobody can attribute, which is why this is a startup refusal (the same
class as OPS-7) rather than a warning.

## What the pool bounds, now that it is a write budget (S12, FR-818)

`PIGEON_DB_MAX_CONNS` is not an ingest queue and never was: it bounds **concurrent
database writes**, and since S12 the write budget's width follows it
(`PIGEON_WRITE_QUEUE_DEPTH=0`, the default, means "follow the pool"). One slot of
the budget is held for one database write - one publish transaction on the ingest
path, one batch transaction on the control-state path - so the pool and the budget
are two names for the same ceiling, and counting them separately would count the
same connection twice.

That is why the shipped default moved 10 to 40 in S12. Pool 10 with 40 concurrent
publishers was measured to serialise a single publish for **10.2 s**; pool 40
measured **86 ms** on the same 2 vCPU box (`docs/load-test.md`). A ceiling that
low is not a knob an operator is expected to find, it is a defect: before S12 the
publish path took a pool connection with no deadline, so past that ceiling the
instance **parked** rather than shedding, and a 30-minute run at 250 events/s
recorded zero 429s with a p99 that grew from 3,593.4 ms to 4,281.6 ms.

The same arithmetic at the new default, against a default Postgres
(`max_connections=100`, reserved 5):

| Replicas | Budget | Fits `max_connections`? |
|---|---|---|
| 1 | `1 × 40 + 5 = 45` | yes |
| 2 | `2 × 40 + 5 = 85` | yes |
| 4 | `4 × 40 + 5 = 165` | **no** - the boot refusal above fires; use pgbouncer, or lower `PIGEON_DB_MAX_CONNS` for that deployment |

## One replica

```
core (1) ──► Postgres        PIGEON_CORE_REPLICAS=1, PIGEON_DB_MAX_CONNS=40
             max_connections ≥ 45 (40 + 5 reserved, Postgres' own default is 100)
```

This is the reference deployment in `compose.yaml`. No pooler.

## More than one replica

Past one core replica, put **pgbouncer** (or an equivalent pooler) in front of
Postgres. `docker compose up --scale core=N` (OPS-9) must not multiply the
connection count against Postgres directly:

```
core × N ──► pgbouncer ──► Postgres        PIGEON_CORE_REPLICAS=N
                                          PIGEON_DB_MAX_CONNS=40 (per replica,
                                          against pgbouncer... see below)
```

Two configurations are legitimate, and they are not the same number:

| Topology | What `PIGEON_DB_MAX_CONNS` counts against | What must fit `max_connections` |
|---|---|---|
| No pooler | Postgres directly | `N × pool + reserved` |
| pgbouncer, `pool_mode=session` | pgbouncer (one Postgres connection held per client while it is connected) | pgbouncer's `max_db_connections`, not the core's pool |
| pgbouncer, `pool_mode=transaction` | pgbouncer's client limit | pgbouncer's `default_pool_size` per database |

The startup check computes `N × pool + reserved` and compares it to
`max_connections`, because it cannot see whether a pooler is in the path. In a
pooled topology set `PIGEON_DB_MAX_CONNS` to the pool size the **core** should
use against the pooler, and set `PIGEON_DB_MAX_CONNS` × `PIGEON_CORE_REPLICAS`
to something pgbouncer can absorb (`max_client_conn`), while pgbouncer's own
`default_pool_size` is what has to fit Postgres' `max_connections`.

`pool_mode=transaction` is required for a core with more replicas than Postgres
can serve connections for. It is safe here because the core does not use session
state: every request sets its scope with `set_config(..., true)`, which is
transaction-local, and nothing relies on a session-level `SET`, prepared
statements outside a transaction, or `LISTEN` on a pooled connection. (The relay's
`LISTEN/NOTIFY` in S2 will need its own dedicated, non-pooled connection — noted
here so the pooler decision does not silently break it.)

## Database roles

Two roles, and the split is a security property rather than tidiness:

| Role | Owns objects | Used by | Privileges |
|---|---|---|---|
| `pigeon_migrator` | yes | the migration path (`PIGEON_MIGRATE_DATABASE_URL`), `psql` during an incident | owns the schema; creates every table and function |
| `pigeon_app` | no | the core's request path (`PIGEON_DATABASE_URL`) | `select/insert/update/delete` on tables, `execute` on functions, no `SUPERUSER`, no `BYPASSRLS`, no ownership |

PostgreSQL exempts a table's owner from row-level security. If one role both
migrates and serves, every policy in `internal/db/migrations` is inert in the
deployed system — the suite can still prove isolation against `pigeon_app` while
production has none. That is why `compose.yaml` carries two URLs, why
`deploy/postgres/01-roles.sql` creates `pigeon_app` without privileges, and why
`config.Validate` refuses a configuration that points both URLs at the same role.

`pigeon_app` may still `set_config('pigeon.instance_admin', 'on')`; that GUC is a
bug-and-misrouting gate, not a privilege boundary (SPEC.md), and the tenant
policies do not consult it. What the split guarantees is that the tenant policies
apply at all, and that the runtime role cannot create or alter the objects it is
constrained by.

## Stateless and singleton processes (OPS-9)

| Process | Scale | Why |
|---|---|---|
| API (HTTP handlers) | N, stateless | holds no state between requests; the session and key tables are the truth |
| delivery workers | N, stateless | leases in Postgres, attempt buffer in Redis (D-1) |
| relays | N, safe by `SKIP LOCKED` | one row is leased by one relay |
| producer portal | N, stateless | thin client of the core API: no database, no workers, no state between requests; may be omitted entirely (AC-40) |
| consumer portal | N, stateless | thin client of the core API: no database, no workers, no state between requests; may be omitted entirely (UI-19, AC-40) |
| **migrator** | 1 | advisory lock, but only one process should contend: set `PIGEON_MIGRATIONS_ON_BOOT=false` on replicas 2..N |
| **sweeper** | 1 active | leadership by advisory lock (D-7); every other replica is a standby |

The portals are deployables of their own - three in total with the core (§14) -
and neither is part of the core process: each holds a connection to the core's
HTTP API and to nothing else, so neither appears in the connection budget above
and neither can hold a Postgres connection out of the pool. The producer portal
(`cmd/producer-portal`, UI-1…UI-11) is producer-facing; the consumer portal
(`cmd/consumer-portal`, UI-12…UI-19) is consumer-facing; both ship as binaries in
the one image and start by overriding the container's entrypoint: `compose.yaml`
names the binary each service runs, and the image's own entrypoint is the core.
Either may be omitted or removed entirely (AC-40): the core keeps serving the
same API with the same server-side scoping, and the audience that had a UI
simply has none.

## Egress addresses (FR-1007)

A receiver that firewalls its ingress needs to know which addresses your
deliveries come from. `GET /v1/meta/egress-ips` is the answer, and it exists so
that list is not folklore passed between support tickets:

```sh
curl -s -H "Authorization: Bearer $SESSION" https://pigeon.example/v1/meta/egress-ips
# {"ips":["203.0.113.7"],"source":"configured"}
```

- **Set `PIGEON_EGRESS_IPS`** to the addresses (or CIDR blocks) receivers will
  actually see. Behind NAT, a load balancer, a proxy or a container network the
  public source address is not one the process can discover, so this is the only
  correct answer there. A malformed entry is refused at boot (OPS-7): the list is
  someone else's allowlist, and a typo surfaces as their delivery outage.
- **Leave it empty on a flat network.** The route then reads the host's own
  non-loopback interfaces and reports `"source":"discovered"` with a note saying
  so, including the knob that fixes it. That is deliberately not presented as
  authoritative - an allowlist built from a container's private address is a
  thing that looks right and never matches.
- **Any authenticated session may read it** (`kindSession`). The list describes
  the deployment rather than a tenant, and the people who need it are whoever
  configures an allowlist - a producer member adding an endpoint and a consumer
  user running one alike.

## Verifying a deployment

`pigeon doctor` (OPS-4, OPS-10, OPS-11) reports the same arithmetic without
writing anything: Postgres reachability and the connection budget, Redis and its
AOF setting, clock skew, disk headroom, and `ulimit -n` against the concurrency
the shipped caps need. It exits 0 when nothing failed, 1 on a failure, 2 on
warnings only and 3 when it could not run; `--json` emits the same facts.
`show max_connections` plus `pg_stat_activity` grouped by
`application_name = 'pigeon-core'` still answers "what is actually connected".
