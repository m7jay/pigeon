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
pigeon: unsafe configuration: OPS-8 connection budget: 4 replicas x 10 pool + 5
reserved = 45 > max_connections 44. Lower PIGEON_DB_MAX_CONNS, lower
PIGEON_CORE_REPLICAS, or put pgbouncer in front of Postgres
```

A replica that starts anyway and then steals another replica's connections is an
outage nobody can attribute, which is why this is a startup refusal (the same
class as OPS-7) rather than a warning.

## One replica

```
core (1) ──► Postgres        PIGEON_CORE_REPLICAS=1, PIGEON_DB_MAX_CONNS=10
             max_connections ≥ 15 (10 + 5 reserved, Postgres' own default is 100)
```

This is the reference deployment in `compose.yaml`. No pooler.

## More than one replica

Past one core replica, put **pgbouncer** (or an equivalent pooler) in front of
Postgres. `docker compose up --scale core=N` (OPS-9) must not multiply the
connection count against Postgres directly:

```
core × N ──► pgbouncer ──► Postgres        PIGEON_CORE_REPLICAS=N
                                          PIGEON_DB_MAX_CONNS=10 (per replica,
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

## Stateless and singleton processes (OPS-9)

| Process | Scale | Why |
|---|---|---|
| API (HTTP handlers) | N, stateless | holds no state between requests; the session and key tables are the truth |
| delivery workers | N, stateless | leases in Postgres, attempt buffer in Redis (D-1) |
| relays | N, safe by `SKIP LOCKED` | one row is leased by one relay |
| **migrator** | 1 | advisory lock, but only one process should contend: set `PIGEON_MIGRATIONS_ON_BOOT=false` on replicas 2..N |
| **sweeper** | 1 active | leadership by advisory lock (D-7); every other replica is a standby |

## Verifying a deployment

`pigeon doctor` (OPS-4, S10) reports the same arithmetic. Until it exists, the
boot refusal above is the check, and `show max_connections` plus `pg_stat_activity`
grouped by `application_name = 'pigeon-core'` answers "what is actually connected".
