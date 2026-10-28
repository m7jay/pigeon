# Retention and storage sizing

`deliveries` and `attempts` are range-partitioned by day (UTC), and retention works
by dropping whole day partitions. There is no `DELETE` on either table: deleting
rows leaves bloat and vacuum pressure on the same disk the delivery path needs
(SB-2). A partition is only ever dropped once its day has fully elapsed, so a drop
can never remove a row a request is still writing.

This page gives an operator the per-row numbers to size retention for their own
volume (SB-8), and states plainly what sustained throughput does to the configured
windows (SB-5).

## Per-row cost

Measured on the row shapes this schema writes; the dominant term is the captured
response body, which `PIGEON_DELIVERY_MAX_BODY_BYTES` truncates (default 2048,
configurable down to 0, SB-4).

| Row | Fixed overhead | Variable | Planning figure |
|---|---|---|---|
| `deliveries` | ~200 B (ids, event type, endpoint, state, timestamps) | event type, URL, headers copied per subscription | **≈ 400 B** |
| `attempts` | ~150 B (ids, lease, status, timestamps) | response body, first 2 KB | **≈ 300 B** |
| `delivery_dedupe` (the dedupe archive) | **≈ 200 B** (24 B tuple header, three 26-character text ids, one `timestamptz`, plus the primary-key btree entry) | — one row per `(endpoint, event)` pair, no payload | **≈ 200 B** |
| `events` (not partitioned yet) | ~150 B | payload (bounded by the ingest limit) | ≈ 300 B + payload |

A successful first attempt writes **no attempt row** (§8.1), so the common case is
one `deliveries` row per delivery, not one plus an attempt. The table above is the
*worst* case — every attempt recorded — because retention must never be sized
optimistically.

Partitioning adds no per-row bytes; it adds one relation per day per table, which
is negligible against these numbers. Indexes on the partitioned parents are
inherited by each day's partition, so index cost scales with retained rows the same
way the heap does.

## Sizing from throughput

Rows/day ≈ `deliveries/s × 86,400 × (1 + attempts per delivery)`. At the worst case
of one attempt row per delivery:

| Load | Rows/day | Disk/day | Fits 40 GB for |
|---|---|---|---|
| 100 deliveries/s | 8.6 M | ~6 GB | ~6 days |
| 1,000 deliveries/s | 86 M | ~55 GB | < 1 day |
| 5,000 deliveries/s | 432 M | ~280 GB | hours |

So the 40 GB VPS figure is a **burst** or **sustained-with-short-retention**
number, not a 24/7 figure. The retention windows are what make it viable, and the
platform shortens them automatically as the disk fills: sustained throughput above
roughly 100 deliveries/s means the age defaults below stop being the binding
constraint and the disk budget takes over.

## The two limits, and which wins

Retention has two independent limits:

1. **Age windows** — `deliveries 14 d`, `attempts 7 d`, `events 14 d` by default
   (`PIGEON_RETENTION_DELIVERIES`, `…_ATTEMPTS`, `…_EVENTS`, Go duration strings
   such as `336h`). The dedupe archive has its own window, 30 days by default,
   set with `PIGEON_RETENTION_DEDUPE` in the same duration syntax.
2. **Disk budget** — `retention.max_disk_pct`, default 80% of the volume that holds
   the Postgres data directory (`PIGEON_RETENTION_MAX_DISK_PCT`).

The sweeper checks the disk **first** (SB-3). At or above the budget it drops the
oldest droppable partition, one per tick, and logs at warning level — regardless of
age. Only when the volume is under budget does it enforce the age windows. This is
deliberate: the age window is a promise about how long history is kept, but the disk
budget is the constraint that keeps the database writable. A busy deployment will
therefore see history disappear sooner than 14 days, and the warning log names what
was dropped and why.

The archive's partitions are dropped **before** any `deliveries` partition under
pressure: `delivery_dedupe` is derived from `deliveries`, so dropping the source first
would take keys with it and shrink the archive for nothing.

## The dedupe archive (`delivery_dedupe`)

FR-903 promises a duplicate answer for 30 days, and delivery history is kept for 14, so
the 16-day gap is held by `delivery_dedupe`: one row per `(endpoint, event)` pair,
written by the sweeper at the moment it drops the partition those pairs lived in, and
carrying the pair's **first** delivery id and its original timestamp.

- **The copy and the drop are one transaction.** An ordinary (age-driven) drop copies
  the partition's keys and drops it in a single transaction, so at every instant either
  the `deliveries` row or the archive row answers a key. A failed copy aborts both: the
  partition stays, the next tick retries, and nothing is lost.
- **Only a disk-pressure drop may skip the copy**, because the space is what is being
  reclaimed. That loses answers, so it is logged at warning level naming the partition
  and the day range it covered. There are exactly two such loss paths and one pressure
  event can produce both: a `deliveries` partition dropped before its copy ran (its keys
  go, and it may still be inside its first 14 days), and a `delivery_dedupe` partition
  dropped ahead of `dedupe_retention` (whole archived day-ranges, oldest first).
- **No `DELETE` anywhere** (SB-2): the archive is dropped by partition at
  `dedupe_retention`, default 30 days.
- **The answer is read through one view.** `delivery_dedupe_all` is a `UNION` over live
  `deliveries` and the archive (`security_invoker`, so tenant isolation still applies -
  also to each archive partition, which the partition primitive flags and attaches the
  policy to), and it groups by the pair, so a key answerable from both sides - or from
  two archived days - still counts once and keeps its original timestamp. The support
  path never consults Redis for a dedupe answer.
- **Its size is measured, and it is a gauge.** `pigeon_dedupe_archive_bytes` on
  `/metrics` (type `gauge`: it is a level, so do not `rate()` it) is
  `pg_total_relation_size` summed over the archive's partitions. The sum is not
  optional: a partitioned table has no storage of its own, so
  `pg_total_relation_size('delivery_dedupe')` returns **0** and would report a
  multi-gigabyte archive as empty. The same figure travels in `GET /v1/meta/usage` as
  `dedupe_archive_bytes`, with `retention_days.delivery_dedupe` beside it.
- **Cost.** ~200 B per pair; AC-46's 100,000-pair probe measured 217 B, so the planning
  figure in the table above is honest rather than optimistic. At the reference
  35 deliveries/s the 16-day tail is ~48 M pairs, about 10 GB - watch
  `pigeon_dedupe_archive_bytes` against the disk budget, and remember the archive's
  partitions are the first thing a pressure drop takes.

Today's and future partitions are never candidates, so retention cannot delete data
being written now. If the budget is exceeded and nothing is old enough to drop, the
sweeper gives up on dropping and says so, at error level —
`retention: over disk budget but no partition may be dropped; ingest is shedding` —
while `over_budget` stays true in `GET /v1/meta/usage`; ingest sheds instead (SB-7).

## Choosing the volume figure

The gauge reads, in order:

- `PIGEON_DISK_DIR` — the filesystem holding the data directory, via `statfs`
  (source `statfs:<dir>`, e.g. `statfs:/var/lib/postgresql/data`, in the usage
  payload). Use this when core and Postgres share a volume, which is the normal
  single-host deployment.
- `PIGEON_DISK_BYTES` — a number to compare pigeon's own `pg_database_size` against
  (source `database`), for a deployment where core cannot see the data directory's
  filesystem.
- Neither — source `none`: the usage endpoint answers `503` and ingest never sheds.
  This is a documented degraded mode rather than a fabricated 0%.

`GET /v1/meta/usage` and `GET /metrics` are admin-only infrastructure telemetry, not
tenant routes. The payload carries the volume figure, the percentage, whether it is
over budget, the per-table retention windows and the live partition catalogue; a
failed disk read keeps the last good figure and sets `read_error`, so an operator is
never shown a calm number that stopped updating.

## Operational checks

- The drops are visible in `GET /v1/meta/usage` (`dropped_partitions_total`), in
  `pigeon_partition_drops_total` on `/metrics`, and at warning level in the log.
- The dedupe archive's measured size is `pigeon_dedupe_archive_bytes` on `/metrics`
  (a gauge) and `dedupe_archive_bytes` in `GET /v1/meta/usage`. Alert on it approaching
  the disk budget rather than on the percentage alone: it is the derived table, and it
  is the first thing a pressure drop removes.
- `pigeon_publishes_shed_total` rises when ingest is refusing with `429`; a flat
  line while `over_budget` is true means no producer is currently publishing.
- Only one replica sweeps: leadership is a Postgres advisory lock, so extra replicas
  are standbys. A dead holder is replaced within about 12 s (bounded by TCP
  keepalives), comfortably inside the 30 s the design allows.
- A partition drop takes its own transaction-scoped advisory lock with a 5 s
  `lock_timeout`, so two drops cannot run concurrently even across processes; a
  contended drop returns without dropping and is retried on the next tick.
