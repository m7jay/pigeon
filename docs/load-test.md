# The load test

This document is the measured half of the throughput claim. §8.1 is a capacity
model - a row-level arithmetic argument for what 2 vCPU / 4 GB should hold - and
NFR-4 makes the rated load a property of the machine class rather than one number
asserted everywhere. This is where the arithmetic meets a stopwatch.

The claim being published:

> **250 events/s × 10 subscriptions per event = 2,500 deliveries/s on 2 vCPU / 4 GB
> (NFR-4's reference machine), measured.**

Two units are always reported together, because they are not the same workload and
conflating them is the defect §8.1's "Capacity basis" paragraph fixes: ingest is two
rows per event and stays cheap, while delivery is `subscriptions × events/s` rows
plus one completion update each. An "events/s" number without a subscription count
means nothing, and a "deliveries/s" number without the events that produced it
cannot be checked against §8.1.

## The machine it was measured on

A throughput number without this block is marketing. Every field is copy-pasteable
output from the machine that ran the test.

| | |
|---|---|
| CPU | 2 vCPU, AMD EPYC 9354P (32-core host, 2 cores visible in this container) |
| Memory | 7.9 GB total (the §8.1 "2 vCPU / 4 GB" row is the class being approximated; this box is 2× the RAM, which cannot help a CPU- or write-bound path and is recorded rather than rounded away) |
| Disk | 96 GB, 42% used, `ext4` on `/dev/sda1` |
| Kernel | Linux 6.8.0-147-generic x86_64 |
| Go | go1.27.3 linux/amd64 |
| Postgres | 16.2, same host, local socket |
| Redis | 7.2.5, same host, `appendonly yes`, `appendfsync everysec` |
| Revision | `b65d9c7` on `wt/t_b5272d2d-hardening` (rebased onto `f4f20dc`), the binary built from that tree — see *What this run does not prove* |

The instance, the database, the broker, the load generator **and** the sink receiver
all run on this one box. §16 asks for the generator to be off the machine under
test; that is not possible here, so the numbers are a **lower bound** and the
deviation is stated rather than hidden. The generator is deliberately a separate
process that only speaks HTTP to the instance, and it counts what the instance
answered.

## The protocol, as implemented

§16's shape, enforced by `cmd/loadgen` rather than by the person running it:

1. **Warm-up, excluded from every reported number.** Default 5 minutes. JIT, page
   cache, connection pools and the first partition creation all land here.
2. **Measured phase.** Default 30 minutes. Samples are taken once a second.
3. **Criteria are evaluated over the last 10 minutes of the measured phase**, so a
   run that degrades after minute 12 is reported as the degradation it is.
4. **Rates come from the phase's configured length, not from the span of the
   samples it happened to collect.** A rate computed from observed samples silently
   improves whenever the collector is starved, which is exactly when the system is
   in trouble.
5. **Reconciliation is against the database, not the generator's own counter.** The
   number of events the instance actually accepted is `select count(*) from events`
   (and `outbox`, and `deliveries`) over the window; the generator's counter is
   reported separately as *attempted*. The window is widened to include the warm-up
   because a window drawn on the measured phase alone is off by the in-flight set at
   both edges (measured: 45 rows on a 10,000-event run).
6. **`-settle` (default 60 s)** exists because fan-out is asynchronous: without it
   the delivery count describes the relay's drain speed at the moment the generator
   stopped, not the instance's fan-out.

Run it:

```sh
# 1. Provision a producer, a consumer-routed event type and N verified endpoints.
go build -o /tmp/loadgen ./cmd/loadgen
/tmp/loadgen seed -db "$PIGEON_DATABASE_URL" \
  -producer load -event-type load.created -consumer-ref load-consumer -endpoints 10 \
  -url http://127.0.0.1:9001/hook          # prints PIGEON_LOADGEN_KEY= once

# 2. A receiver that answers fast and honestly. A load test with a slow receiver
#    measures the retry path, not the delivery path.
/tmp/loadgen sink -addr 127.0.0.1:9001 &

# 3. The run. -out writes the JSON report; the config block is part of the artifact.
/tmp/loadgen run -base http://127.0.0.1:8080 \
  -event-type load.created -consumer-ref load-consumer \
  -rate 250 -subs 10 -warmup 5m -measure 30m -evaluate 10m -settle 60s \
  -concurrency 512 -label "2vCPU-4GB-1replica-defaults" -out run.json

# 4. Compare a measurement against §8.1's table for its machine class (AC-30's
#    "within 20%" is this command's exit status).
/tmp/loadgen compare -report run.json -class 2vCPU-4GB
```

The seeded endpoints are written with `internal = true`, FR-402's operator escape
hatch, so a loopback sink is reachable. Using the flag is honest; weakening the
SSRF check for the sake of a test would not be.

### The rate limiter is a safety limit, not the ceiling

The shipped defaults are FR-207's per-key 50 rps and per-producer 200 rps. They are
"shed the abusive client" numbers, not a statement about capacity, and §16's rated
load is above them: a 250 events/s run is refused at the default per-producer 200.
The run therefore raises `PIGEON_RATE_LIMIT_PER_KEY_RPS` and
`PIGEON_RATE_LIMIT_PER_PRODUCER_RPS` explicitly, and the values used are recorded in
the report's config block - they are part of the artifact, not a footnote. A
publish that is shed by a *capacity* limit is the `429` AC-30 asserts; a publish
shed by a rate limit says nothing about the machine.

## What this run measured

<!-- MEASURED-BLOCK -->

## What this run does not prove

Reported here so the gap is a sentence rather than a discovery:

- **The delivery leg of AC-31/NFR-2 is measured on a tree where it is known to be
  broken, and is expected to fail.** The instance runs `PIGEON_DELIVERY_WORKERS`'s
  default of **1** (`internal/config/config.go:165`), so a single capped endpoint
  parks the process's only dispatch loop (`internal/delivery/delivery.go:502-514`
  and `:521-534`). The fix is S11's, card `t_558a9f59`; this card does not tune the
  test or the numbers to hide it. The ingest leg is unaffected by it, which is why
  the run is published with both numbers and the cause between them.
- **AC-34 (2,500 concurrent attempts on 2 vCPU: goroutines bounded, FD usage within
  the documented `ulimit`, CPU largely idle) is not measured by this run.** Its
  independent evidence in this slice is `pigeon doctor`'s `ulimit.no_file` and
  `db.connection_budget` checks plus the delivery worker's pool bounds; a dedicated
  probe is the honest form and is listed as outstanding.
- **AC-43 (1 → 2 → 4 replicas scaling ≥ 80% per doubling, and no improvement above
  the Postgres ceiling)** needs more than one host. This run is a single replica and
  cannot speak to it.
- **AC-16's NFR-1 leg (1,000 events/s ingest-only on 8 vCPU / 16 GB)** is not this
  machine's rating, and is not measured here.
- **A first full run was discarded, and why it is worth saying so.** It was taken
  while the test suites were also running on the same 2 vCPU box, which is a
  measurement error: the instance shared its cores with `go test`. It also exposed
  two defects in the harness rather than in the instance - samples were filed under
  the time the pacer had *scheduled* them, so a pacer that fell behind wall clock
  filed every late sample under an early phase and the graded window came out empty;
  and an empty window could pass a criterion vacuously. Both are fixed (see
  `fix(loadgen): grade the window the protocol asks for`), the first run's latencies
  are therefore not usable, and the numbers below are the rerun on a quiet box.
- **Which tree was measured.** The binary was built from this branch at `b65d9c7`,
  which is the same source tree as the published head apart from the documentation
  commits made while the run was in flight. Nothing that runs on the ingest,
  fan-out or delivery path differs between the two.

## The §16.5 fault-injection matrix

Separate document, separate job: the seven-scenario matrix lives in
`internal/faults/faults_test.go` behind the `faults` build tag, and
`.github/workflows/nightly-faults.yml` runs it every night against a real Postgres
and Redis, plus two process-level scenarios against the real binary (SIGKILL a
worker mid-flight; stop Postgres for 30 s). It is behind a build tag so the pull
request gate stays fast and deterministic while the chaotic half runs on a schedule
that can afford it.
