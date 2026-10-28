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
| Revision | `b65d9c7` on `wt/t_b5272d2d-hardening`. **Not the published head**: the base then advanced by FR-905's dedupe archive and the attempt-buffer strand sweep. The delivery and ingest hot path is identical between the two; see *What this run does not prove* |

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
  -producer load -event-type load.created -routing consumer -endpoints 10 \
  -url http://127.0.0.1:9001/hook          # prints PIGEON_LOADGEN_KEY= once

# 2. A receiver that answers fast and honestly. A load test with a slow receiver
#    measures the retry path, not the delivery path.
/tmp/loadgen sink -addr 127.0.0.1:9001 &

# 3. The run. -out writes the JSON report; the generator-side config block is part
#    of that artifact. -db is what turns the reconciliation into a database count
#    instead of the generator's own tally - omit it and the report says the
#    reconciliation did not run rather than quietly grading the generator.
/tmp/loadgen run -base http://127.0.0.1:8080 -db "$PIGEON_DATABASE_URL" \
  -key "$PIGEON_LOADGEN_KEY" \
  -event-type load.created -consumer-ref load-consumer \
  -rate 250 -subs 10 -warmup 5m -measure 30m -evaluate 10m -settle 60s \
  -concurrency 512 -label "2vCPU-4GB-1replica-defaults" -out run.json

# 4. AC-43: compare a run set across replica counts. Two reports minimum, in
#    ascending replica order; the exit status is the verdict. Note what this
#    command does and does not decide: it is the SCALING comparison. AC-30's
#    "the ceiling matches §8.1 within 20%" has no implementation in this harness,
#    so it is not decided here - see "What this run does not prove".
/tmp/loadgen compare -label run-1replica.json -label run-2replica.json -label run-4replica.json
```

The seeded endpoints are written with `internal = true`, FR-402's operator escape
hatch, so a loopback sink is reachable. Using the flag is honest; weakening the
SSRF check for the sake of a test would not be.

### The rate limiter is a safety limit, not the ceiling

The shipped defaults are FR-207's per-key 50 rps and per-producer 200 rps. They are
"shed the abusive client" numbers, not a statement about capacity, and §16's rated
load is above them: a 250 events/s run is refused at the default per-producer 200.
The run therefore raises `PIGEON_RATE_LIMIT_PER_KEY_RPS` and
`PIGEON_RATE_LIMIT_PER_PRODUCER_RPS` explicitly. Those are **instance** settings,
not generator settings, so the JSON report cannot carry them: its config block
records what the generator was told. The instance side of the artifact is the
launcher below, which is what the published run used.

```sh
PIGEON_DB_MAX_CONNS=40 \
PIGEON_ATTEMPT_LOG_MODE=buffered \
PIGEON_RATE_LIMIT_PER_KEY_RPS=5000    PIGEON_RATE_LIMIT_PER_KEY_BURST=10000 \
PIGEON_RATE_LIMIT_PER_PRODUCER_RPS=5000 PIGEON_RATE_LIMIT_PER_PRODUCER_BURST=10000 \
PIGEON_REDIS_URL=127.0.0.1:6390 PIGEON_HTTP_ADDR=127.0.0.1:8080 \
  ./pigeon
```

A publish that is shed by a *capacity* limit is the `429` AC-30 asserts; a publish
shed by a rate limit says nothing about the machine.

**`PIGEON_DB_MAX_CONNS` is the one of those that is a finding, not a knob choice.**
The shipped default of 10 cannot carry the rated load on this machine class: with
the pool at 10 a single publish took 10.2 s on a quiet box (40 concurrent ingest
requests queued behind 10 connections), and with the pool at 40 the same publish
takes 86 ms. The rated run raises it to 40, which is inside OPS-8's arithmetic for
one replica against a default Postgres (`1 × 40 + reserved <= 100`). Either that
default should scale with the documented ceiling or the deployment doc should say
the pool is a rated-load knob; it is recorded here rather than changed, because the
default is not this slice's to move.

## What this run measured

The artifact is `run3-250x10.json`, taken 2026-10-28 11:30:29Z → 12:06:30Z (36
minutes wall clock) on the machine above, against revision **`b65d9c7`** on
`wt/t_b5272d2d-hardening`.

| Phase | Window | Attempted | Accepted | Shed | Accepted/s | p50 | p99 | max |
|---|---|---|---|---|---|---|---|---|
| warm-up (excluded) | 5 m | 69,606 | 69,606 | 0 | 232.0 | 124.8 ms | 3,792.1 ms | 4,792.8 ms |
| measured | 30 m | 369,393 | 369,391 | 0 | 205.2 | 1,748.0 ms | 4,409.5 ms | 6,334.6 ms |
| **graded window (last 10 m)** | 10 m | 124,265 | 124,264 | 0 | **207.1** | 294.9 ms | **4,281.6 ms** | 5,735.4 ms |

The generator's pacer lagged by 17,265 events in the measured phase - events that
waited for a free client slot - so **205.2 events/s is the rate the generator
achieved under this box's contention, not a ceiling the instance imposed.**

Reconciliation, counted in the database over the widened window:

| | |
|---|---|
| the generator accepted | 438,997 |
| `select count(*) from events` | 438,998 |
| `select count(*) from outbox` | 438,998 |
| `select count(*) from deliveries` | 177,170 |
| **delivery unit** | **82.0 deliveries/s** at 10 subscriptions per event |
| commit batching observed | 0.17 xact/s at 1,638.74 rows/transaction |

The event and outbox counts are one row *above* the generator's tally (0.0002%),
which is the warm-up boundary and not a lost event - the count moved in the safe
direction. The harness reports any difference as a mismatch, and does here.

**Neither figure is the claim at the top of this document.** The claim is
250 events/s × 10 subscriptions = 2,500 deliveries/s. This run reached

- **207.1 events/s of the 250 offered** (82.8%), and
- **82.0 deliveries/s of the 2,500** (3.3%), which is 177,170 deliveries where ten
  subscriptions on 438,998 accepted events predicts 4,389,980 (4.0%).

It also **shed nothing**, so it never reached the ceiling AC-30 is about.

### The verdicts the harness decided

```
FAIL AC-30  refusals were 429 only: false (errors 1); p99 3593.4 ms -> 4281.6 ms
            (1.19x) over the run; shed 0.0/s; no load was shed, so this run did not
            reach the ceiling
FAIL AC-31  accepted 207.1 of 250 events/s (82.8%) with p99 4281.6 ms (NFR-1
            ceiling 100 ms); 82.0 deliveries/s at 10 subscription(s) per event
FAIL AC-34  under a sustained 207 events/s the ingest path reported 1 errors and
            p99 4281.6 ms
FAIL §16    reconciliation: events 438998, outbox 438998 against 438997 accepted
```

### Why the delivery number is 3.3% of the claim

Two independent defects, both owned by other cards. Neither is tuned around here.

1. **The dispatch path parks on a capped endpoint (S11, card `t_558a9f59`).**
   `PIGEON_DELIVERY_WORKERS` ships as **1** (`internal/config/config.go`), and
   `dispatch` claims a delivery and then waits *synchronously* for the endpoint's
   slot or token (`internal/delivery/delivery.go`, the `acquireEndpointLimits`
   call) before spawning the attempt goroutine. One slow or capped endpoint
   therefore occupies a process's only dispatch loop, and everything else queues
   behind it. Recorded before this run on `t_4b766094` and in `SPEC.md` A-10; S11
   owns the fix and this card does not tune the test around it.
2. **The relay stalls as well - and that is this run's new observation.** Over a
   five-minute sample the relay published 99 outbox rows while ingest created
   62,391, so deliveries stop at the fan-out before they ever reach the dispatch
   loop. That is a second symptom for S11 to look at, and it is why the reported
   deliveries/s is what it is: a stalled relay, not only a parked dispatcher.

### Why nothing was shed: FR-818 is implemented nowhere

AC-30's second leg is "no silent latency growth", and this run shows exactly that
growth - p99 moved 3,593.4 ms → 4,281.6 ms across the graded window while **zero**
publishes were refused. The cause is not a tuned threshold. It is that FR-818

> Database writes MUST pass through a bounded batch queue. When the queue is full
> the relay and delivery workers MUST block and propagate backpressure to ingest
> (`429`), never grow goroutines or drop accepted events.

has no implementation in this tree, and never had one. `handlePublish`
(`internal/httpapi/events.go`) calls `s.deps.Data.PublishEvent` synchronously; the
only `429` on that path is SB-7's disk-budget gate. There is no bounded batch
queue between ingest and Postgres, so when the write path saturates the instance
queues on the connection pool instead of shedding, and the latency grows silently.
The pool measurement above is the same fact from the other side: at the shipped
`PIGEON_DB_MAX_CONNS=10`, a single publish took 10.2 s.

So AC-30 cannot pass on this tree at any rate, and it is not this slice's to fix:
FR-818 is a P0 with no owning slice. It is carded against the slice table rather
than fixed here.

## What this run does not prove

Reported here so the gap is a sentence rather than a discovery:

- **The tree that was measured is not the tree that is being published.** The
  binary was built from `b65d9c7`, which predates the base branch's FR-905
  landing (dedupe archive, `internal/retention`, `internal/store/retention.go`,
  `internal/db/migrations/0014_dedupe_archive.sql`), the attempt-buffer strand
  sweep and their configuration knobs. The delivery and ingest hot path is
  byte-identical between the two (`internal/delivery/delivery.go`,
  `internal/store/delivery.go`, `internal/store/fanout.go` are untouched by the
  base delta), so the *shape* of the result stands, but the numbers were not taken
  on the published head and are labelled as what they are. A re-run on the head
  was not possible: this is a shared 2 vCPU container whose load average sat
  between 17 and 28 during the session, with another slice's test suites running,
  and §16 already forbids measuring the instance against a generator on the same
  cores. Numbers taken under that contention would be worse, not better, and this
  document would have to caveat them harder.
- **The delivery leg of AC-31/NFR-2 is measured on a tree where it is known to be
  broken, and is expected to fail** - see "Why the delivery number is 3.3% of the
  claim". Publishing both numbers with the cause between them is the honest form;
  the fix is S11's and this card does not tune the numbers to hide it.
- **AC-30's "the ceiling matches the §8.1 table within 20%" is not implemented by
  this harness and is therefore not decided by any run in this repository.** There
  is no `-class` comparison, no §8.1 table in `cmd/loadgen`, and `compare` is the
  AC-43 scaling comparison only. That leg needs the instance to reach its ceiling,
  which the FR-818 gap above prevents. Recorded as a gap, not claimed.
- **AC-34's census is a bound, not 2,500 simultaneous attempts.**
  `TestAttemptFanOutIsBounded` (`internal/delivery`) proves the property AC-34
  actually names - the concurrent attempt count, the goroutines behind it and the
  open descriptors are bounded by the process cap and the per-endpoint cap rather
  than by the queue behind them. Measured on this box: offering 20 deliveries gave a
  peak of 6 attempts in flight, 39 goroutines and 30 descriptors; offering 120 - six
  times the load - gave a peak of 9, 71 and 52. The footprint tracks *concurrency*,
  which is correct and is itself capped, and it grows nothing like the offered load
  (a goroutine per delivery would have produced ~6x the small run's figures and the
  test fails on exactly that). The peak never exceeded the endpoint's cap of 10.
  The test does **not** drive 2,500 attempts at once, and cannot on this box: 2,500 in-flight
  attempts hold roughly 5,000 descriptors between the client and server halves in
  one process, and the soft `ulimit -n` here is **4096**. That number is itself
  AC-34's finding - "FD usage within the documented ulimit" is an operator
  requirement, and `pigeon doctor`'s `ulimit.no_file` check is where it is
  reported. Driving the literal 2,500 needs the limit raised first.
- **AC-43 (1 → 2 → 4 replicas scaling ≥ 80% per doubling, and no improvement above
  the Postgres ceiling)** needs more than one host, and `compare` needs at least
  two reports to say anything. This run is a single replica and cannot speak to it.
- **AC-16's NFR-1 leg (1,000 events/s ingest-only on 8 vCPU / 16 GB)** is not this
  machine's rating, and is not measured here.
- **A first full run was discarded, and why it is worth saying so.** It was taken
  while the test suites were also running on the same 2 vCPU box, which is a
  measurement error: the instance shared its cores with `go test`. It also exposed
  two defects in the harness rather than in the instance - samples were filed under
  the time the pacer had *scheduled* them, so a pacer that fell behind wall clock
  filed every late sample under an early phase and the graded window came out
  empty; and an empty window could pass a criterion vacuously. Both are fixed (see
  `fix(loadgen): grade the window the protocol asks for`), the first run's latencies
  are therefore not usable, and the numbers above are the rerun.
- **`run2-250x10.json` is that discarded run and is kept on purpose.** It is the
  evidence for the two harness defects above, and its verdicts are the ones that
  made the empty-window guard necessary.

## The §16.5 fault-injection matrix

Separate document, separate job: the seven-scenario matrix lives in
`internal/faults/faults_test.go` behind the `faults` build tag, and
`.github/workflows/nightly-faults.yml` runs it every night against a real Postgres
and Redis, plus two process-level scenarios against the real binary (SIGKILL a
worker mid-flight; stop Postgres for 30 s). It is behind a build tag so the pull
request gate stays fast and deterministic while the chaotic half runs on a schedule
that can afford it.
