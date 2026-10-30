# The load test

This document is the measured half of the throughput claim. §8.1 is a capacity
model - a row-level arithmetic argument for what 2 vCPU / 4 GB should hold - and
NFR-4 makes the rated load a property of the machine class rather than one number
asserted everywhere. This is where the arithmetic meets a stopwatch.

The claim this document publishes is the measured result, not the design target:

> **On 2 vCPU / 4 GB (NFR-4's reference machine) one instance accepted 207.1
> events/s at 10 subscriptions per event, and 82.0 deliveries/s were written.**
> That is 82.8% of §8.1's 250 events/s offered and **3.3% of §8.1's
> 250 × 10 = 2,500 deliveries/s target**.
>
> **§8.1's 2,500 deliveries/s is the target this run missed, not a number it
> measured.** The two causes are named below and neither is tuned around: at the
> time of this run FR-818's bounded batch queue was implemented nowhere, so the
> instance never shed and never reached a ceiling; and the relay still fans out one
> row per iteration, so deliveries stop before they reach the dispatch loop. FR-818
> has since landed on the base as slice **S12** (`t_c6afa452`, `SPEC.md` A-11), and
> the graded re-run that exercises it is **`t_0bf3effa`** - which also adds the §8.1
> `-class` comparison this harness did not have (`loadgen grade`, see "AC-30's third
> clause" below); the figures in this document are the pre-FR-818 baseline that run is
> measured against. That graded run is the **operator's, on a host that carries the
> class**, not this box's: see "What this run does not prove". What the run does
> *not* show is that this machine class
> cannot do better - it shows that two required paths were missing, and a ceiling
> cannot be measured on a tree that has none.

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
7. **`pg_stat_database.xact_commit` is sampled across the run, not around the
   reconciliation query** (FR-815's batching, §16). The baseline is taken before the
   first publish, because a delta between two reads taken *after* the load stopped
   measures the interval between the reads and the instance's background sweeper.
   That was the harness's defect until card `t_0bf3effa`, and it is why an artifact
   from before it reports `0.0 xact/s` at `0.00 rows/transaction`; see "What this run
   does not prove".

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
#    This is the reference-box runbook: it claims no §8.1 class, because this box
#    matches no row of §8.1's table. -class belongs to the graded run, which is in
#    "AC-30's third clause" below.
/tmp/loadgen run -base http://127.0.0.1:8080 -db "$PIGEON_DATABASE_URL" \
  -key "$PIGEON_LOADGEN_KEY" \
  -event-type load.created -consumer-ref load-consumer \
  -rate 250 -subs 10 -warmup 5m -measure 30m -evaluate 10m -settle 60s \
  -concurrency 512 -label "reference-box-1replica-defaults" -out run.json

# 4. The artifact's own verdicts. This run declares no class, so `grade` refuses
#    AC-30's third clause by name (exit 1) rather than comparing a number against a
#    row nobody claimed - which is the honest outcome for a reference-box run and
#    not a failure of the harness. Its clauses 1-2 verdict is still decided, from
#    the artifact's own phase numbers.
/tmp/loadgen grade -label run.json

# 5. AC-43: compare a run set across replica counts. Two reports minimum, in
#    ascending replica order; the exit status is the verdict. This is the SCALING
#    comparison and only that: AC-30's ceiling clause is the graded run in "AC-30's
#    third clause", not this one.
/tmp/loadgen compare -label run-1replica.json -label run-2replica.json -label run-4replica.json
```

The seeded endpoints are written with `internal = true`, FR-402's operator escape
hatch, so a loopback sink is reachable. Using the flag is honest; weakening the
SSRF check for the sake of a test would not be.

### AC-30's third clause

The class comparison. AC-30's third clause is "the ceiling matches the §8.1 table
within 20%". It compares **one run of one instance against one row of §8.1's table**,
in both units, and it is not AC-43: `compare` stays the replica-scaling comparison,
because folding scaling into this would lose the clause's own failure mode - a graded
number taken on a machine that does not carry the class. `grade` decides the clause
from the artifact:

```sh
/tmp/loadgen grade -label run.json                    # the exit status is the verdict
/tmp/loadgen grade -label run.json -class 2vCPU-4GB   # assert the class intended
```

#### The graded invocation

The graded run is §12's protocol, on **a host that carries the class**, with the
generator **off the machine under test**. It declares two things, and they are not the
same machine: `-class` is the §8.1 row claimed, and `-sut-cpus`/`-sut-mem-gib` are the
machine under test the row is claimed for. `-rate` must be **above** the row's events/s
(1,200 against the 8 vCPU row's 1,000), or the run never went past the ceiling and
`grade` refuses it as a run that cannot support the claim.

```sh
# (a) Two hosts: the generator on its own box, the instance on the rated one (§12's
#     first arrangement). -base is the instance's non-loopback address, so the run is
#     not on the machine under test and the class is recorded as *attested*: the
#     operator states the machine under test, because the generator cannot see it.
/tmp/loadgen run -base http://<sut-address>:8080 -db "$PIGEON_DATABASE_URL" \
  -key "$PIGEON_LOADGEN_KEY" -event-type load.created -consumer-ref load-consumer \
  -rate 1200 -subs 10 -warmup 5m -measure 30m -evaluate 10m -settle 60s \
  -concurrency 512 -class 8vCPU-16GB -sut-cpus 8 -sut-mem-gib 16 \
  -label "8vCPU-16GB-1replica" -out run.json

# (b) One machine, the generator in its own container so it does not compete for the
#     instance's cores (§12's second arrangement). -generator-on-sut no states that -
#     auto would resolve the loopback -base to "shared" - and -sut-cpus/-sut-mem-gib
#     are the instance's own allowance, which is also what -class is checked against.
/tmp/loadgen run -base http://127.0.0.1:8080 -generator-on-sut no \
  -db "$PIGEON_DATABASE_URL" -key "$PIGEON_LOADGEN_KEY" \
  -event-type load.created -consumer-ref load-consumer \
  -rate 600 -subs 10 -warmup 5m -measure 30m -evaluate 10m -settle 60s \
  -concurrency 512 -class 4vCPU-8GB -sut-cpus 4 -sut-mem-gib 8 \
  -label "4vCPU-8GB-1replica-container" -out run.json

# Then, and only then, the verdict. Both shapes above are *attested*; see below for
# what makes a class observed, and why an observed one is never graded.
/tmp/loadgen grade -label run.json -out run-verdict.json
```

A generator that **shares** the machine under test - the default resolution when
`-base` is loopback or this host and nothing overrides it - is *observed* evidence:
`run` checks the class against the machine it can see **before** spending the 35
minutes, but `grade` refuses the clause from that artifact, because §12 puts the
generator off the machine under test and a run whose generator competed is a lower
bound. That is the one arrangement this document's own published run was made in.

The table is in `cmd/loadgen/class.go`, cited to `REQUIREMENTS.md` §8.1, and pinned
against the requirement's own text row by row by `TestClassTableIsSection81`:

| class | delivery ceiling (deliveries/s) | events/s at 10 subscriptions |
|---|---|---|
| 2 vCPU / 2 GB | ~1,200 | ~120 |
| 2 vCPU / 4 GB | ~2,500 | ~250 |
| 4 vCPU / 8 GB | ~5,000 | ~500 |
| 8 vCPU / 16 GB | ~10,000 | ~1,000 |

Both units are checked, because §8.1 states the same workload twice: the delivery
ceiling is the event column times the subscription count the heading names. A run
passes when **both** measured rates are within 20% of the declared row (inclusive),
and the verdict prints the measured numbers, the row's, and the deviation of each.

**A run must declare its class, and a mismatch is refused rather than explained.**
`run -class NAME` records the claim, and `grade` grades it only when the machine the
class is claimed for carries it:

- The claim is checked against the **machine under test**, which is recorded in the
  artifact as `machine_under_test` and is not the machine block. §12 puts the
  generator off the machine under test and `cmd/loadgen` *is* the generator, so on
  every gradeable run the machine block describes the **generator's** box: a class
  checked against that would be checked against the wrong machine on exactly the runs
  the clause is graded from. `machine_under_test` is the machine block when the
  generator shared it, and `-sut-cpus`/`-sut-mem-gib` (the operator's statement about
  a box the generator cannot see) when it did not. An artifact that records neither
  **cannot support the claim** and is refused: a graded number on an unrated box is
  the failure mode this clause exists to prevent, applied where the graded number is
  actually produced.
- The machine block's CPU and memory are **effective**, not the host's: the CPU count
  is the process's affinity intersected with its cgroup quota, and memory is
  `/proc/meminfo` capped by the cgroup limit, with the host's own figures recorded
  beside them. A container holding two cores of a 32-core host is a 2 vCPU machine,
  and a claim of 8 vCPU is refused **before** the 35-minute run rather than measured
  and explained afterwards.
- Cores are exact and memory is a band (25%), because §8.1's rows differ by vCPU and
  by RAM and no machine reports its class's RAM exactly: a "16 GB" host that reports
  15 GiB is that row, and this repository's 2 vCPU / 7.8 GiB box is no row at all.
  The same comparison decides whether a `-sut-cpus`/`-sut-mem-gib` declaration agrees
  with the machine the harness can see, when the generator shared it.
- A class is **observed** (the generator ran on the machine under test, so the class
  was checked against the machine being graded, before the run) or **attested** (the
  generator ran off it, §12, so the class rests on the declared machine under test).
  The evidence is derived from how the run was launched, never passed in: an artifact
  cannot claim observation while recording that the generator ran elsewhere. An
  observed run is a lower bound and is not graded; the graded run is the attested one.
- `grade` refuses to decide the clause at all when the run cannot support it, naming
  which of these it is: no class declared; no machine under test recorded; the machine
  under test does not carry the declared class; a class §8.1 does not rate; not 10
  subscriptions per event; §12's windows (5 m warm-up excluded, 30 m measured, the last
  10 m graded); an offered rate at or below the row (the run never went past the
  ceiling, so the ceiling is not what it measured); a graded window that was never
  reconciled against the database; or a generator that shared the machine under test.
- The clause reads `events`/`deliveries` **as the database counted them** over the
  graded window (§16); the generator's own tally over the same window is printed as a
  cross-check and is not graded, because the window's edges are the in-flight set at
  both ends. `graded_reconcile` carries the same note, so its `events_match` /
  `outbox_match` false is legible as a deliberate choice rather than a mismatch.

The verdict rule is §12's: pass, or a defect to fix (named, with its owning card), or
an **amendment to §8.1** recorded as one in the gap list with the measurement attached.
A tolerance is not an outcome, and the harness is not tuned to fit either direction:
when the measured ceiling is *above* the row by more than 20% the verdict says
"amendment to §8.1" rather than "within tolerance", because a host that beats its
class's table is a finding about the table.

**The graded run is the operator's, on a host that carries the class.** This box is
2 vCPU / 7.8 GiB and matches no §8.1 row, so a class cannot even be *observed* here -
`run` refuses it before the run, which is the preflight working - and §12 wants the
generator off the machine under test as well (Jay's standing rule: rated-class
measurement happens on his machine, not on this instance). `t_94f397ba` was archived
after it correctly blocked `capability`, and its comments carry the reproduction
recipe. What lands here is the mechanism: the comparison, its refusals, the machine
under test it checks the class against, and the deterministic tests behind them
(`class_test.go`, 23 tests, all registered in `ci.yml`'s required-name guard).

The harness was exercised end to end on this box against a real instance, database and
Redis - a 60 s measurement at 40 offered events/s with 10 subscriptions per event, no
class claimed, `-sut-cpus 2 -sut-mem-gib 4` recorded as the machine under test - and
the parts the clause reads all worked: the graded window counted 436 events and 4,360
deliveries in 20 s (10.0 deliveries per event, the fan-out intact), the machine block
reported "§8.1 row the machine itself matches: none" beside `machine_under_test` 2 vCPU
/ 4.0 GiB (`source: declared`), and `grade` refused the artifact for declaring no class
rather than comparing it against a row nobody claimed. The same artifact doctored to
claim `2vCPU-4GB` was refused for the six named reasons (an observed class whose two
machine blocks disagree, 7.8 GiB against 4.0 GiB; a 20 s warm-up; a 60 s measurement; a
20 s graded window; 40 offered against 250; and the generator on the machine under
test), and all four preflight refusals were reproduced live with the built binary: a
declared machine under test that does not carry the class, an attested class with no
machine under test at all, a declaration that disagrees with the observed machine on a
shared-generator run, and a class this box does not carry. **That is a statement about
the harness, not about any machine class**, and no clause number is claimed from it.

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
            p99 4281.6 ms; the attempt census itself is TestAttemptFanOutIsBounded
FAIL §16    reconciliation: events 438998, outbox 438998 against 438997 accepted
```

AC-34's criterion (goroutines bounded by the caps, FDs within the documented
`ulimit`, CPU largely idle) is decided by `TestAttemptFanOutIsBounded`, which drives
the literal 2,500; its runs, and the one failure among them (the CPU threshold, not
the bound), are recorded under "AC-34 is measured at the criterion's own figure"
below. The harness's AC-34 line is its ingest-path proxy, so its FAIL is
AC-30/AC-31's saturation seen a second time, not the bound.

### Why the delivery number is 3.3% of the claim

Two independent defects, neither tuned around here. Both are fixed on the tree this
report was later annotated against, each by its own card.

1. **The dispatch path parks on a capped endpoint (S11, card `t_558a9f59`).**
   `PIGEON_DELIVERY_WORKERS` ships as **1** (`internal/config/config.go`), and
   `dispatch` claims a delivery and then waits *synchronously* for the endpoint's
   slot or token (`internal/delivery/delivery.go`, the `acquireEndpointLimits`
   call) before spawning the attempt goroutine. One slow or capped endpoint
   therefore occupies a process's only dispatch loop, and everything else queues
   behind it. Recorded before this run on `t_4b766094` and in `SPEC.md` A-10; S11
   owns the fix and this card does not tune the test around it. **Landed** as
   `7a7c997 feat(delivery): stop parking the dispatch path on a capped endpoint
   (FR-1303, FR-1304)`, so the pre-S11 parking is no longer a second cause in the
   delivery number.
2. **The relay's outbox loop, which is a *different* path from S11's.** Over a
   five-minute sample the relay published 99 outbox rows while ingest created
   62,391, so deliveries stop at the fan-out before they ever reach the dispatch
   loop. The mechanism is `internal/relay/relay.go:128-143`: it leases one outbox
   row per iteration and runs publish -> fan-out -> mark serially, with no
   batching anywhere on that path, against §8.1's L1. Measured directly at
   `a944f20` with the instance **idle of ingest**: the unpublished outbox drained
   758 -> 698 rows in 46 s and 647 -> 517 in 92 s, i.e. **~1.3-1.4 events/s**, with
   517 rows still unpublished minutes after the generator stopped, and the run
   reconciled 360 delivery rows for 955 accepted events at 10 subscriptions.
   NFR-3 (outbox lag p99 <= 2 s) and NFR-6 ("a queue that does not drain is a
   defect") are both at risk, and this leg alone caps the system orders of
   magnitude below §8.1.

   **Attribution, corrected.** This is not S11's card: S11 (`t_558a9f59`) is
   chartered to the *dispatch* path (`internal/delivery/delivery.go`
   `dispatch`/`acquireEndpointLimits`, the first-attempt and retry lanes, and the
   hold). The relay's non-batched outbox loop is carded separately as
   **`t_b9850e1b`** (pigeon-dev) with the reproduction above and the instruction to
   measure it on a compliant host before closing it as not-a-defect or fixing it.
   Both legs are real and both are required for the delivery unit; they are simply
   two mechanisms, and sending this one to S11's dispatch scope would have lost it.

   **Fixed (card `t_b9850e1b`).** This leg is a defect, not a latency artifact of
   the box: the loop did one lease, one publish, one fan-out transaction and one
   mark per event, so its rate was one round-trip chain per event by construction,
   and nothing on that path was batched at all - it did not touch the write budget
   FR-818 gives it. The drain is now batched: one lease call claims a batch of
   outbox rows (`pigeon_lease_outbox_batch`, migration `0020`), their delivery sets
   are resolved together, and the whole fan-out - the delivery rows, the §7.5a
   routing outcomes and the outbox marks - is written one write-budget transaction
   per pack that fills FR-815's row bound (`internal/store/relaybatch.go`,
   `internal/relay/relay.go`).

   Measured on this same 2 vCPU box, 1,000 accepted events at 10 subscriptions, the
   instance idle of ingest, the same in-process probe on both sides of the change:
   **41.5 events/s (414.9 deliveries/s) before -> 496.7 events/s (4,966.8
   deliveries/s) after**, the drain finishing in 2.0 s instead of 24.1 s and using
   **8 write-budget transactions (12,000 rows) instead of none**. Both figures are
   *lower bounds on the shape*, not graded claims - §16 wants the generator off the
   machine under test and this box cannot provide that, so the rated-class numbers
   remain the operator's run (see "What this run does not prove"). What the fix
   settles is the mechanism: the fan-out leg is no longer bounded by one round-trip
   chain per event, and at 10 subscriptions it now carries the delivery unit §8.1
   states for it with room to spare on a box far weaker than the reference.

### Why nothing was shed: the measured tree had no FR-818 (it has landed since)

AC-30's second leg is "no silent latency growth", and this run shows exactly that
growth - p99 moved 3,593.4 ms → 4,281.6 ms across the graded window while **zero**
publishes were refused. The cause is not a tuned threshold. It is that FR-818

> Database writes MUST pass through a bounded batch queue. When the queue is full
> the relay and delivery workers MUST block and propagate backpressure to ingest
> (`429`), never grow goroutines or drop accepted events.

had no implementation on the tree this run measured (`b65d9c7`). `handlePublish`
(`internal/httpapi/events.go`) calls `s.deps.Data.PublishEvent` synchronously; the
only `429` on that path was SB-7's disk-budget gate. There was no bounded batch
queue between ingest and Postgres, so when the write path saturated the instance
queued on the connection pool instead of shedding, and the latency grew silently.
The pool measurement above is the same fact from the other side: at the shipped
`PIGEON_DB_MAX_CONNS=10`, a single publish took 10.2 s.

So AC-30 could not pass on the measured tree at any rate, and it was not this
slice's to fix: FR-818 was a P0 with no owning slice. It is now slice **S12**
(`t_c6afa452`, `SPEC.md` A-11), landed on the base after this run, and the graded run
that exercises the mechanism is the **operator's on a rated host** - card `t_0bf3effa`
already landed the harness side of it (the §8.1 `-class` comparison and `grade`). This
card keeps the report and the harness; the numbers above are the pre-FR-818 baseline
the graded run is measured against.

## What this run does not prove

Reported here so the gap is a sentence rather than a discovery:

- **The tree that was measured is not the tree that is being published.** The
  binary was built from `b65d9c7`, which predates the base branch's FR-905
  landing (dedupe archive, `internal/retention`, `internal/store/retention.go`,
  `internal/db/migrations/0014_dedupe_archive.sql`), the attempt-buffer strand
  sweep, their configuration knobs, and **S11's dispatch fix (PR #54,
  FR-1303/FR-1304)**. The ingest hot path (`internal/store/delivery.go`,
  `internal/store/fanout.go`) is untouched by the base delta; the dispatch path is
  not - S11 replaced the `acquireEndpointLimits` wait this run measured with a
  non-blocking `admit` and lane partitioning - so the *shape* of the result stands,
  but the numbers were not taken on the published head and are labelled as what they
  are. The published **82.0 deliveries/s is therefore a lower bound on the landed
  tree**, not that tree's number: the run could not approach the ceiling while
  dispatch parked on a capped endpoint, and S11's fix removes that cause. The
  compliant-host re-measurement is `t_b9850e1b`'s; this card does not re-measure,
  because §16 forbids a generator on the same cores and this box cannot offer a
  clean run. A re-run on the head
  was not possible: this is a shared 2 vCPU container whose load average sat
  between 17 and 28 during the session, with another slice's test suites running,
  and §16 already forbids measuring the instance against a generator on the same
  cores. Numbers taken under that contention would be worse, not better, and this
  document would have to caveat them harder.
- **The delivery leg of AC-31/NFR-2 is measured on a tree where it is known to be
  broken, and is expected to fail** - see "Why the delivery number is 3.3% of the
  claim". Publishing both numbers with the cause between them is the honest form;
  the fix is S11's and this card does not tune the numbers to hide it.
- **AC-30's "the ceiling matches the §8.1 table within 20%" is now implemented, and is
  still not measured here.** The harness has the comparison (`loadgen grade` against
  the §8.1 table in `cmd/loadgen/class.go`, with `run -class` recording the claim and
  the machine block - see "AC-30's third clause"), so the clause is gradeable at all:
  it was not, before card `t_0bf3effa`, and no run in this repository decided it. What
  is still missing is the **run**: it needs an instance that reaches its ceiling, which
  needs a host that carries a §8.1 class (this box matches no row - 2 vCPU / 7.8 GiB)
  and §12's generator-off-the-machine-under-test. That is the **operator's, on a rated
  host**; the harness side lands here, and no clause number is claimed from it.
- **AC-34 is measured at the criterion's own figure, and the earlier reading of it
  was wrong.** `TestAttemptFanOutIsBounded` (`internal/delivery`) has three legs, and
  the third drives AC-34's literal **2,500 due deliveries offered at once** to one
  endpoint capped at 10. Legs 1 and 2 are the comparison: 20 offered gave peak 10
  attempts in flight / 52 goroutines / 37 descriptors, 120 offered gave 10 / 87 / 61,
  and 2,500 offered - twenty times the load - gave **5 / 41 / 36**, with all 2,500
  deliveries served. The footprint tracks *concurrency*, which is capped, not the
  backlog behind it, and the peak never exceeded the endpoint's cap of 10.
  `RLIMIT_NOFILE` was 524287/524288 entering the literal leg and 524288/524288 during
  it - the test raises the soft limit to the hard limit itself, which needs no
  privilege, so "the limit was raised" is a fact in the log rather than a claim in
  this document.

  An earlier version of this paragraph said that 2,500 in-flight attempts "hold
  roughly 5,000 descriptors" and that the run "needs the soft `ulimit -n` raised
  first (here 4096)". Both statements were wrong: 5,000 descriptors is the
  goroutine-per-offered-delivery model this criterion exists to refute, and the
  soft limit here is not 4096. The figures above are the correction, and the
  literal leg is now in the suite so the claim cannot drift back.

  **The drain is bounded by the property, not by the clock, and the leg is
  load-sensitive in a way worth naming.** The wait ends when the receiver is idle,
  every attempt has started and nothing is left pending; the 10-minute deadline
  behind it is a hang escape, so a completed drain never spends it. Measured on this
  box: **6m56.8s for the literal leg's drain at load average 15**, the property
  breaking the loop 183 s before the escape, and 231 s for the whole test binary at
  load ~3 (the pre-change baseline run on the same box). A repeat of the same three
  legs on this box reported 8 and 10 attempts in flight where this run reported 10
  and 10, 44 and 78 goroutines where it reported 52 and 87, and 34 and 56 descriptors
  where it reported 37 and 61 - the legs measure concurrency under a scheduler, and
  the criterion's assertion is the bound (never above the endpoint's cap of 10; the
  footprint does not grow with the offered backlog), not the sample. The leg is slow
  because the dispatch loop claims **one delivery per database round trip** - 2,500
  of them drain at the box's claim rate, not at the endpoint's cap - and on a box
  that is already saturated by someone else's `-race` suite (load average 18 on
  2 vCPU, 251 of the 2,500 still pending at a five-minute mark) that rate collapses.
  Both are contention over a claim path this criterion does not govern, not a broken
  bound: the legs that reported stayed inside the cap, and neither the goroutine nor
  the descriptor ceiling was threatened.

  **AC-34's CPU clause is asserted comparatively, and the literal leg's absolute
  figure is reported rather than asserted.** `cpuCores` is the whole test binary's
  user+system CPU (the sampler, the in-process receiver, the Go runtime, the race
  detector and the claim loop) over wall clock, so it lands on either side of half a
  core on a shared 2 vCPU runner whether or not the bound holds: three runs of the
  same file on the same head (`c807966`, the previous review's SHA) measured **0.44,
  0.49 and 0.51 cores** on this leg - two passed, and the third failed a `0.5`
  threshold while the bound held in all three (peaks of 3, 10 and 7 attempts in
  flight, all 2,500 deliveries served, 45, 61 and 52 descriptors). A threshold sitting
  on the sample's own boundary is a coin flip, and one high enough to survive the
  spread stops biting, so that assertion is gone. What replaced it is the property the
  clause is about: the process's CPU *rate* must not track the offered backlog. Legs 1
  and 2 offer the same endpoint 20 and 120 deliveries at the same one second of
  service, so they attempt at the same rate and must cost the same CPU per second; a
  fan-out that paid per offered delivery would make the 6x leg cost about 6x the rate.
  Those same three CI runs measured **0.12/0.12, 0.09/0.11 and 0.12/0.14 cores/s** on
  that pair, against an allowance of twice the small leg's rate plus a fifth of a core,
  and this box's latest run measured **0.08 and 0.13 cores/s**. The literal leg's
  figure is logged either way: 0.13 cores over its 6m56.8s drain here, 0.44, 0.49 and
  0.51 on CI.
- **AC-43 (1 → 2 → 4 replicas scaling ≥ 80% per doubling, and no improvement above
  the Postgres ceiling)** needs more than one host, and `compare` needs at least
  two reports to say anything. This run is a single replica and cannot speak to it.
  Owned by the operator on a rated host, with AC-16 and AC-17 (`t_bcfda850`).
- **AC-16 and AC-17 were this card's to record, and this box cannot rate them.**
  Both are recorded here explicitly rather than dropped: `PLAN.md`'s slice notes
  and S11's card both said AC-16 and AC-17 remain S10's measurements, and silence
  in this document would delete two criteria from the project. Each is split into the part
  that is measured, the part that is unmeasurable here, and the part that needs the
  rated host - all three are dispositioned on `t_bcfda850`, and the unmeasured parts
  move to the operator on a rated host.
  - **AC-16** asks for 1,000 events/s with 10 subscriptions and one hanging endpoint
    on **8 vCPU / 16 GB**. This box is 2 vCPU / 7.9 GB, a quarter of the cores, and
    it accepted 207.1 of 250 offered events/s with a 4.3 s p99: a 1,000 events/s run
    here would measure the box. Its legs individually:
    - *hanging endpoint capped* - **measured**, in-process:
      `TestAC41ConcurrencyCapIsIdenticalAtOneAndTwoReplicas` (an endpoint capped at
      10 never observes more than 10 in flight, identically at 1 and 2 replicas),
      and AC-34's census below (never above the cap with 2,500 deliveries offered).
    - *commit rate stays under 100/s* - **recorded here as measured, and the figure
      is not a measurement of the run.** The harness bracketed
      `pg_stat_database.xact_commit` around its *reconciliation queries*, which run
      after the load has stopped, so the delta was the instance's own background
      commits between two reads milliseconds apart: "0.17 xact/s at 1,638.74 rows per
      transaction" is that too-small delta read as a rate, and the ratio beside it is
      its reciprocal rather than a batching figure. The bracket is fixed on card
      `t_0bf3effa` (the baseline is taken before the first publish), and on this box
      the same shape of run then reads **98.9 xact/s at 2.42 rows/transaction** - a
      figure about that 100 s run at 28.15 accepted events/s *in its graded 20 s
      window* (21.8 events/s over the run's whole 100.42 s, 2,188 accepted), not about
      the rated load,
      and one that sits on the wrong side of a coin toss against this leg's 100/s.
      **Owner: the operator on a rated host**, with the rest of AC-16. No leg of
      AC-16 is claimed by this run.
    - *NFR-1 at 1,000 events/s* - **unmeasurable on this machine class**; needs the
      8 vCPU / 16 GB reference host.
    - *NFR-2 (publish -> first attempt <= 60 s)* - **unmeasured because the rated
      load cannot be offered on this machine class**, not blocked on S11: S11 has
      landed (PR #54, FR-1303/FR-1304) and removed the synchronous
      `acquireEndpointLimits` wait that used to park the process's only dispatch
      loop. The measurement it enables is now waiting only on the 8 vCPU / 16 GB
      reference host.
    - **Owner: the operator on a rated host.** AC-16, AC-17 and AC-43 are measured
      there, not on this box; the deferral card `t_94f397ba` was archived after it
      blocked `capability`, and its comment keeps the reproduction recipe (harness,
      seed, sinks, the rated `run` invocation, `compare` for AC-43, AC-17's
      differential leg, and the instance-side env block).

    **Disposition (`t_bcfda850`):** the split above is accepted as S10's recording -
    the two measured legs stand, and the remaining legs are deferred to the operator
    on a rated host with the measured legs named. AC-16 is not S10's exit leg.
  - **AC-17** asks that with one producer at 50% of the load the other producer's
    p99 degrades by <= 20%. It is not a machine rating, but it *is* a differential
    measurement, and on a box already saturated at 207 of 250 events/s with a 4.3 s
    p99 both producers queue behind the same connection pool and the same missing
    bounded batch queue, so the comparison would report the box's contention rather
    than producer isolation. It needs a host with headroom at the rated load (the
    8 vCPU class again) and two independent generators.

    **Disposition (`t_bcfda850`):** AC-17 moves to the operator on a rated host and
    is measured on the 8 vCPU / 16 GB host with two independent generators. It is
    not claimed by this run and is not S10's exit leg.
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

Separate document, separate job: the seven-scenario matrix of §16.5 lives in
`internal/faults/faults_test.go` behind the `faults` build tag, and
`.github/workflows/nightly-faults.yml` runs it every night against a real Postgres
and Redis, plus two process-level scenarios against the real binary (SIGKILL a
worker mid-flight; stop Postgres for 30 s). It is behind a build tag so the pull
request gate stays fast and deterministic while the chaotic half runs on a schedule
that can afford it.

The job migrates its own disposable database before the matrix runs (`fix(ci): the
fault matrix migrates its own disposable database`), which is what makes it passable
on a fresh `postgres:16` service container: it was red on its first night without
that, with 6 of 8 scenarios dying on `relation "producers" does not exist`. The
matrix is 8 PASS / 0 FAIL / 0 SKIP on a freshly created empty database - the seven
§16.5 scenarios plus the guard that asserts the matrix names every one of them. Its
**first real scheduled run is the run after this branch merges**, because GitHub only
schedules or dispatches a workflow that is present on the default branch; that
residual is stated rather than hidden.
