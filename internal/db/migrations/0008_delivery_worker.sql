-- 0008_delivery_worker.sql — S4: the delivery worker's tables and the sanctioned
-- functions it drives.
--
-- A delivery row (S3) is created at fan-out with its lease set (FR-813) and waits
-- in `pending`. The delivery worker leases the next due row the way the relay
-- leases the outbox (SKIP LOCKED, extend the lease), signs the envelope with the
-- endpoint's own secret (FR-1001), POSTs it with a 5 s ceiling and no redirects
-- (FR-802, FR-803), classifies the outcome (FR-805 retry vs FR-806 terminal), and
-- writes ONLY at the end of the attempt:
--
--   * a successful first attempt writes NO attempt row — the four columns on the
--     delivery row carry it (FR-811);
--   * a failed attempt writes one evidence row in `attempts` (FR-804);
--   * a terminal failure ends the delivery row `failed`; a success ends it
--     `delivered`. Both are control state (§6, FR-812 `synchronous_commit=on`).
--
-- The worker is a system worker like the relay: it drains deliveries across every
-- tenant, so it has no producer scope and reaches the tenant tables through the
-- SECURITY DEFINER functions below (the same sanctioned-bypass pattern as 0006).
--
-- FR-813 reconciliation. The lease is set when the row is created at fan-out and
-- the worker's only *lifecycle* writes are the completion / terminal transitions
-- (and the attempt evidence rows FR-811 requires). Renewing the lease when the
-- worker claims a row is the claim primitive, not a lifecycle write: it is the
-- exact operation `pigeon_lease_outbox` performs for the relay, which S3's own
-- migration directs the worker to use ("the way the relay leases the outbox"). A
-- crashed worker lets its lease lapse and the row becomes claimable again (FR-809,
-- D-6); the sweeper re-queues it within 10 s.

-- ---------------------------------------------------------------------------
-- Attempts: the per-attempt evidence trail (FR-804, FR-1103).
--
-- A row exists only for a FAILED attempt (FR-811): a successful first attempt is
-- the delivery row's four columns, not a row here. Every failed attempt, whether
-- it is then retried or ends the delivery, leaves exactly one row.
--
-- FR-814 hot-table index budget for `attempts`: two indexes and counting.
--   attempts_pkey                    primary key (id).
--   attempts_delivery_attempt_key    uniqueness: no two attempts share a number
--                                    (AC-07), and its (delivery_id, attempt)
--                                    prefix IS the delivery-detail ordered-trail
--                                    scan (FR-1102/FR-1103), so no third index is
--                                    needed until S7 adds a query that needs one.
-- ---------------------------------------------------------------------------
create table attempts (
    id             bigserial primary key,
    delivery_id    bigint  not null references deliveries (id) on delete cascade,
    -- 1-based per delivery (FR-902). Assigned from the current max here, never
    -- from the delivery's `attempts` counter, so a crashed mid-attempt worker
    -- that re-claims the row cannot hand out a duplicate number (AC-07).
    attempt        int     not null check (attempt >= 1),
    -- Was this failure worth another try (FR-805) or the last one (FR-806)?
    outcome        text    not null check (outcome in ('retryable', 'terminal')),
    -- The peer's answer, or null for a timeout / connection / DNS error (FR-804).
    response_status int    check (response_status is null or response_status between 100 and 599),
    -- A short machine class for the trail and the UI (FR-1103): timeout, a dial /
    -- DNS failure, a retryable HTTP status, or a terminal HTTP status.
    error_class    text    not null check (error_class in (
        'timeout', 'dial_error', 'dns_error', 'http_retryable', 'http_terminal')),
    -- FR-804 / SB-4: the first 2 KB of the response body, truncated. Configurable
    -- down to 0 (S6), so this is the default cap, not a hard one.
    response_body  text,
    duration_ms    int     not null check (duration_ms >= 0),
    occurred_at    timestamptz not null default now(),
    -- When the next attempt is scheduled for (a retryable failure). Null on the
    -- last attempt of a delivery (terminal failure, or the delivery succeeded).
    next_retry_at  timestamptz
);

comment on table attempts is
    'FR-811: one row per FAILED attempt only. A successful first attempt is the delivery row four columns, not a row here (AC-28).';
comment on column attempts.attempt is
    'FR-902/AC-07: 1-based per delivery, assigned from max(attempts)+1 so a re-claimed row never reuses a number.';
comment on column attempts.response_status is
    'FR-804: the HTTP status code, or null for a timeout / connection / DNS error.';
comment on column attempts.response_body is
    'FR-804/SB-4: the first 2 KB of the response body, truncated.';

alter table attempts enable row level security;
-- The runtime role owns nothing here; it reaches attempts only through the
-- definer functions below, so no tenant policy is needed on the table itself.

create unique index attempts_delivery_attempt_key
    on attempts (delivery_id, attempt);

-- ---------------------------------------------------------------------------
-- Claim: lease the next due pending delivery (FR-813, FR-809), the way the relay
-- leases the outbox.
--
-- Picks the oldest pending row whose lease has come due (null or lapsed) with
-- SKIP LOCKED so two workers never claim the same row, extends the lease so a
-- concurrent claim sees it leased and moves on, and returns the whole row plus
-- the envelope fields and the endpoint's signing secret the worker needs to sign
-- and POST — one round trip, no second unsynchronized read.
--
-- The signing secret travels out only to the worker's sign call and is never
-- logged, stored in an attempt row, or returned by any API (FR-1008).
-- ---------------------------------------------------------------------------
create or replace function pigeon_claim_delivery(p_lease interval default interval '40 seconds')
    returns table (
        delivery_id     bigint,
        event_id        text,
        producer_id     text,
        env             text,
        endpoint_id     text,
        endpoint_url    text,
        signing_secret  text,
        event_type      text,
        payload_version int,
        consumer_ref    text,
        occurred_at     timestamptz,
        payload         jsonb,
        -- How many attempts have already failed for this row, so the worker can
        -- log progress; the authoritative next number is pigeon_next_attempt.
        prior_attempts  int,
        -- The time of the earliest failed attempt, or null on a first attempt. The
        -- completing worker records this as deliveries.first_attempt_at so the
        -- timestamp is stable across the re-claims a crash produces; a first attempt
        -- (no rows yet) falls back to the worker's own attempt start time.
        first_attempt_at timestamptz
        )
    language sql
    security definer
    set search_path = public, pg_temp
as $$
    with picked as (
        select d.id
        from deliveries d
        where d.status = 'pending'
          and (d.lease_expires_at is null or d.lease_expires_at < now())
        order by d.id
        for update skip locked
        limit 1
    ),
    leased as (
        update deliveries d set lease_expires_at = now() + p_lease
        from picked
        where picked.id = d.id
        returning d.id
    )
    select l.id, e.id, d.producer_id, d.env, d.endpoint_id,
           ep.url, ep.signing_secret,
           et.name, et.payload_version, coalesce(e.consumer_ref, ''),
           e.occurred_at, e.payload,
           coalesce(da.n, 0), da.first_attempt_at
    from leased l
    join deliveries d on d.id = l.id
    join events e on e.id = d.event_id
    join event_types et on et.producer_id = e.producer_id
                         and et.env = e.env
                         and et.name = e.event_type
    join endpoints ep on ep.producer_id = d.producer_id
                      and ep.env = d.env
                      and ep.id = d.endpoint_id
    left join lateral (
        select count(*)::int as n, min(a.occurred_at) as first_attempt_at
        from attempts a where a.delivery_id = l.id
    ) da on true;
$$;

-- ---------------------------------------------------------------------------
-- Next attempt number for a delivery (AC-07: unique, never duplicated). Derived
-- from the attempts that already exist, so it is correct even after a crash that
-- left a partially-written attempt. Single-worker-per-lease (the lease serializes
-- claimants), so this needs no lock of its own.
-- ---------------------------------------------------------------------------
create or replace function pigeon_next_attempt(p_delivery_id bigint)
    returns int
    language sql
    security definer
    set search_path = public, pg_temp
as $$
    select coalesce(max(a.attempt), 0) + 1
    from attempts a
    where a.delivery_id = p_delivery_id;
$$;

-- ---------------------------------------------------------------------------
-- Record one failed attempt (FR-804). Evidence class: the caller runs this inside
-- db.EvidenceTx (synchronous_commit=off), so the set_config the pool applied
-- reaches this definer on the same transaction. Idempotent on (delivery_id,
-- attempt): a worker that double-writes the same attempt is a no-op, not a dup.
-- ---------------------------------------------------------------------------
create or replace function pigeon_record_attempt(
        p_delivery_id bigint,
        p_attempt     int,
        p_outcome     text,
        p_error_class text,
        p_status      int,
        p_body        text,
        p_duration_ms int,
        p_next_retry_at timestamptz
    )
    returns void
    language sql
    security definer
    set search_path = public, pg_temp
as $$
    insert into attempts
        (delivery_id, attempt, outcome, error_class, response_status,
         response_body, duration_ms, next_retry_at)
    values
        (p_delivery_id, p_attempt, p_outcome, p_error_class, p_status,
         left(coalesce(p_body, ''), 2048), p_duration_ms, p_next_retry_at)
    on conflict (delivery_id, attempt) do nothing;
$$;

-- ---------------------------------------------------------------------------
-- Complete a delivery: the successful attempt. Idempotent — it transitions a
-- still-pending row only — so a worker that double-completes (or a re-claim of a
-- delivery whose completion was lost) is a no-op rather than a duplicate. The
-- four columns FR-811 promises (status, attempts, first_attempt_at, delivered_at)
-- are the whole record of a success; no attempt row is written for it (AC-28).
-- Returns true when this call performed the transition, false when the row had
-- already left pending.
-- ---------------------------------------------------------------------------
create or replace function pigeon_complete_delivery(
        p_delivery_id   bigint,
        p_attempts      int,
        p_first_attempt timestamptz,
        p_delivered_at  timestamptz
    )
    returns boolean
    language sql
    security definer
    set search_path = public, pg_temp
as $$
    update deliveries
    set status = 'delivered',
        attempts = p_attempts,
        first_attempt_at = p_first_attempt,
        delivered_at = p_delivered_at,
        lease_expires_at = null
    where id = p_delivery_id and status = 'pending'
    returning true;
$$;

-- ---------------------------------------------------------------------------
-- Finalize a delivery as terminally failed (FR-806): the last attempt gave a
-- terminal response, or the retry budget is spent. Idempotent like complete.
-- ---------------------------------------------------------------------------
create or replace function pigeon_finalize_delivery(
        p_delivery_id   bigint,
        p_attempts      int,
        p_first_attempt timestamptz
    )
    returns boolean
    language sql
    security definer
    set search_path = public, pg_temp
as $$
    update deliveries
    set status = 'failed',
        attempts = p_attempts,
        first_attempt_at = p_first_attempt,
        lease_expires_at = null
    where id = p_delivery_id and status = 'pending'
    returning true;
$$;

-- ---------------------------------------------------------------------------
-- Re-lease a delivery for its next attempt after a retryable failure. The
-- backoff is the schedule: setting the lease into the future makes the claim
-- gate (pigeon_claim_delivery) skip the row until the backoff elapses, so the
-- retry cadence lives in the lease, not in a worker sleep. A worker that crashes
-- here leaves the lease at the claim's value; the sweeper covers it (D-6).
-- ---------------------------------------------------------------------------
create or replace function pigeon_requeue_delivery(p_delivery_id bigint, p_backoff interval)
    returns void
    language sql
    security definer
    set search_path = public, pg_temp
as $$
    update deliveries
    set lease_expires_at = now() + p_backoff
    where id = p_delivery_id and status = 'pending';
$$;

-- ---------------------------------------------------------------------------
-- Sweeper (D-6): re-queue EVERY delivery whose lease has lapsed, so a crashed
-- worker costs one lease interval, not a lost delivery. "Any" is literal: a lease
-- that lapsed while no sweep was running (a leadership move, a stalled tick) is
-- the case the requirement exists for, so there is no lookback bound here. A
-- retry's backoff lease is in the future, so a requeued row is never clobbered.
-- The sweeper stamps the lease to now() (a durable "re-queued at this time"
-- marker) and returns the ids of the rows it touched, so the caller counts them
-- for the operator alerter. It is the guaranteed path: the worker claim loop is
-- opportunistic, this runs on a fixed cadence clamped to the D-6 10 s ceiling. It
-- returns a set (not a scalar count) so an empty result is zero rows, not a null,
-- and the count is exact no matter how many rows were recovered in one sweep.
-- ---------------------------------------------------------------------------
create or replace function pigeon_sweeper_requeue()
    returns setof bigint
    language sql
    security definer
    set search_path = public, pg_temp
as $$
    update deliveries
    set lease_expires_at = now()
    where status = 'pending'
      and lease_expires_at < now()
    returning id;
$$;

-- The runtime role needs every one of these (it is not and must not be
-- tenant-scoped for the delivery path, exactly like the outbox functions).
grant execute on function pigeon_claim_delivery(interval) to current_user;
grant execute on function pigeon_next_attempt(bigint) to current_user;
grant execute on function pigeon_record_attempt(bigint,int,text,text,int,text,int,timestamptz) to current_user;
grant execute on function pigeon_complete_delivery(bigint,int,timestamptz,timestamptz) to current_user;
grant execute on function pigeon_finalize_delivery(bigint,int,timestamptz) to current_user;
grant execute on function pigeon_requeue_delivery(bigint,interval) to current_user;
grant execute on function pigeon_sweeper_requeue() to current_user;
