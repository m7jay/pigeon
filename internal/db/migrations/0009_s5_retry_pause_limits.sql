-- 0009_s5_retry_pause_limits.sql — S5: the retry window, per-endpoint limits,
-- the threshold pauses, and the terminal-state vocabulary.
--
-- Four things land here, each with the requirement that forces it:
--
--   * the retry WINDOW as durable state. §9 gives every delivery a 72 h window
--     (per-endpoint override 1 h - 7 d) and FR-1201 makes the window take
--     precedence over the retry count: an attempt scheduled beyond the deadline
--     is not made. A deadline that only lives in a worker's memory cannot
--     survive a restart, so `deliveries.retry_deadline` is a column, and
--     `next_attempt_at` (FR-1208) is the other half of what FR-1208 promises a
--     consumer can read: how long the platform intends to keep trying.
--   * the per-endpoint retry / limit knobs (§9 "per endpoint" column, FR-1302
--     concurrency, FR-1306 RPS). The values are a property of the endpoint, so
--     they live on `endpoints`, not in the worker's options.
--   * the pause state (FR-1202, FR-1203, FR-1205, FR-1207, D-8). A pause is a
--     durable Postgres column written as control state; the failure windows that
--     decide it are cheap Redis counters that may be lost — losing them can
--     DELAY a pause for at most a window, and can never invent one, because the
--     flags live here (D-8).
--   * `failed_terminal` as the terminal status name. I-4 says a delivery is
--     terminal only in `delivered`, `failed_terminal`, `cancelled`, and AC-36
--     asserts `failed_terminal` at the window boundary; 0007 wrote `failed`.
--     One name for one state, so the column check is widened and existing rows
--     are moved rather than left as a second spelling.
--
-- The delivery worker is a system worker like the relay (S4): no producer scope,
-- reaching these tables only through the SECURITY DEFINER functions below. The
-- pause and resume functions are called from two sides — the worker on a
-- threshold trip, and the API on FR-1205's one-action resume — so they return
-- enough for the caller to decide whether a notification is owed and which Redis
-- windows to clear.

-- ---------------------------------------------------------------------------
-- Terminal state vocabulary (I-4, AC-36).
--
-- The check is replaced rather than added to: `failed` and `failed_terminal`
-- naming the same state is exactly the drift I-4 forbids, and AC-36 asserts the
-- name. Existing rows are moved in the same statement that widens the check, so
-- there is no instant where the table holds a value the constraint rejects.
-- ---------------------------------------------------------------------------
alter table deliveries drop constraint deliveries_status_check;
update deliveries set status = 'failed_terminal' where status = 'failed';
alter table deliveries
    add constraint deliveries_status_check
    check (status in ('pending', 'skipped', 'delivered', 'failed_terminal'));

comment on column deliveries.status is
    'I-4: pending | skipped | delivered | failed_terminal. skipped/delivered/failed_terminal are terminal.';

-- ---------------------------------------------------------------------------
-- The retry window and the next-attempt instant (FR-1201, FR-1207, FR-1208).
--
-- next_attempt_at and lease_expires_at are deliberately the same instant for a
-- pending row: the lease IS the claim gate (FR-813, D-6), and a second instant
-- that could disagree with it would be a bug waiting to happen. next_attempt_at
-- exists because FR-1208 exposes it to a consumer as "when the platform will try
-- next", and because the pause extension (FR-1207) has to move it in lockstep.
--
-- retry_deadline is the wall clock by which the delivery must have succeeded or
-- it becomes failed_terminal *regardless of remaining retries* (FR-1201, the
-- window takes precedence). It is stamped at fan-out from the endpoint's window
-- so it exists before the first attempt ever runs.
-- ---------------------------------------------------------------------------
alter table deliveries add column next_attempt_at timestamptz;
alter table deliveries add column retry_deadline  timestamptz;

comment on column deliveries.next_attempt_at is
    'FR-1208: when the platform intends to attempt this delivery next. Equal to lease_expires_at for a pending row.';
comment on column deliveries.retry_deadline is
    'FR-1201/FR-1208: the end of the retry window. An attempt scheduled beyond it is not made; the row becomes failed_terminal. Extended by any paused period (FR-1207).';

-- ---------------------------------------------------------------------------
-- Per-endpoint retry policy and limits (§9, FR-1302, FR-1306).
--
-- Defaults are §9's and FR-1302/FR-1306's documented values. The bounds are the
-- requirements' bounds: 3-30 retries, a 1 h - 7 d window, and FR-802's 5 s hard
-- ceiling on an attempt. A concurrency or RPS of 0 is NOT allowed: FR-1307 says
-- the limit is a property of the endpoint, and "no limit" is the one value that
-- makes AC-41 unverifiable. An operator who wants a bigger number raises the
-- number.
-- ---------------------------------------------------------------------------
-- §9, FR-1201, FR-1302, FR-1306: the per-endpoint overrides.
--
-- NULL means "no override": the delivery worker applies the deployment default,
-- which is the same §9 row the platform documents - 15 retries, 30 s base, 12 h
-- cap, 72 h window, 5 s timeout (the FR-802 ceiling), 10 concurrent, 25 RPS.
-- The distinction matters: a NOT NULL default would make "the endpoint set 5000
-- ms" indistinguishable from "the endpoint said nothing", so a deployment whose
-- per-attempt ceiling is short would still run 5 s attempts, and the deployment
-- setting that exists precisely to bound them would be silently inert.
--
-- Each bound below is the range §9/FR-1302/FR-1306 grants an endpoint, enforced
-- where the value is set rather than trusted from the caller.
alter table endpoints add column max_retries int
    check (max_retries is null or max_retries between 3 and 30);
alter table endpoints add column retry_backoff_base interval
    check (retry_backoff_base is null or retry_backoff_base > interval '0');
alter table endpoints add column retry_backoff_max interval
    check (retry_backoff_max is null
           or retry_backoff_base is null
           or retry_backoff_max >= retry_backoff_base);
alter table endpoints add column retry_window interval
    check (retry_window is null or retry_window between interval '1 hour' and interval '7 days');
-- FR-802's hard ceiling is 5 s and it is absolute; §9's operator-facing range is
-- 2-5 s, enforced where an operator sets it (the admin surface, S6). The column
-- accepts a lower value so a deployment can shorten the ceiling - the load tests
-- in §12 and the delivery package's own tests both rely on that - and the worker
-- clamps to 5 s regardless of what the row says.
alter table endpoints add column delivery_timeout_ms int
    check (delivery_timeout_ms is null
           or (delivery_timeout_ms > 0 and delivery_timeout_ms <= 5000));
-- FR-1302: leased slots, cluster-wide, default 10.
alter table endpoints add column max_concurrency int
    check (max_concurrency is null or max_concurrency >= 1);
-- FR-1306: token bucket, default 25 RPS.
alter table endpoints add column max_rps int
    check (max_rps is null or max_rps >= 1);

comment on column endpoints.max_concurrency is
    'FR-1302/FR-1307: per-endpoint in-flight cap, cluster-wide via a Redis lease. NULL = the deployment default (10). Also FR-817 transport connection cap.';
comment on column endpoints.max_rps is
    'FR-1306: per-endpoint RPS, enforced cluster-wide by a Redis token bucket. A bucket bounds rate over time; a leased slot bounds in-flight count (FR-1306 forbids conflating them).';
comment on column endpoints.retry_window is
    '§9/FR-1201: the per-endpoint retry window (1 h - 7 d). AC-37 overrides the 72 h default to 1 h and proves it governs.';

-- ---------------------------------------------------------------------------
-- Pause state (FR-1202, FR-1203, FR-1205, D-8).
--
-- The flag is a column, never a Redis key (D-8). pause_reason distinguishes an
-- operator's manual pause (FR-406) from a threshold trip, which is what makes
-- "resume clears the windows the threshold paused on" (FR-1211) decidable.
-- ---------------------------------------------------------------------------
alter table endpoints add column pause_reason text
    check (pause_reason is null or pause_reason in ('failure_threshold', 'operator'));
alter table subscriptions add column pause_reason text
    check (pause_reason is null or pause_reason in ('failure_threshold', 'operator'));
alter table subscriptions add column paused_at timestamptz;

comment on column subscriptions.status is
    'FR-506/FR-1202: active | paused | deleted. A paused subscription collects no new deliveries and holds in-flight ones without spending retry budget.';
comment on column subscriptions.pause_reason is
    'FR-1202 vs FR-406: failure_threshold (the 10-failure trip) or operator (an explicit pause). FR-1211 clears counter state on resume of either.';

-- ---------------------------------------------------------------------------
-- The one pause notification per episode (FR-1204, AC-14, AC-44).
--
-- "MUST notify the configured address once" is a statement about the record,
-- not about a mailer: v1.0 has no SMTP path until the portals land (S9), so the
-- notification is recorded durably here — naming the endpoint, the event type
-- (subscription pause) or the count of failing subscriptions (endpoint pause),
-- and the first and last failing response, which is what FR-1204 requires the
-- message to name. The Notifier seam in internal/pause is what a mailer will
-- implement; this row is what makes "exactly one" true and testable today.
--
-- The unique index is the "once": one row per (endpoint, kind, event type, pause
-- episode). paused_at changes on every resume/re-pause, so a later episode gets
-- its own row and its own message, while a second failure inside one episode
-- cannot produce a second notification.
-- ---------------------------------------------------------------------------
create table pause_notifications (
    id                    bigserial primary key,
    kind                  text not null check (kind in ('subscription', 'endpoint')),
    producer_id           text not null,
    env                   text not null,
    endpoint_id           text not null,
    -- Set for a subscription pause; null for an endpoint pause.
    event_type            text,
    -- Set for an endpoint pause: how many distinct subscriptions were failing.
    failing_subscriptions int,
    -- FR-1204: the first and last failing response in the window that tripped it.
    first_response        text,
    last_response         text,
    paused_at             timestamptz not null,
    occurred_at           timestamptz not null default now()
);

comment on table pause_notifications is
    'FR-1204/AC-14: exactly one row per pause episode. The unique index is the "once"; the fields are what the notification must name.';

alter table pause_notifications enable row level security;
-- System-written, operator-read via the definer functions; no tenant policy.

create unique index pause_notifications_once
    on pause_notifications (endpoint_id, kind, coalesce(event_type, ''), paused_at);

create index pause_notifications_endpoint_idx
    on pause_notifications (endpoint_id, occurred_at desc);

-- ---------------------------------------------------------------------------
-- Seed the new delivery columns for rows that predate them.
--
-- next_attempt_at is backfilled from the lease, because the two are the same
-- instant for a pending row (0008's lease IS its due time). retry_deadline is
-- deliberately left alone: the claim computes it (created_at + the endpoint's
-- override or the deployment window) and the first failure stamps it durably, so
-- there is exactly one source for the window - the deployment config - instead of
-- a constant here that would silently disagree with it.
-- ---------------------------------------------------------------------------
update deliveries
set next_attempt_at = lease_expires_at
where status = 'pending'
  and next_attempt_at is null;

-- ---------------------------------------------------------------------------
-- Claim: lease the next due pending delivery.
--
-- Unchanged from 0008 except for the pause gate (FR-1202, FR-406/FR-407,
-- FR-1207): a delivery whose endpoint or subscription is paused is NOT claimed.
-- That is what "a paused endpoint/subscription collects no new deliveries and
-- holds in-flight ones" means for work that was already fanned out - the row
-- keeps its pending status, its retry_deadline is extended by the paused period
-- on resume, and the retry schedule picks up where it left off. Without this
-- gate a pause would keep hammering the receiver through deliveries that were
-- already in the queue, which is the opposite of what FR-1202 is for.
--
-- The subscription is identified by (endpoint_id, event_type) rather than by a
-- column on deliveries: a subscription IS the pair (FR-501), and the partial
-- unique index on the live pairs makes the join exact.
-- ---------------------------------------------------------------------------
-- 0010_attempt_buffer (S6) recreated the claim with the same argument list but a
-- different OUT row (it added row_attempts); this version keeps that column and
-- adds S5's, so both argument lists go and one comes back with the union.
drop function if exists pigeon_claim_delivery(interval);
drop function if exists pigeon_claim_delivery(interval, interval);

create or replace function pigeon_claim_delivery(
        p_lease                 interval default interval '40 seconds',
        -- The deployment's §9 retry window, applied when the endpoint sets no
        -- override of its own. It is a parameter rather than a constant so the
        -- default has ONE source (the deployment config) instead of two that
        -- drift: a delivery whose deadline is not yet stamped, at an endpoint
        -- with no override, gets exactly the window the worker schedules with.
        p_default_retry_window  interval default interval '72 hours'
    )
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
        prior_attempts  int,
        -- D-2: deliveries.attempts, the control-state attempt counter advanced by
        -- pigeon_requeue_delivery. Always current, in both attempt-log modes,
        -- because the attempt number must not depend on the evidence buffer.
        row_attempts    int,
        first_attempt_at timestamptz,
        -- S5: the retry window state (FR-1201, FR-1207, FR-1208).
        next_attempt_at timestamptz,
        retry_deadline  timestamptz,
        -- S5: the endpoint's retry policy and limits (§9, FR-1302, FR-1306).
        -- The three durations travel as epoch SECONDS rather than as intervals,
        -- because that is what the Go caller can read without an interval type:
        -- the worker needs a time.Duration, and a cast here is one place instead
        -- of a conversion on every call site.
        --
        -- Every one of them is NULL-able on the endpoint row, and NULL travels
        -- as 0: the caller applies its own default. That way the row says only
        -- what it overrides.
        max_retries           int,
        retry_backoff_base_s  double precision,
        retry_backoff_max_s   double precision,
        retry_window_s        double precision,
        delivery_timeout_ms   int,
        max_concurrency       int,
        max_rps               int
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
          -- FR-1202/FR-406/FR-407: hold while the endpoint is paused or deleted.
          and exists (
              select 1 from endpoints ep
              where ep.producer_id = d.producer_id and ep.env = d.env
                and ep.id = d.endpoint_id and ep.status = 'active'
          )
          -- FR-1202: hold while the subscription for this event type is paused.
          and exists (
              select 1
              from subscriptions s
              join events e on e.id = d.event_id
              where s.producer_id = d.producer_id and s.env = d.env
                and s.endpoint_id = d.endpoint_id
                and s.event_type = e.event_type
                and s.status = 'active'
          )
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
           coalesce(da.n, 0), d.attempts, da.first_attempt_at,
           coalesce(d.next_attempt_at, d.lease_expires_at),
           coalesce(d.retry_deadline,
                    d.created_at + coalesce(ep.retry_window, p_default_retry_window)),
           coalesce(ep.max_retries, 0),
           coalesce((extract(epoch from ep.retry_backoff_base))::double precision, 0),
           coalesce((extract(epoch from ep.retry_backoff_max))::double precision, 0),
           coalesce((extract(epoch from ep.retry_window))::double precision, 0),
           coalesce(ep.delivery_timeout_ms, 0),
           coalesce(ep.max_concurrency, 0),
           coalesce(ep.max_rps, 0)
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
-- Re-lease a delivery for its next attempt: the backoff, the next-attempt
-- instant and the (possibly extended) retry deadline move together, in one
-- write. FR-1207 is why the deadline travels with the requeue rather than being
-- recomputed from a wall-clock constant: a delivery that has been paused and
-- resumed must still see the window it was promised.
-- ---------------------------------------------------------------------------
-- 0008 created pigeon_requeue_delivery(bigint, interval); 0010_attempt_buffer (S6)
-- recreated it as (bigint, interval, int) to advance deliveries.attempts (D-2).
-- This version keeps that counter advance and takes the absolute instants S5
-- schedules with, so all three drops go and one comes back.
drop function if exists pigeon_requeue_delivery(bigint, interval);
drop function if exists pigeon_requeue_delivery(bigint, interval, int);

create or replace function pigeon_requeue_delivery(
        p_delivery_id     bigint,
        p_next_attempt_at timestamptz,
        p_retry_deadline  timestamptz,
        p_attempt         int
    )
    returns void
    language sql
    security definer
    set search_path = public, pg_temp
as $$
    update deliveries
    set lease_expires_at = p_next_attempt_at,
        next_attempt_at  = p_next_attempt_at,
        retry_deadline   = p_retry_deadline,
        -- D-2: greatest(), never assigned, so a re-delivered requeue (a retried
        -- control write) cannot walk the attempt number backwards while the
        -- evidence row may still be sitting in the Redis buffer.
        attempts         = greatest(attempts, coalesce(p_attempt, 0))
    where id = p_delivery_id and status = 'pending';
$$;

comment on function pigeon_requeue_delivery(bigint,timestamptz,timestamptz,int) is
    'FR-805/FR-1208/D-2: moves the lease, next_attempt_at and the retry deadline together and advances deliveries.attempts, so the attempt number never depends on the attempt evidence buffer.';

-- ---------------------------------------------------------------------------
-- Sweeper (D-6): re-queue EVERY delivery whose lease has lapsed.
--
-- Same guarantee as 0008 - any lapsed lease, no lookback bound - with one S5
-- addition: next_attempt_at moves with the lease, so the pair FR-1208 exposes
-- stays equal to the claim gate instead of drifting behind it.
-- ---------------------------------------------------------------------------
create or replace function pigeon_sweeper_requeue()
    returns setof bigint
    language sql
    security definer
    set search_path = public, pg_temp
as $$
    update deliveries
    set lease_expires_at = now(),
        next_attempt_at  = now()
    where status = 'pending'
      and lease_expires_at < now()
    returning id;
$$;

-- ---------------------------------------------------------------------------
-- Finalize a delivery as terminally failed: the last attempt gave a terminal
-- response, the retry budget is spent, or the retry WINDOW is exhausted
-- (FR-1201; the window takes precedence over the count). next_attempt_at is
-- cleared, because there is no next attempt. Idempotent like complete.
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
    set status = 'failed_terminal',
        attempts = p_attempts,
        first_attempt_at = p_first_attempt,
        lease_expires_at = null,
        next_attempt_at = null
    where id = p_delivery_id and status = 'pending'
    returning true;
$$;

-- ---------------------------------------------------------------------------
-- How many of an endpoint's subscriptions are unpaused (FR-1203). `active` is
-- read once per newly failing subscription, never per attempt, and it is the
-- numerator of the breadth threshold N = min(active, max(3, ceil(active/4))).
-- ---------------------------------------------------------------------------
create or replace function pigeon_endpoint_subscription_stats(p_endpoint_id text)
    returns table (active int, paused int, total int)
    language sql
    security definer
    set search_path = public, pg_temp
as $$
    select count(*) filter (where status = 'active')::int,
           count(*) filter (where status = 'paused')::int,
           count(*)::int
    from subscriptions
    where endpoint_id = p_endpoint_id and status <> 'deleted';
$$;

-- ---------------------------------------------------------------------------
-- Threshold pause of one subscription (FR-1202, FR-1204).
--
-- The transition is one-shot: the flag is set only from 'active', so a second
-- failure inside the same episode changes nothing and sends nothing - which is
-- what "notify once" (FR-1204) means. The notification row is inserted in the
-- same statement as the transition, so the "exactly one" record cannot diverge
-- from the pause it describes.
--
-- The guard `and the endpoint is still active` is what FR-1210 asks for: when
-- the endpoint has tripped, no per-subscription pause is written for it, so a
-- resume of the endpoint restores exactly the subscriptions that did not trip
-- their own threshold.
--
-- first_response / last_response come from the evidence table (FR-804 rows),
-- which is the only place the peer's actual answer is kept.
-- ---------------------------------------------------------------------------
create or replace function pigeon_pause_subscription(
        p_endpoint_id text,
        p_event_type  text,
        p_reason      text
    )
    returns table (
        paused           boolean,
        producer_id      text,
        env              text,
        subscription_id  text,
        event_type       text,
        first_response   text,
        last_response    text,
        paused_at        timestamptz
        )
    language sql
    security definer
    set search_path = public, pg_temp
as $$
    with target as (
        select s.id, s.producer_id, s.env, s.event_type
        from subscriptions s
        where s.endpoint_id = p_endpoint_id
          and s.event_type = p_event_type
          and s.status = 'active'
          and exists (
              select 1 from endpoints ep
              where ep.id = s.endpoint_id and ep.status = 'active'
          )
    ),
    moved as (
        update subscriptions s
        set status = 'paused', paused_at = now(), pause_reason = p_reason
        from target t
        where s.id = t.id
        returning s.id, s.producer_id, s.env, s.event_type, s.paused_at
    ),
    windowed as (
        select a.occurred_at,
               format('%s %s', coalesce(a.response_status::text, a.error_class),
                      left(coalesce(a.response_body, ''), 200)) as response
        from attempts a
        join deliveries d on d.id = a.delivery_id
        join events e on e.id = d.event_id
        where d.endpoint_id = p_endpoint_id
          and e.event_type = p_event_type
          and a.occurred_at >= now() - interval '1 hour'
    ),
    first_seen as (select response from windowed order by occurred_at asc limit 1),
    last_seen  as (select response from windowed order by occurred_at desc limit 1),
    noted as (
        insert into pause_notifications
            (kind, producer_id, env, endpoint_id, event_type,
             first_response, last_response, paused_at)
        select 'subscription', m.producer_id, m.env, p_endpoint_id, m.event_type,
               (select response from first_seen), (select response from last_seen),
               m.paused_at
        from moved m
        on conflict do nothing
        returning 1
    )
    select exists (select 1 from moved),
           coalesce((select producer_id from moved), ''),
           coalesce((select env from moved), ''),
           coalesce((select id from moved), ''),
           coalesce((select event_type from moved), ''),
           coalesce((select response from first_seen), ''),
           coalesce((select response from last_seen), ''),
           (select paused_at from moved);
$$;

-- ---------------------------------------------------------------------------
-- Threshold pause of an endpoint (FR-1203, FR-1204, FR-1210).
--
-- One flag, no per-subscription pause rows (FR-1210). The caller passes the
-- breadth count it measured, because the breadth set lives in Redis (§A-1) and
-- the count is what the notification must name.
-- ---------------------------------------------------------------------------
create or replace function pigeon_pause_endpoint(
        p_endpoint_id           text,
        p_reason                text,
        p_failing_subscriptions int
    )
    returns table (
        paused                boolean,
        producer_id           text,
        env                   text,
        endpoint_id           text,
        failing_subscriptions int,
        first_response        text,
        last_response         text,
        paused_at             timestamptz
        )
    language sql
    security definer
    set search_path = public, pg_temp
as $$
    with target as (
        select ep.id, ep.producer_id, ep.env
        from endpoints ep
        where ep.id = p_endpoint_id and ep.status = 'active'
    ),
    moved as (
        update endpoints ep
        set status = 'paused', paused_at = now(), pause_reason = p_reason
        from target t
        where ep.id = t.id
        returning ep.id, ep.producer_id, ep.env, ep.paused_at
    ),
    windowed as (
        select a.occurred_at,
               format('%s %s', coalesce(a.response_status::text, a.error_class),
                      left(coalesce(a.response_body, ''), 200)) as response
        from attempts a
        join deliveries d on d.id = a.delivery_id
        where d.endpoint_id = p_endpoint_id
          and a.occurred_at >= now() - interval '1 hour'
    ),
    first_seen as (select response from windowed order by occurred_at asc limit 1),
    last_seen  as (select response from windowed order by occurred_at desc limit 1),
    noted as (
        insert into pause_notifications
            (kind, producer_id, env, endpoint_id, failing_subscriptions,
             first_response, last_response, paused_at)
        select 'endpoint', m.producer_id, m.env, p_endpoint_id,
               p_failing_subscriptions,
               (select response from first_seen), (select response from last_seen),
               m.paused_at
        from moved m
        on conflict do nothing
        returning 1
    )
    select exists (select 1 from moved),
           coalesce((select producer_id from moved), ''),
           coalesce((select env from moved), ''),
           coalesce((select id from moved), ''),
           p_failing_subscriptions,
           coalesce((select response from first_seen), ''),
           coalesce((select response from last_seen), ''),
           (select paused_at from moved);
$$;

-- ---------------------------------------------------------------------------
-- Resume (FR-1205, FR-1207, FR-1211).
--
-- One action, no replay (FR-1205): resuming clears the flag and extends the
-- window of everything that was held, and does NOT re-run anything. The caller
-- clears the Redis counter state in the same breath (FR-1211) - an endpoint
-- resume must clear its members' windows too, which is why the SQL side is
-- deliberately only the durable half.
--
-- FR-1207 is the arithmetic below: every pending delivery on the paused scope
-- gets its retry_deadline, next_attempt_at and lease pushed forward by exactly
-- the paused duration, so a pause cannot consume a delivery's remaining window
-- and the schedule resumes at its next interval rather than resetting.
-- ---------------------------------------------------------------------------
create or replace function pigeon_resume_endpoint(p_endpoint_id text)
    returns table (
        resumed    boolean,
        producer_id text,
        env        text,
        paused_for interval
        )
    language sql
    security definer
    set search_path = public, pg_temp
as $$
    with target as (
        select ep.id, ep.producer_id, ep.env, ep.paused_at
        from endpoints ep
        where ep.id = p_endpoint_id and ep.status = 'paused'
    ),
    moved as (
        update endpoints ep
        set status = 'active', pause_reason = null
        from target t
        where ep.id = t.id
        returning ep.id, t.producer_id, t.env, t.paused_at
    ),
    extended as (
        update deliveries d
        set retry_deadline  = coalesce(d.retry_deadline, now()) + (now() - m.paused_at),
            next_attempt_at = coalesce(d.next_attempt_at, now()) + (now() - m.paused_at),
            lease_expires_at = coalesce(d.lease_expires_at, now()) + (now() - m.paused_at)
        from moved m
        where d.endpoint_id = m.id and d.status = 'pending'
        returning d.id
    )
    select exists (select 1 from moved),
           coalesce((select producer_id from moved), ''),
           coalesce((select env from moved), ''),
           (select now() - paused_at from moved);
$$;

create or replace function pigeon_resume_subscription(p_endpoint_id text, p_event_type text)
    returns table (
        resumed    boolean,
        producer_id text,
        env        text,
        paused_for interval
        )
    language sql
    security definer
    set search_path = public, pg_temp
as $$
    with target as (
        select s.id, s.producer_id, s.env, s.paused_at
        from subscriptions s
        where s.endpoint_id = p_endpoint_id
          and s.event_type = p_event_type
          and s.status = 'paused'
    ),
    moved as (
        update subscriptions s
        set status = 'active', pause_reason = null
        from target t
        where s.id = t.id
        returning s.id, t.producer_id, t.env, t.paused_at
    ),
    extended as (
        update deliveries d
        set retry_deadline  = coalesce(d.retry_deadline, now()) + (now() - m.paused_at),
            next_attempt_at = coalesce(d.next_attempt_at, now()) + (now() - m.paused_at),
            lease_expires_at = coalesce(d.lease_expires_at, now()) + (now() - m.paused_at)
        from moved m, events e
        where e.id = d.event_id
          and d.endpoint_id = p_endpoint_id
          and e.event_type = p_event_type
          and d.status = 'pending'
        returning d.id
    )
    select exists (select 1 from moved),
           coalesce((select producer_id from moved), ''),
           coalesce((select env from moved), ''),
           (select now() - paused_at from moved);
$$;

-- The pause counts as a notification-worthy event for an operator looking at
-- the endpoint (FR-1204's "configured address" has no mailer in v1.0; the row is
-- the record the operator and the portal read).
create or replace function pigeon_pause_notifications(p_endpoint_id text, p_limit int default 20)
    returns setof pause_notifications
    language sql
    security definer
    set search_path = public, pg_temp
as $$
    select * from pause_notifications
    where endpoint_id = p_endpoint_id
    order by occurred_at desc
    limit greatest(p_limit, 1);
$$;

grant execute on function pigeon_claim_delivery(interval, interval) to current_user;
grant execute on function pigeon_requeue_delivery(bigint, timestamptz, timestamptz, int) to current_user;
grant execute on function pigeon_finalize_delivery(bigint, int, timestamptz) to current_user;
grant execute on function pigeon_endpoint_subscription_stats(text) to current_user;
grant execute on function pigeon_pause_subscription(text, text, text) to current_user;
grant execute on function pigeon_pause_endpoint(text, text, int) to current_user;
grant execute on function pigeon_resume_endpoint(text) to current_user;
grant execute on function pigeon_resume_subscription(text, text) to current_user;
grant execute on function pigeon_pause_notifications(text, int) to current_user;
