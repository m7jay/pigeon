-- 0015_s11_delivery_lanes_and_holds.sql — S11: the attempt-class lanes, the
-- claim path that can select a class, and the visible hold (FR-1303, FR-1304;
-- SPEC.md A-10).
--
-- 0015 is already claimed by two slices in flight (S10's hardening branch,
-- wt/t_b5272d2d-hardening, and the endpoint-custom-headers branch), so this takes
-- the next number free on the BASE and on every ref reachable from here: 0014
-- (dedupe/archive) is the highest on feat/m0-skeleton, and 0016 is the lowest
-- number no live ref holds. `internal/db/migrate_test.go` enforces unique ascending
-- versions, and a collision with another slice only shows up at rebase time as
-- `migrate: duplicate migration version`, so the number is picked from the refs
-- rather than from the base's directory listing alone.
--
-- Three things land here, each with the requirement that forces it:
--
--   * the attempt-class predicate on the claim path. FR-1303 requires the claim
--     path to be able to select a class, "or a retry backlog is claimed ahead of
--     every newer first attempt": the claim orders by id over every pending row
--     (`order by d.id limit 1`), so the oldest row always won and a due retry
--     backlog starved the first attempts behind it. The worker runs one claim loop
--     per lane against this predicate, so each lane sees only its own class.
--   * the hold columns. FR-1304 requires a delivery an endpoint's limits cannot
--     admit to be visible to the producer as "holding back deliveries", not as
--     silent latency: the worker records WHO held it and SINCE WHEN on the row,
--     and the delivery view returns both. `held_back_since` is the start of the
--     hold episode and is preserved across repeated refusals; it is cleared when
--     an attempt actually starts (the worker calls `pigeon_clear_delivery_hold`
--     when the claim it just made reported a hold).
--   * the two writer functions. `pigeon_hold_delivery` moves the claim gate with
--     the hold - the row must not be re-claimed in the same instant, exactly as
--     `pigeon_requeue_delivery` moves `lease_expires_at`/`next_attempt_at` - and
--     consumes no attempt, which is why it is not a requeue: `retry_deadline` and
--     `attempts` are untouched, because a hold is not a pause (FR-1207 extends the
--     deadline for a pause) and not an attempt (FR-1304).
--
-- A hold is durable state on the delivery row, and it is control state: it is
-- what the producer reads through FR-1102/FR-1103. The per-endpoint counters
-- (OBS-13) are the other half and live in the process's metrics, in Redis-free
-- memory, because a scrape is what reads them.

-- ---------------------------------------------------------------------------
-- FR-1304: the hold, as durable state on the delivery row.
--
-- Both columns are NULL on every existing row and on every row whose attempt has
-- started. `held_back_reason` is the limiter's own name for what refused the
-- admission ("endpoint_concurrency_cap" or "endpoint_rps"), so the producer sees
-- which of FR-1302 and FR-1306 is holding it rather than a bare "held".
-- ---------------------------------------------------------------------------
alter table deliveries add column held_back_since  timestamptz;
alter table deliveries add column held_back_reason text;

comment on column deliveries.held_back_since is
    'FR-1304: when this delivery started being held back by its endpoint''s limits. Preserved across repeated refusals; cleared when an attempt starts.';
comment on column deliveries.held_back_reason is
    'FR-1304: the limiter that refused admission - endpoint_concurrency_cap (FR-1302) or endpoint_rps (FR-1306).';

-- ---------------------------------------------------------------------------
-- The claim, with the attempt class (FR-1303).
--
-- The argument list grows by one, so the old signature goes the way 0013 removed
-- 0008's and 0010's: every previous form is dropped and one comes back with the
-- union. `p_attempt_class` is one of 'first' (deliveries.attempts = 0), 'retry'
-- (attempts > 0) or 'any'; an unknown value selects nothing rather than silently
-- meaning 'any' (the only caller is the worker's `store.AttemptClass`, which
-- refuses an unknown value before the query is sent).
--
-- The OUT row also grows a `held_back` flag: the worker clears the hold when an
-- attempt actually starts, and this is what tells it whether there is one to
-- clear, without a second read of the same row.
-- ---------------------------------------------------------------------------
drop function if exists pigeon_claim_delivery(interval);
drop function if exists pigeon_claim_delivery(interval, interval);

create or replace function pigeon_claim_delivery(
        -- The deployment's lease and §9 retry window. They are parameters rather
        -- than constants so each default has ONE source (the deployment config)
        -- instead of two that drift, and neither is defaulted here: this version
        -- takes the attempt class, and a defaulted parameter cannot be followed by
        -- a required one. The caller (internal/store's ClaimDelivery) always
        -- passes all three, so nothing is lost but a 2-argument call - which would
        -- be exactly the pre-S11 claim, silently. It is refused instead.
        p_lease                 interval,
        -- Applied to a delivery whose deadline is not yet stamped at an endpoint
        -- with no override of its own: the window the worker schedules with.
        p_default_retry_window  interval,
        -- FR-1303: the attempt class this lane claims. A replay creates a new
        -- delivery row (FR-1106), so a replayed delivery's first attempt is a
        -- first attempt and its class is the same as any other first attempt's.
        --
        -- No default on purpose: a caller that forgets the class would silently
        -- get the pre-S11 behaviour (the oldest row wins, whatever its class),
        -- which is the defect FR-1303 names.
        p_attempt_class         text
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
        max_rps               int,
        -- FR-1304: is this delivery being held back right now? The worker clears
        -- the hold the moment an attempt starts, and only asks when this is true.
        held_back             boolean
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
          -- FR-1303: this lane's attempt class, and only its class. A delivery's
          -- class is decided by its OWN counter, never by how it was published.
          and (p_attempt_class = 'any'
               or (p_attempt_class = 'first' and d.attempts = 0)
               or (p_attempt_class = 'retry' and d.attempts > 0))
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
           coalesce(ep.max_rps, 0),
           (d.held_back_since is not null)
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
-- FR-1304: record a hold and move the claim gate with it.
--
-- The row is NOT re-claimable before `p_until`: the worker has already taken the
-- claim (the claim is where the delivery came from), so releasing it without
-- moving the gate would re-claim the same row in the same millisecond and spin.
-- This is the same pair `pigeon_requeue_delivery` moves, for the same reason.
--
-- `held_back_since` is coalesced, so a delivery refused a hundred times over a
-- minute reports the minute. `retry_deadline` is deliberately absent: a hold is
-- not a pause (FR-1207), and an endpoint that is up and merely at its cap has
-- given no reason to extend the window. `attempts` is absent for the same kind of
-- reason: a hold consumes no attempt (FR-1304), so the delivery still owes the
-- attempt it owed before it was held.
--
-- The one-millisecond floor is the degenerate case of the limiter reporting a
-- zero wait (the slot that just freed belongs to another worker): without it the
-- row would be claimable in the same instant, which is the spin the gate exists
-- to prevent.
--
-- The CTE + coalesce is not decoration: a `returns boolean` function whose
-- UPDATE ... RETURNING matched nothing yields SQL NULL rather than an empty
-- result, and the caller reads the flag to tell "held" from "another worker
-- finished it first". Returning a real false for the no-row case keeps that
-- distinction in the function instead of in a nullable scan.
-- ---------------------------------------------------------------------------
create or replace function pigeon_hold_delivery(
        p_delivery_id   bigint,
        p_reason        text,
        p_until         timestamptz
    )
    returns boolean
    language sql
    security definer
    set search_path = public, pg_temp
as $$
    with held as (
        update deliveries d
        set held_back_since  = coalesce(d.held_back_since, now()),
            held_back_reason = p_reason,
            lease_expires_at = greatest(p_until, now() + interval '1 millisecond'),
            next_attempt_at  = greatest(p_until, now() + interval '1 millisecond')
        where d.id = p_delivery_id
          and d.status = 'pending'
        returning true
    )
    select coalesce((select true from held), false);
$$;

-- ---------------------------------------------------------------------------
-- FR-1304: the hold ends when an attempt starts.
--
-- Called by the worker only when the claim it just made reported `held_back`, so
-- ordinary traffic pays nothing for this function existing. A delivery that is
-- held again reports the new episode's start: the previous hold ended when its
-- attempt ran, which is the honest answer and the reason this is a clear rather
-- than a coalesce-on-write.
-- ---------------------------------------------------------------------------
create or replace function pigeon_clear_delivery_hold(p_delivery_id bigint)
    returns void
    language sql
    security definer
    set search_path = public, pg_temp
as $$
    update deliveries
    set held_back_since = null, held_back_reason = null
    where id = p_delivery_id
      and (held_back_since is not null or held_back_reason is not null);
$$;

grant execute on function pigeon_claim_delivery(interval, interval, text) to current_user;
grant execute on function pigeon_hold_delivery(bigint, text, timestamptz) to current_user;
grant execute on function pigeon_clear_delivery_hold(bigint) to current_user;
