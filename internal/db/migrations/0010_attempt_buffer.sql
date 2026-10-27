-- 0010_attempt_buffer.sql — S6: the buffered evidence path, and the attempt
-- counter that must not depend on it.
--
-- Two things change here, and they belong together.
--
-- 1. **Attempts are written in batches.** D-1 puts the attempt buffer in Redis
--    (internal/attemptbuf) and FR-815 requires the flusher to insert at least
--    2,000 rows per transaction, so the per-row definer in 0008 gains a batched
--    sibling: one statement, one transaction, one commit. Evidence rows stay the
--    evidence class of §6/FR-812 — the caller runs this inside db.EvidenceTx, so
--    the transaction-local synchronous_commit=off reaches this definer on the
--    same transaction, and so does the pg_stat_activity class marker AC-29 reads.
--
-- 2. **The attempt number stops depending on the attempt rows.** 0008 derived the
--    next attempt number from `max(attempts.attempt) + 1`. With buffered evidence
--    that is wrong: the previous attempt's row may still be sitting in Redis, so
--    a re-claim would hand out a number the buffer has already used, and the
--    unique (delivery_id, attempt) key would then silently drop the later row.
--    D-2 settles it — "retry and status MUST NOT depend on the attempt buffer"
--    and the attempt counter is not the buffer's to own — so the counter moves
--    onto the delivery row, which is control state and always current:
--
--      * `pigeon_requeue_delivery` advances `deliveries.attempts` when it
--        schedules the retry. That write already happens exactly once per
--        retryable failure and is already a control write (synchronous_commit=on),
--        so this costs no extra round trip and no extra fsync.
--      * `pigeon_claim_delivery` returns that counter, and the worker takes
--        max(delivery counter, attempt rows) + 1 — the maximum, so a sync-mode
--        database (where both track the same thing) and a buffered one (where
--        only the counter is current) number attempts identically.
--
--    Nothing else about the delivery lifecycle moves: the lease is still set at
--    fan-out (FR-813), the worker still writes only on completion, terminal
--    failure, or a retryable failure's requeue (FR-813, L4), and a lapsed lease
--    still re-queues through the sweeper (D-6) without touching the counter,
--    because a lease lapsing is not a new attempt.

-- ---------------------------------------------------------------------------
-- The batched evidence insert (FR-804, FR-811, FR-815, SB-4).
--
-- The arrays are zipped positionally by a SINGLE multi-argument unnest: two
-- comma-separated unnest() calls in one FROM would be a cross join, which
-- produces the right ROW COUNT after the conflict check while pairing each row
-- with the wrong values — the failure mode this shape exists to avoid.
--
-- Returns the number of rows actually inserted. A row Postgres already has is
-- not an error and is not counted: `on conflict do nothing` is what makes a
-- replayed batch (the buffer's own recovery after a crash) idempotent, and the
-- caller compares the count with the batch size to detect a genuine collision.
--
-- p_body_cap is SB-4's truncation, applied on the database side as well as at
-- capture: >= 0 truncates to that many bytes (0 stores nothing), < 0 stores the
-- body as given. The default is the configured cap, so the 2 KB default in 0008
-- is a default and no longer a ceiling.
-- ---------------------------------------------------------------------------
create or replace function pigeon_record_attempts(
        p_delivery_ids  bigint[],
        p_attempts      int[],
        p_outcomes      text[],
        p_error_classes text[],
        p_statuses      int[],
        p_bodies        text[],
        p_durations     int[],
        p_next_retries  timestamptz[],
        p_occurred_ats  timestamptz[],
        p_body_cap      int
    )
    returns int
    language sql
    security definer
    set search_path = public, pg_temp
as $$
    with src as (
        select b.delivery_id, b.attempt, b.outcome, b.error_class, b.status,
               b.body, b.duration_ms, b.next_retry_at, b.occurred_at
        from unnest(
                 p_delivery_ids,
                 p_attempts,
                 p_outcomes,
                 p_error_classes,
                 coalesce(p_statuses, array[]::int[]),
                 coalesce(p_bodies, array[]::text[]),
                 p_durations,
                 coalesce(p_next_retries, array[]::timestamptz[]),
                 coalesce(p_occurred_ats, array[]::timestamptz[])
             ) as b(delivery_id, attempt, outcome, error_class, status, body,
                    duration_ms, next_retry_at, occurred_at)
    ),
    ins as (
        insert into attempts
            (delivery_id, attempt, outcome, error_class, response_status,
             response_body, duration_ms, next_retry_at, occurred_at)
        select s.delivery_id, s.attempt, s.outcome, s.error_class, s.status,
               case when p_body_cap is null or p_body_cap < 0
                    then coalesce(s.body, '')
                    else left(coalesce(s.body, ''), p_body_cap) end,
               s.duration_ms, s.next_retry_at, coalesce(s.occurred_at, now())
        from src s
        on conflict (delivery_id, attempt) do nothing
        returning 1
    )
    select count(*)::int from ins;
$$;

comment on function pigeon_record_attempts(bigint[],int[],text[],text[],int[],text[],int[],timestamptz[],timestamptz[],int) is
    'FR-815: one transaction, one statement, >= 2000 evidence rows. Evidence class (db.EvidenceTx, synchronous_commit=off). Idempotent on (delivery_id, attempt); returns the rows inserted.';

-- 0008's per-row evidence insert is dropped rather than left beside the batched
-- one: two insert paths would mean two places SB-4's body cap and the attempt
-- timestamp are applied, and they would drift. Sync mode (D-2) calls the batched
-- function with a one-row batch, so both modes truncate and timestamp
-- identically. Losing the old signature is deliberate — nothing may keep calling
-- the path with the 2 KB cap baked into it.
drop function if exists pigeon_record_attempt(bigint, int, text, text, int, text, int, timestamptz);

-- ---------------------------------------------------------------------------
-- Claim, now returning the delivery row's own attempt counter (D-2).
--
-- The return type changes, so this is a drop and recreate rather than a
-- replace: PostgreSQL cannot alter a function's OUT parameter list in place.
-- Losing the old signature is the point — leaving it would make calls ambiguous
-- and preserve the stale-numbering path.
-- ---------------------------------------------------------------------------
drop function if exists pigeon_claim_delivery(interval);

create function pigeon_claim_delivery(p_lease interval default interval '40 seconds')
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
        -- How many attempts have already failed for this row: the evidence-row
        -- count. Current in sync mode, stale in buffered mode (which is why it
        -- is no longer the authority).
        prior_attempts  int,
        -- deliveries.attempts: the control-state attempt counter, advanced by
        -- pigeon_requeue_delivery. Always current, in both modes.
        row_attempts    int,
        -- The time of the earliest failed attempt, or null when no failed
        -- attempt row exists yet. The completing worker records this as
        -- deliveries.first_attempt_at so it is stable across re-claims.
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
           coalesce(da.n, 0), d.attempts, da.first_attempt_at
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

-- Next attempt number, now the same maximum the worker takes (D-2). Unchanged
-- return type, so this is a replace rather than a drop. It stays exported for
-- callers that hold only a delivery id (the inspect API in S7); the worker reads
-- both counters from the claim's single round trip.
create or replace function pigeon_next_attempt(p_delivery_id bigint)
    returns int
    language sql
    security definer
    set search_path = public, pg_temp
as $$
    select greatest(
               coalesce((select max(a.attempt) from attempts a
                         where a.delivery_id = p_delivery_id), 0),
               coalesce((select d.attempts from deliveries d
                         where d.id = p_delivery_id), 0)
           ) + 1;
$$;

-- ---------------------------------------------------------------------------
-- Re-queue for the next attempt, advancing the attempt counter (D-2).
--
-- The counter is advanced with greatest(), never assigned, so a re-delivered
-- requeue (a retried control write) cannot walk it backwards, and a terminal or
-- completing write of the true attempt count (0008's complete/finalize) still
-- sets it exactly.
-- ---------------------------------------------------------------------------
drop function if exists pigeon_requeue_delivery(bigint, interval);

create function pigeon_requeue_delivery(p_delivery_id bigint, p_backoff interval, p_attempt int)
    returns void
    language sql
    security definer
    set search_path = public, pg_temp
as $$
    update deliveries
    set lease_expires_at = now() + p_backoff,
        attempts = greatest(attempts, coalesce(p_attempt, 0))
    where id = p_delivery_id and status = 'pending';
$$;

comment on function pigeon_requeue_delivery(bigint,interval,int) is
    'FR-805/D-2: schedules the retry by pushing the lease out AND advances deliveries.attempts, so the attempt number never depends on the attempt evidence buffer.';

-- The runtime role needs every one of these (it is not and must not be
-- tenant-scoped for the delivery path, exactly like the outbox functions).
grant execute on function pigeon_record_attempts(bigint[],int[],text[],text[],int[],text[],int[],timestamptz[],timestamptz[],int) to current_user;
grant execute on function pigeon_claim_delivery(interval) to current_user;
grant execute on function pigeon_requeue_delivery(bigint,interval,int) to current_user;
