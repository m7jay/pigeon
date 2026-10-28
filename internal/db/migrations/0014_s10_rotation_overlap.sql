-- ---------------------------------------------------------------------------
-- S10 (FR-1004): the delivery claim carries the endpoint's PREVIOUS signing
-- secret and its expiry, so an attempt made during a rotation's 24 h overlap can
-- be signed with both secrets at once.
--
-- Why this has to travel in the claim rather than be looked up in the worker:
-- the signature is built per attempt, on the hot path, from the row the claim
-- already returned. A second read per attempt would double the round trips to
-- Postgres for the value that decides whether a consumer can verify at all, and a
-- cached copy would sign with a secret that has already been superseded - the
-- one window during which getting it wrong is invisible to the receiver, because
-- the receiver is precisely the party that is mid-switch.
--
-- The expiry is returned unfiltered and the signer applies it, so the "up to
-- 24 h" half of FR-1004 is a fact about a pure function that a unit test can
-- assert, rather than a fact about a WHERE clause that only a database test can
-- reach. The worker still signs with only the current secret once the expiry has
-- passed, which is what "up to" means.
--
-- 0013 created pigeon_claim_delivery(interval, interval). A function's OUT row is
-- part of its identity, so this is a drop-and-create rather than a
-- create-or-replace: adding two columns to the returned table is not something
-- CREATE OR REPLACE can do. 0008's single-argument overload is left alone; it is
-- stale, unreferenced, and dropping it is not this slice's business.
-- ---------------------------------------------------------------------------
drop function if exists pigeon_claim_delivery(interval, interval);

create function pigeon_claim_delivery(
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
        -- FR-1004: the secret this endpoint used before its most recent rotation,
        -- and when that overlap ends. Null when the endpoint has never rotated,
        -- and null-ed by the rotation itself once the window has closed, so the
        -- returned value is either live or absent rather than a stale secret the
        -- caller has to know to ignore.
        previous_signing_secret    text,
        previous_secret_expires_at timestamptz,
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
           ep.previous_signing_secret, ep.previous_secret_expires_at,
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

comment on column endpoints.previous_signing_secret is
    'FR-1004: the secret in force before the most recent rotation. Cleared when previous_secret_expires_at passes (pigeon_clear_expired_previous_secrets), so a present value is always one a receiver may still be using.';
comment on column endpoints.previous_secret_expires_at is
    'FR-1004: end of the 24 h dual-signature overlap after a rotation.';

-- ---------------------------------------------------------------------------
-- FR-1004's other half: the overlap ENDS. A previous secret left in place forever
-- is a second live credential for the endpoint, which is the opposite of what
-- rotating is for. The sweeper calls this on its tick; it is one statement, and
-- clearing the expiry with the secret keeps the pair consistent (a null secret
-- with a stale expiry would read as "there was a rotation, and it is still open").
-- ---------------------------------------------------------------------------
create or replace function pigeon_clear_expired_previous_secrets()
    returns int
    language sql
    security definer
    set search_path = public, pg_temp
as $$
    with cleared as (
        update endpoints
        set previous_signing_secret = null,
            previous_secret_expires_at = null
        where previous_signing_secret is not null
          and previous_secret_expires_at is not null
          and previous_secret_expires_at <= now()
        returning 1
    )
    select count(*)::int from cleared;
$$;

comment on function pigeon_clear_expired_previous_secrets() is
    'FR-1004: drops the previous signing secret once its 24 h overlap has passed. Read-only with respect to the current secret; safe to run on every sweeper tick.';
