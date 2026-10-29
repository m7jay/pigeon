-- 0019_s10_claim_rotation_columns.sql — FR-1004's claim columns, restored on top
-- of S11 and FR-405.
--
-- 0015 (this slice) re-created pigeon_claim_delivery so that the claim row carries
-- endpoints.previous_signing_secret and endpoints.previous_secret_expires_at: the
-- signer has to be able to render both signatures during a rotation's 24 h overlap
-- (FR-1004), and the value has to travel in the claim or the signer cannot see it.
--
-- 0016 (S11: attempt lanes and holds) and 0018 (FR-405/FR-1006: custom headers)
-- both landed on the base *after* this branch was written, and both re-created
-- this function from the base's shape. A `returns table` signature cannot change
-- in place, so each of them drops and re-creates it; the last one to run (0018) is
-- therefore the definition the database ends up with, and it has no rotation
-- columns. The symptom is not a missing column on `endpoints` - 0015 added those -
-- but a claim that cannot be scanned: internal/store's ClaimDelivery selects
-- previous_signing_secret out of the function's row and Postgres answers 42703
-- ("column \"previous_signing_secret\" does not exist"), so every FR-1004 test
-- fails and rotation is broken on the merged tree.
--
-- This file restates 0018's definition with the two OUT columns and their select
-- expressions put back, in the position internal/store's scan expects them:
-- immediately after signing_secret. Nothing else changes. The attempt-class
-- predicate (FR-1303), the held_back flag (FR-1304) and the sealed
-- endpoints.custom_headers column (FR-405/FR-1006) are carried over verbatim, and
-- the argument list is unchanged from 0016/0018.
-- ---------------------------------------------------------------------------
drop function if exists pigeon_claim_delivery(interval);
drop function if exists pigeon_claim_delivery(interval, interval);
drop function if exists pigeon_claim_delivery(interval, interval, text);

create or replace function pigeon_claim_delivery(
        -- The deployment's lease and §9 retry window. They are parameters rather
        -- than constants so each default has ONE source (the deployment config)
        -- instead of two that drift, and neither is defaulted here: this version
        -- takes the attempt class, and a defaulted parameter cannot be followed by
        -- a required one. The caller (internal/store's ClaimDelivery) always
        -- passes all three.
        p_lease                 interval,
        p_default_retry_window  interval,
        -- FR-1303: the attempt class this lane claims. No default on purpose: a
        -- caller that forgot it would silently get the pre-S11 behaviour.
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
        -- FR-1004: the secret the most recent rotation replaced, and the instant
        -- its overlap closes. NULL until the endpoint has rotated; the signer
        -- renders both signatures while the expiry is in the future.
        previous_signing_secret    text,
        previous_secret_expires_at timestamptz,
        event_type      text,
        payload_version int,
        consumer_ref    text,
        occurred_at     timestamptz,
        payload         jsonb,
        prior_attempts  int,
        -- D-2: deliveries.attempts, the control-state attempt counter advanced by
        -- pigeon_requeue_delivery. Always current, in both attempt-log modes.
        row_attempts    int,
        first_attempt_at timestamptz,
        -- S5: the retry window state (FR-1201, FR-1207, FR-1208).
        next_attempt_at timestamptz,
        retry_deadline  timestamptz,
        -- S5: the endpoint's retry policy and limits (§9, FR-1302, FR-1306). The
        -- three durations travel as epoch SECONDS rather than as intervals,
        -- because that is what the Go caller can read without an interval type.
        max_retries           int,
        retry_backoff_base_s  double precision,
        retry_backoff_max_s   double precision,
        retry_window_s        double precision,
        delivery_timeout_ms   int,
        max_concurrency       int,
        max_rps               int,
        -- FR-1304: is this delivery being held back right now?
        held_back             boolean,
        -- FR-405/FR-1006: the endpoint's custom headers, still sealed. The
        -- worker's store layer decrypts them for this request only.
        custom_headers        jsonb
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
           coalesce(ep.max_rps, 0),
           (d.held_back_since is not null),
           coalesce(ep.custom_headers, '[]'::jsonb)
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

grant execute on function pigeon_claim_delivery(interval, interval, text) to current_user;
