-- 0022_delivery_claim_internal_flag.sql — FR-402's escape hatch, carried to the
-- delivery worker in the claim it already makes (FR-1005).
--
-- The defect this closes, measured on head 5f6d124: an endpoint registered with
-- `internal = true` and a private/loopback URL could not receive anything. The
-- worker's request-time SSRF check reads the flag through
-- `internal/delivery/ssrf.go`'s `endpointAllowsPrivate`, which called
-- `store.EndpointIsInternal` — a bare
--
--     select internal from endpoints where id = $1
--
-- on the RLS-constrained runtime pool. The endpoint policies are tenant-scoped
-- (`endpoints_tenant`: producer_id + env), the query set no scope, so the policy
-- matched no row, the read was `pgx.ErrNoRows`, and the worker cached the flag as
-- "not internal" and treated the endpoint as public. `ssrfRefusal` then refused
-- the stored URL as a private address and the delivery ended terminally with
-- `dial_error` before a socket was opened. The escape hatch was inert: not a hole
-- (a private address the flag does not cover is still refused) but a feature that
-- could never work, and it silently broke the loopback-sink path the tree's own
-- load-test runbook uses.
--
-- The fix is shape (1) from that card: carry the flag in the row the worker
-- already reads machine-to-machine data from. `pigeon_claim_delivery` is
-- SECURITY DEFINER and already returns the endpoint's url, signing_secret,
-- previous_signing_secret and custom_headers from the same `endpoints` row, so
-- `internal` travels beside them: no second read, no new bypass, and the failure
-- mode that caused this ("the read failed, so treat it as public") stops being
-- reachable at all — a claim that cannot return the flag yields no delivery to
-- attempt. Nothing else about the function changes: the attempt-class predicate
-- (FR-1303), the hold flag (FR-1304), the rotation columns (FR-1004) and the
-- sealed custom headers (FR-405/FR-1006) are carried over verbatim from 0019.
--
-- `endpoint_internal` is the LAST OUT column, so it is appended after
-- custom_headers where internal/store's scan reads it; the position of every
-- existing column is unchanged.
--
-- ---------------------------------------------------------------------------
-- Why this file is 0022
--
-- `feat/m0-skeleton` holds up to 0021, and no other live ref holds anything at
-- or above 0019 (`git for-each-ref refs/remotes/origin` crossed with
-- `git ls-tree --name-only <ref> internal/db/migrations/`), so 0022 is the lowest
-- free number. It is picked from the refs rather than from the base's directory
-- listing: a sibling slice that picks the same number on its own branch only
-- shows up at merge time, as `migrate: duplicate migration version` in every
-- DB-backed package — which reads like a mass regression.
--
-- A `returns table` signature cannot be extended in place, so this file drops and
-- re-creates the function, exactly as 0015/0016/0018/0019 each had to. Any slice
-- written against an older base that re-creates `pigeon_claim_delivery` after
-- this one must carry `endpoint_internal` forward, or the claim stops returning it
-- and every delivery test fails with 42703 (the same trap 0019 documents).
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
        custom_headers        jsonb,
        -- FR-402/FR-1005: endpoints.internal, the operator's escape hatch for a
        -- private or loopback URL. It travels here because it is what the
        -- worker's request-time SSRF check has to consult, and the claim is the
        -- only read the worker makes before an attempt. The value is read under
        -- this function's definer rights from the same row the url beside it
        -- comes from, so no tenant scope is needed and no new bypass is added.
        endpoint_internal     boolean
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
           coalesce(ep.custom_headers, '[]'::jsonb),
           ep.internal
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
