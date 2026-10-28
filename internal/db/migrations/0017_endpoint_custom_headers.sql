-- 0017_endpoint_custom_headers.sql — FR-405 + FR-1006: custom endpoint headers,
-- with their values encrypted at rest.
--
-- FR-405 lets an endpoint carry up to 5 custom headers, and refuses four names
-- outright (`Host`, `Content-Length`, `Content-Type`, `Authorization`) rather
-- than dropping them silently. FR-1006 is its hardening leg: the values are
-- personal data about the receiver (an API key, a routing token), so they are
-- stored encrypted and are never read back in the clear - not by the API, not by
-- the portal, not by a log line.
--
-- The column is a jsonb array of `{"name": "...", "value_ct": "<base64 ciphertext>"}`
-- entries:
--
--   * the NAME stays in the clear on purpose. It is not a secret, the operator
--     and the portal need it to know what is configured, and it is what a
--     redacted read returns.
--   * the VALUE is ciphertext only. The plaintext of a value exists in three
--     places and nowhere else: the API request that registered it, the seal
--     call in internal/store, and the outbound HTTP request in the delivery
--     worker. `internal/atrest` is the primitive; the AAD is
--     `pigeon:endpoint-custom-headers:<endpoint id>`, so a ciphertext moved to
--     another endpoint's row (or another column) does not authenticate and is
--     dropped rather than sent.
--
-- The bound and the shape are enforced here as well as in the store, in the same
-- spirit as the rest of this schema: a hand-written UPDATE cannot leave the table
-- holding six headers or a JSON object, whatever the code in front of it does.
--
-- ---------------------------------------------------------------------------
-- Why this file is 0017 and not 0015
--
-- This slice first landed as `0015_endpoint_custom_headers.sql`, and S11 took
-- 0016 around it. 0016 re-creates `pigeon_claim_delivery` to select by attempt
-- class (FR-1303) and to report `held_back` (FR-1304), and it runs *after* 0015 -
-- so its version of the function, which knows nothing about headers, replaced the
-- one 0015 had written and the claim would have stopped returning the column. The
-- file therefore moves to the number that runs last on this base, and re-creates
-- the claim as 0016's function *plus* the `custom_headers` OUT column: S11's lane
-- predicate and hold flag are kept intact rather than reverted. The column, its
-- constraints and its comment are unaffected by 0016 and are unchanged.
-- ---------------------------------------------------------------------------

alter table endpoints add column custom_headers jsonb not null default '[]'::jsonb;

alter table endpoints add constraint endpoints_custom_headers_shape
    check (jsonb_typeof(custom_headers) = 'array');

-- FR-405's bound, in SQL as well as in the store. The CASE (rather than an
-- `and`) is deliberate: jsonb_array_length raises on a non-array, and SQL does
-- not promise short-circuit evaluation of a conjunction.
alter table endpoints add constraint endpoints_custom_headers_count
    check (case when jsonb_typeof(custom_headers) = 'array'
                then jsonb_array_length(custom_headers) <= 5
                else true end);

comment on column endpoints.custom_headers is
    'FR-405/FR-1006: up to 5 custom headers, as [{"name": "...", "value_ct": "<base64 AES-256-GCM ciphertext>"}]. Names are in the clear (not secret, needed for redacted reads); values are ciphertext only and are decrypted on the delivery path alone.';

-- ---------------------------------------------------------------------------
-- The delivery claim returns the sealed headers with the rest of the endpoint
-- row: they belong to the request the worker is about to send, and a second
-- round trip (or a second definer function) would only add a way for the two
-- reads to disagree. The plaintext never reaches this function - it returns the
-- ciphertext, and internal/store decrypts it with PIGEON_HEADERS_KEY.
--
-- The body below is 0016's, unchanged except for the one appended OUT column and
-- its select expression: the attempt-class predicate (FR-1303) and the `held_back`
-- flag (FR-1304) are S11's and are not restated here. A `returns table`
-- signature cannot change in place, so the function is dropped and recreated
-- with one more OUT column; the argument list is unchanged from 0016, and the
-- grant is re-issued below for the same reason.
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
