-- 0009_s7_inspect_replay.sql — S7: the inspect surface (FR-1101..FR-1111),
-- replay (FR-1106..FR-1108) and the dedupe answer (FR-903).
--
-- Three things land here, and each one is a decision recorded in SPEC.md's S7
-- section rather than an accident:
--
-- 1. Replay is a NEW delivery, never a mutation (FR-1106). A replayed delivery
--    carries `replay_of_delivery_id` pointing at the row it repairs, the same
--    event and endpoint, its own attempt trail and its own envelope delivery id
--    (FR-902). Webhook-id stays the event id, so a consumer's own dedupe still
--    works (FR-1001, §10).
--
--    The uniqueness of (event_id, endpoint_id) therefore has to become partial.
--    I-3 said that pair is unique "so fan-out and replay are idempotent", but a
--    replay must create a second delivery for that same pair: both cannot be a
--    table constraint. The amended I-3 (G2 gap 4, A-4) makes the index
--    `UNIQUE (event_id, endpoint_id) WHERE replay_of_delivery_id IS NULL` -
--    first fan-out only - and replay gets its own idempotency from a request
--    key (FR-1107, `replay_requests` below).
--
-- 2. Replay is bounded and idempotent (FR-1107): <= 1000 deliveries per request,
--    and a request key that returns the SAME set of new deliveries on a repeat.
--    `replay_requests` is the record, keyed by the hash of the caller's key, so
--    a retry that arrives after the first one committed returns the original
--    delivery ids instead of creating a second batch. 50/s per endpoint is not a
--    column here: the replayed rows are released by the lease (see the insert in
--    internal/store/inspect.go), which is what makes a 1,000-delivery replay to
--    one endpoint cost the endpoint 20 s of releases instead of a burst.
--
-- 3. The 30-day dedupe answer (FR-903) is served from Postgres, never Redis, and
--    is read through ONE name: `delivery_dedupe_all`. G2 gap 4's decision (A-4)
--    is that the answer comes from the `deliveries` row while its partition
--    exists (0 to 14 d) and from the range-partitioned `delivery_dedupe` archive
--    copied out of each partition as the sweeper drops it (14 to 30 d). S7 owns
--    the view and the support path over it; the archive table and the
--    copy-at-drop are S8's (its slice owns the partition drop). This migration
--    deliberately creates the view with A-4's four columns and no more, so S8's
--    `create or replace view ... as (<live>) union (<archive>)` is a one-line
--    change to one file. Read through `security_invoker = true`: a view is not a
--    way around RLS on the table under it.

-- ---------------------------------------------------------------------------
-- FR-1106: replay links to the delivery it repairs.
--
-- `on delete set null` rather than cascade: the replayed delivery is a real
-- delivery with its own history, so dropping the original (partition retention,
-- SB-2) must not take the replay with it. The link is provenance, not ownership.
-- ---------------------------------------------------------------------------
alter table deliveries add column replay_of_delivery_id bigint
    references deliveries (id) on delete set null;

comment on column deliveries.replay_of_delivery_id is
    'FR-1106/FR-1108: the delivery this row replays, or null for a first fan-out. A replay creates a new row and never mutates the one it repairs.';

-- ---------------------------------------------------------------------------
-- I-3, amended (A-4): the pair is unique for the FIRST fan-out only.
--
-- The table constraint becomes a partial unique index. FR-704's
-- `ON CONFLICT (event_id, endpoint_id) WHERE replay_of_delivery_id IS NULL`
-- infers it, so a re-fan-out is still a no-op while a replay can insert.
--
-- FR-814 budget for this hot table: the primary key is structural, and three
-- further indexes are declared here and in 0007 - fan-out idempotency (this
-- one), the worker's claim scan (deliveries_pending_idx) and the event-level
-- view (deliveries_event_idx, below). Every one of them maps to a query the API
-- or a worker actually issues; the delivery list's cursor rides the primary key
-- and is the reason no fourth index is added (SPEC.md, S7 decisions).
-- ---------------------------------------------------------------------------
alter table deliveries drop constraint deliveries_event_endpoint_key;
create unique index deliveries_first_fanout_key
    on deliveries (event_id, endpoint_id)
    where replay_of_delivery_id is null;

-- ---------------------------------------------------------------------------
-- FR-1109/FR-1111: the event-level view and its rollup.
--
-- "Did my event succeed?" is answered per event, so the read is
-- `producer_id = ? and env = ? and event_id = ?` over every delivery of that
-- event - first fan-out rows AND replays. The partial fan-out index above
-- cannot serve it (its predicate excludes replays), so this is the index that
-- maps to FR-1109's query.
-- ---------------------------------------------------------------------------
create index deliveries_event_idx on deliveries (producer_id, env, event_id);

-- ---------------------------------------------------------------------------
-- FR-1107: replay idempotency. One row per (tenant, request key).
--
-- The key is stored as a SHA-256 hash, like an api_key and like FR-603's
-- idempotency_keys, so the record is not a credential store. `delivery_ids` is
-- the whole answer to "what did that request create": a repeat with the same key
-- returns exactly those ids and creates nothing, which is AC-12.
--
-- `filter` records what was asked for, so a support question about a replay is
-- answerable without guessing, and `matched` is the bound check's number
-- (FR-1107: > 1000 is refused before anything is written, so no row exists for a
-- refused request - the whole store call is one transaction).
-- ---------------------------------------------------------------------------
create table replay_requests (
    producer_id      text not null,
    env              text not null check (env in ('test', 'live')),
    request_key_hash bytea not null,
    filter           jsonb not null default '{}'::jsonb,
    matched          int not null default 0 check (matched >= 0),
    created          int not null default 0 check (created >= 0),
    delivery_ids     bigint[] not null default '{}',
    created_at       timestamptz not null default now(),
    primary key (producer_id, env, request_key_hash)
);

comment on table replay_requests is
    'FR-1107: a replay request key and the deliveries it created. A repeat returns delivery_ids and creates nothing (AC-12).';
comment on column replay_requests.delivery_ids is
    'FR-1107: the NEW deliveries the request created, in creation order. The idempotent answer to a retried request.';

alter table replay_requests enable row level security;
create policy replay_requests_tenant on replay_requests
    using (producer_id = current_setting('pigeon.producer_id', true)
           and env = current_setting('pigeon.env', true))
    with check (producer_id = current_setting('pigeon.producer_id', true)
           and env = current_setting('pigeon.env', true));

-- ---------------------------------------------------------------------------
-- FR-1102/FR-1103: the attempt trail becomes readable by its tenant.
--
-- 0008 turned RLS on for `attempts` and added no policy, because the only path
-- to the table was the delivery worker's definer functions and an unreadable
-- table is the safest possible default. The inspect API is the read path that
-- did not exist then, so 0009 adds exactly one policy - select - whose predicate
-- is the owning delivery's own scope. The write path is unchanged: the worker
-- still reaches `attempts` only through `pigeon_record_attempt`, and a tenant
-- cannot write or delete an attempt row through the API at all.
--
-- The `exists` subquery reads `deliveries` under RLS as well, so the check is
-- the same one the delivery read already passed, and it rides the primary key.
-- ---------------------------------------------------------------------------
create policy attempts_tenant_read on attempts for select
    using (exists (
        select 1 from deliveries d
        where d.id = attempts.delivery_id
          and d.producer_id = current_setting('pigeon.producer_id', true)
          and d.env = current_setting('pigeon.env', true)));

-- ---------------------------------------------------------------------------
-- FR-903/FR-905: the dedupe answer, read through one name.
--
-- The support question is "was this event a duplicate for this endpoint?" - an
-- exact-key lookup on (endpoint_id, event_id) that must survive a Redis wipe and
-- outlive delivery retention. It is answered from the `deliveries` rows while
-- their partition exists; S8's sweeper copies each partition into the
-- `delivery_dedupe` archive before dropping it, and re-defines this view as a
-- UNION of the two (A-4).
--
-- security_invoker: without it the view runs as its owner (the migration role,
-- which owns `deliveries` and is therefore exempt from RLS) and would hand one
-- tenant another's keys. With it, the policies on `deliveries` apply to the
-- caller, so the view is not a way around tenant isolation (MT-1).
--
-- Four columns, exactly A-4's: `create or replace view` can only append columns,
-- so the archive side must be able to produce the same list.
-- ---------------------------------------------------------------------------
create view delivery_dedupe_all with (security_invoker = true) as
    select endpoint_id,
           event_id,
           min(id)          as first_delivery_id,
           min(created_at)  as first_seen_at
    from deliveries
    group by endpoint_id, event_id;

comment on view delivery_dedupe_all is
    'FR-903/FR-905: the 30-day dedupe answer. Live deliveries 0-14 d; the delivery_dedupe archive S8 fills at partition drop covers 14-30 d (A-4).';
