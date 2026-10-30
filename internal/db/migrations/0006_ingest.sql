-- 0006_ingest.sql — S2: event types, the accepted event, and the transactional
-- outbox.
--
-- This is the slice §17 refuses to defer. The outbox row is written in the SAME
-- transaction as the event (FR-604): a caller that received 202 can never have
-- an event without a publish intent, and the relay can never lose one. The
-- durability contract in §6 and AC-08/AC-09 live or die in this file.
--
-- Tables are tenant-owned (producer_id + env, RLS), exactly like the S1 tables.
-- The relay is a system worker, not a tenant: it cannot carry a producer scope,
-- so it leases and marks outbox rows through the SECURITY DEFINER functions at
-- the foot of this file, the same sanctioned-bypass pattern as 0002/0004.

-- ---------------------------------------------------------------------------
-- Event types (FR-301..FR-306)
-- ---------------------------------------------------------------------------
create table event_types (
    id              text primary key,
    producer_id     text not null references producers (id) on delete cascade,
    env             text not null check (env in ('test', 'live')),
    name            text not null,
    -- FR-302: name regex, immutable after creation. Immutability is structural:
    -- there is no UPDATE name path, and name is part of the composite key.
    constraint event_types_name_format check (name ~ '^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$'),
    -- routing_mode (FR-7.5a): who this event is about. Immutable at creation.
    routing_mode    text not null default 'broadcast' check (routing_mode in ('broadcast', 'consumer')),
    -- event_class (FR-7.7a): priority vs normal, declared on the type, not per event.
    event_class     text not null default 'normal'    check (event_class in ('priority', 'normal')),
    -- The payload schema and its version (FR-304). payload_version is bumped by
    -- PATCH; the delivered shape for endpoints on an older version is preserved
    -- upstream in fan-out (S3), which reads the version the endpoint is on.
    payload_schema  jsonb not null default '{}'::jsonb,
    payload_version int   not null default 1 check (payload_version between 1 and 1000),
    enabled         boolean not null default true,   -- FR-305: disable without deleting history
    examples        jsonb not null default '[]'::jsonb, -- FR-306: seeded examples
    created_at      timestamptz not null default now(),
    constraint event_types_name_key unique (producer_id, env, name)
);

comment on column event_types.routing_mode is
    'FR-7.5a: consumer events carry exactly one of consumer_id/consumer_ref; broadcast events carry neither.';
comment on column event_types.payload_schema is
    'FR-301: JSON Schema for the payload, validated on publish (FR-303).';
comment on column event_types.payload_version is
    'FR-304: bumping the version must not change what endpoints on the old version receive.';

alter table event_types enable row level security;
create policy event_types_tenant on event_types
    using (producer_id = current_setting('pigeon.producer_id', true)
           and env = current_setting('pigeon.env', true))
    with check (producer_id = current_setting('pigeon.producer_id', true)
           and env = current_setting('pigeon.env', true));

-- ---------------------------------------------------------------------------
-- Accepted events (the record of what a producer published)
-- ---------------------------------------------------------------------------
create table events (
    id              text primary key,            -- evt_ ULID, time-sortable (FR-901)
    producer_id     text not null,
    env             text not null check (env in ('test', 'live')),
    event_type      text not null,
    payload_version int not null,
    routing_mode    text not null check (routing_mode in ('broadcast', 'consumer')),
    event_class     text not null check (event_class in ('priority', 'normal')),
    consumer_id     text,
    consumer_ref    text,
    occurred_at     timestamptz not null,
    created_at      timestamptz not null default now(),
    payload         jsonb not null,
    -- FR-607 / §7.5a: broadcast <=> neither key; consumer <=> exactly one.
    constraint events_broadcast_xor_keys check (
        (routing_mode = 'broadcast') = (consumer_id is null and consumer_ref is null)),
    constraint events_consumer_exactly_one check (
        routing_mode <> 'consumer' or (consumer_id is not null) <> (consumer_ref is not null)),
    constraint events_not_both_keys check (consumer_id is null or consumer_ref is null),
    -- The event belongs to a real event type in this producer+env (FK backstop
    -- for the structural validation the handler does first).
    constraint events_type_fkey foreign key (producer_id, env, event_type)
        references event_types (producer_id, env, name) on delete cascade
);

comment on column events.payload is
    'The validated data object (§10). Stored with the event; the envelope is rebuilt by the relay.';

alter table events enable row level security;
create policy events_tenant on events
    using (producer_id = current_setting('pigeon.producer_id', true)
           and env = current_setting('pigeon.env', true))
    with check (producer_id = current_setting('pigeon.producer_id', true)
           and env = current_setting('pigeon.env', true));
create index events_producer_type_idx on events (producer_id, env, event_type, occurred_at desc);

-- ---------------------------------------------------------------------------
-- Transactional outbox (FR-604, FR-701, FR-702)
-- ---------------------------------------------------------------------------
-- One row per accepted event, in the publish state. The relay leases a pending
-- row with SKIP LOCKED, publishes it to the broker, and marks it published only
-- after the broker acknowledges (FR-701). A relay that dies between publish and
-- mark leaves the row pending with an expired lease, so it is re-leased and
-- re-published on the next pass — at-least-once at the broker, made exactly-once
-- at the delivery by the consumer's (event_id, endpoint_id) dedupe (S3).
create table outbox (
    id               bigserial primary key,
    event_id         text not null unique references events (id) on delete cascade,
    producer_id      text not null,
    env              text not null,
    status           text not null default 'pending' check (status in ('pending', 'published')),
    published_at     timestamptz,
    lease_expires_at timestamptz,                  -- set while a relay holds the row (FR-702)
    created_at       timestamptz not null default now()
);

comment on column outbox.lease_expires_at is
    'FR-702: while set and in the future the row is leased; a crashed relay lets it lapse and it becomes claimable again (D-6).';

alter table outbox enable row level security;
-- The owning tenant may read and insert its own outbox (inspect + ingest). The
-- relay does not use this policy; it goes through the definer functions below.
create policy outbox_tenant on outbox
    using (producer_id = current_setting('pigeon.producer_id', true)
           and env = current_setting('pigeon.env', true))
    with check (producer_id = current_setting('pigeon.producer_id', true)
           and env = current_setting('pigeon.env', true));
-- The lease query scans pending rows in order; this index is exactly that scan.
create index outbox_pending_idx on outbox (id) where status = 'pending';

-- ---------------------------------------------------------------------------
-- Idempotency keys (FR-603)
-- ---------------------------------------------------------------------------
-- A caller Idempotency-Key maps to the event it first produced, for 24 h. A
-- retried publish within the window returns the original event id; after it the
-- key is reusable and a new event is produced.
create table idempotency_keys (
    producer_id text not null,
    env         text not null,
    key_hash    bytea not null,
    event_id    text not null references events (id) on delete cascade,
    created_at  timestamptz not null default now(),
    expires_at  timestamptz not null,
    primary key (producer_id, env, key_hash)
);

alter table idempotency_keys enable row level security;
create policy idempotency_keys_tenant on idempotency_keys
    using (producer_id = current_setting('pigeon.producer_id', true)
           and env = current_setting('pigeon.env', true))
    with check (producer_id = current_setting('pigeon.producer_id', true)
           and env = current_setting('pigeon.env', true));

-- ---------------------------------------------------------------------------
-- Relay functions: the sanctioned bypass (like 0002/0004) for a system worker
-- that is not and must not be tenant-scoped.
-- ---------------------------------------------------------------------------

-- pigeon_lease_outbox: atomically claim the next pending row with SKIP LOCKED
-- (FR-702) so two relays never lease the same row, and return the whole event it
-- carries so the relay can build the §10 envelope without a second, unsynchronized
-- read. The lease is how a crash is detected: the row stays pending but
-- lease_expires_at moves into the future, so a later call skips it until the lease
-- lapses and it becomes claimable again (D-6, FR-809).
create or replace function pigeon_lease_outbox(p_lease interval default interval '30 seconds')
    returns table (
        outbox_id      bigint,
        event_id       text,
        producer_id    text,
        env            text,
        event_type     text,
        payload_version int,
        routing_mode   text,
        event_class    text,
        consumer_id    text,
        consumer_ref   text,
        occurred_at    timestamptz,
        payload        jsonb
    )
    language sql
    security definer
    set search_path = public, pg_temp
as $$
    with picked as (
        select o.id
        from outbox o
        where o.status = 'pending'
          and (o.lease_expires_at is null or o.lease_expires_at < now())
        order by o.id
        for update skip locked
        limit 1
    ),
    leased as (
        update outbox o set lease_expires_at = now() + p_lease
        from picked
        where picked.id = o.id
        returning o.id, o.event_id, o.producer_id, o.env
    )
    select l.id, l.event_id, l.producer_id, l.env,
           e.event_type, e.payload_version, e.routing_mode, e.event_class,
           e.consumer_id, e.consumer_ref, e.occurred_at, e.payload
    from leased l
    join events e on e.id = l.event_id;
$$;

-- pigeon_mark_outbox_published: mark a row published after the broker acks
-- (FR-701). Idempotent: it only transitions a still-pending row, so a relay that
-- double-marks (or a retried publish of an already-published event) is a no-op
-- rather than an error.
create or replace function pigeon_mark_outbox_published(p_id bigint)
    returns void
    language sql
    security definer
    set search_path = public, pg_temp
as $$
    update outbox set status = 'published', published_at = now()
    where id = p_id and status = 'pending';
$$;

-- pigeon_outbox_lag_seconds: OBS-2. The age in seconds of the oldest unpublished
-- row, or 0 when the outbox is empty. The relay alarms on this (> 30 s for 2 min);
-- AC-09 is that it grows unbounded while the relay is stopped yet nothing is lost.
create or replace function pigeon_outbox_lag_seconds()
    returns double precision
    language sql
    security definer
    set search_path = public, pg_temp
as $$
    select coalesce(max(extract(epoch from (now() - created_at))), 0)::double precision
    from outbox
    where status = 'pending';
$$;

-- The runtime role (current_user at migrate time in the single-role deployment,
-- pigeon_app via the harness's blanket grant) needs to call these.
grant execute on function pigeon_lease_outbox(interval) to current_user;
grant execute on function pigeon_mark_outbox_published(bigint) to current_user;
grant execute on function pigeon_outbox_lag_seconds() to current_user;
