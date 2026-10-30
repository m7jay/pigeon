-- 0007_routing.sql — S3: consumers' endpoints, subscriptions, and the fan-out
-- delivery rows.
--
-- Routing is where a producer's event becomes N deliveries (FR-7.5a, FR-703).
-- This file adds the three tables the fan-out intersects:
--
--   endpoints      a consumer's addressable URL, with its own signing secret
--                  (FR-401, FR-404), an ownership-challenge token (FR-403), and
--                  a status that carries active / paused / soft-deleted.
--   subscriptions  the (endpoint, event_type) pair a consumer opts into
--                  (FR-501), independently creatable and deletable.
--   deliveries     one row per (event, endpoint) match, created at fan-out
--                  before any attempt (FR-703), unique on (event_id, endpoint_id)
--                  so a re-fan-out is a no-op (FR-704), with the lease set here
--                  and never at claim (FR-813).
--
-- All three are tenant-owned (producer_id + env, RLS) exactly like the S2 tables.
-- Fan-out runs as the runtime role with the tenant scope set on the transaction
-- (store.scoped), so no SECURITY DEFINER bypass is needed: the platform is not a
-- system worker for routing, it acts inside the producer's own scope.

-- ---------------------------------------------------------------------------
-- Endpoints (FR-401..FR-404, FR-406, FR-407)
-- ---------------------------------------------------------------------------
create table endpoints (
    id                    text primary key,
    producer_id           text not null references producers (id) on delete cascade,
    env                   text not null check (env in ('test', 'live')),
    consumer_id           text not null,
    -- FR-401: a human name and the destination URL.
    name                  text not null,
    url                   text not null,
    -- internal (FR-402): an endpoint on a private network the operator explicitly
    -- opted to accept. Only then is a private/metadata address allowed.
    internal              boolean not null default false,
    -- status (FR-406 pause, FR-407 soft delete). "active" is the only deliverable
    -- state; "paused" collects no new deliveries and holds in-flight ones;
    -- "deleted" is a soft delete that retains history and never orphans deliveries
    -- (FR-407), so the value is terminal, not a prelude to a hard delete.
    status                text not null default 'active'
                          check (status in ('active', 'paused', 'deleted')),
    -- FR-403: an ownership challenge token, shown once at creation. verified_at is
    -- null until the challenge is answered; a live endpoint with a null
    -- verified_at MUST NOT receive events (enforced at fan-out).
    verification_token_hash bytea not null,
    verification_hint       text not null,
    verified_at             timestamptz,
    -- FR-404: the endpoint's own signing secret, stored so it can be applied at
    -- delivery time (Standard Webhooks, FR-1001). It is returned once at creation
    -- and never again; a real secret that must be usable at sign time cannot be
    -- hashed the way an api_key can. Access is limited to the delivery path.
    signing_secret          text not null,
    signing_secret_hint     text not null,
    -- FR-404 rotation with a 24 h overlap: after a rotate the new secret is current
    -- and the old one is kept, with an expiry, so an in-flight consumer still on
    -- the previous secret can verify one more day before it is dropped.
    previous_signing_secret     text,
    previous_secret_expires_at  timestamptz,
    paused_at               timestamptz,
    deleted_at              timestamptz,
    created_at              timestamptz not null default now(),
    constraint endpoints_producer_id_key unique (producer_id, id),
    -- The owning consumer is in this producer+env; the composite FK enforces it.
    constraint endpoints_consumer_fkey
        foreign key (producer_id, consumer_id) references consumers (producer_id, id)
        on delete cascade
);

comment on column endpoints.consumer_id is
    'FR-401: the consumer this endpoint belongs to. A producer never names a URL at a consumer (FR-7.5a); the consumer owns its own endpoints.';
comment on column endpoints.verification_token_hash is
    'FR-403: SHA-256 of the ownership-challenge secret, shown once at creation. verified_at stays null until it is answered.';
comment on column endpoints.signing_secret is
    'FR-404/FR-1001: the whsec_ secret used to sign the deliveries to this endpoint. Usable at sign time, so stored rather than hashed; returned once.';
comment on column endpoints.status is
    'FR-406/FR-407: active | paused | deleted. Paused collects no new deliveries; deleted is a soft delete that retains history.';

alter table endpoints enable row level security;
create policy endpoints_tenant on endpoints
    using (producer_id = current_setting('pigeon.producer_id', true)
           and env = current_setting('pigeon.env', true))
    with check (producer_id = current_setting('pigeon.producer_id', true)
           and env = current_setting('pigeon.env', true));

-- ---------------------------------------------------------------------------
-- Subscriptions (FR-501)
-- ---------------------------------------------------------------------------
create table subscriptions (
    id            text primary key,
    producer_id   text not null references producers (id) on delete cascade,
    env           text not null check (env in ('test', 'live')),
    endpoint_id   text not null,
    event_type    text not null,
    -- A subscription can be paused independently of its endpoint (FR-506 parity):
    -- a paused subscription collects no new deliveries. "deleted" is a soft delete.
    status        text not null default 'active'
                  check (status in ('active', 'paused', 'deleted')),
    created_at    timestamptz not null default now(),
    -- FR-501: at most one *active* subscription per (endpoint, event_type);
    -- re-adding an active pair is a conflict. The uniqueness is a partial index on
    -- status <> 'deleted', so a soft-deleted pair does not block recreating it -
    -- the history of what an endpoint opted out of is retained.
    constraint subscriptions_endpoint_fkey
        foreign key (producer_id, endpoint_id) references endpoints (producer_id, id)
        on delete cascade,
    -- The type must exist in this producer+env; a subscription cannot name a type
    -- the producer never published.
    constraint subscriptions_type_fkey
        foreign key (producer_id, env, event_type)
        references event_types (producer_id, env, name) on delete restrict
);

comment on column subscriptions.event_type is
    'FR-501: the event type this endpoint wants. Fan-out intersects these with the events type (FR-502).';

alter table subscriptions enable row level security;
create policy subscriptions_tenant on subscriptions
    using (producer_id = current_setting('pigeon.producer_id', true)
           and env = current_setting('pigeon.env', true))
    with check (producer_id = current_setting('pigeon.producer_id', true)
           and env = current_setting('pigeon.env', true));

-- FR-501: at most one active subscription per (endpoint, event_type). A partial
-- unique index, not a table constraint, so a soft-deleted pair does not block a
-- fresh subscription to the same type - the deleted row is history, not a key.
create unique index subscriptions_active_pair_key
    on subscriptions (producer_id, env, endpoint_id, event_type)
    where status <> 'deleted';

-- ---------------------------------------------------------------------------
-- Deliveries (FR-703, FR-704, FR-813)
-- ---------------------------------------------------------------------------
-- One row per (event, endpoint) match, created at fan-out BEFORE any delivery is
-- attempted (FR-703), one transaction per event. Unique on (event_id, endpoint_id)
-- so a re-fan-out after a crash is a no-op rather than a duplicate (FR-704). The
-- lease is set here, at creation, never at claim (FR-813): a worker only writes on
-- completion or terminal failure, and an expired lease makes the row claimable
-- again (D-6, FR-809).
--
-- status:
--   pending   created at fan-out, waiting for the worker (S4).
--   skipped   a matched endpoint that could not take the event at fan-out time
--             (unverified in live, or paused); recorded with non_delivery_reason,
--             never handed to the worker.
--   delivered / failed are written by the worker (S4).
create table deliveries (
    id                  bigserial primary key,
    event_id            text not null references events (id) on delete cascade,
    endpoint_id         text not null,
    producer_id         text not null,
    env                 text not null,
    consumer_id         text not null,
    status              text not null default 'pending'
                        check (status in ('pending', 'skipped', 'delivered', 'failed')),
    -- §7.5a's distinct non-delivery reasons (FR-508, FR-509). Set on a skipped row
    -- so each has its own record and its own metric label.
    non_delivery_reason text
                        check (non_delivery_reason is null or non_delivery_reason in (
                            'unroutable:consumer_not_registered',
                            'not_delivered:no_matching_subscription',
                            'not_delivered:endpoint_unverified',
                            'not_delivered:endpoint_paused')),
    attempts            int  not null default 0,
    first_attempt_at    timestamptz,
    delivered_at        timestamptz,
    lease_expires_at    timestamptz,
    created_at          timestamptz not null default now(),
    -- FR-704: the dedupe key the whole at-least-once pipeline rests on.
    constraint deliveries_event_endpoint_key unique (event_id, endpoint_id)
);

comment on column deliveries.lease_expires_at is
    'FR-813: set at fan-out, never at claim. A crashed worker lets it lapse and the row becomes claimable again (D-6).';

alter table deliveries enable row level security;
create policy deliveries_tenant on deliveries
    using (producer_id = current_setting('pigeon.producer_id', true)
           and env = current_setting('pigeon.env', true))
    with check (producer_id = current_setting('pigeon.producer_id', true)
           and env = current_setting('pigeon.env', true));

-- The worker leases the next deliverable row the way the relay leases the outbox:
-- scan pending rows in id order with SKIP LOCKED. This partial index is exactly
-- that scan (FR-814: an index maps to a query the worker actually issues).
create index deliveries_pending_idx on deliveries (id) where status = 'pending';

-- FR-814 budget for this hot table, two indexes and counting:
--   deliveries_event_endpoint_key  idempotency (FR-704) and the event-level view
--                                  (join deliveries by event_id).
--   deliveries_pending_idx         the worker's claim scan.
-- The consumer delivery log (UI-11) reads by consumer_id; that is S7/S9's query,
-- added when the query lands so the index is justified, not speculative.

-- ---------------------------------------------------------------------------
-- Fan-out records the event-level routing outcome (AC-23 vs AC-24) on the event
-- itself, so the UI-facing event view can tell an unroutable ref from an
-- un-subscribed consumer without joining the deliveries. Endpoint-level reasons
-- (unverified, paused) live on the skipped delivery rows instead.
-- ---------------------------------------------------------------------------
alter table events add column if not exists routing_state text
    check (routing_state is null or routing_state in ('routable', 'unroutable'));
alter table events add column if not exists non_delivery_reason text
    check (non_delivery_reason is null or non_delivery_reason in (
        'unroutable:consumer_not_registered',
        'not_delivered:no_matching_subscription',
        'not_delivered:endpoint_unverified',
        'not_delivered:endpoint_paused'));

comment on column events.routing_state is
    'AC-23/AC-24: "unroutable" when a consumer_ref matched no consumer, "routable" when it did (or for broadcast). Set at fan-out.';
comment on column events.non_delivery_reason is
    'AC-23/AC-24: the §7.5a event-level reason when the event produced zero deliveries; null once at least one delivery exists.';
