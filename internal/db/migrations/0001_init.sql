-- 0001_init.sql — M0 schema: tenancy, identity, API keys.
--
-- Only control-state tables live here; deliveries and attempts are added in a
-- later migration because their partitioning is a design decision (§8, SB-1),
-- not a detail.
--
-- Every tenant-owned table carries producer_id (I-1) and is protected by
-- row-level security keyed on the `pigeon.producer_id` session variable, so a
-- repository bug is not a data breach (MT-1). The application role used by the
-- core is NOT a superuser and owns no table except via the migration role.

create table producers (
    id            text        primary key,
    slug          text        not null unique,
    name          text        not null,
    suspended_at  timestamptz,
    created_at    timestamptz not null default now(),
    constraint producers_slug_format check (slug ~ '^[a-z0-9][a-z0-9-]{1,62}$')
);

comment on column producers.suspended_at is
    'FR-107: suspending stops ingestion and non-terminal deliveries, reversibly.';

-- users are global: one human may belong to several producers (FR-105).
create table users (
    id                 text        primary key,
    email              text        not null,
    password_hash      text        not null,
    is_instance_admin  boolean     not null default false,
    created_at         timestamptz not null default now()
);
create unique index users_email_lower_key on users (lower(email));

-- A consumer user is bound to exactly one consumer (FR-109/FR-110); everyone
-- else is a producer member. The two roles are disjoint by construction.
create table producer_members (
    producer_id  text        not null references producers (id) on delete cascade,
    user_id      text        not null references users (id) on delete cascade,
    role         text        not null,
    created_at   timestamptz not null default now(),
    primary key (producer_id, user_id),
    constraint producer_members_role check (role in ('owner', 'developer'))
);

create table consumers (
    id           text        primary key,
    producer_id  text        not null references producers (id) on delete cascade,
    env          text        not null,
    ref          text        not null,          -- the producer's own identifier (§7.5a)
    name         text        not null,
    created_at   timestamptz not null default now(),
    -- FR-506: consumer_ref is unique per producer, case-sensitive, immutable.
    constraint consumers_ref_key unique (producer_id, env, ref),
    -- Lets foreign keys enforce "this row belongs to that producer" (§7.5a).
    constraint consumers_producer_id_key unique (producer_id, id),
    constraint consumers_env check (env in ('test', 'live'))
);

-- A consumer user's membership is the scoping claim in FR-110. It is a separate
-- table from producer_members so revoking portal access never deletes the
-- consumer record (FR-109) or the producer membership.
create table consumer_users (
    consumer_id  text        not null references consumers (id) on delete cascade,
    producer_id  text        not null,
    user_id      text        not null references users (id) on delete cascade,
    created_at   timestamptz not null default now(),
    primary key (consumer_id, user_id),
    foreign key (producer_id, consumer_id) references consumers (producer_id, id) on delete cascade
);
create unique index consumer_users_one_consumer_key on consumer_users (user_id);

create table api_keys (
    id            text        primary key,
    producer_id   text        not null references producers (id) on delete cascade,
    env           text        not null,
    key_hash      bytea       not null unique,   -- FR-202: only the hash is stored
    display_hint  text        not null,          -- first 12 chars, safe to show
    replay_scope  boolean     not null default false, -- FR-208
    last_used_at  timestamptz,                   -- FR-206
    revoked_at    timestamptz,
    created_at    timestamptz not null default now(),
    constraint api_keys_env check (env in ('test', 'live'))
);
create index api_keys_producer_idx on api_keys (producer_id, env) where revoked_at is null;

-- FR-103: test and live are separate data planes. The environment is carried on
-- every tenant-owned row rather than in a second schema, so a query that forgets
-- it fails a constraint check rather than leaking data across environments.
alter table producers enable row level security;

alter table consumers        enable row level security;
alter table consumer_users   enable row level security;
alter table api_keys         enable row level security;
alter table producer_members enable row level security;

create policy consumers_tenant on consumers
    using (producer_id = current_setting('pigeon.producer_id', true));
create policy consumer_users_tenant on consumer_users
    using (producer_id = current_setting('pigeon.producer_id', true));
create policy api_keys_tenant on api_keys
    using (producer_id = current_setting('pigeon.producer_id', true));
create policy producer_members_tenant on producer_members
    using (producer_id = current_setting('pigeon.producer_id', true));
-- No policy value can isolate a producer list from itself; producers is readable
-- only by the instance admin path, which runs with producer_id unset and
-- therefore sees nothing under RLS. Admin reads must set
-- `pigeon.instance_admin=on` explicitly; the policy for that is added with the
-- admin API in M1.
create policy producers_none on producers using (false);
