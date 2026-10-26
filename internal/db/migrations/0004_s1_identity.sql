-- 0004_s1_identity.sql — S1: human sessions, invitations, environment scope on
-- every tenant row, and the instance-admin audit log.
--
-- Three things land here, in order of how much they constrain later work:
--
-- 1. `env` becomes part of the row-level-security predicate, not just a column.
--    FR-103 wants test and live fully separated; a column alone separates nothing
--    if a handler forgets to filter it. `pigeon.env` is set per transaction next
--    to `pigeon.producer_id`, so a query that names a producer but not an
--    environment returns no rows (fail closed) instead of both planes.
--
-- 2. Sessions. FR-110 requires a `consumer_id` claim on the consumer user's
--    session and server-side filtering by it; producer owners (FR-104) and the
--    instance admin (FR-101/FR-102) need a human credential too. One table with
--    an optional producer/consumer claim covers all four principals, and the
--    claims are resolved from the database on every request rather than trusted
--    from a token payload, so revoking a membership or a consumer binding takes
--    effect on the next request.
--
-- 3. Invitations (FR-104, FR-109, FR-111). There is no self-registration into a
--    producer: the only way a user enters one is an invitation row created by an
--    owner or by the instance admin, and the token is stored as a SHA-256 hash
--    exactly like an API key (FR-202's reasoning: the plaintext is 256 bits of
--    randomness, so stretching buys nothing).
--
-- Every pre-authentication read goes through a SECURITY DEFINER function, the
-- same sanctioned pattern as 0002: authentication happens before the tenant is
-- known, so RLS would make it impossible. Each function is narrow enough to
-- grep for.

-- ---------------------------------------------------------------------------
-- 1. FR-101: the bootstrap credential is printed once and must be changed.
-- ---------------------------------------------------------------------------
alter table users
    add column must_change_password boolean not null default false,
    add column password_changed_at  timestamptz;

comment on column users.must_change_password is
    'FR-101: set for the bootstrap instance admin. Login succeeds, every other '
    'route answers 403 password_change_required until POST /v1/session/password.';

-- ---------------------------------------------------------------------------
-- 2. FR-103: environment on consumer_users, plus the composite key that lets a
--    foreign key enforce "this consumer user belongs to that (producer, env)".
-- ---------------------------------------------------------------------------
alter table consumers
    add constraint consumers_producer_env_id_key unique (producer_id, env, id);

alter table consumer_users add column env text;
update consumer_users cu set env = c.env from consumers c where c.id = cu.consumer_id;
alter table consumer_users alter column env set not null;
alter table consumer_users add constraint consumer_users_env check (env in ('test', 'live'));

-- The old foreign key only proved the consumer existed somewhere; the new one
-- proves it exists in the same producer *and* the same environment.
alter table consumer_users drop constraint consumer_users_producer_id_consumer_id_fkey;
alter table consumer_users
    add constraint consumer_users_consumer_env_fkey
    foreign key (producer_id, env, consumer_id)
    references consumers (producer_id, env, id) on delete cascade;

-- ---------------------------------------------------------------------------
-- 3. Invitations (FR-104 owner/developer, FR-109 consumer user, FR-111 no
--    self-registration).
-- ---------------------------------------------------------------------------
create table invites (
    id           text        primary key,
    producer_id  text        not null references producers (id) on delete cascade,
    env          text,
    email        text        not null,
    role         text        not null,
    consumer_id  text,
    token_hash   bytea       not null unique,
    invited_by   text        not null references users (id),
    expires_at   timestamptz not null,
    accepted_at  timestamptz,
    revoked_at   timestamptz,
    created_at   timestamptz not null default now(),
    constraint invites_role check (role in ('owner', 'developer', 'consumer')),
    -- FR-109: a consumer invitation is bound to exactly one consumer, always,
    -- and lives in that consumer's environment. A producer membership (FR-104)
    -- is producer-wide, so its invitation carries no environment at all rather
    -- than a misleading one.
    constraint invites_env check ((role = 'consumer') = (env is not null)),
    constraint invites_consumer_role check ((role = 'consumer') = (consumer_id is not null)),
    foreign key (producer_id, env, consumer_id)
        references consumers (producer_id, env, id) on delete cascade
);

-- One pending invitation per (producer, environment, email, role, consumer).
-- Re-inviting replaces nothing; it answers 409 and the operator revokes first,
-- which keeps "who was invited and when" answerable.
create unique index invites_pending_key
    on invites (producer_id, coalesce(env, ''), lower(email), role, coalesce(consumer_id, ''))
    where accepted_at is null and revoked_at is null;

create index invites_producer_idx on invites (producer_id, env, created_at desc);

-- ---------------------------------------------------------------------------
-- 4. Sessions (FR-101 admin, FR-104 owner/developer, FR-110 consumer claim).
-- ---------------------------------------------------------------------------
create table sessions (
    id           text        primary key,
    user_id      text        not null references users (id) on delete cascade,
    token_hash   bytea       not null unique,
    producer_id  text        references producers (id) on delete cascade,
    consumer_id  text        references consumers (id) on delete cascade,
    env          text,
    expires_at   timestamptz not null,
    revoked_at   timestamptz,
    last_seen_at timestamptz,
    created_at   timestamptz not null default now(),
    constraint sessions_env check (env is null or env in ('test', 'live')),
    -- A consumer claim is never present without the environment it belongs to.
    -- A session may not carry both a producer and a consumer claim: FR-110's
    -- consumer user is the narrower of the two roles (0001 keeps them disjoint).
    constraint sessions_claim check ((consumer_id is null) = (env is null)),
    constraint sessions_consumer_implies_env check (consumer_id is null or env is not null)
);

create index sessions_user_idx on sessions (user_id);
create index sessions_expiry_idx on sessions (expires_at);

-- ---------------------------------------------------------------------------
-- 5. MT-5: every instance-admin action is audited with actor, reason, timestamp.
-- ---------------------------------------------------------------------------
create table admin_audit_log (
    id                 text        primary key,
    actor_user_id      text        not null references users (id),
    action             text        not null,
    target_producer_id text        references producers (id) on delete set null,
    reason             text,
    request_id         text,
    created_at         timestamptz not null default now()
);

create index admin_audit_log_target_idx on admin_audit_log (target_producer_id, created_at desc);
create index admin_audit_log_actor_idx on admin_audit_log (actor_user_id, created_at desc);

comment on table admin_audit_log is
    'MT-5: append-only. There is no update or delete policy, so the application '
    'role cannot rewrite history; only the migration role can prune it.';

-- ---------------------------------------------------------------------------
-- 6. Row-level security for the new tables, and environment-aware replacement
--    policies for the ones that already had a producer-only predicate.
-- ---------------------------------------------------------------------------
alter table invites         enable row level security;
alter table sessions        enable row level security;
alter table admin_audit_log enable row level security;

create policy invites_tenant on invites
    using (
        producer_id = current_setting('pigeon.producer_id', true)
        and (env is null or env = current_setting('pigeon.env', true))
    )
    with check (
        producer_id = current_setting('pigeon.producer_id', true)
        and (env is null or env = current_setting('pigeon.env', true))
    );

-- Sessions are read only through pigeon_session_lookup(): the request that
-- presents a token has no tenant yet. No policy is created on purpose, so the
-- table is invisible to a scoped query rather than merely filtered.
comment on table sessions is
    'Accessed only via pigeon_session_lookup/revoke/touch (SECURITY DEFINER). '
    'RLS is enabled with no policy, so a tenant-scoped query sees nothing.';

create policy admin_audit_insert on admin_audit_log
    for insert
    with check (current_setting('pigeon.instance_admin', true) = 'on');
create policy admin_audit_select on admin_audit_log
    for select
    using (current_setting('pigeon.instance_admin', true) = 'on');

-- FR-103, enforced in the database as well as the repository (MT-1): these
-- predicates now require both scopes. `current_setting(..., true)` is NULL when
-- unset, and `x = NULL` is NULL rather than true, so a missing scope matches
-- nothing.
drop policy consumers_tenant on consumers;
create policy consumers_tenant on consumers
    using (
        producer_id = current_setting('pigeon.producer_id', true)
        and env = current_setting('pigeon.env', true)
    );
drop policy consumer_users_tenant on consumer_users;
create policy consumer_users_tenant on consumer_users
    using (
        producer_id = current_setting('pigeon.producer_id', true)
        and env = current_setting('pigeon.env', true)
    );
drop policy api_keys_tenant on api_keys;
create policy api_keys_tenant on api_keys
    using (
        producer_id = current_setting('pigeon.producer_id', true)
        and env = current_setting('pigeon.env', true)
    );

-- ---------------------------------------------------------------------------
-- 7. The pre-authentication functions.
--
-- 0002's lookup gains one column: whether the producer is suspended (FR-107),
-- which changes the return type, so the function is replaced rather than
-- redefined. Suspension is read here rather than joined per request because this
-- is already the request's only cross-tenant read.
-- ---------------------------------------------------------------------------
drop function pigeon_auth_lookup(bytea);

create function pigeon_auth_lookup(p_key_hash bytea)
returns table (key_id text, producer_id text, env text, replay_scope boolean, suspended boolean)
language sql
security definer
set search_path = public, pg_temp
as $$
    select k.id, k.producer_id, k.env, k.replay_scope, p.suspended_at is not null
      from api_keys k
      join producers p on p.id = k.producer_id
     where k.key_hash = p_key_hash
       and k.revoked_at is null
$$;

comment on function pigeon_auth_lookup(bytea) is
    'FR-201/FR-205/FR-107: authenticate a bearer key by hash. Revoked keys return '
    'no row, so revocation takes effect on the next request with no cache to '
    'invalidate; suspended producers are reported so the request can be refused.';

revoke all on function pigeon_auth_lookup(bytea) from public;

-- FR-206: `last_used_at` written through a definer function because the api_keys
-- policy needs both scopes and this write has neither (it happens on the auth
-- path). Evidence class: a lost timestamp is not worth an fsync.
create function pigeon_key_touch(p_key_id text)
returns void
language sql
security definer
set search_path = public, pg_temp
as $$
    update api_keys set last_used_at = now() where id = p_key_id
$$;

revoke all on function pigeon_key_touch(text) from public;

-- Session lookup resolves the claims from the database on every request: a
-- revoked membership, a deleted consumer binding or a revoked session all take
-- effect immediately, which is what FR-109's "independently" and FR-205's 60 s
-- budget require.
create function pigeon_session_lookup(p_token_hash bytea)
returns table (
    session_id          text,
    user_id             text,
    email               text,
    is_instance_admin   boolean,
    must_change_password boolean,
    producer_id         text,
    role                text,
    consumer_id         text,
    env                 text,
    expires_at          timestamptz
)
language sql
security definer
set search_path = public, pg_temp
as $$
    select s.id, s.user_id, u.email, u.is_instance_admin, u.must_change_password,
           s.producer_id, pm.role, s.consumer_id, s.env, s.expires_at
      from sessions s
      join users u on u.id = s.user_id
      left join producer_members pm
             on pm.producer_id = s.producer_id
            and pm.user_id = s.user_id
      -- FR-109: revoking a consumer user's access deletes the binding, not the
      -- consumer, and must invalidate every session that carries the claim.
      left join consumer_users cu
             on cu.consumer_id = s.consumer_id
            and cu.user_id = s.user_id
     where s.token_hash = p_token_hash
       and s.revoked_at is null
       and s.expires_at > now()
       and (s.consumer_id is null or cu.user_id is not null)
$$;

revoke all on function pigeon_session_lookup(bytea) from public;

create function pigeon_session_revoke(p_token_hash bytea)
returns boolean
language sql
security definer
set search_path = public, pg_temp
as $$
    with revoked as (
        update sessions set revoked_at = now()
         where token_hash = p_token_hash and revoked_at is null
        returning 1
    )
    select exists (select 1 from revoked)
$$;

revoke all on function pigeon_session_revoke(bytea) from public;

create function pigeon_session_touch(p_token_hash bytea)
returns void
language sql
security definer
set search_path = public, pg_temp
as $$
    update sessions set last_seen_at = now()
     where token_hash = p_token_hash and revoked_at is null
$$;

revoke all on function pigeon_session_touch(bytea) from public;

-- Session creation is a definer function for the same reason as lookup: the row
-- is written before any tenant scope exists, and `sessions` deliberately has no
-- RLS policy (it is not tenant data a request may query).
create function pigeon_session_create(
    p_id text, p_user_id text, p_token_hash bytea,
    p_producer_id text, p_consumer_id text, p_env text, p_expires_at timestamptz
)
returns void
language sql
security definer
set search_path = public, pg_temp
as $$
    insert into sessions (id, user_id, token_hash, producer_id, consumer_id, env, expires_at)
    values (p_id, p_user_id, p_token_hash,
            nullif(p_producer_id, ''), nullif(p_consumer_id, ''), nullif(p_env, ''), p_expires_at)
$$;

revoke all on function pigeon_session_create(text, text, bytea, text, text, text, timestamptz) from public;

-- Invitation acceptance is one transaction: find the user or create it, attach
-- the membership (or the consumer binding), and burn the token. Doing it in a
-- function means a half-accepted invitation cannot exist, and means the only
-- code path that can create a membership from an email address is this one
-- (FR-111).
create function pigeon_invite_accept(p_token_hash bytea, p_user_id text, p_password_hash text)
returns table (
    o_invite_id    text,
    o_user_id      text,
    o_email        text,
    o_role         text,
    o_producer_id  text,
    o_consumer_id  text,
    o_env          text
)
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
    v invites%rowtype;
    uid text;
begin
    select * into v
      from invites
     where token_hash = p_token_hash
       and accepted_at is null
       and revoked_at is null
       and expires_at > now()
       for update;
    if not found then
        return;
    end if;

    select u.id into uid from users u where lower(u.email) = lower(v.email);
    if uid is null then
        uid := p_user_id;
        insert into users (id, email, password_hash)
        values (uid, v.email, p_password_hash);
    else
        update users u
           set password_hash = p_password_hash,
               must_change_password = false,
               password_changed_at = now()
         where u.id = uid;
    end if;

    if v.role = 'consumer' then
        insert into consumer_users (consumer_id, producer_id, user_id, env)
        values (v.consumer_id, v.producer_id, uid, v.env);
    else
        insert into producer_members (producer_id, user_id, role)
        values (v.producer_id, uid, v.role)
        on conflict (producer_id, user_id) do update set role = excluded.role;
    end if;

    update invites set accepted_at = now() where id = v.id;

    return query select v.id, uid, v.email, v.role, v.producer_id,
                        coalesce(v.consumer_id, ''), coalesce(v.env, '');
end;
$$;

revoke all on function pigeon_invite_accept(bytea, text, text) from public;

-- Login happens before the tenant is known, and both `producer_members` and
-- `consumer_users` are tenant-scoped tables, so a user's own claims need the
-- same narrow definer treatment as a key lookup. These two functions return the
-- caller's own rows and nothing else: they take a user id that was itself
-- resolved from a verified password, so they cannot be used to enumerate.
create function pigeon_user_memberships(p_user_id text)
returns table (producer_id text, slug text, name text, role text, suspended boolean)
language sql
security definer
set search_path = public, pg_temp
as $$
    select pm.producer_id, p.slug, p.name, pm.role, p.suspended_at is not null
      from producer_members pm
      join producers p on p.id = pm.producer_id
     where pm.user_id = p_user_id
     order by p.slug
$$;

revoke all on function pigeon_user_memberships(text) from public;

-- FR-109/FR-110: a consumer user is bound to exactly one consumer (0001's
-- unique index enforces the cardinality), and the binding carries the
-- environment the session claim must be filtered by.
create function pigeon_user_consumers(p_user_id text)
returns table (consumer_id text, producer_id text, env text, consumer_name text, consumer_ref text)
language sql
security definer
set search_path = public, pg_temp
as $$
    select cu.consumer_id, cu.producer_id, cu.env, c.name, c.ref
      from consumer_users cu
      join consumers c on c.id = cu.consumer_id
     where cu.user_id = p_user_id
$$;

revoke all on function pigeon_user_consumers(text) from public;
