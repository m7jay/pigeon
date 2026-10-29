-- 0020_s9a_producer_portal.sql — S9a / M2a: the two things the producer portal
-- needs from the core that did not exist anywhere in the tree.
--
-- 1. A durable marker for a *synthetic* test event (UI-4: "\"Send test event\",
--    clearly marked synthetic"). The marker is a column on `events` rather than a
--    label on the button that sent it, because "clearly marked" has to survive the
--    page it was sent from: the delivery log, the event view and the API's own
--    answer are all places a producer looks at a test event, and a badge rendered
--    only by the portal that sent it would be gone by the time anyone asks whether
--    the event in front of them was real. Everything else about the event is
--    unchanged: it is validated, fanned out, delivered and retried exactly like a
--    production event, so a test delivery exercises the real path rather than a
--    mock of it.
--
-- 2. FR-111's password reset. The invitation half of FR-111 already ships (0004,
--    `invites` + `pigeon_invite_accept`); the reset half did not exist, and its
--    absence is `SPEC.md`'s gap-list item 7: an account that lost its last
--    binding cannot sign in, and since 0005 an invitation addressed to an existing
--    account must be redeemed with that account's own session, so re-inviting such
--    a user is a dead end. The reset flow is the account's own way back to a
--    credential it controls. It deliberately grants **no claim** - it rewrites one
--    account's password and nothing else (no membership, no consumer binding, no
--    invitation is redeemed), so FR-111's second clause ("MUST NOT be able to
--    self-register into a producer") holds by construction rather than by a check.
--
-- Both tables follow the established shapes: RLS on, no policy for the
-- pre-authentication table (0004's `sessions` reasoning), and every read or write
-- from the unauthenticated side goes through a narrow SECURITY DEFINER function
-- so the cross-tenant surface stays greppable.

-- ---------------------------------------------------------------------------
-- 1. UI-4: the synthetic test event's durable marker.
-- ---------------------------------------------------------------------------
alter table events add column is_test boolean not null default false;

comment on column events.is_test is
    'UI-4: true for an event published as a synthetic test. Set by the publish '
    'path (a `test: true` field in the body, FR-601). The event is otherwise '
    'ordinary - validated, fanned out, delivered and retried like any other - so '
    'the marker is the only difference and it survives into the delivery log, '
    'the event view and the API answer.';

-- The producer portal lists "my test events" newest first, which is a scan of
-- this producer's own rows in a range that is pruned by created_at; a partial
-- index over the test rows only keeps that from touching the production rows.
create index events_test_idx on events (producer_id, env, created_at desc)
    where is_test;

-- ---------------------------------------------------------------------------
-- 2. FR-111: password reset.
-- ---------------------------------------------------------------------------
-- One row per issued reset. The token is stored as a SHA-256 hash exactly like a
-- session or an invitation token (the plaintext is 256 bits of randomness, so
-- stretching buys nothing - FR-202's reasoning, applied to the third secret type
-- in the tree). `used_at` is set when the token is spent *and* when it is
-- superseded, so there is at most one outstanding reset per account.
create table password_resets (
    id          text        primary key,
    user_id     text        not null references users (id) on delete cascade,
    token_hash  bytea       not null unique,
    expires_at  timestamptz not null,
    used_at     timestamptz,
    created_at  timestamptz not null default now()
);

create index password_resets_user_idx on password_resets (user_id, created_at desc);

alter table password_resets enable row level security;

comment on table password_resets is
    'FR-111: issued password resets, hashed. Accessed only via '
    'pigeon_password_reset_create/accept (SECURITY DEFINER): like `sessions`, the '
    'request that presents a reset token has no tenant yet, so RLS is enabled with '
    'no policy and the table is invisible to a scoped query rather than filtered.';

-- Issuing is one transaction, and it answers only "was there an account": the
-- caller must not be able to tell an unknown address from a known one by anything
-- else (FR-111's flow is a public form, so the response would otherwise be a user
-- enumeration oracle - the same reasoning 0004 applies to login).
create function pigeon_password_reset_create(
    p_id text, p_email text, p_token_hash bytea, p_ttl interval
)
returns boolean
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
    uid text;
begin
    select u.id into uid from users u where lower(u.email) = lower(p_email);
    if uid is null then
        return false;
    end if;
    -- At most one outstanding reset per account: issuing a new one retires the
    -- previous token, so an old link in a mailbox is not a second live
    -- credential, and the row count stays bounded by the accounts that asked.
    update password_resets set used_at = now()
     where user_id = uid and used_at is null;
    insert into password_resets (id, user_id, token_hash, expires_at)
    values (p_id, uid, p_token_hash, now() + p_ttl);
    return true;
end;
$$;

revoke all on function pigeon_password_reset_create(text, text, bytea, interval) from public;
grant execute on function pigeon_password_reset_create(text, text, bytea, interval) to current_user;

-- Accepting is one transaction: burn the token, set the new password, and revoke
-- every session the account had. The revocation is not optional: a reset is how
-- an account is recovered *because* someone else may have the old password, and a
-- session minted with it would survive the change (FR-112's "revocation takes
-- effect within 5 s" is the same guarantee one step earlier).
--
-- The function writes `users.password_hash` and nothing else. It creates no
-- membership, attaches no consumer and redeems no invitation, so it cannot be a
-- way into a producer (FR-111) - and an account with no claim still cannot log in
-- after a reset, which `SPEC.md`'s gap list records rather than hides.
create function pigeon_password_reset_accept(p_token_hash bytea, p_password_hash text)
returns table (o_reset_id text, o_user_id text, o_email text)
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
    r password_resets%rowtype;
begin
    select * into r
      from password_resets
     where token_hash = p_token_hash
       and used_at is null
       and expires_at > now()
       for update;
    if not found then
        return;
    end if;

    update users
       set password_hash = p_password_hash,
           must_change_password = false,
           password_changed_at = now()
     where id = r.user_id;

    update password_resets set used_at = now() where id = r.id;

    update sessions set revoked_at = now()
     where user_id = r.user_id and revoked_at is null;

    return query
        select r.id, r.user_id, u.email from users u where u.id = r.user_id;
end;
$$;

revoke all on function pigeon_password_reset_accept(bytea, text) from public;
grant execute on function pigeon_password_reset_accept(bytea, text) to current_user;
