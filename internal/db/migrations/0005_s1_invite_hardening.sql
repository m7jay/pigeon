-- 0005_s1_invite_hardening.sql — S1 review fixes: invitation acceptance can no
-- longer mutate an existing account, and the lookup happens before bcrypt.
--
-- Why this is a new file rather than an edit to 0004: applied migrations are
-- checksummed, so rewriting 0004 would refuse to boot on every database that has
-- already run it (SPEC.md, "Migrations"). Forward-only.
--
-- The defect this closes (reproduced by review): `pigeon_invite_accept` looked the
-- invited address up and, when a user already owned it, overwrote `password_hash`
-- and cleared `must_change_password`. Because there is no mailer yet and the
-- invitation token is returned to the *inviter*, the owner of any producer could
-- invite admin@pigeon.local, take the token out of the API response, accept it with
-- a password of their choosing, and log in as the instance admin.
--
-- Two rules replace it:
--
--   1. An invitation never touches an existing account's credentials. The only
--      thing acceptance may add for an existing account is the membership (or the
--      consumer binding) it was created for.
--   2. A token addressed to an address that already has an account is not a
--      credential on its own: `p_caller_user_id` must be that account, so the
--      invitee proves control by authenticating. A new address has no account to
--      take over, so the token plus the chosen password still creates one (FR-104,
--      FR-111's "invitation is how a user enters a producer").
--
-- `pigeon_invite_lookup` exists for the second half of the review fix: the store
-- resolves the token *before* hashing a password, so an unknown token costs one
-- indexed read instead of ~250 ms of bcrypt. POST /v1/invites/accept is the only
-- anonymous write in the API, which is what made that a CPU-exhaustion vector
-- rather than a curiosity.

-- ---------------------------------------------------------------------------
-- 1. Read the invitation before doing any work for it.
-- ---------------------------------------------------------------------------
create function pigeon_invite_lookup(p_token_hash bytea)
returns table (
    invite_id        text,
    email            text,
    role             text,
    producer_id      text,
    consumer_id      text,
    env              text,
    existing_user_id text
)
language sql
security definer
set search_path = public, pg_temp
as $$
    select i.id,
           i.email,
           i.role,
           i.producer_id,
           coalesce(i.consumer_id, ''),
           coalesce(i.env, ''),
           coalesce(u.id, '')
      from invites i
      left join users u on lower(u.email) = lower(i.email)
     where i.token_hash = p_token_hash
       and i.accepted_at is null
       and i.revoked_at is null
       and i.expires_at > now()
$$;

comment on function pigeon_invite_lookup(bytea) is
    'Pre-authentication read of one invitation by token hash. Also reports whether '
    'the invited address already has an account, which decides whether acceptance '
    'needs that account''s own session.';

revoke all on function pigeon_invite_lookup(bytea) from public;

-- ---------------------------------------------------------------------------
-- 2. Acceptance: never rewrite credentials; require the account holder.
--
-- The signature changes (one more argument), so the old function is dropped
-- rather than replaced: `create or replace` cannot change an OUT/IN signature.
-- ---------------------------------------------------------------------------
drop function pigeon_invite_accept(bytea, text, text);

create function pigeon_invite_accept(
    p_token_hash     bytea,
    p_user_id        text,
    p_password_hash  text,
    p_caller_user_id text
)
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
        -- A new human. The invitation plus the chosen password is the whole
        -- credential, and there is no existing account to take over (FR-111).
        if coalesce(p_password_hash, '') = '' then
            return;
        end if;
        uid := p_user_id;
        insert into users (id, email, password_hash)
        values (uid, v.email, p_password_hash);
    else
        -- An existing human. The token is not proof of account control (the
        -- inviter holds it), so the request must carry that account's session.
        -- And nothing about the credentials changes: this is a membership, not a
        -- password reset.
        if p_caller_user_id is null or p_caller_user_id = '' or p_caller_user_id <> uid then
            return;
        end if;
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

comment on function pigeon_invite_accept(bytea, text, text, text) is
    'FR-104/FR-109/FR-111. Creates the user for a new address, or binds an '
    'existing account that presented its own session; never rewrites an existing '
    'account''s credentials.';

revoke all on function pigeon_invite_accept(bytea, text, text, text) from public;
