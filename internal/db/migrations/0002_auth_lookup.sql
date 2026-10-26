-- 0002_auth_lookup.sql — the one sanctioned row-level-security bypass.
--
-- Key lookup happens before the caller's producer is known, so RLS on api_keys
-- (0001) would make authentication impossible: fail-closed is correct for data
-- reads and wrong for the lookup that establishes the identity in the first
-- place. A SECURITY DEFINER function is the narrow exception: it can read one
-- row by hash and returns nothing else, so it cannot be used to enumerate.
--
-- The function returns exactly what authentication needs and no key material, so
-- a compromised application role still cannot recover a plaintext key.

create or replace function pigeon_auth_lookup(p_key_hash bytea)
returns table (key_id text, producer_id text, env text, replay_scope boolean)
language sql
security definer
set search_path = public, pg_temp
as $$
    select k.id, k.producer_id, k.env, k.replay_scope
      from api_keys k
     where k.key_hash = p_key_hash
       and k.revoked_at is null
$$;

comment on function pigeon_auth_lookup(bytea) is
    'FR-201/FR-205: authenticate a bearer key by hash. Revoked keys return no row, '
    'so revocation takes effect on the next request with no cache to invalidate.';

revoke all on function pigeon_auth_lookup(bytea) from public;
grant execute on function pigeon_auth_lookup(bytea) to current_user;
