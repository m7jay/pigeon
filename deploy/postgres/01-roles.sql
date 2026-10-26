-- Roles for the reference deployment (compose.yaml, docs/topology.md).
--
-- Runs once, as the bootstrap role, before the core starts. Two roles, because
-- PostgreSQL exempts a table's *owner* from row-level security: if the process
-- that serves requests owns the tenant tables, every policy in
-- internal/db/migrations is inert for it, which is exactly the hole the S1 review
-- found in the first version of compose.yaml.
--
--   pigeon_migrator  owns the database and every object the migrations create.
--                    Used only by the migration path
--                    (PIGEON_MIGRATE_DATABASE_URL) and by an operator with psql.
--   pigeon_app       the runtime role (PIGEON_DATABASE_URL). No superuser, no
--                    BYPASSRLS, no ownership of anything, DML and function EXECUTE
--                    only. Every policy applies to it.
--
-- The password comes from PIGEON_APP_PASSWORD so it is configurable per deployment
-- and never baked into an image. psql's \getenv is what makes that possible in an
-- init script, which otherwise only runs SQL.

\getenv app_password PIGEON_APP_PASSWORD

-- \gexec runs the statement the query returns, which is how a CREATE ROLE (which
-- cannot run inside a DO block's dynamic SQL with an interpolated password literal)
-- is made idempotent.
select format(
         'create role pigeon_app login password %L nosuperuser nobypassrls nocreatedb nocreaterole noinherit',
         :'app_password')
 where not exists (select 1 from pg_roles where rolname = 'pigeon_app')
\gexec

grant connect on database pigeon to pigeon_app;
grant usage on schema public to pigeon_app;

-- Tables and functions are created by pigeon_migrator *after* this script runs, so
-- the grants have to be the default privileges of the role that creates them. The
-- function grant matters: every pre-authentication lookup (0002, 0004, 0005) is a
-- SECURITY DEFINER function the runtime role must be allowed to call.
alter default privileges in schema public
    grant select, insert, update, delete on tables to pigeon_app;
alter default privileges in schema public
    grant usage, select on sequences to pigeon_app;
alter default privileges in schema public
    grant execute on functions to pigeon_app;

-- Defence in depth for a database that already has objects (a restore, or a re-run
-- of this script). On an empty schema these are no-ops.
grant select, insert, update, delete on all tables in schema public to pigeon_app;
grant usage, select on all sequences in schema public to pigeon_app;
grant execute on all functions in schema public to pigeon_app;
