-- Runtime role for the isolated load-test database (test.compose.yaml).
-- The disposable integration database is named pigeon_test, unlike the
-- deployment database, so its grants are stated here rather than reusing
-- 01-roles.sql's production database name.

\getenv app_password PIGEON_APP_PASSWORD

select format(
         'create role pigeon_app login password %L nosuperuser nobypassrls nocreatedb nocreaterole noinherit',
         :'app_password')
 where not exists (select 1 from pg_roles where rolname = 'pigeon_app')
\gexec

grant connect on database pigeon_test to pigeon_app;
grant usage on schema public to pigeon_app;
alter default privileges for role pigeon_test in schema public
    grant select, insert, update, delete on tables to pigeon_app;
alter default privileges for role pigeon_test in schema public
    grant usage, select on sequences to pigeon_app;
alter default privileges for role pigeon_test in schema public
    grant execute on functions to pigeon_app;
