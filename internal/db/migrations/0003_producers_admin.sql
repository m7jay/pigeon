-- 0003_producers_admin.sql — the instance-admin read/write path for producers.
--
-- 0001 made producers fail closed for every tenant-scoped query
-- (`producers_none using (false)`), because no producer id can isolate a list of
-- producers from itself. That left the instance-admin path with no way to read
-- or manage producers at all: `pigeon.instance_admin=on` was named in a comment
-- but no policy implemented it. This migration implements it.
--
-- RLS policies are permissive and OR'd: this policy widens access only when the
-- session explicitly sets `pigeon.instance_admin=on`, and `current_setting(...,
-- true)` is NULL when unset, so the comparison is false rather than an error and
-- the default remains "see nothing".
--
-- Honest scope: this is a bug-and-misrouting gate, not a privilege boundary. The
-- application role can set this GUC, so it defends against a forgotten tenant
-- scope in a request path, not against a compromised app role. Splitting the
-- admin path onto a separate database role is the real boundary and belongs to
-- the admin API work in S1 (recorded in PLAN.md).
create policy producers_admin on producers
    for all
    using (current_setting('pigeon.instance_admin', true) = 'on')
    with check (current_setting('pigeon.instance_admin', true) = 'on');

comment on policy producers_admin on producers is
    'Instance-admin path (FR-101): reads and writes producers only when the session sets pigeon.instance_admin=on. Fail closed otherwise; the policy name in 0001 is producers_none.';
