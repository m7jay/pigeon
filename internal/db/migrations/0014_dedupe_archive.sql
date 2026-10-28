-- 0014_dedupe_archive.sql — FR-905/AC-46: the dedupe answer outlives delivery
-- retention, in Postgres, and the copy is atomic with the drop that ends it.
--
-- 0012 turned `deliveries` and `attempts` into daily partitions and made the
-- sweeper drop whole days. That is exactly the window FR-903 asks about: after a
-- delivery's partition is dropped the dedupe answer would be gone 16 days short
-- of the 30 FR-903 promises. This file adds the second half A-4 decided:
-- `delivery_dedupe`, a range-partitioned archive filled from the partition being
-- dropped, in the same transaction as the drop.
--
-- Why 0014 and not an edit to 0012: db.Migrate records each applied file's
-- checksum, so an applied migration is never edited. 0012 stays as it landed and
-- this file appends. It restates both partition primitives in full
-- (`pigeon_ensure_partition`, `pigeon_drop_partition`) because they are
-- `create or replace` and their closed table set has to learn the archive; the
-- bodies are 0012's plus the archive's branch, so the tree holds one definition
-- of each rather than two that drift.
--
-- What FR-905 requires of the drop, and how each part is met here:
--
--   * An ordinary (age-driven) drop of a `deliveries` partition copies the
--     partition's dedupe keys into `delivery_dedupe` and then drops it, in ONE
--     transaction: the Go caller (internal/store/retention.go) opens a
--     transaction and commits it only after the function reports the drop, and
--     the two statements are inside one function call, so there is no instant at
--     which neither the `deliveries` row nor the archive answers a key. A failed
--     copy raises out of the function, the statement aborts, and the partition
--     stays for the next tick.
--   * The partition's own lock is taken BEFORE the copy. If a writer holds it,
--     the function returns false having copied nothing - a drop that cannot
--     happen must not leave a copy behind (the `lock table` below is the line
--     that makes that true, and the partition is left for the next tick).
--   * Only an SB-3 disk-pressure drop may skip the copy, because the space is
--     what is being reclaimed. It raises a warning naming the partition and the
--     day range it covered (FR-903's first loss path).
--   * The archive is derived, so under pressure its partitions go oldest-first
--     BEFORE any `deliveries` partition; each such drop warns with the days it
--     removed (FR-903's second loss path). The ordering itself is policy and lives
--     in internal/retention (oldestDroppable), not here.
--   * The archive is dropped by partition too (SB-2): there is no `DELETE` on
--     `delivery_dedupe` anywhere, at any age.
--
-- Schema. A-4 fixes the columns as
-- `(endpoint_id, event_id, first_delivery_id, first_seen_at)` with the primary key
-- `(endpoint_id, event_id)` and no surrogate id. Partitioning by day forces the
-- same growth 0012 forced on `deliveries`: a unique index on a partitioned table
-- MUST contain the partition key (SQLSTATE 0A000), so the key is
-- `(endpoint_id, event_id, first_seen_at)` - `first_seen_at` IS the day, and it is
-- a column the archive has to carry anyway (it is the original timestamp AC-46
-- reads back), so nothing is invented to satisfy the planner. The support question
-- stays an index-only probe on the first two columns, and `min(first_seen_at)` is
-- the pair's first delivery - the archive's day, so partitioning by it and
-- dropping by day is the same statement. SPEC.md's A-4 records the growth.
--
-- Isolation (MT-1). The archive holds tenant data - endpoint ids, event ids,
-- delivery ids - and PostgreSQL propagates neither the row level security flag nor
-- a policy to a partition, so the flag and `delivery_dedupe_tenant` are applied
-- on the parent here and on every leaf by `pigeon_ensure_partition`. The predicate
-- cannot be the archive's own columns (it has no producer_id): it is the owning
-- endpoint's scope, read from `endpoints`, which retention never drops - unlike
-- `deliveries`, whose partition and therefore whose rows are gone by design when
-- the archive matters. The policy is FOR SELECT only: the application never writes
-- the archive, the copy is the SECURITY DEFINER function's job, and a runtime role
-- that could insert into it could forge a support answer.
--
-- Everything here runs inside the one transaction db.Migrate opens per file.
-- There is deliberately no `begin;`/`commit;`: a file-level COMMIT would make the
-- schema durable before its version row was recorded.

-- ---------------------------------------------------------------------------
-- The archive. One row per (endpoint, event, day) the day's partition held, with
-- the pair's first delivery and its original timestamp.
-- ---------------------------------------------------------------------------
create table delivery_dedupe (
    endpoint_id       text        not null,
    event_id          text        not null,
    first_delivery_id bigint      not null,
    -- The partition key: the day of the pair's FIRST delivery in the partition the
    -- copy came from. A daily `deliveries` partition spans exactly one UTC day, so
    -- every row copied from it lands in exactly one archive partition and the copy
    -- can never straddle a boundary (23514 if it could).
    first_seen_at     timestamptz not null,
    -- A-4: no surrogate id, primary key (endpoint_id, event_id) - grown by the
    -- partition key, which a partitioned table's unique index must contain.
    constraint delivery_dedupe_pkey primary key (endpoint_id, event_id, first_seen_at)
) partition by range (first_seen_at);

comment on table delivery_dedupe is
    'FR-903/FR-905 (A-4): the dedupe archive. Filled by pigeon_drop_partition from each deliveries partition as retention drops it, in the same transaction as the drop, and dropped by partition at dedupe_retention (default 30 d). A key answerable from both sides is counted once by delivery_dedupe_all (a UNION).';
comment on column delivery_dedupe.first_delivery_id is
    'The pair''s first delivery id in the partition it was copied from - FR-1106 replays do not move it (they are outside deliveries_first_fanout_key).';
comment on column delivery_dedupe.first_seen_at is
    'The pair''s FIRST delivery timestamp (min(created_at) in the dropped partition), i.e. the original time AC-46 reads back. Also the partition key: the copy of a daily deliveries partition lands wholly in one day partition of the archive.';

alter table delivery_dedupe enable row level security;

-- The archive outlives `deliveries`, so its tenant predicate is derived from
-- `endpoints` (which retention does not touch) rather than from the delivery row it
-- came from: `first_delivery_id` may name a row in a partition that no longer
-- exists. Read-only: the copy is the SECURITY DEFINER function's write.
create policy delivery_dedupe_tenant on delivery_dedupe for select
    using (exists (
        select 1 from endpoints ep
        where ep.id = delivery_dedupe.endpoint_id
          and ep.producer_id = current_setting('pigeon.producer_id', true)
          and ep.env = current_setting('pigeon.env', true)));

comment on policy delivery_dedupe_tenant on delivery_dedupe is
    'MT-1: the archive holds tenant data and is scoped to the owning endpoint''s producer and env. Derived from the endpoint, not the delivery, because the delivery partition is exactly what FR-905 let go.';

-- ---------------------------------------------------------------------------
-- Partition management, restated from 0012 with the archive in the closed set.
-- ---------------------------------------------------------------------------

-- Create one day's partition if it is missing. Idempotent; safe to call on every
-- fan-out and every sweep tick. Days are UTC days so the boundary is stable
-- regardless of the server's TimeZone.
--
-- The leaf is created WITH its parent's row level security (MT-1). PostgreSQL
-- does not propagate RLS to partitions: the flag and the policies belong to each
-- relation, so a new child is unprotected however protected its parent is -
-- measured on 16.2, `deliveries.relrowsecurity` true and `deliveries_YYYYMMDD`
-- false. Every partition is created here (the cutover loop 0012 ran, fan-out, the
-- sweeper, pigeon_record_attempts, and the FR-905 copy), so this is the one place
-- that has to remember it for all three tables.
create or replace function pigeon_ensure_partition(p_table text, p_day date)
    returns text
    language plpgsql
    security definer
    set search_path = public, pg_temp
as $$
declare
    v_name     text;
    v_from     timestamptz;
    v_to       timestamptz;
    -- Policy names are per relation, so a leaf carries its parent's name.
    v_policy   text;
    -- The policy expression in the leaf's own terms: a leaf's policy must name the
    -- leaf, because neither `attempts` nor `delivery_dedupe` is in scope on a child
    -- relation (0012's attempts policy learned this the hard way).
    v_using    text;
    -- The WITH CHECK expression, or null for a read-only policy.
    v_check    text;
    v_isolated boolean;
begin
    if p_table not in ('deliveries', 'attempts', 'delivery_dedupe') then
        raise exception 'pigeon_ensure_partition: unknown partitioned table %', p_table;
    end if;
    if p_day is null then
        raise exception 'pigeon_ensure_partition: null day';
    end if;

    v_name := p_table || '_' || to_char(p_day, 'YYYYMMDD');
    v_from := (p_day::timestamp) at time zone 'UTC';
    v_to   := ((p_day + 1)::timestamp) at time zone 'UTC';

    if p_table = 'deliveries' then
        v_policy := 'deliveries_tenant';
        v_using  := 'producer_id = current_setting(''pigeon.producer_id'', true) and env = current_setting(''pigeon.env'', true)';
        v_check  := v_using;
    elsif p_table = 'attempts' then
        -- 0011's read policy: the predicate is the owning delivery's scope, and
        -- the subquery reads deliveries under the caller's policies too.
        v_policy := 'attempts_tenant_read';
        v_using  := format(
            'exists (select 1 from deliveries d where d.id = %I.delivery_id and d.producer_id = current_setting(''pigeon.producer_id'', true) and d.env = current_setting(''pigeon.env'', true))',
            v_name);
        v_check  := null;
    else
        -- FR-905: the archive's scope comes from the owning endpoint, because the
        -- delivery the key came from is in the partition being dropped. Read-only:
        -- the copy writes as this function's owner, never as the runtime role.
        v_policy := 'delivery_dedupe_tenant';
        v_using  := format(
            'exists (select 1 from endpoints ep where ep.id = %I.endpoint_id and ep.producer_id = current_setting(''pigeon.producer_id'', true) and ep.env = current_setting(''pigeon.env'', true))',
            v_name);
        v_check  := null;
    end if;

    if to_regclass(v_name) is null then
        execute format(
            'create table %I partition of %I for values from (%L) to (%L)',
            v_name, p_table, v_from, v_to);

        -- AC-07's defensive double-write guard lives per partition, because a
        -- partitioned table cannot carry a global unique index on non-key columns.
        -- The lease serialises per-delivery writers; this catches a bug that
        -- double-writes the same attempt within the day. The archive needs none:
        -- its primary key comes from the parent, and the copy aggregates per pair
        -- before it inserts.
        if p_table = 'attempts' then
            execute format(
                'create unique index %I on %I (delivery_id, attempt)',
                v_name || '_delivery_attempt_key', v_name);
        end if;

        raise notice 'pigeon: created partition %', v_name;
    end if;

    -- Idempotent, and the repair path for a leaf that predates this file (or was
    -- attached by an operator): a leaf is isolated iff it has the flag AND the
    -- policy. Both are checked, because the flag alone without the policy denies
    -- the tenant its own rows and the policy alone without the flag allows
    -- everyone everyone's.
    select (c.relrowsecurity
            and exists (select 1 from pg_policy p
                         where p.polrelid = c.oid and p.polname = v_policy))
      into v_isolated
      from pg_class c
     where c.oid = to_regclass(v_name);

    if not coalesce(v_isolated, false) then
        execute format('alter table %I enable row level security', v_name);
        if not exists (select 1 from pg_policy p
                        where p.polrelid = to_regclass(v_name) and p.polname = v_policy) then
            if v_check is null then
                execute format('create policy %I on %I for select using (%s)',
                               v_policy, v_name, v_using);
            else
                execute format('create policy %I on %I using (%s) with check (%s)',
                               v_policy, v_name, v_using, v_check);
            end if;
        end if;
    end if;

    return v_name;
end;
$$;

-- Drop one partition, by name, if and only if it is a leaf partition of one of the
-- three retained tables and its day has fully elapsed. Returns true when this call
-- dropped it and false when it was already gone, or when a writer held it (so a
-- retry is a no-op and a contended drop is left for the next tick).
--
-- FR-905's atomicity lives here. In order:
--
--   1. The transaction-scoped advisory lock (D-7) and the 5 s lock_timeout. The
--      timeout has to be in force when the advisory lock is taken, or a contended
--      drop would wait forever instead of leaving the partition for the next tick.
--   2. `lock table ... in access exclusive mode`, BEFORE anything is copied. If a
--      writer holds the partition this returns false in step 2 having written
--      nothing, so "the copy and the drop commit together or neither does" holds
--      for the drop-did-not-happen case too, not only for the copy-failed case.
--      After this line the drop itself cannot fail for a lock reason.
--   3. The copy (ordinary drops of a `deliveries` partition only), or a warning
--      naming the loss (a pressure drop that deliberately skips it).
--   4. The drop.
--
-- A failed copy (step 3) raises out of this function, so the statement aborts with
-- the drop and the caller's transaction rolls back: the partition is still there
-- and the next tick tries again. Nothing in this function catches that error - the
-- handlers below cover only "already gone" and "a writer held it", and both roll
-- the function's own changes back with them.
--
-- 0012's two-argument primitive is replaced, not overloaded: leaving both would
-- make a two-argument call ambiguous, and the caller (internal/store/retention.go)
-- has to say why the partition is leaving, because the reason is what decides
-- whether the copy may be skipped.
drop function if exists pigeon_drop_partition(text, text);

create or replace function pigeon_drop_partition(p_table text, p_name text, p_reason text default 'age')
    returns boolean
    language plpgsql
    security definer
    set search_path = public, pg_temp
as $$
declare
    v_parent oid;
    v_day    date;
begin
    if p_table not in ('deliveries', 'attempts', 'delivery_dedupe') then
        raise exception 'pigeon_drop_partition: unknown partitioned table %', p_table;
    end if;
    -- The reason decides whether the copy may be skipped, so it is validated
    -- rather than trusted: only SB-3 disk pressure may lose an answer.
    if p_reason not in ('age', 'pressure') then
        raise exception 'pigeon_drop_partition: unknown drop reason % (FR-905: age copies, pressure may skip)', p_reason;
    end if;
    -- The name must be exactly <table>_<8 digits>; caller text never reaches DDL.
    if p_name !~ ('^' || p_table || '_[0-9]{8}$') then
        raise exception 'pigeon_drop_partition: % is not a % partition name', p_name, p_table;
    end if;

    -- Already gone: a retry after a successful drop is a no-op, not an error.
    if to_regclass(p_name) is null then
        return false;
    end if;

    -- Confirm the relation really is a leaf partition of the named parent.
    select i.inhparent into v_parent
    from pg_inherits i
    where i.inhrelid = to_regclass(p_name);

    if v_parent is null or v_parent <> to_regclass(p_table) then
        raise exception 'pigeon_drop_partition: % is not a partition of %', p_name, p_table;
    end if;

    v_day := to_date(substring(p_name from '[0-9]{8}$'), 'YYYYMMDD');
    if v_day >= (now() at time zone 'UTC')::date then
        raise exception 'pigeon_drop_partition: refusing to drop %, its day has not ended', p_name;
    end if;

    -- Serialise with any other drop, in this process or another replica. The
    -- transaction-scoped lock releases on commit/rollback and needs no cleanup.
    perform set_config('lock_timeout', '5s', true);
    perform pg_advisory_xact_lock(pigeon_partition_drop_lock_id());

    -- FR-905, step 2: the partition's own lock, before the copy. `lock table`
    -- waits for at most lock_timeout; a writer that holds it longer leaves the
    -- partition for the next tick with nothing copied. Everything after this line
    -- holds ACCESS EXCLUSIVE, so `drop table` below cannot fail on a lock.
    begin
        execute format('lock table %I in access exclusive mode', p_name);
    exception
        when lock_not_available or query_canceled then
            return false;
    end;

    -- FR-905, step 3. Only an ordinary drop of a `deliveries` partition copies:
    -- the archive has nothing to copy into, `attempts` hold no dedupe key, and
    -- SB-3 pressure is the one case allowed to skip the copy because the space is
    -- what is being reclaimed. Both loss paths are logged at warn with the
    -- partition and the day range (FR-903), so a missing answer is known rather
    -- than discovered.
    if p_table = 'deliveries' and p_reason = 'pressure' then
        raise warning 'pigeon: SB-3 pressure drop of % (% to %) skipped the FR-905 dedupe copy; the dedupe answers for that day are lost (FR-903)',
            p_name, v_day, v_day + 1;
    elsif p_table = 'delivery_dedupe' and p_reason = 'pressure' then
        raise warning 'pigeon: SB-3 pressure drop of % (% to %) removes archived dedupe days ahead of dedupe_retention; those answers are lost (FR-903)',
            p_name, v_day, v_day + 1;
    end if;

    if p_table = 'deliveries' and p_reason <> 'pressure' then
        -- The archive partition for the day has to exist before the insert: a copy
        -- lands wholly in one day partition (see the table comment). Idempotent.
        perform pigeon_ensure_partition('delivery_dedupe', v_day);

        -- The pair's FIRST delivery, aggregated the same way delivery_dedupe_all
        -- does it (min(id), min(created_at)), so the archive row and the live row
        -- agree on the answer. `on conflict do nothing` keeps a re-copy a no-op:
        -- the copy runs at most once per partition, but a retried drop after a
        -- crash must not be able to fail the tick.
        execute format($copy$
            insert into delivery_dedupe (endpoint_id, event_id, first_delivery_id, first_seen_at)
            select d.endpoint_id, d.event_id, min(d.id), min(d.created_at)
              from %I d
             group by d.endpoint_id, d.event_id
            on conflict do nothing
        $copy$, p_name);
    end if;

    -- FR-905, step 4.
    execute format('drop table %I', p_name);
    return true;
exception
    when undefined_table then
        return false;
    when lock_not_available or query_canceled then
        -- A writer held the partition past the lock_timeout. Leave it for the
        -- next tick; dropping it while a delivery is mid-write would lose work.
        -- (Reached by the advisory lock as well; the copy has not run yet in either
        -- case, and catching an exception here rolls back anything this block did.)
        return false;
end;
$$;

comment on function pigeon_drop_partition(text, text, text) is
    'FR-905/SB-2: drops one ended leaf partition. An ordinary (age) drop of a deliveries partition copies the day''s dedupe keys into delivery_dedupe in the same transaction and aborts with the copy if the copy fails; an SB-3 pressure drop may skip the copy and warns with the partition and the day range (FR-903). Takes the partition lock before copying, so a contended drop leaves neither a copy nor a partition gone.';

-- The archive answers a support question, so it needs no write grant for the
-- runtime role beyond what deploy/postgres/01-roles.sql already gives; the copy
-- runs as this file's owner.
grant execute on function pigeon_ensure_partition(text, date) to current_user;
grant execute on function pigeon_drop_partition(text, text, text) to current_user;
grant execute on function pigeon_partition_drop_lock_id() to current_user;

-- ---------------------------------------------------------------------------
-- The one name the support path reads (A-4): live deliveries UNION the archive.
-- ---------------------------------------------------------------------------

-- FR-903/FR-905 (0011, restated): the dedupe answer, read through one name.
-- `security_invoker` keeps the caller's policies in force on BOTH sides, so the
-- view is not a way around tenant isolation (MT-1) - the archive's own policy is
-- what filters its half. It is a UNION, not a UNION ALL, so a key answerable from
-- both sides (a partition dropped and copied, or a replayed pair) counts once; the
-- outer min() then agrees with what A-4 promises: the pair's first delivery and its
-- original timestamp, whichever side answers.
create or replace view delivery_dedupe_all with (security_invoker = true) as
    select endpoint_id,
           event_id,
           min(first_delivery_id) as first_delivery_id,
           min(first_seen_at)     as first_seen_at
      from (
            select d.endpoint_id, d.event_id, d.id as first_delivery_id, d.created_at as first_seen_at
              from deliveries d
            union
            select a.endpoint_id, a.event_id, a.first_delivery_id, a.first_seen_at
              from delivery_dedupe a
           ) sides
     group by endpoint_id, event_id;

comment on view delivery_dedupe_all is
    'FR-903/FR-905: the 30-day dedupe answer. A UNION over live deliveries (0 to deliveries retention) and the delivery_dedupe archive (the difference between the two retentions), so a key present on both sides counts once (A-4).';
