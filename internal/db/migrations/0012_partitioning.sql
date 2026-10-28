-- 0012_partitioning.sql — SB-1/SB-2: deliveries and attempts become daily range
-- partitions so retention can drop whole days instead of DELETE.
--
-- Why this is a swap and not an ALTER: PostgreSQL cannot convert a plain table
-- into a partitioned one. The migration therefore freezes both tables,
-- moves the originals aside, creates partitioned replacements with the same
-- columns, copies the rows into per-day partitions and drops the originals — all
-- in the one transaction db.Migrate gives each migration file. A failure leaves
-- the plain tables intact.
--
-- The number is 0012, not 0010: S6 took 0010 (0010_attempt_buffer.sql) and S7 took
-- 0011 (0011_s7_inspect_replay.sql) while this slice was open. Migrations run in
-- ascending order, so both of those apply to the tables as they were and this file
-- runs last, which is what makes the reconciliation below a plain append rather
-- than an edit to a migration that has already run (that would break every existing
-- database with a checksum mismatch).
--
-- What this file must carry across the swap: everything S6 and S7 added to the two
-- tables, because the swap recreates them from an explicit column and index list.
--   * deliveries.replay_of_delivery_id (0011, FR-1106) — and its foreign key, which
--     cannot survive: a partitioned table's referenced unique key must contain the
--     partition key, so the column becomes provenance (a plain bigint), exactly as
--     attempts' FK to deliveries did. Dropping the original partition no longer
--     cascades to the replay; that was 0011's intent anyway ("the link is
--     provenance, not ownership").
--   * deliveries_first_fanout_key (0011, amended I-3 / A-4) — the partial unique
--     index that keeps the pair unique for the FIRST fan-out only. On a partitioned
--     table a unique index must contain the partition key, so it becomes
--     unique (event_id, endpoint_id, created_at) WHERE replay_of_delivery_id IS NULL.
--     The predicate — which is the invariant — is preserved; the key grows the
--     partition column, which is what the partitioning forces. Fan-out infers it
--     (internal/store/fanout.go) with the same predicate.
--   * deliveries_event_idx (0011, FR-1109) and the attempts read policy (0011,
--     FR-1102/1103), which die with the tables they hang off and are recreated here.
--   * the row level security flag and policy on every partition (MT-1). PostgreSQL
--     propagates neither to a child, so pigeon_ensure_partition applies both where it
--     creates the leaf: without that, a query that named a day partition as the
--     runtime role read every tenant's rows, which the single protected relation it
--     replaced would have refused. The cutover loop below creates the first days
--     through that same function, so they are covered too.
--   * delivery_dedupe_all (0011, FR-903) — a view over deliveries, so it is dropped
--     and recreated rather than blocked by the swap.
--   * pigeon_record_attempts (0010, FR-815): the batched evidence insert. Its
--     `on conflict (delivery_id, attempt)` cannot be inferred against a partitioned
--     parent — the per-partition unique index lives on each child, not on the parent
--     — so it becomes the targetless form, which keeps the same within-day
--     idempotency. It also creates the day's partition itself, because an attempt is
--     stamped now() and a first attempt just after midnight must not fail on a
--     partition the sweeper has not created yet.
--
-- The dedupe keys change shape because a partitioned table's unique key MUST
-- contain the partition key. That is a real change and SPEC.md records it:
--
--   deliveries  (event_id, endpoint_id) WHERE replay_of_delivery_id IS NULL
--               -> (event_id, endpoint_id, created_at) WHERE replay_of_delivery_id IS NULL
--   attempts    (delivery_id, attempt)   -> per-partition unique (delivery_id, attempt)
--
-- FR-704's re-fan-out idempotency is preserved by making the day deterministic:
-- fan-out stamps deliveries.created_at from the EVENT's created_at instead of
-- now(), so a retried fan-out lands in the same partition and conflicts on the
-- same key. Replay (S7) is unaffected: it creates rows on its own day with
-- replay_of_delivery_id set, and those rows are outside the partial index. Both
-- notes are in SPEC.md's S8 section.
--
-- attempts loses its FK to deliveries: an incoming foreign key to a partitioned
-- table must include the partition key on both sides. The lease serialises
-- per-delivery writes and pigeon_next_attempt derives the number from the rows,
-- so the FK was a backstop, not the uniqueness mechanism.
--
-- The whole file runs inside the one transaction db.Migrate opens per migration
-- (internal/db/migrate.go), which records the schema_migrations row inside that
-- same transaction. There is deliberately no `begin;`/`commit;` here: a file-level
-- COMMIT would end that transaction early, making the swap durable *before* it was
-- recorded, and a crash in the window would leave a database that can neither boot
-- (the body is not idempotent) nor be repaired by re-running it. No statement here
-- needs to be outside a transaction.

-- Writes pause for the swap; both writers (fan-out and the delivery worker)
-- retry on error, so a short exclusive lock is safe and beats a half-swapped
-- schema.
lock table deliveries, attempts in access exclusive mode;

-- Keep the id sequences: dropping a serial's table drops its sequence with it,
-- and restarting ids would collide with anything that still holds one.
alter sequence deliveries_id_seq owned by none;
alter sequence attempts_id_seq owned by none;

-- attempts: drop the FK and rename the unique index before the table moves, so
-- the replacement can reuse both names.
alter table attempts drop constraint attempts_delivery_id_fkey;
alter index attempts_delivery_attempt_key rename to attempts_delivery_attempt_key_legacy;

-- deliveries: 0011's self-referencing FK cannot exist on the partitioned
-- replacement, so it goes here (see the header). The column and its comment are
-- recreated with the table.
alter table deliveries drop constraint deliveries_replay_of_delivery_id_fkey;

-- delivery_dedupe_all reads deliveries, so it would block the drop of the legacy
-- table. Dropped here and recreated below; the definition is 0011's, unchanged.
drop view delivery_dedupe_all;

-- Move the originals aside. Index names are schema-wide (constraint names are
-- not), so every index-backed name the replacements need is freed here — including
-- the two 0011 created and deliveries_pending_idx from 0007.
alter table deliveries rename to deliveries_legacy;
alter table attempts rename to attempts_legacy;
alter table deliveries_legacy rename constraint deliveries_pkey to deliveries_legacy_pkey;
drop index deliveries_pending_idx;
drop index deliveries_first_fanout_key;
drop index deliveries_event_idx;
alter table attempts_legacy rename constraint attempts_pkey to attempts_legacy_pkey;

-- ---------------------------------------------------------------------------
-- The partitioned replacements. Columns, checks and defaults are copied from
-- 0007/0008; the structural changes are the composite primary key (id must be
-- accompanied by the partition key) and deliveries' day-bearing unique key.
-- ---------------------------------------------------------------------------
create table deliveries (
    id                  bigint      not null default nextval('deliveries_id_seq'),
    event_id            text        not null
                        constraint deliveries_event_id_fkey references events (id) on delete cascade,
    endpoint_id         text        not null,
    producer_id         text        not null,
    env                 text        not null,
    consumer_id         text        not null,
    status              text        not null default 'pending'
                        constraint deliveries_status_check
                        check (status in ('pending', 'skipped', 'delivered', 'failed')),
    non_delivery_reason text
                        constraint deliveries_non_delivery_reason_check
                        check (non_delivery_reason is null or non_delivery_reason in (
                            'unroutable:consumer_not_registered',
                            'not_delivered:no_matching_subscription',
                            'not_delivered:endpoint_unverified',
                            'not_delivered:endpoint_paused')),
    attempts            int         not null default 0,
    first_attempt_at    timestamptz,
    delivered_at        timestamptz,
    lease_expires_at    timestamptz,
    -- FR-1106/FR-1108 (0011): the delivery this row replays, or null for a first
    -- fan-out. A plain provenance column, not a foreign key: a partitioned table's
    -- referenced key must contain the partition key, so a self-reference cannot be
    -- enforced — the same reason attempts lost its FK. Retention dropping the
    -- original partition must not take the replay with it, and without an FK it
    -- cannot.
    replay_of_delivery_id bigint,
    created_at          timestamptz not null default now(),
    -- SB-1: the partition key is the day. The unique key includes it so it is
    -- legal on a partitioned table; fan-out supplies the event's created_at so a
    -- retried fan-out lands in the same partition and still conflicts (FR-704).
    constraint deliveries_pkey primary key (id, created_at)
) partition by range (created_at);

comment on column deliveries.replay_of_delivery_id is
    'FR-1106/FR-1108: the delivery this row replays, or null for a first fan-out. A replay creates a new row and never mutates the one it repairs.';

comment on column deliveries.lease_expires_at is
    'FR-813: set at fan-out, never at claim. A crashed worker lets it lapse and the row becomes claimable again (D-6).';
comment on column deliveries.created_at is
    'SB-1: the partition key. Fan-out stamps it from the event''s created_at so a retried fan-out stays in one partition and still dedupes on (event_id, endpoint_id) (FR-704).';

alter table deliveries enable row level security;
create policy deliveries_tenant on deliveries
    using (producer_id = current_setting('pigeon.producer_id', true)
           and env = current_setting('pigeon.env', true))
    with check (producer_id = current_setting('pigeon.producer_id', true)
           and env = current_setting('pigeon.env', true));

create table attempts (
    id              bigint      not null default nextval('attempts_id_seq'),
    delivery_id     bigint      not null,
    attempt         int         not null constraint attempts_attempt_check check (attempt >= 1),
    outcome         text        not null constraint attempts_outcome_check
                    check (outcome in ('retryable', 'terminal')),
    response_status int         constraint attempts_response_status_check
                    check (response_status is null or response_status between 100 and 599),
    error_class     text        not null constraint attempts_error_class_check
                    check (error_class in (
        'timeout', 'dial_error', 'dns_error', 'http_retryable', 'http_terminal')),
    response_body   text,
    duration_ms     int         not null constraint attempts_duration_ms_check check (duration_ms >= 0),
    occurred_at     timestamptz not null default now(),
    next_retry_at   timestamptz,
    constraint attempts_pkey primary key (id, occurred_at)
) partition by range (occurred_at);

comment on table attempts is
    'FR-811: one row per FAILED attempt only. A successful first attempt is the delivery row four columns, not a row here (AC-28).';
comment on column attempts.attempt is
    'FR-902/AC-07: 1-based per delivery, assigned from max(attempts)+1 so a re-claimed row never reuses a number.';
comment on column attempts.occurred_at is
    'SB-1: the partition key.';

alter table attempts enable row level security;

-- ---------------------------------------------------------------------------
-- Partition management. All partition DDL runs through these SECURITY DEFINER
-- functions: the runtime role (pigeon_app) owns nothing and must not hold CREATE
-- on the schema, but the sweeper has to create tomorrow's partition and drop
-- expired ones. Names are built from a fixed table name and a date, never from
-- caller text, and the drop refuses anything that is not an ended leaf partition
-- of one of the two retained tables.
-- ---------------------------------------------------------------------------

-- The advisory-lock key that serialises partition drops (D-7's second half: no
-- partition drop may run concurrently with itself, even across processes).
create or replace function pigeon_partition_drop_lock_id()
    returns bigint language sql immutable
as $$ select 4755598712897799028::bigint $$; -- "pigen_pd" as a stable key

-- Create one day's partition if it is missing. Idempotent; safe to call on every
-- fan-out and every sweep tick. Days are UTC days so the boundary is stable
-- regardless of the server's TimeZone.
--
-- The leaf is created WITH its parent's row level security (MT-1). PostgreSQL
-- does not propagate RLS to partitions: the flag and the policies belong to each
-- relation, so a new child is unprotected however protected its parent is —
-- measured on 16.2, `deliveries.relrowsecurity` true and `deliveries_YYYYMMDD`
-- false. That is exactly the isolation the swap would otherwise have lost: the
-- old single relation filtered a query that named it, and its replacement only
-- filters queries that name the parent — the same query naming the day (a
-- repository bug, a fixture, psql) reached every tenant's rows. Every partition
-- is created here (the cutover loop below, fan-out, the sweeper,
-- pigeon_record_attempts), so this is the one place that has to remember it.
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
    -- The policy expression in the leaf's own terms: an attempts leaf must name
    -- itself in the subquery, because `attempts` is not in scope on the leaf.
    v_using    text;
    -- The WITH CHECK expression, or null for a read-only policy.
    v_check    text;
    v_isolated boolean;
begin
    if p_table not in ('deliveries', 'attempts') then
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
    else
        -- 0011's read policy: the predicate is the owning delivery's scope, and
        -- the subquery reads deliveries under the caller's policies too.
        v_policy := 'attempts_tenant_read';
        v_using  := format(
            'exists (select 1 from deliveries d where d.id = %I.delivery_id and d.producer_id = current_setting(''pigeon.producer_id'', true) and d.env = current_setting(''pigeon.env'', true))',
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
        -- double-writes the same attempt within the day.
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

-- Drop one partition, by name, if and only if it is a leaf partition of one of
-- the two retained tables and its day has fully elapsed. Returns true when this
-- call dropped it and false when it was already gone (so a retry is a no-op).
--
-- The drop is serialised with a transaction-scoped advisory lock and bounded by a
-- lock_timeout: a partition a worker is mid-write on is skipped this tick and
-- retried, rather than stalling the sweeper behind a long transaction.
create or replace function pigeon_drop_partition(p_table text, p_name text)
    returns boolean
    language plpgsql
    security definer
    set search_path = public, pg_temp
as $$
declare
    v_parent oid;
    v_day    date;
begin
    if p_table not in ('deliveries', 'attempts') then
        raise exception 'pigeon_drop_partition: unknown partitioned table %', p_table;
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
    -- lock_timeout is set FIRST: it has to be in force when the advisory lock is
    -- taken, or a contended drop would wait forever instead of leaving the
    -- partition for the next tick.
    perform set_config('lock_timeout', '5s', true);
    perform pg_advisory_xact_lock(pigeon_partition_drop_lock_id());

    execute format('drop table %I', p_name);
    return true;
exception
    when undefined_table then
        return false;
    when lock_not_available or query_canceled then
        -- A writer held the partition past the lock_timeout. Leave it for the
        -- next tick; dropping it while a delivery is mid-write would lose work.
        return false;
end;
$$;

-- ---------------------------------------------------------------------------
-- Copy the existing rows. A partition is created for every day present in the
-- legacy table (plus a small window so the cutover does not race midnight), then
-- the rows move across in id order. The legacy tables are dropped last, inside
-- the same transaction, so a failure leaves the original data intact.
-- ---------------------------------------------------------------------------
do $$
declare
    d date;
begin
    for d in select distinct (created_at at time zone 'UTC')::date from deliveries_legacy order by 1 loop
        perform pigeon_ensure_partition('deliveries', d);
    end loop;
    for d in select distinct (occurred_at at time zone 'UTC')::date from attempts_legacy order by 1 loop
        perform pigeon_ensure_partition('attempts', d);
    end loop;
    -- Today and the next week, so the cutover is safe even when the tables are
    -- empty or the clock crosses midnight during the migration.
    for d in select generate_series((now() at time zone 'UTC')::date, (now() at time zone 'UTC')::date + 7, interval '1 day')::date loop
        perform pigeon_ensure_partition('deliveries', d);
        perform pigeon_ensure_partition('attempts', d);
    end loop;
end;
$$;

insert into deliveries (id, event_id, endpoint_id, producer_id, env, consumer_id, status,
                       non_delivery_reason, attempts, first_attempt_at, delivered_at,
                       lease_expires_at, replay_of_delivery_id, created_at)
select id, event_id, endpoint_id, producer_id, env, consumer_id, status,
       non_delivery_reason, attempts, first_attempt_at, delivered_at,
       lease_expires_at, replay_of_delivery_id, created_at
from deliveries_legacy order by id;

insert into attempts (id, delivery_id, attempt, outcome, response_status, error_class,
                      response_body, duration_ms, occurred_at, next_retry_at)
select id, delivery_id, attempt, outcome, response_status, error_class,
       response_body, duration_ms, occurred_at, next_retry_at
from attempts_legacy order by id;

-- The sequences must not go backwards: they may already be ahead of max(id) if
-- rows were deleted, and reusing an id would be worse than a gap.
do $$
declare
    mx bigint;
    lv bigint;
begin
    select coalesce(max(id), 0) into mx from deliveries_legacy;
    select last_value into lv from deliveries_id_seq;
    if mx > lv then
        perform setval('deliveries_id_seq', mx, true);
    end if;
    select coalesce(max(id), 0) into mx from attempts_legacy;
    select last_value into lv from attempts_id_seq;
    if mx > lv then
        perform setval('attempts_id_seq', mx, true);
    end if;
end;
$$;

drop table attempts_legacy;
drop table deliveries_legacy;

-- FR-814: the worker's claim scan. Created after the partitions so the parent
-- index cascades to every child; partitions created later get theirs when the
-- partition is created.
create index deliveries_pending_idx on deliveries (id) where status = 'pending';

-- ---------------------------------------------------------------------------
-- The objects 0011 created on deliveries/attempts, recreated on the partitioned
-- replacements. Index names are schema-wide and the swap freed them above; the
-- policy and the view died with the tables they hung off. Definitions are 0011's,
-- with the one change the partitioning forces.
-- ---------------------------------------------------------------------------

-- Amended I-3 / A-4 (FR-704): the first-fan-out arbiter. 0011's index was
-- (event_id, endpoint_id) WHERE replay_of_delivery_id IS NULL; on a partitioned
-- table the key must contain the partition key, so created_at joins it and the
-- predicate — the invariant A-4 records — is kept. Fan-out infers exactly this
-- (internal/store/fanout.go): a re-fan-out of the same event lands on the same day
-- and conflicts, while a replay (replay_of_delivery_id not null) is outside the
-- index and may insert a second row for the pair.
create unique index deliveries_first_fanout_key
    on deliveries (event_id, endpoint_id, created_at)
    where replay_of_delivery_id is null;

-- FR-1109/FR-1111 (0011): "did my event succeed?" is answered per event across
-- first fan-out rows AND replays, so it cannot ride the partial index above.
create index deliveries_event_idx on deliveries (producer_id, env, event_id);

-- FR-1102/FR-1103 (0011): the attempt trail is readable by its tenant, and only by
-- its tenant. The predicate is the owning delivery's scope, and the subquery reads
-- deliveries under RLS as well, so this is the same check the delivery read passed.
-- The write path is unchanged: the worker reaches attempts only through
-- pigeon_record_attempts, and a tenant cannot write or delete an attempt row.
create policy attempts_tenant_read on attempts for select
    using (exists (
        select 1 from deliveries d
        where d.id = attempts.delivery_id
          and d.producer_id = current_setting('pigeon.producer_id', true)
          and d.env = current_setting('pigeon.env', true)));

-- FR-903/FR-905 (0011): the dedupe answer, read through one name.
-- security_invoker keeps the caller's policies in force, so the view is not a way
-- around tenant isolation (MT-1). The swap replaced the relation underneath it, so
-- it is recreated here; the definition and the comment are 0011's.
create view delivery_dedupe_all with (security_invoker = true) as
    select endpoint_id,
           event_id,
           min(id)          as first_delivery_id,
           min(created_at)  as first_seen_at
    from deliveries
    group by endpoint_id, event_id;

comment on view delivery_dedupe_all is
    'FR-903/FR-905: the 30-day dedupe answer. Live deliveries 0-14 d; the delivery_dedupe archive S8 fills at partition drop covers 14-30 d (A-4).';

-- ---------------------------------------------------------------------------
-- pigeon_record_attempts: S6's batched evidence insert (0010, FR-815/D-1),
-- restated for a partitioned attempts table. Nothing else about it changes:
-- same signature, same return value (rows actually inserted), same
-- SECURITY DEFINER, same SB-4 body cap, same evidence transaction on the caller's
-- side (db.EvidenceTx).
--
--   1. `on conflict do nothing` replaces `on conflict (delivery_id, attempt)`.
--      The AC-07 double-write guard is a per-partition unique index, created by
--      pigeon_ensure_partition on each day's leaf, and a partitioned parent has no
--      unique index of its own — so the targeted form cannot be inferred (42P10).
--      The targetless form has the same effect: a row Postgres already has is
--      skipped and not counted, which is what makes the buffer's own replay after
--      a crash idempotent.
--   2. It creates the day's partition itself (SB-1), before the insert. Evidence
--      is stamped now() (or the buffer's own occurred_at), so a batch flushed just
--      after midnight must not fail on a partition the sweeper has not reached yet.
--      The call is a no-op when the partition exists, and fan-out and the sweeper
--      create days ahead, so this is a fallback rather than the common path. The
--      language becomes plpgsql for that reason alone.
--
-- 0008's single-row pigeon_record_attempt is not restored: 0010 dropped it on
-- purpose (two insert paths would mean two places the body cap and the attempt
-- timestamp are applied, and they would drift). Sync mode (D-2) calls this
-- function with a one-row batch.
-- ---------------------------------------------------------------------------
drop function pigeon_record_attempts(bigint[],int[],text[],text[],int[],text[],int[],timestamptz[],timestamptz[],int);

create function pigeon_record_attempts(
        p_delivery_ids  bigint[],
        p_attempts      int[],
        p_outcomes      text[],
        p_error_classes text[],
        p_statuses      int[],
        p_bodies        text[],
        p_durations     int[],
        p_next_retries  timestamptz[],
        p_occurred_ats  timestamptz[],
        p_body_cap      int
    )
    returns int
    language plpgsql
    security definer
    set search_path = public, pg_temp
as $$
declare
    v_inserted int;
begin
    perform pigeon_ensure_partition('attempts', (now() at time zone 'UTC')::date);

    with src as (
        select b.delivery_id, b.attempt, b.outcome, b.error_class, b.status,
               b.body, b.duration_ms, b.next_retry_at, b.occurred_at
        from unnest(
                 p_delivery_ids,
                 p_attempts,
                 p_outcomes,
                 p_error_classes,
                 coalesce(p_statuses, array[]::int[]),
                 coalesce(p_bodies, array[]::text[]),
                 p_durations,
                 coalesce(p_next_retries, array[]::timestamptz[]),
                 coalesce(p_occurred_ats, array[]::timestamptz[])
             ) as b(delivery_id, attempt, outcome, error_class, status, body,
                    duration_ms, next_retry_at, occurred_at)
    ),
    ins as (
        insert into attempts
            (delivery_id, attempt, outcome, error_class, response_status,
             response_body, duration_ms, next_retry_at, occurred_at)
        select s.delivery_id, s.attempt, s.outcome, s.error_class, s.status,
               case when p_body_cap is null or p_body_cap < 0
                    then coalesce(s.body, '')
                    else left(coalesce(s.body, ''), p_body_cap) end,
               s.duration_ms, s.next_retry_at, coalesce(s.occurred_at, now())
        from src s
        on conflict do nothing
        returning 1
    )
    select count(*)::int into v_inserted from ins;
    return v_inserted;
end;
$$;

comment on function pigeon_record_attempts(bigint[],int[],text[],text[],int[],text[],int[],timestamptz[],timestamptz[],int) is
    'FR-815: one transaction, one statement, >= 2000 evidence rows. Evidence class (db.EvidenceTx, synchronous_commit=off). Idempotent on (delivery_id, attempt); returns the rows inserted.';

grant execute on function pigeon_record_attempts(bigint[],int[],text[],text[],int[],text[],int[],timestamptz[],timestamptz[],int) to current_user;

grant execute on function pigeon_ensure_partition(text, date) to current_user;
grant execute on function pigeon_drop_partition(text, text) to current_user;
grant execute on function pigeon_partition_drop_lock_id() to current_user;
