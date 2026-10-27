-- 0010_partitioning.sql — SB-1/SB-2: deliveries and attempts become daily range
-- partitions so retention can drop whole days instead of DELETE.
--
-- Why this is a swap and not an ALTER: PostgreSQL cannot convert a plain table
-- into a partitioned one in place. The migration therefore freezes both tables,
-- moves the originals aside, creates partitioned replacements with the same
-- columns, copies the rows into per-day partitions and drops the originals — all
-- in the one transaction db.Migrate gives each migration file. A failure leaves
-- the plain tables untouched.
--
-- The dedupe keys change shape because a partitioned table's unique key MUST
-- contain the partition key. That is a real change and SPEC.md records it:
--
--   deliveries  (event_id, endpoint_id)  -> (event_id, endpoint_id, created_at)
--   attempts    (delivery_id, attempt)   -> per-partition unique (delivery_id, attempt)
--
-- FR-704's re-fan-out idempotency is preserved by making the day deterministic:
-- fan-out stamps deliveries.created_at from the EVENT's created_at instead of
-- now(), so a retried fan-out lands in the same partition and conflicts on the
-- same key. Replay (S7) is unaffected because it creates rows on its own day
-- with replay_of_delivery_id set. Both notes are in SPEC.md's S8 section.
--
-- attempts loses its FK to deliveries: an incoming foreign key to a partitioned
-- table must include the partition key on both sides. The lease serialises
-- per-delivery writes and pigeon_next_attempt derives the number from the rows,
-- so the FK was a backstop, not the uniqueness mechanism.

begin;

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

-- Move the originals aside. Index names are schema-wide (constraint names are
-- not), so every index-backed name the replacements need is freed here.
alter table deliveries rename to deliveries_legacy;
alter table attempts rename to attempts_legacy;
alter table deliveries_legacy rename constraint deliveries_pkey to deliveries_legacy_pkey;
alter table deliveries_legacy drop constraint deliveries_event_endpoint_key;
drop index deliveries_pending_idx;
alter table attempts_legacy rename constraint attempts_pkey to attempts_legacy_pkey;

-- ---------------------------------------------------------------------------
-- The partitioned replacements. Columns, checks and defaults are copied from
-- 0007/0008; the structural changes are the composite primary key (id must be
-- accompanied by the partition key) and deliveries' day-bearing unique key.
-- ---------------------------------------------------------------------------
create table deliveries (
    id                  bigint      not null default nextval('deliveries_id_seq'),
    event_id            text        not null references events (id) on delete cascade,
    endpoint_id         text        not null,
    producer_id         text        not null,
    env                 text        not null,
    consumer_id         text        not null,
    status              text        not null default 'pending'
                        check (status in ('pending', 'skipped', 'delivered', 'failed')),
    non_delivery_reason text
                        check (non_delivery_reason is null or non_delivery_reason in (
                            'unroutable:consumer_not_registered',
                            'not_delivered:no_matching_subscription',
                            'not_delivered:endpoint_unverified',
                            'not_delivered:endpoint_paused')),
    attempts            int         not null default 0,
    first_attempt_at    timestamptz,
    delivered_at        timestamptz,
    lease_expires_at    timestamptz,
    created_at          timestamptz not null default now(),
    -- SB-1: the partition key is the day. The unique key includes it so it is
    -- legal on a partitioned table; fan-out supplies the event's created_at so a
    -- retried fan-out lands in the same partition and still conflicts (FR-704).
    constraint deliveries_pkey primary key (id, created_at),
    constraint deliveries_event_endpoint_key unique (event_id, endpoint_id, created_at)
) partition by range (created_at);

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
    attempt         int         not null check (attempt >= 1),
    outcome         text        not null check (outcome in ('retryable', 'terminal')),
    response_status int         check (response_status is null or response_status between 100 and 599),
    error_class     text        not null check (error_class in (
        'timeout', 'dial_error', 'dns_error', 'http_retryable', 'http_terminal')),
    response_body   text,
    duration_ms     int         not null check (duration_ms >= 0),
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
create or replace function pigeon_ensure_partition(p_table text, p_day date)
    returns text
    language plpgsql
    security definer
    set search_path = public, pg_temp
as $$
declare
    v_name text;
    v_from timestamptz;
    v_to   timestamptz;
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

    if to_regclass(v_name) is not null then
        return v_name;
    end if;

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
                       lease_expires_at, created_at)
select id, event_id, endpoint_id, producer_id, env, consumer_id, status,
       non_delivery_reason, attempts, first_attempt_at, delivered_at,
       lease_expires_at, created_at
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
-- pigeon_record_attempt: the partitioned parent cannot infer an ON CONFLICT
-- target for a per-partition unique index, so the targetless form is used. It
-- keeps the same within-day idempotency and works on the parent.
--
-- It also creates the day's partition itself (SB-1). An attempt is stamped
-- occurred_at = now(), so it lands in today's partition: a first attempt on a
-- replica that started after midnight must not fail because the sweeper has not
-- ticked yet. The call is a no-op when the partition exists, and fan-out and the
-- sweeper create days ahead, so this is a fallback rather than the common path.
-- ---------------------------------------------------------------------------
create or replace function pigeon_record_attempt(
        p_delivery_id bigint,
        p_attempt     int,
        p_outcome     text,
        p_error_class text,
        p_status      int,
        p_body        text,
        p_duration_ms int,
        p_next_retry_at timestamptz
    )
    returns void
    language plpgsql
    security definer
    set search_path = public, pg_temp
as $$
begin
    perform pigeon_ensure_partition('attempts', (now() at time zone 'UTC')::date);

    insert into attempts
        (delivery_id, attempt, outcome, error_class, response_status,
         response_body, duration_ms, next_retry_at)
    values
        (p_delivery_id, p_attempt, p_outcome, p_error_class, p_status,
         left(coalesce(p_body, ''), 2048), p_duration_ms, p_next_retry_at)
    on conflict do nothing;
end;
$$;

grant execute on function pigeon_ensure_partition(text, date) to current_user;
grant execute on function pigeon_drop_partition(text, text) to current_user;
grant execute on function pigeon_partition_drop_lock_id() to current_user;

commit;
