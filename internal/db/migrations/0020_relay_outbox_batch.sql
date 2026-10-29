-- 0020: the relay leases a BATCH of outbox rows, not one at a time.
--
-- The measured defect (card `t_b9850e1b`, found by S10's load test): the relay's
-- drain loop leased one outbox row per iteration and ran publish -> fan-out ->
-- mark serially per event, so the fan-out rate was bounded by one whole
-- round-trip chain per event. On a quiet 2 vCPU box with no ingest running, 1,000
-- accepted events at 10 subscriptions drained at 41.5 events/s (414.9
-- deliveries/s); on the loaded box S10 measured it at ~1.3-1.4 events/s. Either
-- way it is far below §8.1's 2,500 deliveries/s fan-out row at the 250 events/s
-- reference rate, and NFR-6 makes a queue that does not drain a defect rather
-- than a backlog. The same probe on the batch lease below gives 496.7 events/s
-- (4,966.8 deliveries/s) from the same rows.
--
-- FR-818 (rewritten by `SPEC.md` A-11) says the fan-out delivery rows, the
-- delivery completion/terminal updates and the outbox marks are batched into one
-- transaction per batch through the process's write budget (FR-815). A batch
-- needs more than one row, so the relay needs to claim more than one row: this
-- function is that claim, and `pigeon_lease_outbox` becomes a one-row call of it
-- so the S2 primitive FR-702 is stated once.

-- pigeon_lease_outbox_batch: claim up to p_limit pending rows with SKIP LOCKED
-- (FR-702) in ONE round trip, and return each row's event so the relay can build
-- the §10 envelope without a second, unsynchronized read. Leasing is the same
-- operation as the one-row form - extend lease_expires_at, which is how a crash
-- is detected (D-6, FR-809) - applied to a whole batch: a crashed relay lets
-- every row in its batch lapse together and another relay re-claims them.
--
-- p_limit is clamped, never trusted: a NULL limit would mean "no limit" and a
-- zero or negative one would claim nothing, so both are read as 1, and the
-- ceiling keeps one call from claiming the whole outbox into memory.
create or replace function pigeon_lease_outbox_batch(p_lease interval, p_limit int)
    returns table (
        outbox_id      bigint,
        event_id       text,
        producer_id    text,
        env            text,
        event_type     text,
        payload_version int,
        routing_mode   text,
        event_class    text,
        consumer_id    text,
        consumer_ref   text,
        occurred_at    timestamptz,
        payload        jsonb
    )
    language sql
    security definer
    set search_path = public, pg_temp
as $$
    with picked as (
        select o.id
        from outbox o
        where o.status = 'pending'
          and (o.lease_expires_at is null or o.lease_expires_at < now())
        order by o.id
        for update skip locked
        limit greatest(1, least(coalesce(p_limit, 1), 10000))
    ),
    leased as (
        update outbox o set lease_expires_at = now() + p_lease
        from picked
        where picked.id = o.id
        returning o.id, o.event_id, o.producer_id, o.env
    )
    select l.id, l.event_id, l.producer_id, l.env,
           e.event_type, e.payload_version, e.routing_mode, e.event_class,
           e.consumer_id, e.consumer_ref, e.occurred_at, e.payload
    from leased l
    join events e on e.id = l.event_id
    order by l.id;
$$;

-- pigeon_lease_outbox: the one-row claim (S2, FR-702). It is the batch function's
-- p_limit = 1 case and nothing else, so there is one implementation of the lease
-- and a caller that wants a single row keeps the signature it has always had.
create or replace function pigeon_lease_outbox(p_lease interval default interval '30 seconds')
    returns table (
        outbox_id      bigint,
        event_id       text,
        producer_id    text,
        env            text,
        event_type     text,
        payload_version int,
        routing_mode   text,
        event_class    text,
        consumer_id    text,
        consumer_ref   text,
        occurred_at    timestamptz,
        payload        jsonb
    )
    language sql
    security definer
    set search_path = public, pg_temp
as $$
    select * from pigeon_lease_outbox_batch(p_lease, 1);
$$;

grant execute on function pigeon_lease_outbox_batch(interval, int) to current_user;
grant execute on function pigeon_lease_outbox(interval) to current_user;
