-- 0009_consumer_portal.sql — S9 (M2b): the two columns UI-13 and UI-14 need.
--
-- Both settings are consumer-owned: UI-13's notification email hangs off the
-- consumer's own endpoint and UI-14's gate hangs off the producer's event type.
-- Neither introduces a new table, so the tenant scope on both is the RLS policy
-- that already exists (endpoints_tenant, event_types_tenant).

-- ---------------------------------------------------------------------------
-- UI-13: the consumer user's own notification email
-- ---------------------------------------------------------------------------
-- Where this endpoint's operator wants to hear about deliveries (§11's failure
-- notification). Null means "not set" - the consumer sets it, edits it or clears
-- it through PATCH /v1/consumer/endpoints/{id}, and the address shape is validated
-- at that edge exactly as an invitation address is.
alter table endpoints add column notification_email text;

comment on column endpoints.notification_email is
    'UI-13: the consumer user''s own notification address for this endpoint. Null until they set one.';

-- ---------------------------------------------------------------------------
-- UI-14: the producer's publicly-subscribable gate
-- ---------------------------------------------------------------------------
-- Default TRUE, deliberately. AC-38 requires a consumer user to add an endpoint
-- and subscribe to an event type from the consumer portal with no producer member
-- acting; the types they subscribe to are the seeded examples (FR-306), which the
-- producer already created. A default of false would make AC-38 depend on the
-- producer flipping a flag per type after the fact, which is the coupling UI-14
-- describes rather than creates. The producer opts a type OUT with
-- PATCH /v1/event-types/{name}, and the gate is enforced on the subscription
-- write itself (store.CreateConsumerSubscription), never merely in the portal.
alter table event_types add column publicly_subscribable boolean not null default true;

comment on column event_types.publicly_subscribable is
    'UI-14: whether a consumer user may subscribe to this type from the consumer portal. Enforced server-side on POST /v1/consumer/endpoints/{endpointID}/subscriptions.';
