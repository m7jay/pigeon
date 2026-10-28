-- 0012_attempt_comments.sql - the attempt-number and response-body comments, made
-- true again.
--
-- 0010 changed two things that 0008's comments still describe, and a comment that
-- contradicts the schema is worse than no comment: it is read as a specification.
-- Nothing here changes behaviour. The only reason this is a migration at all is
-- that PostgreSQL keeps column comments in the catalog, so they are data about the
-- database rather than text in a file: `\d+ attempts` is where an operator (and
-- the inspect API's authors) actually read them.
--
-- **Why 0008 is not edited in place.** db.Migrate records each applied migration's
-- sha256 in schema_migrations and refuses a file whose checksum changed ("was
-- applied with a different checksum: the file changed after it ran"), so a
-- corrected 0008 would stop every existing database from booting. The correction
-- therefore lands here, and this header is the pointer for anyone reading 0008:
--
--   * 0008's `comment on column attempts.attempt` and its inline comment on the
--     same column say the number is "assigned from max(attempts)+1 so a re-claimed
--     row never reuses a number". That is only half the rule. **SPEC.md A-7 (PR
--     #27)**, which records what 0010 already shipped, states the full one: "The
--     attempt counter is `deliveries.attempts`: control state, advanced by the
--     same write that schedules a retry, and an attempt's number is
--     `max(deliveries.attempts, the attempt rows already written) + 1`." The
--     reason is buffered evidence (D-1): the previous attempt's row may still be
--     sitting in Redis when the next claim numbers itself, so
--     max(attempts.attempt)+1 could hand out a number the buffer had already used
--     and the unique (delivery_id, attempt) key would then silently drop the later
--     row. The maximum is computed by pigeon_next_attempt and by
--     pigeon_claim_delivery, whose row_attempts payload the worker reads; A-7
--     also names `LeasedDelivery.NextAttemptNumber`.
--   * The same 0008 comment says the number is "never from the delivery's
--     `attempts` counter". **Superseded by 0010**: that counter is now half of the
--     maximum, because it is the control-state half - always current in both D-2
--     modes, buffered and sync - and pigeon_requeue_delivery advances it with
--     greatest(attempts, p_attempt) as it schedules a retry. The inline comment
--     at 0008:114-116 ("how many attempts have already failed ... the
--     authoritative next number is pigeon_next_attempt") describes the evidence
--     row count as prior_attempts; 0010 records that this count is stale in
--     buffered mode and is therefore no longer the authority.
--   * 0008's `comment on column attempts.response_body` says "the first 2 KB of the
--     response body, truncated". **Superseded by S6's SB-4 work**: 2 KB is a
--     default (PIGEON_DELIVERY_MAX_BODY_BYTES, store.DefaultAttemptBodyCap), not a
--     ceiling. SB-4 requires the cap configurable down to 0, where no body is
--     stored at all, and 0010's batched insert applies the configured cap on the
--     database side (a negative value stores the body as given).
--
-- The rest of 0008's comments - the table comment (FR-811/AC-28), response_status,
-- error_class - still describe the shipped behaviour and are left alone.

comment on column attempts.attempt is
    'SPEC.md A-7 (PR #27) / D-2 / FR-902: 1-based per delivery. The attempt counter is deliveries.attempts: control state, advanced by pigeon_requeue_delivery with greatest(attempts, p_attempt) when the retry is scheduled. An attempt''s number is max(deliveries.attempts, the attempt rows already written) + 1, computed by pigeon_next_attempt and returned by pigeon_claim_delivery as row_attempts (LeasedDelivery.NextAttemptNumber). The maximum is what makes buffered evidence safe: the previous attempt''s row may still be sitting in Redis when the next claim numbers itself, so max(attempts)+1 could reuse a number the buffer already holds. This supersedes 0008''s "assigned from max(attempts)+1" comment in 0010_attempt_buffer.sql.';

comment on column attempts.response_body is
    'FR-804/SB-4: at most PIGEON_DELIVERY_MAX_BODY_BYTES bytes of the response body, truncated at capture and again in pigeon_record_attempts. 2048 bytes (2 KB) is the default configuration cap, not a ceiling: SB-4 makes the cap configurable, and a value of 0 stores no body at all. This supersedes 0008''s "the first 2 KB of the response body, truncated", which read 2 KB as a fixed cap.';
