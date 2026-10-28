-- 0017_attempt_comments.sql - the attempt-number and response-body comments, made
-- true again, and the response-status comment 0012's rebuild dropped, restored.
--
-- 0010 changed two things that 0008's comments still describe, and 0012 re-applied
-- one of those sentences verbatim when it re-created `attempts` as a partitioned
-- table - carrying the table comment and attempts.attempt over and dropping the
-- rest. A comment that contradicts the schema is worse than no comment: it is read
-- as a specification. Nothing here changes behaviour. The only reason this is a
-- migration at all is that PostgreSQL keeps column comments in the catalog, so they
-- are data about the database rather than text in a file: `\d+ attempts` is where an
-- operator (and the inspect API's authors) actually read them.
--
-- **Why 0008 and 0012 are not edited in place.** db.Migrate records each applied
-- migration's sha256 in schema_migrations and refuses a file whose checksum changed
-- ("was applied with a different checksum: the file changed after it ran"), so a
-- corrected 0008 would stop every existing database from booting. The correction
-- therefore lands here, and this header is the pointer for anyone reading either of
-- them:
--
--   * 0008's `comment on column attempts.attempt` says the number is "assigned from
--     max(attempts)+1 so a re-claimed row never reuses a number". 0012 renamed that
--     table to `attempts_legacy` and built the partitioned `attempts` in its place,
--     carrying the sentence over, so it is the value `\d+ attempts` prints today and
--     it is only half the rule. **SPEC.md A-7**, which records what 0010 already
--     shipped, states the full one: "The attempt counter is `deliveries.attempts`:
--     control state, advanced by the same write that schedules a retry, and an
--     attempt's number is `max(deliveries.attempts, the attempt rows already
--     written) + 1`."
--     The reason is buffered evidence (D-1): the previous attempt's row may still be
--     sitting in Redis when the next claim numbers itself, so max(attempts.attempt)+1
--     could hand out a number the buffer had already used, and the unique
--     (delivery_id, attempt) key would then silently drop the later row. The maximum
--     is computed by pigeon_next_attempt and by pigeon_claim_delivery, whose
--     row_attempts payload the worker reads; A-7 also names
--     `LeasedDelivery.NextAttemptNumber`.
--   * The same 0008 comment says the number is "never from the delivery's
--     `attempts` counter". **Superseded by 0010**: that counter is now half of the
--     maximum, because it is the control-state half - always current in both D-2
--     modes, buffered and sync - and pigeon_requeue_delivery advances it with
--     greatest(attempts, p_attempt) as it schedules a retry. The inline comment at
--     0008:113-114 ("how many attempts have already failed ... the authoritative
--     next number is pigeon_next_attempt") describes the evidence row count as
--     prior_attempts; 0010 records that this count is stale in buffered mode and is
--     therefore no longer the authority.
--   * 0008's `comment on column attempts.response_body` said "the first 2 KB of the
--     response body, truncated". **Superseded by S6's SB-4 work**: 2 KB is a default
--     (PIGEON_DELIVERY_MAX_BODY_BYTES, store.DefaultAttemptBodyCap), not a ceiling.
--     SB-4 requires the cap configurable down to 0, where no body is stored at all,
--     and 0010's batched insert applies the configured cap on the database side.
--     0012's swap did not re-apply that comment to the re-created table the way it
--     did the table comment and attempts.attempt, so the catalog carries no comment
--     for this column until this statement runs - which is why the wording below is
--     written to stand on its own.
--
--   * 0008's `comment on column attempts.response_status` ("FR-804: the HTTP status
--     code, or null for a timeout / connection / DNS error") was dropped by the same
--     swap, which re-applied only the table comment, attempts.attempt and
--     attempts.occurred_at (SB-1). Its text still describes the shipped behaviour -
--     a transport error has no HTTP response to record, so the column is null for a
--     timeout, a dial or a DNS failure (internal/delivery's transport-error path,
--     store.Attempt.ResponseStatus) - so this migration re-applies it verbatim
--     rather than correcting it.
--
-- 0012's rebuild left three comments in the catalog: the table comment
-- (FR-811/AC-28, re-applied verbatim), attempts.attempt (the stale sentence this
-- migration rewrites) and attempts.occurred_at (SB-1). 0008's error_class never had
-- a catalog comment at all - it is an inline `--` in its DDL (0008:56-57) - so
-- there is nothing to correct or restore for that column.

comment on column attempts.attempt is
    'SPEC.md A-7 / D-2 / FR-902: 1-based per delivery. The attempt counter is deliveries.attempts: control state, advanced by pigeon_requeue_delivery with greatest(attempts, p_attempt) when the retry is scheduled. An attempt''s number is max(deliveries.attempts, the attempt rows already written) + 1, computed by pigeon_next_attempt and returned by pigeon_claim_delivery as row_attempts (LeasedDelivery.NextAttemptNumber). The maximum is what makes buffered evidence safe: the previous attempt''s row may still be sitting in Redis when the next claim numbers itself, so max(attempts)+1 could reuse a number the buffer already holds. This supersedes 0008''s "assigned from max(attempts)+1" wording, which 0012 re-applied to the table it rebuilt.';

comment on column attempts.response_body is
    'FR-804/SB-4: at most PIGEON_DELIVERY_MAX_BODY_BYTES bytes of the response body, truncated at capture and again in pigeon_record_attempts. 2048 bytes (2 KB) is the default configuration cap, not a ceiling: SB-4 makes the cap configurable, and a value of 0 stores no body at all. This supersedes 0008''s "the first 2 KB of the response body, truncated", which read 2 KB as a fixed cap.';

comment on column attempts.response_status is
    'FR-804: the HTTP status code, or null for a timeout / connection / DNS error.';
