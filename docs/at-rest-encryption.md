# Encrypting a column at rest

`internal/atrest` is the tree's at-rest encryption primitive. It exists because
FR-1006 requires endpoint custom header values to be encrypted at rest and
redactable everywhere they are read, and there was nothing in the tree to reuse:
endpoint signing secrets are stored as plaintext `whsec_...` values because the
signer needs the key material at sign time (FR-1001), and a hash is useless for a
value that has to be sent back out on the wire.

This page is the pattern for the next column that needs it, and the rules that
keep the first one honest. The worked example throughout is FR-405's custom
endpoint headers, which is the primitive's only caller today.

## The primitive

AES-256-GCM under one deployment key. The ciphertext is
`base64(nonce || ciphertext || tag)` in a single column - no separate nonce
column, nothing else to keep in step with the value.

```go
key, err := atrest.ParseKey(cfg.HeadersKey)  // validates at boot; never per request
ct, err := key.Seal([]byte("sk-live-..."), aad)   // aad identifies the row
pt, err := key.Open(ct, aad)                      // same aad, or ErrNotAuthentic
```

Four properties matter, and each is a test in `internal/atrest`:

- **A fresh nonce per seal.** Sealing the same value twice produces different
  ciphertext, so equal values are not visible as equal at rest.
- **The AAD names the row.** For custom headers it is
  `pigeon:endpoint-custom-headers:<endpoint id>`, so a ciphertext copied into
  another endpoint's row (or another column, or another deployment) fails to open
  instead of being delivered from the wrong place.
- **One error, `ErrNotAuthentic`,** covers a wrong key, a tampered ciphertext and
  a mismatched AAD. The caller cannot tell them apart, and does not need to.
- **No error ever carries the plaintext or the ciphertext** (FR-1008). A refusal
  names the field, never its content - the messages travel into client logs and
  the API's own log, which is exactly where a secret must not land.

A zero `Key` is inert rather than a panic: `Configured()` is false, `Seal` and
`Open` return `atrest.ErrNoKey`, and the caller decides what that means.

## The key

`PIGEON_HEADERS_KEY`, 32 bytes of base64 (`openssl rand -base64 32`). It is
**required**: `config.Validate` refuses to boot without it, or with a key of the
wrong length, using the same parse the runtime uses, so a deployment cannot start
in a state where it accepts a header value and has nowhere safe to put it.
`compose.yaml` refuses to start without one too, and `.env.example` carries a
placeholder that is deliberately not a valid key.

**There is no rotation support.** Rotating the key would mean re-sealing every
stored value in a migration; losing it means the values already stored cannot be
read (they can be deleted and re-registered, not recovered). A deployment that
loses the key keeps working: the affected headers are dropped from deliveries
with a log line naming them, rather than failing the delivery.

## Rules for a new encrypted column

1. **One key per deployment, one ciphertext column per value.** No per-row keys,
   no key material in the row, no nonce column.
2. **The AAD must identify the row and the field.** Include the table's identity
   and, when one row holds several encrypted values, the field name, so a value
   cannot be moved between them.
3. **Write the plaintext in exactly one place and read it in exactly one place.**
   For custom headers: the seal in `CreateEndpoint`/`CreateConsumerEndpoint`, and
   the decrypt in the delivery claim (`openSealedHeaders`). If a third place
   appears, the design is wrong - that is the whole of FR-1006.
4. **Reads return a redacted view, not the column.** `CustomHeaderView` has a
   `Value` field that is always `MaskedHeaderValue`; there is no code path that
   produces a plaintext read, so the API and the portal cannot leak one by
   forgetting to redact.
5. **Refuse rather than degrade.** A `Store` with no key returns `ErrNoKey` for a
   write that would need encryption instead of storing plaintext. An endpoint with
   no custom headers is still registrable, because there is nothing to encrypt.
6. **What will not authenticate is dropped, not sent, and not fatal.** A value
   sealed for another row, or under a key this process does not have, is skipped:
   the delivery still goes out and the worker logs the header **name**. One bad
   entry must not wedge a delivery, and a receiver must never get a value nobody
   can vouch for.
7. **Never log a value.** Not the plaintext, not the ciphertext. The refusal
   messages name the field; the drop path names the header; the attempt trail and
   the delivery rows carry neither.

## The custom headers application

`endpoints.custom_headers` is `jsonb not null default '[]'`: an array of
`{"name": "...", "value_ct": "<base64 ciphertext>"}`. Names are in the clear -
they are what a redacted read returns, what the operator needs to see, and they
are not secret. The table constrains the shape (array, at most 5 entries) so a
hand-written row cannot bypass the bound.

FR-405's rejection list is applied on both registration paths before the insert,
so `Host`, `Content-Length`, `Content-Type` and `Authorization` are a 422 that
names the header, never a silently dropped header. The platform's own four
(`Webhook-Id`, `Webhook-Timestamp`, `Webhook-Signature`, `User-Agent`) are refused
by the same rule, because a custom header that overwrote a signature header would
break the consumer's verifier with no error message to explain it. Names are
matched case-insensitively (RFC 9110), and duplicates are refused.

Where a plaintext exists, in full:

| Place | Why |
|---|---|
| the registration request | the caller is setting the value |
| the seal in `internal/store` | the one write |
| the outbound request in the delivery worker | the one read, for the request it is about to send |

Everything else - the API's reads, the portal, the logs, the attempt trail - sees
the mask.

## Operational checks

- After a registration, the column holds ciphertext:
  `select custom_headers from endpoints where id = '<id>';` shows `value_ct`
  entries and no plaintext.
- A wrong or missing key shows up as a warning on the delivery path -
  `delivery: custom headers dropped, they did not authenticate (FR-1006)` - naming
  the header and the endpoint. The delivery still succeeds.
- The key is not in any API response, any log line, or any attempt row. `rg` the
  deployment's logs for a stored value if in doubt: the only place it should ever
  appear is the request that set it.
