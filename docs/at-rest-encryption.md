# Encrypting a column at rest

`internal/atrest` is the tree's at-rest encryption primitive. It exists because
FR-1006 requires endpoint custom header values to be encrypted at rest and
redactable everywhere they are read, and there was nothing in the tree to reuse:
endpoint signing secrets are stored as plaintext `whsec_...` values because the
signer needs the key material at sign time (FR-1001), and a hash is useless for a
value that has to be sent back out on the wire.

This page is the pattern for the next column that needs it, and the rules that
keep the first one honest. The worked example throughout is FR-405's custom
endpoint headers, which is the primitive's only caller today. Two limits are
stated below rather than left to be discovered: there is no key rotation, and
there is no per-producer key derivation (§15.1's MT-7, deferred to v1.1 and
recorded in `REQUIREMENTS.md` §17.1).

## The primitive

AES-256-GCM under one deployment key. The ciphertext is
`base64(nonce || ciphertext || tag)` in a single column - no separate nonce
column, nothing else to keep in step with the value.

```go
key, err := atrest.ParseKey(cfg.HeadersKey)       // validates at boot; never per request
ct, err := key.Seal([]byte("sk-live-..."), aad)   // aad names the row and the field
pt, err := key.Open(ct, aad)                      // same aad, or ErrNotAuthentic
```

Four properties matter, and each is a test in `internal/atrest`:

- **A fresh nonce per seal.** Sealing the same value twice produces different
  ciphertext, so equal values are not visible as equal at rest.
- **The AAD names the row and the field.** For custom headers it is
  `pigeon:endpoint-custom-headers:<endpoint id>:<header name>`, so a ciphertext
  copied into another endpoint's row, or from one header name onto another inside
  the same row, fails to open instead of being delivered from the wrong place or
  under a name it was never sealed for. The field is the name the row stores, so
  the write and the read cannot drift on a trim or a re-render.
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

**There is no per-producer key derivation (MT-7).** §15.1's MT-7 asks for secrets
"encrypted with per-producer key derivation, so one producer's ciphertext is not
decryptable under another's context". This primitive seals under one deployment
key, and what that gives is weaker and has to be said plainly: a ciphertext cannot
be *moved* between producers, because the AAD is part of what authenticates, but
anyone holding `PIGEON_HEADERS_KEY` - every process of the deployment - opens
every producer's value. v1.0 ships the one key with that limitation stated rather
than implied; the derivation is deferred to v1.1 and `REQUIREMENTS.md` §17.1
states the cost of the deferral.

What a v1.1 slice has to be, and why the deferral is not a one-line derivation: a
subkey per producer computed from this same key, `HKDF(master, producer_id)`,
changes the stored format without drawing the boundary, because a holder of the
deployment key can derive every producer's subkey. The boundary needs per-producer
key *material* - a per-producer key wrapped by the deployment key - and therefore
a migration that re-seals every stored value. That is the shape the deferral
defers.

## Rules for a new encrypted column

1. **One key per deployment, one ciphertext column per value.** No per-row keys,
   no key material in the row, no nonce column. That is the v1.0 shape, not the
   end of §15.1 MT-7's per-producer derivation: the one key opens every producer's
   values, and both "The key" above and `REQUIREMENTS.md` §17.1 record that rather
   than leaving the next column to assume otherwise.
2. **The AAD must identify the table, the row and the field.** Include the table's
   identity and, when one row holds several encrypted values, the field name, so a
   value cannot be moved between them. One `endpoints` row holds up to five values
   (FR-405's bound), which is why the name is in this one:
   `pigeon:endpoint-custom-headers:<endpoint id>:<header name>`, built by
   `headerAAD` from the name the row stores. A row-level AAD would let a ciphertext
   be copied from one header onto another *inside* one endpoint's row and still
   authenticate, and the delivery would go out under a name the value was never
   sealed for.
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
   sealed for another row or another name, or under a key this process does not
   have, is skipped: the delivery still goes out and the worker logs the header
   **name**. One bad entry must not wedge a delivery, and a receiver must never
   get a value nobody can vouch for.
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
- A ciphertext moved out of the row it was sealed in, or onto another header name
  in the same row, does not authenticate either: the delivery goes out without
  that header and the same warning names it. Check with
  `git grep -n headerAAD internal/store` that the write and the read still build
  the same string, because the AAD is the caller's to get wrong.
- The key is not in any API response, any log line, or any attempt row. `rg` the
  deployment's logs for a stored value if in doubt: the only place it should ever
  appear is the request that set it.
