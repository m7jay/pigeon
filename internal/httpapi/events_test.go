package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// The publish and event-type functional tests. They run on the shared harness
// (real HTTP stack, real Postgres, app role), each against its own producer so a
// re-run's leftover rows never collide. The cross-producer suite in
// crossproducer_test.go proves scoping; these prove the accept contract
// (FR-303, FR-603, FR-604, FR-605, FR-606).

func ctx() context.Context { return context.Background() }

// createType registers a broadcast event type for a producer. The schema requires
// an integer "amount", so the schema-violation test has something to break.
func createType(t *testing.T, e *env, tn *tenant, name string) {
	t.Helper()
	code, raw := e.do(http.MethodPost, "/v1/event-types", tn.KeyPlain, map[string]any{
		"name":           name,
		"payload_schema": map[string]any{"type": "object", "required": []string{"amount"}, "properties": map[string]any{"amount": map[string]any{"type": "integer"}}},
	})
	mustStatus(t, code, raw, http.StatusCreated)
}

func publish(t *testing.T, e *env, key string, body any) (int, []byte) {
	t.Helper()
	return e.do(http.MethodPost, "/v1/events", key, body)
}

// TestPublishAcceptedWritesEventAndOutbox is FR-604: a 202 means the event row and
// the outbox row both committed, and the outbox row is pending until the relay
// marks it. This is the accept contract the whole slice rests on.
func TestPublishAcceptedWritesEventAndOutbox(t *testing.T) {
	e := setup(t)
	tn := e.seedTenant(t, "evt-ok")
	createType(t, e, tn, "payment.succeeded")

	code, raw := publish(t, e, tn.KeyPlain, map[string]any{
		"event_type": "payment.succeeded",
		"payload":    map[string]any{"amount": 49900},
	})
	mustStatus(t, code, raw, http.StatusAccepted)
	pub := decode[publishResponse](t, raw)
	if !strings.HasPrefix(pub.ID, "evt_") {
		t.Fatalf("accepted event id %q is not a ULID", pub.ID)
	}
	if pub.RoutingMode != "broadcast" || pub.PayloadVersion != 1 {
		t.Fatalf("accepted event %+v has wrong routing/version defaults", pub)
	}

	var status string
	err := e.admin.QueryRow(ctx(),
		`select o.status from events e join outbox o on o.event_id = e.id where e.id = $1`, pub.ID).Scan(&status)
	if err != nil {
		t.Fatalf("read outbox for accepted event: %v", err)
	}
	if status != "pending" {
		t.Fatalf("outbox status = %q, want pending after a fresh accept", status)
	}
}

// TestPublishPayloadTooLarge is FR-606: a body over 64 KB is a 413 with a
// payload_too_large code, not a 400 invalid_json and not a 500.
func TestPublishPayloadTooLarge(t *testing.T) {
	e := setup(t)
	tn := e.seedTenant(t, "evt-413")
	createType(t, e, tn, "evt.big")

	big := strings.Repeat("a", 70*1024)
	code, raw := publish(t, e, tn.KeyPlain, map[string]any{
		"event_type": "evt.big",
		"payload":    map[string]any{"blob": big},
	})
	mustStatus(t, code, raw, http.StatusRequestEntityTooLarge)
	if got := errorCode(t, raw); got != "payload_too_large" {
		t.Fatalf("oversized body code = %q, want payload_too_large", got)
	}

	// And nothing was written: a rejected body leaves no event row.
	var n int
	if err := e.admin.QueryRow(ctx(),
		`select count(*) from events e join event_types et on et.producer_id = e.producer_id and et.name = 'evt.big'`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Fatalf("%d events written for an oversized body, want 0", n)
	}
}

// TestPublishSchemaViolationReturnsPointer is FR-303: a payload that fails the
// event type's schema is a 422 whose error carries the failing JSON pointer, so
// the producer learns exactly which field was wrong.
func TestPublishSchemaViolationReturnsPointer(t *testing.T) {
	e := setup(t)
	tn := e.seedTenant(t, "evt-422")
	createType(t, e, tn, "order.created")

	code, raw := publish(t, e, tn.KeyPlain, map[string]any{
		"event_type": "order.created",
		"payload":    map[string]any{"amount": "not-an-integer"},
	})
	mustStatus(t, code, raw, http.StatusUnprocessableEntity)
	var body struct {
		Error struct {
			Code    string  `json:"code"`
			Message string  `json:"message"`
			Pointer string  `json:"pointer"`
			Keyword *string `json:"keyword"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decode 422: %v", err)
	}
	if body.Error.Code != "invalid" {
		t.Fatalf("schema violation code = %q, want invalid", body.Error.Code)
	}
	if body.Error.Pointer != "/amount" {
		t.Fatalf("schema violation pointer = %q, want /amount", body.Error.Pointer)
	}
	if body.Error.Keyword == nil || *body.Error.Keyword != "type" {
		t.Fatalf("schema violation keyword = %v, want type", body.Error.Keyword)
	}
}

// TestPublishUnknownEventType is FR-305: an unknown type is a 422, and the
// producer cannot use it to probe which types exist (422, not 404/403).
func TestPublishUnknownEventType(t *testing.T) {
	e := setup(t)
	tn := e.seedTenant(t, "evt-unknown")
	code, raw := publish(t, e, tn.KeyPlain, map[string]any{
		"event_type": "ghost.happened",
		"payload":    map[string]any{},
	})
	mustStatus(t, code, raw, http.StatusUnprocessableEntity)
	if got := errorCode(t, raw); got != "invalid" {
		t.Fatalf("unknown type code = %q, want invalid", got)
	}
}

// TestPublishIdempotentOnKey is FR-603: the same idempotency key within its window
// returns the original event, never a second one, and writes no second outbox row.
func TestPublishIdempotentOnKey(t *testing.T) {
	e := setup(t)
	tn := e.seedTenant(t, "evt-idem")
	createType(t, e, tn, "audit.log")

	code, raw := publish(t, e, tn.KeyPlain, map[string]any{
		"event_type": "audit.log", "payload": map[string]any{"amount": 1, "n": 1}, "idempotency_key": "idem-1",
	})
	mustStatus(t, code, raw, http.StatusAccepted)
	first := decode[publishResponse](t, raw).ID

	code, raw = publish(t, e, tn.KeyPlain, map[string]any{
		"event_type": "audit.log", "payload": map[string]any{"amount": 1, "n": 1}, "idempotency_key": "idem-1",
	})
	mustStatus(t, code, raw, http.StatusAccepted)
	second := decode[publishResponse](t, raw).ID

	if first != second {
		t.Fatalf("idempotent replay returned %q, want original %q", second, first)
	}
	var n int
	if err := e.admin.QueryRow(ctx(),
		`select count(*) from outbox o join events e on e.id = o.event_id where e.producer_id = $1`, tn.ID).Scan(&n); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if n != 1 {
		t.Fatalf("%d outbox rows after an idempotent replay, want 1", n)
	}
}

// TestPublishIdempotencyKeyHeader is FR-603's header form: the Idempotency-Key
// request header is honored the same as the body field.
func TestPublishIdempotencyKeyHeader(t *testing.T) {
	e := setup(t)
	tn := e.seedTenant(t, "evt-idem-h")
	createType(t, e, tn, "audit.hdr")

	do := func() string {
		body, _ := json.Marshal(map[string]any{"event_type": "audit.hdr", "payload": map[string]any{"amount": 1, "n": 7}})
		req, err := http.NewRequest(http.MethodPost, e.srv.URL+"/v1/events", strings.NewReader(string(body)))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+tn.KeyPlain)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "header-key")
		resp, err := e.srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("status = %d, want 202; body %s", resp.StatusCode, raw)
		}
		return decode[publishResponse](t, raw).ID
	}
	if a := do(); a == "" {
		t.Fatal("first publish returned no id")
	} else if b := do(); b != a {
		t.Fatalf("header-key replay returned %q, want %q", b, a)
	}
}

// TestPublishOccurredAtRoundTrip is FR-605: an explicit occurred_at is stored and
// returned unchanged.
func TestPublishOccurredAtRoundTrip(t *testing.T) {
	e := setup(t)
	tn := e.seedTenant(t, "evt-occ")
	createType(t, e, tn, "timing.event")

	when := time.Now().UTC().Truncate(time.Millisecond)
	code, raw := publish(t, e, tn.KeyPlain, map[string]any{
		"event_type":  "timing.event",
		"payload":     map[string]any{"amount": 1},
		"occurred_at": when.Format(time.RFC3339Nano),
	})
	mustStatus(t, code, raw, http.StatusAccepted)
	got := decode[publishResponse](t, raw).OccurredAt
	if !got.Equal(when) {
		t.Fatalf("occurred_at round-trip = %v, want %v", got, when)
	}
}

// TestEventTypeCreateAndList is FR-301/FR-302: a create returns the registered
// type with its defaults, and the list shows exactly the caller's environment.
func TestEventTypeCreateAndList(t *testing.T) {
	e := setup(t)
	tn := e.seedTenant(t, "evt-list")

	code, raw := e.do(http.MethodPost, "/v1/event-types", tn.KeyPlain, map[string]any{
		"name":           "invoice.created",
		"payload_schema": map[string]any{"type": "object"},
	})
	mustStatus(t, code, raw, http.StatusCreated)
	created := decode[eventTypeJSON](t, raw)
	if created.Name != "invoice.created" || created.RoutingMode != "broadcast" ||
		created.EventClass != "normal" || created.PayloadVersion != 1 || !created.Enabled {
		t.Fatalf("created type %+v has wrong defaults", created)
	}

	code, raw = e.do(http.MethodGet, "/v1/event-types", tn.KeyPlain, nil)
	mustStatus(t, code, raw, http.StatusOK)
	var list []eventTypeJSON
	if err := json.Unmarshal(raw, &list); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(list) != 4 || list[0].Name != "invoice.created" {
		t.Fatalf("event type list = %+v, want four (three seeded examples + invoice.created)", list)
	}
}

// TestEventTypeNameRejected is FR-302: a name that breaks the regex is a 422 with
// a reason, before any transaction.
func TestEventTypeNameRejected(t *testing.T) {
	e := setup(t)
	tn := e.seedTenant(t, "evt-name")
	code, raw := e.do(http.MethodPost, "/v1/event-types", tn.KeyPlain, map[string]any{
		"name":           "Bad-Name",
		"payload_schema": map[string]any{"type": "object"},
	})
	mustStatus(t, code, raw, http.StatusUnprocessableEntity)
	if got := errorCode(t, raw); got != "invalid" {
		t.Fatalf("bad name code = %q, want invalid", got)
	}
}

// TestPatchEventTypeLifecycle is FR-304/FR-305: PATCH disables a type (and a
// disabled type is then refused at publish, not accepted), bumps its version, a
// field left out of a patch is preserved, and a PATCH to a name the caller's
// environment does not own is a 404, never a 403 (FR-108 + FR-103).
func TestPatchEventTypeLifecycle(t *testing.T) {
	e := setup(t)
	tn := e.seedTenant(t, "evt-patch")
	createType(t, e, tn, "billing.invoice")

	// Disable it. The response is the updated type with enabled flipped.
	code, raw := e.do(http.MethodPatch, "/v1/event-types/billing.invoice", tn.KeyPlain, map[string]any{"enabled": false})
	mustStatus(t, code, raw, http.StatusOK)
	if d := decode[eventTypeJSON](t, raw); d.Enabled {
		t.Fatalf("patched type %+v still enabled after {enabled:false}", d)
	}

	// A disabled type is refused at publish - the accept never happens.
	code, raw = publish(t, e, tn.KeyPlain, map[string]any{"event_type": "billing.invoice", "payload": map[string]any{"amount": 1}})
	mustStatus(t, code, raw, http.StatusUnprocessableEntity)
	if got := errorCode(t, raw); got != "invalid" {
		t.Fatalf("publish to a disabled type code = %q, want invalid", got)
	}

	// Re-enable and bump the version in one patch.
	code, raw = e.do(http.MethodPatch, "/v1/event-types/billing.invoice", tn.KeyPlain, map[string]any{"enabled": true, "payload_version": 2})
	mustStatus(t, code, raw, http.StatusOK)
	if p := decode[eventTypeJSON](t, raw); !p.Enabled || p.PayloadVersion != 2 {
		t.Fatalf("patched type %+v, want enabled v2", p)
	}

	// A field left out of the patch is preserved (partial patch).
	code, raw = e.do(http.MethodPatch, "/v1/event-types/billing.invoice", tn.KeyPlain, map[string]any{"payload_version": 3})
	mustStatus(t, code, raw, http.StatusOK)
	if o := decode[eventTypeJSON](t, raw); !o.Enabled || o.PayloadVersion != 3 {
		t.Fatalf("partial patch %+v, want enabled v3 (enabled preserved)", o)
	}

	// A name the caller owns no environment's copy of is a 404, not a 403.
	code, raw = e.do(http.MethodPatch, "/v1/event-types/ghost.happened", tn.KeyPlain, map[string]any{"enabled": false})
	mustStatus(t, code, raw, http.StatusNotFound)
}
