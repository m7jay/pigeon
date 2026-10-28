package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/m7jay/pigeon/internal/atrest"
)

// --- FR-405: the rejection list, applied before anything is written ----------

// TestCustomHeaderValidationIsFR405 pins the write-path rules: the bound, the
// four names FR-405 names, the platform's own wire headers, and the shapes that
// are not header names or header values at all. Every refusal wraps ErrInvalid,
// which is what the API turns into a 422 with the reason.
func TestCustomHeaderValidationIsFR405(t *testing.T) {
	ok := func(name string, headers ...CustomHeader) {
		t.Helper()
		if err := validateCustomHeaders(headers); err != nil {
			t.Errorf("%s: validateCustomHeaders = %v, want nil", name, err)
		}
	}
	refuse := func(name, wantInMessage string, headers ...CustomHeader) {
		t.Helper()
		err := validateCustomHeaders(headers)
		if err == nil {
			t.Fatalf("%s: validateCustomHeaders = nil, want a refusal", name)
		}
		if !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: error %v is not ErrInvalid (the API would answer 500, not 422)", name, err)
		}
		if !strings.Contains(err.Error(), wantInMessage) {
			t.Errorf("%s: refusal %q does not mention %q", name, err, wantInMessage)
		}
	}

	ok("no headers")
	ok("one header", CustomHeader{Name: "X-Tenant", Value: "acme"})
	ok("the whole bound of five",
		CustomHeader{Name: "X-1", Value: "a"},
		CustomHeader{Name: "X-2", Value: "b"},
		CustomHeader{Name: "X-3", Value: "c"},
		CustomHeader{Name: "X-4", Value: "d"},
		CustomHeader{Name: "X-5", Value: "e"},
	)
	// An empty value is a legal header value: the name is what is configured.
	ok("an empty value", CustomHeader{Name: "X-Empty", Value: ""})
	ok("a tab in the value", CustomHeader{Name: "X-Tab", Value: "a\tb"})

	refuse("six headers", "at most 5",
		CustomHeader{Name: "X-1"}, CustomHeader{Name: "X-2"}, CustomHeader{Name: "X-3"},
		CustomHeader{Name: "X-4"}, CustomHeader{Name: "X-5"}, CustomHeader{Name: "X-6"},
	)

	// FR-405's four, exactly as written.
	for _, name := range []string{"Host", "Content-Length", "Content-Type", "Authorization"} {
		refuse("a custom "+name, name, CustomHeader{Name: name, Value: "x"})
	}
	// ... and case-insensitively: a header name is not case-sensitive, so
	// `content-TYPE` must not slip past as an unknown name (the review finding
	// this assertion exists for).
	for _, name := range []string{"HOST", "content-length", "CONTENT-TYPE", "authorization", "Content-type"} {
		refuse("a custom "+name+" in another case", name, CustomHeader{Name: name, Value: "x"})
	}
	// The platform's own headers: a custom one would overwrite the signature or
	// the User-Agent the consumer sees (FR-1001, FR-810).
	for _, name := range []string{"Webhook-Id", "Webhook-Timestamp", "webhook-signature", "User-Agent"} {
		refuse("a custom "+name, name, CustomHeader{Name: name, Value: "x"})
	}

	refuse("an empty name", "name is required", CustomHeader{Name: "   ", Value: "x"})
	refuse("a name with a space", "not a valid HTTP header name", CustomHeader{Name: "X Space", Value: "x"})
	refuse("a name with a colon", "not a valid HTTP header name", CustomHeader{Name: "X:Y", Value: "x"})
	refuse("a non-ASCII name", "not a valid HTTP header name", CustomHeader{Name: "X-héader", Value: "x"})
	refuse("an over-long name", "over the 128-byte limit",
		CustomHeader{Name: "X-" + strings.Repeat("a", maxHeaderNameLen), Value: "x"})
	refuse("the same name twice", "more than once",
		CustomHeader{Name: "X-Dup", Value: "a"}, CustomHeader{Name: "x-dup", Value: "b"})

	// A CR or an LF in a value is header injection, and a NUL is refused with it.
	refuse("a CRLF in a value", "control byte 0x0d", CustomHeader{Name: "X-Inject", Value: "a\r\nX-Evil: 1"})
	refuse("a bare LF in a value", "control byte 0x0a", CustomHeader{Name: "X-Inject", Value: "a\nb"})
	refuse("a NUL in a value", "control byte 0x00", CustomHeader{Name: "X-Nul", Value: "a\x00b"})
	refuse("an over-long value", "over the 4096-byte limit",
		CustomHeader{Name: "X-Long", Value: strings.Repeat("v", maxHeaderValueLen+1)})
}

// TestCustomHeaderRefusalNeverQuotesTheValue (FR-1006): the refusal travels to
// the caller and into the API's error log, so it must not carry the value it
// rejected. The name is not a secret and is quoted on purpose.
func TestCustomHeaderRefusalNeverQuotesTheValue(t *testing.T) {
	const canary = "sk-live-must-not-appear-in-an-error"
	for _, h := range []CustomHeader{
		{Name: "Host", Value: canary},
		{Name: "X-Key", Value: canary + "\r\n"},
		{Name: "X-Key", Value: strings.Repeat(canary, 200)},
		{Name: "", Value: canary},
	} {
		err := validateCustomHeaders([]CustomHeader{h})
		if err == nil {
			t.Fatalf("validateCustomHeaders(%q) = nil, want a refusal", h.Name)
		}
		if strings.Contains(err.Error(), canary) {
			t.Fatalf("the refusal quotes the value it rejected: %v", err)
		}
	}
}

// --- FR-1006: sealing, masking, and the AAD ---------------------------------

func headersTestKey(t *testing.T) atrest.Key {
	t.Helper()
	k, _, err := atrest.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	return k
}

// TestSealThenSummariseNeverShowsTheValue: the column can be read back on every
// read path and never yields a plaintext (FR-1006).
func TestSealThenSummariseNeverShowsTheValue(t *testing.T) {
	const canary = "sk-live-canary-value"
	st := &Store{headersKey: headersTestKey(t)}
	sealed, err := st.sealHeaders("end_1", []CustomHeader{
		{Name: "X-Api-Key", Value: canary},
		{Name: "X-Empty", Value: ""},
	})
	if err != nil {
		t.Fatalf("sealHeaders: %v", err)
	}
	if strings.Contains(string(sealed), canary) {
		t.Fatalf("the sealed column contains the plaintext: %s", sealed)
	}
	views, err := summariseHeaders(sealed)
	if err != nil {
		t.Fatalf("summariseHeaders: %v", err)
	}
	if len(views) != 2 {
		t.Fatalf("summariseHeaders returned %d headers, want 2", len(views))
	}
	if views[0].Name != "X-Api-Key" || views[0].Value != MaskedHeaderValue {
		t.Fatalf("read back %+v, want the name in the clear and the value masked", views[0])
	}
	for _, v := range views {
		if strings.Contains(v.Value, canary) {
			t.Fatalf("a read leaked the plaintext: %+v", v)
		}
	}
}

// TestOpenSealedHeadersIsBoundToTheEndpoint is the AAD: a ciphertext sealed for
// one endpoint does not open for another, and the entry is reported as dropped
// rather than sent (FR-1006).
func TestOpenSealedHeadersIsBoundToTheEndpoint(t *testing.T) {
	st := &Store{headersKey: headersTestKey(t)}
	sealed, err := st.sealHeaders("end_1", []CustomHeader{{Name: "X-Api-Key", Value: "sk-live-canary"}})
	if err != nil {
		t.Fatalf("sealHeaders: %v", err)
	}

	headers, dropped, err := st.openSealedHeaders("end_1", sealed)
	if err != nil {
		t.Fatalf("openSealedHeaders(the same endpoint): %v", err)
	}
	if len(headers) != 1 || headers[0].Value != "sk-live-canary" || len(dropped) != 0 {
		t.Fatalf("openSealedHeaders = %+v dropped %v, want the value and no drops", headers, dropped)
	}

	headers, dropped, err = st.openSealedHeaders("end_2", sealed)
	if err != nil {
		t.Fatalf("openSealedHeaders(another endpoint): %v", err)
	}
	if len(headers) != 0 {
		t.Fatalf("another endpoint's row opened the ciphertext: %+v", headers)
	}
	if len(dropped) != 1 || dropped[0] != "X-Api-Key" {
		t.Fatalf("dropped = %v, want the header name so the worker can log what it refused", dropped)
	}
}

// TestOpenSealedHeadersDropsUnderAnotherKey: a rotated (or wrong) deployment key
// must not send a garbage value - the header is dropped, the delivery is not
// wedged, and the failure is attributed by name.
func TestOpenSealedHeadersDropsUnderAnotherKey(t *testing.T) {
	sealer := &Store{headersKey: headersTestKey(t)}
	sealed, err := sealer.sealHeaders("end_1", []CustomHeader{{Name: "X-Api-Key", Value: "sk-live-canary"}})
	if err != nil {
		t.Fatalf("sealHeaders: %v", err)
	}
	other := &Store{headersKey: headersTestKey(t)}
	headers, dropped, err := other.openSealedHeaders("end_1", sealed)
	if err != nil {
		t.Fatalf("openSealedHeaders: %v", err)
	}
	if len(headers) != 0 || len(dropped) != 1 {
		t.Fatalf("openSealedHeaders = %+v dropped %v, want no headers and one drop", headers, dropped)
	}
}

// TestSealingNeedsAKey: a Store built without the at-rest key refuses to store a
// value rather than writing it in the clear. An endpoint with no headers is
// still registrable, because there is nothing to encrypt.
func TestSealingNeedsAKey(t *testing.T) {
	st := &Store{}
	if _, err := st.sealHeaders("end_1", []CustomHeader{{Name: "X-Api-Key", Value: "v"}}); !errors.Is(err, atrest.ErrNoKey) {
		t.Fatalf("sealHeaders without a key: err = %v, want atrest.ErrNoKey", err)
	}
	sealed, err := st.sealHeaders("end_1", nil)
	if err != nil {
		t.Fatalf("sealHeaders with no headers: %v", err)
	}
	if string(sealed) != "[]" {
		t.Fatalf("empty header list sealed to %s, want []", sealed)
	}
}

// TestMalformedColumnIsAnError: a column written outside this package must not
// read as "this endpoint has no headers" - that would hide a hand-edit or a bug.
func TestMalformedColumnIsAnError(t *testing.T) {
	if _, err := summariseHeaders([]byte(`{"name":"X"}`)); err == nil {
		t.Fatal("summariseHeaders accepted a JSON object; want an error")
	}
	views, err := summariseHeaders(nil)
	if err != nil || views != nil {
		t.Fatalf("summariseHeaders(nil) = %v, %v; want no headers and no error", views, err)
	}
}

// --- The registration path, against a real database -------------------------

// TestCreateEndpointStoresHeaderValuesEncrypted is FR-1006's at-rest evidence:
// after registration the column holds ciphertext, the plaintext is nowhere in the
// row, and every read returns the name plus a mask.
func TestCreateEndpointStoresHeaderValuesEncrypted(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	ctx := context.Background()
	key := headersTestKey(t)
	st := New(appPool(t, admin), WithHeadersKey(key))
	pid := seedRoutingProducer(t, admin, "fr405est")
	sc := Scope{ProducerID: pid, Env: "live"}
	con := mustConsumer(t, st, sc, "school_1", "School 1")

	const canary = "sk-live-fr1006-canary"
	created, err := st.CreateEndpoint(ctx, sc, NewEndpoint{
		ConsumerID: con.ID, Name: "orders", URL: "https://8.8.8.8/hook",
		CustomHeaders: []CustomHeader{
			{Name: "X-Api-Key", Value: canary},
			{Name: "X-Tenant", Value: "acme"},
		},
	})
	if err != nil {
		t.Fatalf("CreateEndpoint: %v", err)
	}

	// The database, read as the admin (bypassing nothing but the API's masking):
	// an array of two entries, each with a value_ct, and no plaintext.
	var raw, kind string
	var count int
	if err := admin.QueryRow(ctx, `select custom_headers::text, jsonb_typeof(custom_headers),
			jsonb_array_length(custom_headers) from endpoints where id = $1`, created.Endpoint.ID,
	).Scan(&raw, &kind, &count); err != nil {
		t.Fatalf("read the column: %v", err)
	}
	if kind != "array" || count != 2 {
		t.Fatalf("custom_headers is %s with %d entries, want an array of 2", kind, count)
	}
	if strings.Contains(raw, canary) || strings.Contains(raw, "acme") {
		t.Fatalf("the stored column contains a plaintext value: %s", raw)
	}
	if !strings.Contains(raw, "value_ct") || !strings.Contains(raw, "X-Api-Key") {
		t.Fatalf("the stored column is not [{\"name\",\"value_ct\"}]: %s", raw)
	}

	// The creation response is a read like any other: masked.
	assertMasked(t, "the creation response", created.Endpoint.CustomHeaders, canary)

	got, err := st.GetEndpoint(ctx, sc, created.Endpoint.ID)
	if err != nil {
		t.Fatalf("GetEndpoint: %v", err)
	}
	assertMasked(t, "GetEndpoint", got.CustomHeaders, canary)

	list, err := st.ListEndpointsForConsumer(ctx, sc, con.ID)
	if err != nil {
		t.Fatalf("ListEndpointsForConsumer: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("list returned %d endpoints, want 1", len(list))
	}
	assertMasked(t, "ListEndpointsForConsumer", list[0].CustomHeaders, canary)

	consumerRead, err := st.GetConsumerEndpoint(ctx, sc, con.ID, created.Endpoint.ID)
	if err != nil {
		t.Fatalf("GetConsumerEndpoint: %v", err)
	}
	assertMasked(t, "GetConsumerEndpoint", consumerRead.CustomHeaders, canary)

	toggled, err := st.SetEndpointPaused(ctx, sc, created.Endpoint.ID, true)
	if err != nil {
		t.Fatalf("SetEndpointPaused: %v", err)
	}
	assertMasked(t, "SetEndpointPaused", toggled.CustomHeaders, canary)

	// The consumer's own registration path writes the same column, so it gets the
	// same treatment: sealed at rest, masked on the way out.
	consumerCreated, err := st.CreateConsumerEndpoint(ctx, sc, con.ID, NewConsumerEndpoint{
		Name: "consumer-orders", URL: "https://1.1.1.1/consumer-hook",
		CustomHeaders: []CustomHeader{{Name: "X-Api-Key", Value: canary}},
	})
	if err != nil {
		t.Fatalf("CreateConsumerEndpoint: %v", err)
	}
	assertMasked(t, "CreateConsumerEndpoint", consumerCreated.Endpoint.CustomHeaders, canary)
	var consumerRaw string
	if err := admin.QueryRow(ctx, `select custom_headers::text from endpoints where id = $1`,
		consumerCreated.Endpoint.ID).Scan(&consumerRaw); err != nil {
		t.Fatalf("read the consumer row's column: %v", err)
	}
	if strings.Contains(consumerRaw, canary) {
		t.Fatalf("the consumer registration path stored a plaintext value: %s", consumerRaw)
	}
}

// assertMasked fails when a read leaked the plaintext, and requires the names to
// be there in the clear: a redacted read that also lost the names would tell the
// operator nothing.
func assertMasked(t *testing.T, what string, headers []CustomHeaderView, canary string) {
	t.Helper()
	if len(headers) == 0 {
		t.Fatalf("%s returned no headers; the names must survive redaction", what)
	}
	for _, h := range headers {
		if h.Name == "" {
			t.Errorf("%s returned an entry with no name", what)
		}
		if h.Value != MaskedHeaderValue {
			t.Errorf("%s returned value %q, want the mask %q (FR-1006)", what, h.Value, MaskedHeaderValue)
		}
		if strings.Contains(h.Value, canary) {
			t.Errorf("%s leaked the plaintext", what)
		}
	}
}

// TestCreateEndpointRejectsReservedHeadersBeforeAnyRow (FR-405 + AC-19's shape):
// a refused header is a 422 and leaves no row behind, exactly like a refused URL.
func TestCreateEndpointRejectsReservedHeadersBeforeAnyRow(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	ctx := context.Background()
	st := New(appPool(t, admin), WithHeadersKey(headersTestKey(t)))
	pid := seedRoutingProducer(t, admin, "fr405rej")
	sc := Scope{ProducerID: pid, Env: "live"}
	con := mustConsumer(t, st, sc, "school_1", "School 1")

	for _, name := range []string{"Host", "Content-Length", "Content-Type", "Authorization"} {
		_, err := st.CreateEndpoint(ctx, sc, NewEndpoint{
			ConsumerID: con.ID, Name: "x", URL: "https://8.8.8.8/hook",
			CustomHeaders: []CustomHeader{{Name: name, Value: "v"}},
		})
		if !errors.Is(err, ErrInvalid) {
			t.Fatalf("a custom %s header: err = %v, want ErrInvalid", name, err)
		}
		if !strings.Contains(err.Error(), name) {
			t.Errorf("the refusal for %s does not name it: %v", name, err)
		}
	}
	var n int
	if err := admin.QueryRow(ctx, `select count(*) from endpoints where producer_id = $1`, pid).Scan(&n); err != nil {
		t.Fatalf("count endpoints: %v", err)
	}
	if n != 0 {
		t.Fatalf("%d endpoints persisted from refused registrations, want 0", n)
	}
}

// TestClaimDropsACiphertextMovedToAnotherEndpointsRow is FR-1006's AAD evidence
// end to end: a value sealed under one endpoint's id and copied into another
// endpoint's row does not authenticate on the claim, so it is NOT sent - the
// delivery still goes out, with the value dropped and the header named for the
// log. The endpoint that legitimately owns the ciphertext still gets its value.
func TestClaimDropsACiphertextMovedToAnotherEndpointsRow(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	ctx := context.Background()
	st := New(appPool(t, admin), WithHeadersKey(headersTestKey(t)))
	pid := seedRoutingProducer(t, admin, "fr405aad")
	sc := Scope{ProducerID: pid, Env: "live"}

	et := mustConsumerEvent(t, st, sc, "signup.completed")
	con := mustConsumer(t, st, sc, "school_1", "School 1")

	register := func(name, url, value string) Endpoint {
		t.Helper()
		created, err := st.CreateEndpoint(ctx, sc, NewEndpoint{
			ConsumerID: con.ID, Name: name, URL: url,
			CustomHeaders: []CustomHeader{{Name: "X-Api-Key", Value: value}},
		})
		if err != nil {
			t.Fatalf("CreateEndpoint(%s): %v", name, err)
		}
		if err := st.VerifyEndpoint(ctx, sc, created.Endpoint.ID, created.VerificationToken); err != nil {
			t.Fatalf("VerifyEndpoint(%s): %v", name, err)
		}
		mustSubscribe(t, st, sc, created.Endpoint.ID, et.Name)
		return created.Endpoint
	}
	owner := register("owner", "https://8.8.8.8/hook", "sk-live-owner")
	thief := register("thief", "https://1.1.1.1/hook", "sk-live-thief")

	pub, err := st.PublishEvent(ctx, sc, PublishInput{
		EventType: et.Name, Payload: json.RawMessage(`{}`), ConsumerRef: ptr(con.Ref),
	})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	if _, err := st.FanOutEvent(ctx, sc, pub.ID); err != nil {
		t.Fatalf("FanOutEvent: %v", err)
	}

	// The row-swap a compromised write path (or a hand-edited row) could do: the
	// owner's sealed value now sits on the other endpoint.
	if _, err := admin.Exec(ctx, `update endpoints set custom_headers =
			(select custom_headers from endpoints where id = $1) where id = $2`, owner.ID, thief.ID); err != nil {
		t.Fatalf("move the ciphertext to another endpoint's row: %v", err)
	}

	got := map[string]LeasedDelivery{}
	for i := 0; i < 2; i++ {
		d, ok, err := st.ClaimDelivery(ctx, 40*time.Second, 72*time.Hour)
		if err != nil {
			t.Fatalf("ClaimDelivery: %v", err)
		}
		if !ok {
			t.Fatalf("only %d deliveries were claimable, want 2", i)
		}
		got[d.EndpointID] = d
	}

	ownerDelivery, ok := got[owner.ID]
	if !ok {
		t.Fatalf("the owner's delivery was not claimed (claimed: %v)", keysOf(got))
	}
	if len(ownerDelivery.CustomHeaders) != 1 || ownerDelivery.CustomHeaders[0].Value != "sk-live-owner" {
		t.Fatalf("the owner's own ciphertext did not open: %+v (dropped %v)",
			ownerDelivery.CustomHeaders, ownerDelivery.DroppedCustomHeaders)
	}
	if len(ownerDelivery.DroppedCustomHeaders) != 0 {
		t.Fatalf("the owner's delivery dropped %v, want none", ownerDelivery.DroppedCustomHeaders)
	}

	movedDelivery, ok := got[thief.ID]
	if !ok {
		t.Fatalf("the second endpoint's delivery was not claimed (claimed: %v)", keysOf(got))
	}
	if len(movedDelivery.CustomHeaders) != 0 {
		t.Fatalf("a ciphertext moved between rows was decrypted and would be sent: %+v",
			movedDelivery.CustomHeaders)
	}
	if len(movedDelivery.DroppedCustomHeaders) != 1 || movedDelivery.DroppedCustomHeaders[0] != "X-Api-Key" {
		t.Fatalf("dropped = %v, want the header name (AAD mismatch must be visible, not silent)",
			movedDelivery.DroppedCustomHeaders)
	}
}

func keysOf(m map[string]LeasedDelivery) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
