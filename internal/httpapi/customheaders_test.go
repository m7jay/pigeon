package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/m7jay/pigeon/internal/ratelimit"
	"github.com/m7jay/pigeon/internal/store"
)

// The API's half of FR-405 + FR-1006: the producer and consumer registration
// routes accept `custom_headers`, refuse the names FR-405 names (and the
// platform's own) before writing anything, and never hand a value back. The
// at-rest behaviour is asserted in internal/store; these tests are about the
// wire contract and the log.

// canaryHeaderValue stands in for a credential a producer would configure. Every
// assertion below is "this string is absent", which is only meaningful because it
// is long and distinctive.
const canaryHeaderValue = "sk-live-fr1006-never-echo-this"

// TestEndpointCustomHeadersRoundTripIsRedacted: register with headers, then read
// the endpoint back through every read route the API has, and never see a value.
func TestEndpointCustomHeadersRoundTripIsRedacted(t *testing.T) {
	e := setup(t)
	a := e.seedTenant(t, "hdrs-redacted")
	seedCrossProducerEventType(t, e, a)

	code, raw := e.do(http.MethodPost,
		"/v1/producer/consumers/"+a.ConsumerID+"/endpoints?env=live", a.OwnerToken,
		map[string]any{
			"name": "primary", "url": "https://8.8.8.8/hook",
			"custom_headers": []map[string]string{
				{"name": "X-Api-Key", "value": canaryHeaderValue},
				{"name": "X-Tenant", "value": "acme"},
			},
		})
	if code != http.StatusCreated {
		t.Fatalf("create endpoint with custom headers: %d, want 201 (%s)", code, raw)
	}
	created := decode[endpointCreatedJSON](t, raw)
	assertRedactedHeaders(t, "the 201 body", created.CustomHeaders)
	if bytes.Contains(raw, []byte(canaryHeaderValue)) {
		t.Fatalf("the creation response echoed the header value: %s", raw)
	}
	if len(created.CustomHeaders) != 2 {
		t.Fatalf("the 201 body carried %d custom headers, want 2", len(created.CustomHeaders))
	}

	// GET one endpoint.
	code, raw = e.do(http.MethodGet,
		"/v1/producer/consumers/"+a.ConsumerID+"/endpoints/"+created.ID+"?env=live", a.OwnerToken, nil)
	if code != http.StatusOK {
		t.Fatalf("get endpoint: %d (%s)", code, raw)
	}
	got := decode[endpointJSON](t, raw)
	assertRedactedHeaders(t, "GET .../endpoints/{id}", got.CustomHeaders)
	if bytes.Contains(raw, []byte(canaryHeaderValue)) {
		t.Fatalf("GET echoed the header value: %s", raw)
	}

	// GET the list.
	code, raw = e.do(http.MethodGet,
		"/v1/producer/consumers/"+a.ConsumerID+"/endpoints?env=live", a.OwnerToken, nil)
	if code != http.StatusOK {
		t.Fatalf("list endpoints: %d (%s)", code, raw)
	}
	listed := decode[struct {
		Endpoints []endpointJSON `json:"endpoints"`
	}](t, raw)
	found := false
	for _, ep := range listed.Endpoints {
		if ep.ID != created.ID {
			continue
		}
		found = true
		assertRedactedHeaders(t, "GET .../endpoints (list)", ep.CustomHeaders)
	}
	if !found {
		t.Fatalf("the endpoint is not in the producer's list: %s", raw)
	}
	// A read of an endpoint with no custom headers is an empty array, not null: a
	// client must not have to tell "configured none" from "field missing".
	if !bytes.Contains(raw, []byte(`"custom_headers":[]`)) {
		t.Errorf("an endpoint with no custom headers does not read as []: %s", raw)
	}
	if bytes.Contains(raw, []byte(canaryHeaderValue)) {
		t.Fatalf("the list echoed the header value: %s", raw)
	}

	// PATCH pause returns the endpoint too, so it is a read path as well.
	code, raw = e.do(http.MethodPatch,
		"/v1/producer/consumers/"+a.ConsumerID+"/endpoints/"+created.ID+"?env=live",
		a.OwnerToken, map[string]bool{"paused": true})
	if code != http.StatusOK {
		t.Fatalf("patch endpoint: %d (%s)", code, raw)
	}
	paused := decode[endpointJSON](t, raw)
	assertRedactedHeaders(t, "PATCH .../endpoints/{id}", paused.CustomHeaders)
	if bytes.Contains(raw, []byte(canaryHeaderValue)) {
		t.Fatalf("PATCH echoed the header value: %s", raw)
	}

	// And the column on disk is ciphertext, read as the superuser - the API has no
	// route that could show this, which is the point.
	var stored string
	if err := e.admin.QueryRow(context.Background(),
		`select custom_headers::text from endpoints where id = $1`, created.ID).Scan(&stored); err != nil {
		t.Fatalf("read the stored column: %v", err)
	}
	if strings.Contains(stored, canaryHeaderValue) {
		t.Fatalf("the header value is in the database in the clear: %s", stored)
	}
	if !strings.Contains(stored, "value_ct") || !strings.Contains(stored, "X-Api-Key") {
		t.Fatalf("the stored column is not [{name, value_ct}]: %s", stored)
	}
}

// TestEndpointCustomHeadersRefusals: FR-405's list, the bound, the platform's own
// names and a value that is not a value - each a 422 that names the problem, and
// none of them leaves an endpoint behind.
func TestEndpointCustomHeadersRefusals(t *testing.T) {
	e := setup(t)
	a := e.seedTenant(t, "hdrs-refused")
	seedCrossProducerEventType(t, e, a)
	path := "/v1/producer/consumers/" + a.ConsumerID + "/endpoints?env=live"

	// The four FR-405 names, case-insensitively, plus two of the platform's own.
	for _, name := range []string{"Host", "Content-Length", "Content-Type",
		"Authorization", "HOST", "content-type", "Webhook-Signature", "User-Agent"} {
		code, raw := e.do(http.MethodPost, path, a.OwnerToken, map[string]any{
			"name": "refused", "url": "https://8.8.8.8/hook",
			"custom_headers": []map[string]string{{"name": name, "value": "whatever"}},
		})
		if code != http.StatusUnprocessableEntity {
			t.Fatalf("a custom %s header: %d, want 422 (%s)", name, code, raw)
		}
		if !strings.Contains(strings.ToLower(fieldOf(t, raw, "message")), strings.ToLower(name)) {
			t.Errorf("the refusal for %s does not name it: %s", name, raw)
		}
	}

	// A sixth header is over FR-405's bound.
	six := make([]map[string]string, 0, 6)
	for _, n := range []string{"X-1", "X-2", "X-3", "X-4", "X-5", "X-6"} {
		six = append(six, map[string]string{"name": n, "value": "v"})
	}
	code, raw := e.do(http.MethodPost, path, a.OwnerToken, map[string]any{
		"name": "too-many", "url": "https://8.8.8.8/hook", "custom_headers": six,
	})
	if code != http.StatusUnprocessableEntity || !strings.Contains(fieldOf(t, raw, "message"), "5") {
		t.Fatalf("six custom headers: %d %s, want 422 naming the bound of 5", code, raw)
	}

	// A value carrying CRLF is header injection, and the refusal must not quote it.
	code, raw = e.do(http.MethodPost, path, a.OwnerToken, map[string]any{
		"name": "injection", "url": "https://8.8.8.8/hook",
		"custom_headers": []map[string]string{
			{"name": "X-Evil", "value": canaryHeaderValue + "\r\nX-Smuggled: 1"},
		},
	})
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("a CRLF header value: %d, want 422 (%s)", code, raw)
	}
	if bytes.Contains(raw, []byte(canaryHeaderValue)) {
		t.Fatalf("the refusal quoted the value it rejected: %s", raw)
	}
	if !strings.Contains(fieldOf(t, raw, "message"), "0x0d") {
		t.Errorf("the refusal does not say what is wrong with the value: %s", raw)
	}

	// None of the refusals created an endpoint: FR-405 is checked before the insert,
	// not after it (the store test asserts the row count; this asserts the API view).
	code, raw = e.do(http.MethodGet, "/v1/producer/consumers/"+a.ConsumerID+"/endpoints?env=live", a.OwnerToken, nil)
	if code != http.StatusOK {
		t.Fatalf("list endpoints: %d (%s)", code, raw)
	}
	listed := decode[struct {
		Endpoints []endpointJSON `json:"endpoints"`
	}](t, raw)
	for _, ep := range listed.Endpoints {
		if strings.HasPrefix(ep.Name, "refused") || ep.Name == "too-many" || ep.Name == "injection" {
			t.Fatalf("a refused registration created %q: %s", ep.Name, raw)
		}
	}
}

// TestConsumerEndpointCustomHeadersUseTheSameRules: the consumer's own portal
// route writes the same column, so it applies the same list and returns the same
// mask - a second implementation of FR-405 would be a second place to get it
// wrong.
func TestConsumerEndpointCustomHeadersUseTheSameRules(t *testing.T) {
	e := setup(t)
	a := e.seedTenant(t, "hdrs-consumer")
	seedCrossProducerEventType(t, e, a)

	code, raw := e.do(http.MethodPost, "/v1/consumer/endpoints", a.ConsumerToken, map[string]any{
		"name": "consumer-refused", "url": "https://1.1.1.1/hook",
		"custom_headers": []map[string]string{{"name": "Authorization", "value": canaryHeaderValue}},
	})
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("the consumer route accepted an Authorization header: %d (%s)", code, raw)
	}
	if bytes.Contains(raw, []byte(canaryHeaderValue)) {
		t.Fatalf("the consumer refusal quoted the value: %s", raw)
	}

	code, raw = e.do(http.MethodPost, "/v1/consumer/endpoints", a.ConsumerToken, map[string]any{
		"name": "consumer-headers", "url": "https://1.1.1.1/hook",
		"custom_headers": []map[string]string{{"name": "X-Api-Key", "value": canaryHeaderValue}},
	})
	if code != http.StatusCreated {
		t.Fatalf("the consumer route refused a legal header: %d (%s)", code, raw)
	}
	created := decode[endpointCreatedJSON](t, raw)
	assertRedactedHeaders(t, "POST /v1/consumer/endpoints", created.CustomHeaders)
	if bytes.Contains(raw, []byte(canaryHeaderValue)) {
		t.Fatalf("the consumer 201 echoed the value: %s", raw)
	}

	var stored string
	if err := e.admin.QueryRow(context.Background(),
		`select custom_headers::text from endpoints where id = $1`, created.ID).Scan(&stored); err != nil {
		t.Fatalf("read the stored column: %v", err)
	}
	if strings.Contains(stored, canaryHeaderValue) {
		t.Fatalf("the consumer path stored a plaintext value: %s", stored)
	}

	// And the consumer's read of the producer's endpoint is masked too: the two
	// routes share one scan, which is why this one test covers both.
	code, raw = e.do(http.MethodGet,
		"/v1/producer/consumers/"+a.ConsumerID+"/endpoints/"+created.ID+"?env=live", a.OwnerToken, nil)
	if code != http.StatusOK {
		t.Fatalf("producer read of a consumer-created endpoint: %d (%s)", code, raw)
	}
	assertRedactedHeaders(t, "the producer's read of a consumer endpoint",
		decode[endpointJSON](t, raw).CustomHeaders)
}

// TestCustomHeaderValuesNeverReachTheLogs is FR-1006's other half: the value is
// refused, sealed and delivered, and nowhere in between does it reach a log line.
// The server is rebuilt over the same database with a capturing debug logger, so
// the assertion covers every level the API can emit.
func TestCustomHeaderValuesNeverReachTheLogs(t *testing.T) {
	e := setup(t)
	a := e.seedTenant(t, "hdrs-logs")
	seedCrossProducerEventType(t, e, a)

	var logged bytes.Buffer
	loggedEnv := e.withLogger(t, slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug})))

	// A successful registration, a read, a refusal, and a 404 (which is logged by
	// design, via logProbe) - the last one guarantees the buffer is not empty, so
	// "the value is absent" cannot pass vacuously.
	code, raw := loggedEnv.do(http.MethodPost,
		"/v1/producer/consumers/"+a.ConsumerID+"/endpoints?env=live", a.OwnerToken, map[string]any{
			"name": "logged", "url": "https://8.8.8.8/hook",
			"custom_headers": []map[string]string{{"name": "X-Api-Key", "value": canaryHeaderValue}},
		})
	if code != http.StatusCreated {
		t.Fatalf("create endpoint: %d (%s)", code, raw)
	}
	created := decode[endpointCreatedJSON](t, raw)
	_ = loggedEnv.doNoBody(http.MethodGet,
		"/v1/producer/consumers/"+a.ConsumerID+"/endpoints/"+created.ID+"?env=live", a.OwnerToken, nil)
	_ = loggedEnv.doNoBody(http.MethodPost,
		"/v1/producer/consumers/"+a.ConsumerID+"/endpoints?env=live", a.OwnerToken, map[string]any{
			"name": "logged-refused", "url": "https://8.8.8.8/hook",
			"custom_headers": []map[string]string{{"name": "Authorization", "value": canaryHeaderValue}},
		})
	_ = loggedEnv.doNoBody(http.MethodGet,
		"/v1/producer/consumers/"+a.ConsumerID+"/endpoints/end_does_not_exist?env=live", a.OwnerToken, nil)

	if strings.Contains(logged.String(), canaryHeaderValue) {
		t.Fatalf("a header value reached the log: %s", logged.String())
	}
	if logged.Len() == 0 {
		t.Fatal("nothing was logged, so this test proves nothing about the log path")
	}
	if !strings.Contains(logged.String(), "end_does_not_exist") {
		t.Errorf("the 404 probe was not logged; the capture is not covering the log path: %s", logged.String())
	}
}

// withLogger builds a second server over the same database and store with the
// given logger, so a test can assert on what the API logged.
func (e *env) withLogger(t *testing.T, log *slog.Logger) *env {
	t.Helper()
	api := New(Deps{
		Lookup:          e.st.LookupAPIKey,
		Sessions:        e.st.LookupSession,
		Data:            e.st,
		Version:         "test-logs",
		SessionTTL:      time.Hour,
		KeyLimiter:      ratelimit.New(1000, 1000),
		ProducerLimiter: ratelimit.New(1000, 1000),
	}, log)
	out := &env{admin: e.admin, app: e.app, st: e.st, api: api, adminToken: e.adminToken}
	out.srv = httptest.NewServer(api.Handler())
	t.Cleanup(out.srv.Close)
	return out
}

// assertRedactedHeaders fails when a read leaked a value, and requires the names
// to survive: a mask with no name would tell an operator nothing about what is
// configured.
func assertRedactedHeaders(t *testing.T, what string, headers []customHeaderJSON) {
	t.Helper()
	if len(headers) == 0 {
		t.Fatalf("%s returned no custom headers; the names must survive redaction", what)
	}
	for _, h := range headers {
		if h.Name == "" {
			t.Errorf("%s returned a header with no name", what)
		}
		if h.Value != store.MaskedHeaderValue {
			t.Errorf("%s returned value %q, want the mask %q (FR-1006)", what, h.Value, store.MaskedHeaderValue)
		}
		if strings.Contains(h.Value, canaryHeaderValue) {
			t.Errorf("%s leaked the plaintext", what)
		}
	}
}

// fieldOf pulls one field out of the nested error body the API writes:
// {"error": {"code": ..., "message": ...}}.
func fieldOf(t *testing.T, raw []byte, key string) string {
	t.Helper()
	var body struct {
		Error map[string]string `json:"error"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("error body is not JSON: %v (%s)", err, raw)
	}
	return body.Error[key]
}
