package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/m7jay/pigeon/internal/ratelimit"
	"github.com/stretchr/testify/require"
)

// logBuffer is a slog sink the tests can read. FR-108's AC-15 says the refusal is
// "404, logged", so the log line is part of the acceptance criterion and not an
// implementation detail.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// withLog returns a server over the same database whose log lands in buf.
func (e *env) withLog(t *testing.T) (*env, *logBuffer) {
	t.Helper()
	buf := &logBuffer{}
	api := New(Deps{
		Lookup:          e.st.LookupAPIKey,
		Sessions:        e.st.LookupSession,
		Data:            e.st,
		Version:         "test-log",
		SessionTTL:      time.Hour,
		KeyLimiter:      ratelimit.New(1000, 1000),
		ProducerLimiter: ratelimit.New(1000, 1000),
	}, slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	out := &env{admin: e.admin, app: e.app, st: e.st, api: api, adminToken: e.adminToken, tenants: e.tenants}
	out.srv = httptest.NewServer(api.Handler())
	t.Cleanup(out.srv.Close)
	return out, buf
}

// cpCase describes what a producer-scoped route must do when the caller aims it
// at another tenant's identifier. Exactly one of these exists per producer route,
// and TestCrossProducerSuiteCoversEveryRoute fails if that stops being true.
type cpCase struct {
	// build returns the request path for tenant B's identifiers, plus an optional
	// body.
	build func(a, b *tenant) (path string, body any)
	// credential is the bearer token the case is aimed with. Nil means "A's owner
	// session", which is the shape every id-addressed route needs. Routes that
	// take no identifier in the path (a key, a consumer session) are aimed with
	// the *other* tenant's credential and must answer with its own scope only.
	credential func(a, b *tenant) string
	// want is the status the route must answer for a cross-tenant request.
	want int
	// leaks are substrings that must not appear in the response body (collection
	// routes have no id to 404, so they must prove absence instead).
	leaks func(a, b *tenant) []string
	// after runs an extra assertion, for routes that create rows: it proves the
	// row landed in A and is invisible to B.
	after func(t *testing.T, e *env, created []byte)
}

// crossProducerCases is AC-20's table. "Every resource" is literal: the coverage
// test below compares its keys against s.routes().
var crossProducerCases = map[string]cpCase{
	"GET /v1/producer/members": {
		build: func(a, b *tenant) (string, any) { return "/v1/producer/members", nil },
		want:  http.StatusOK,
		leaks: func(a, b *tenant) []string { return []string{b.OwnerEmail, b.DeveloperEmail, b.ID} },
	},
	"POST /v1/producer/invites": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/invites", map[string]string{"email": "cp-probe@example.test", "role": "developer"}
		},
		want: http.StatusCreated,
		after: func(t *testing.T, e *env, created []byte) {
			// The invitation must land in A, not in B: B's list cannot contain it.
			b := e.tenantFor(t, "cp-beta")
			code, raw := e.do(http.MethodGet, "/v1/producer/invites", b.OwnerToken, nil)
			mustStatus(t, code, raw, http.StatusOK)
			if strings.Contains(string(raw), "cp-probe@example.test") {
				t.Fatalf("an invitation created in producer A is visible in producer B: %s", raw)
			}
		},
	},
	"GET /v1/producer/invites": {
		build: func(a, b *tenant) (string, any) { return "/v1/producer/invites", nil },
		want:  http.StatusOK,
		leaks: func(a, b *tenant) []string { return []string{b.PendingInviteEmail, b.PendingInviteID} },
	},
	"DELETE /v1/producer/invites/{inviteID}": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/invites/" + b.PendingInviteID + "?env=live", nil
		},
		want: http.StatusNotFound,
	},
	"GET /v1/producer/consumers": {
		build: func(a, b *tenant) (string, any) { return "/v1/producer/consumers?env=live", nil },
		want:  http.StatusOK,
		leaks: func(a, b *tenant) []string { return []string{b.ConsumerID, b.ConsumerRef} },
	},
	"POST /v1/producer/consumers": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/consumers?env=live", map[string]string{"ref": "cp-probe-ref", "name": "Probe"}
		},
		want: http.StatusCreated,
		after: func(t *testing.T, e *env, created []byte) {
			b := e.tenantFor(t, "cp-beta")
			code, raw := e.do(http.MethodGet, "/v1/producer/consumers?env=live", b.OwnerToken, nil)
			mustStatus(t, code, raw, http.StatusOK)
			if strings.Contains(string(raw), "cp-probe-ref") {
				t.Fatalf("a consumer created in producer A is visible in producer B: %s", raw)
			}
		},
	},
	"GET /v1/producer/consumers/{consumerID}": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/consumers/" + b.ConsumerID + "?env=live", nil
		},
		want: http.StatusNotFound,
	},
	"POST /v1/producer/consumers/{consumerID}/invites": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/consumers/" + b.ConsumerID + "/invites?env=live",
				map[string]string{"email": "cp-probe-consumer@example.test"}
		},
		want: http.StatusNotFound,
	},
	"GET /v1/producer/consumers/{consumerID}/users": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/consumers/" + b.ConsumerID + "/users?env=live", nil
		},
		want: http.StatusNotFound,
	},
	"DELETE /v1/producer/consumers/{consumerID}/users/{userID}": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/consumers/" + b.ConsumerID + "/users/" + b.ConsumerUser + "?env=live", nil
		},
		want: http.StatusNotFound,
	},
	"POST /v1/producer/consumers/{consumerID}/rekey": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/consumers/" + b.ConsumerID + "/rekey?env=live", map[string]string{"ref": "cp-rekey-probe"}
		},
		want: http.StatusNotFound,
	},
	"GET /v1/producer/consumers/{consumerID}/endpoints": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/consumers/" + b.ConsumerID + "/endpoints?env=live", nil
		},
		want:  http.StatusOK,
		leaks: func(a, b *tenant) []string { return []string{b.EndpointID} },
	},
	"POST /v1/producer/consumers/{consumerID}/endpoints": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/consumers/" + b.ConsumerID + "/endpoints?env=live", map[string]string{"name": "Probe", "url": "https://1.1.1.1/hook"}
		},
		want: http.StatusNotFound,
	},
	"GET /v1/producer/consumers/{consumerID}/endpoints/{endpointID}": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/consumers/" + b.ConsumerID + "/endpoints/" + b.EndpointID + "?env=live", nil
		},
		want: http.StatusNotFound,
	},
	"PATCH /v1/producer/consumers/{consumerID}/endpoints/{endpointID}": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/consumers/" + b.ConsumerID + "/endpoints/" + b.EndpointID + "?env=live", map[string]bool{"paused": true}
		},
		want: http.StatusNotFound,
	},
	"DELETE /v1/producer/consumers/{consumerID}/endpoints/{endpointID}": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/consumers/" + b.ConsumerID + "/endpoints/" + b.EndpointID + "?env=live", nil
		},
		want: http.StatusNotFound,
	},
	"POST /v1/producer/consumers/{consumerID}/endpoints/{endpointID}/verify": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/consumers/" + b.ConsumerID + "/endpoints/" + b.EndpointID + "/verify?env=live", map[string]string{"token": b.EndpointVerifyToken}
		},
		want: http.StatusNotFound,
	},
	"GET /v1/producer/endpoints/{endpointID}/subscriptions": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/endpoints/" + b.EndpointID + "/subscriptions?env=live", nil
		},
		want:  http.StatusOK,
		leaks: func(a, b *tenant) []string { return []string{b.EventTypeName} },
	},
	"POST /v1/producer/endpoints/{endpointID}/subscriptions": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/endpoints/" + b.EndpointID + "/subscriptions?env=live", map[string]string{"event_type": b.EventTypeName}
		},
		want: http.StatusNotFound,
	},
	"DELETE /v1/producer/endpoints/{endpointID}/subscriptions/{eventType}": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/endpoints/" + b.EndpointID + "/subscriptions/" + b.EventTypeName + "?env=live", nil
		},
		want: http.StatusNotFound,
	},
	"POST /v1/producer/keys": {
		build: func(a, b *tenant) (string, any) { return "/v1/producer/keys?env=live", nil },
		want:  http.StatusCreated,
		after: func(t *testing.T, e *env, created []byte) {
			minted := decode[keyJSON](t, created)
			b := e.tenantFor(t, "cp-beta")
			code, raw := e.do(http.MethodGet, "/v1/producer/keys?env=live", b.OwnerToken, nil)
			mustStatus(t, code, raw, http.StatusOK)
			if strings.Contains(string(raw), minted.ID) {
				t.Fatalf("a key minted for producer A is visible in producer B: %s", raw)
			}
			// And the minted key must not authenticate as anything but A.
			code, raw = e.do(http.MethodGet, "/v1/whoami", *minted.Key, nil)
			mustStatus(t, code, raw, http.StatusOK)
			alpha := e.tenantFor(t, "cp-alpha")
			if who := decode[struct {
				ProducerID string `json:"producer_id"`
			}](t, raw); who.ProducerID != alpha.ID {
				t.Fatalf("key minted in A authenticated as %s", who.ProducerID)
			}
		},
	},
	"GET /v1/producer/keys": {
		build: func(a, b *tenant) (string, any) { return "/v1/producer/keys?env=live", nil },
		want:  http.StatusOK,
		leaks: func(a, b *tenant) []string { return []string{b.KeyID, b.KeyPlain} },
	},
	"GET /v1/producer/keys/{keyID}": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/keys/" + b.KeyID + "?env=live", nil
		},
		want: http.StatusNotFound,
	},
	"DELETE /v1/producer/keys/{keyID}": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/keys/" + b.KeyID + "?env=live", nil
		},
		want: http.StatusNotFound,
	},
	// The two routes that name their tenant only through the credential. There is
	// no identifier to aim at another tenant, so the case is the other way round:
	// present the other tenant's credential and prove the answer is scoped to it.
	"GET /v1/whoami": {
		build:      func(a, b *tenant) (string, any) { return "/v1/whoami", nil },
		credential: func(a, b *tenant) string { return b.KeyPlain },
		want:       http.StatusOK,
		leaks: func(a, b *tenant) []string {
			return []string{a.ID, a.KeyID, a.ConsumerID, a.ConsumerRef}
		},
		after: func(t *testing.T, e *env, created []byte) {
			b := e.tenantFor(t, "cp-beta")
			who := decode[struct {
				ProducerID string `json:"producer_id"`
				KeyID      string `json:"key_id"`
			}](t, created)
			if who.ProducerID != b.ID || who.KeyID != b.KeyID {
				t.Fatalf("producer B's key resolved as producer_id=%q key_id=%q; want %q/%q",
					who.ProducerID, who.KeyID, b.ID, b.KeyID)
			}
		},
	},
	"GET /v1/consumer/me": {
		build:      func(a, b *tenant) (string, any) { return "/v1/consumer/me", nil },
		credential: func(a, b *tenant) string { return b.ConsumerToken },
		want:       http.StatusOK,
		leaks: func(a, b *tenant) []string {
			return []string{a.ConsumerID, a.ConsumerRef, a.ID}
		},
		after: func(t *testing.T, e *env, created []byte) {
			b := e.tenantFor(t, "cp-beta")
			if me := decode[consumerJSON](t, created); me.ID != b.ConsumerID {
				t.Fatalf("producer B's consumer session resolved consumer %q; want %q", me.ID, b.ConsumerID)
			}
		},
	},
	// Consumer portal self-service (M2b, UI-13/UI-14). These are scoped by the
	// consumer id on the session claim, not by a tenant id in the path, so the
	// cross-producer proof is cross-CONSUMER: B's consumer session must 404 on
	// every one of A's endpoint and subscription ids when aimed directly (AC-39's
	// "including by guessing ids"), while the two collection routes see only B's
	// own. Aiming A's ids with B's token is order-independent, because A's ids
	// never belong to B's consumer however the other cases mutate B's own rows.
	"GET /v1/consumer/endpoints": {
		build:      func(a, b *tenant) (string, any) { return "/v1/consumer/endpoints", nil },
		credential: func(a, b *tenant) string { return b.ConsumerToken },
		want:       http.StatusOK,
		leaks:      func(a, b *tenant) []string { return []string{a.EndpointID, a.ConsumerID, a.ConsumerRef} },
	},
	"POST /v1/consumer/endpoints": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/consumer/endpoints", map[string]string{"name": "cp-consumer-probe", "url": "https://1.1.1.1/hook"}
		},
		credential: func(a, b *tenant) string { return b.ConsumerToken },
		want:       http.StatusCreated,
		leaks:      func(a, b *tenant) []string { return []string{a.EndpointID, a.ConsumerID} },
		after: func(t *testing.T, e *env, created []byte) {
			b := e.tenantFor(t, "cp-beta")
			if ep := decode[endpointCreatedJSON](t, created); ep.ConsumerID != b.ConsumerID {
				t.Fatalf("a consumer registered an endpoint under consumer %q, want %q (FR-110)", ep.ConsumerID, b.ConsumerID)
			}
		},
	},
	"GET /v1/consumer/event-types": {
		build:      func(a, b *tenant) (string, any) { return "/v1/consumer/event-types", nil },
		credential: func(a, b *tenant) string { return b.ConsumerToken },
		want:       http.StatusOK,
		// The list is scoped to B's producer: A's type name must not appear in it
		// even though the route takes no identifier to aim with.
		leaks: func(a, b *tenant) []string { return []string{a.EventTypeName} },
	},
	"GET /v1/consumer/endpoints/{endpointID}": {
		build:      func(a, b *tenant) (string, any) { return "/v1/consumer/endpoints/" + a.EndpointID, nil },
		credential: func(a, b *tenant) string { return b.ConsumerToken },
		want:       http.StatusNotFound,
	},
	"PATCH /v1/consumer/endpoints/{endpointID}": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/consumer/endpoints/" + a.EndpointID, map[string]bool{"paused": true}
		},
		credential: func(a, b *tenant) string { return b.ConsumerToken },
		want:       http.StatusNotFound,
	},
	"POST /v1/consumer/endpoints/{endpointID}/rotate": {
		build:      func(a, b *tenant) (string, any) { return "/v1/consumer/endpoints/" + a.EndpointID + "/rotate", nil },
		credential: func(a, b *tenant) string { return b.ConsumerToken },
		want:       http.StatusNotFound,
	},
	"DELETE /v1/consumer/endpoints/{endpointID}": {
		build:      func(a, b *tenant) (string, any) { return "/v1/consumer/endpoints/" + a.EndpointID, nil },
		credential: func(a, b *tenant) string { return b.ConsumerToken },
		want:       http.StatusNotFound,
	},
	"POST /v1/consumer/endpoints/{endpointID}/verify": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/consumer/endpoints/" + a.EndpointID + "/verify", map[string]string{"token": a.EndpointVerifyToken}
		},
		credential: func(a, b *tenant) string { return b.ConsumerToken },
		want:       http.StatusNotFound,
	},
	"GET /v1/consumer/endpoints/{endpointID}/subscriptions": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/consumer/endpoints/" + a.EndpointID + "/subscriptions", nil
		},
		credential: func(a, b *tenant) string { return b.ConsumerToken },
		want:       http.StatusNotFound,
	},
	"POST /v1/consumer/endpoints/{endpointID}/subscriptions": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/consumer/endpoints/" + a.EndpointID + "/subscriptions", map[string]string{"event_type": b.EventTypeName}
		},
		credential: func(a, b *tenant) string { return b.ConsumerToken },
		want:       http.StatusNotFound,
	},
	"DELETE /v1/consumer/endpoints/{endpointID}/subscriptions/{eventType}": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/consumer/endpoints/" + a.EndpointID + "/subscriptions/" + b.EventTypeName, nil
		},
		credential: func(a, b *tenant) string { return b.ConsumerToken },
		want:       http.StatusNotFound,
	},
	// Ingest and event types (FR-301..FR-306, FR-601..FR-607). None of the three
	// takes a cross-tenant identifier in the path - publish and event-type writes
	// are scoped by the credential - so the cross-producer proof is that the row
	// lands in the caller's tenant and never crosses.
	"POST /v1/event-types": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/event-types", map[string]any{
				"name":           "cp.event",
				"payload_schema": map[string]any{"type": "object"},
			}
		},
		credential: func(a, b *tenant) string { return a.KeyPlain },
		want:       http.StatusCreated,
		after: func(t *testing.T, e *env, created []byte) {
			// The type landed in A; B's list must not contain it.
			b := e.tenantFor(t, "cp-beta")
			code, raw := e.do(http.MethodGet, "/v1/event-types", b.KeyPlain, nil)
			mustStatus(t, code, raw, http.StatusOK)
			if strings.Contains(string(raw), "cp.event") {
				t.Fatalf("an event type created in producer A is visible in producer B: %s", raw)
			}
		},
	},
	"GET /v1/event-types": {
		build:      func(a, b *tenant) (string, any) { return "/v1/event-types", nil },
		credential: func(a, b *tenant) string { return a.KeyPlain },
		want:       http.StatusOK,
		leaks:      func(a, b *tenant) []string { return []string{b.EventTypeName} },
	},
	"PATCH /v1/event-types/{name}": {
		// FR-304/FR-305: version bump and disable are PATCH operations. It targets a
		// SEEDED example (order.shipped), not a.EventTypeName - the POST /v1/events
		// case still publishes to a.EventTypeName, and a disabled type is rejected
		// there (TestPublishDisabledTypeIsInvalid), so the two cases must not share
		// a type or the suite's ordering would make publish flaky.
		build: func(a, b *tenant) (string, any) {
			return "/v1/event-types/order.shipped", map[string]any{"enabled": false}
		},
		credential: func(a, b *tenant) string { return a.KeyPlain },
		want:       http.StatusOK,
		after: func(t *testing.T, e *env, raw []byte) {
			var got map[string]any
			require.NoError(t, json.Unmarshal(raw, &got))
			if v, _ := got["enabled"].(bool); v {
				t.Errorf("event type should be disabled after PATCH: %s", raw)
			}
		},
	},
	"POST /v1/events": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/events", map[string]any{
				"event_type": a.EventTypeName,
				"payload":    map[string]any{"note": "cross-producer-probe"},
			}
		},
		credential: func(a, b *tenant) string { return a.KeyPlain },
		want:       http.StatusAccepted,
		after: func(t *testing.T, e *env, created []byte) {
			// The accept is scoped to A: the event row carries A's producer_id, and
			// its outbox row is pending in the same transaction (FR-604).
			a := e.tenantFor(t, "cp-alpha")
			pub := decode[publishResponse](t, created)
			var producerID, status string
			err := e.admin.QueryRow(context.Background(),
				`select e.producer_id, o.status from events e join outbox o on o.event_id = e.id
				 where e.id = $1`, pub.ID).Scan(&producerID, &status)
			if err != nil {
				t.Fatalf("read accepted event: %v", err)
			}
			if producerID != a.ID {
				t.Fatalf("event accepted under producer_id %q; want %q (publish is credential-scoped)", producerID, a.ID)
			}
			if status != "pending" {
				t.Fatalf("accepted event's outbox row is %q, want pending (FR-604)", status)
			}
		},
	},

	// --- S7: the inspect and replay surface (§11, FR-1101..FR-1111) --------

	"GET /v1/producer/deliveries": {
		build: func(a, b *tenant) (string, any) { return "/v1/producer/deliveries?env=live", nil },
		want:  http.StatusOK,
		// A collection route has no id to 404, so it proves absence instead: B's
		// endpoint, event and delivery must not appear in A's list (FR-1101).
		leaks: func(a, b *tenant) []string {
			return []string{b.EndpointID, b.DeliveryEventID, fmt.Sprintf("\"id\":%d", b.DeliveryID)}
		},
	},
	"GET /v1/producer/deliveries/dedupe": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/deliveries/dedupe?env=live&endpoint_id=" + b.EndpointID +
				"&event_id=" + b.DeliveryEventID, nil
		},
		// FR-903 asked about another tenant's endpoint is a miss (FR-108), not an
		// answer about whether that endpoint was a duplicate.
		want: http.StatusNotFound,
	},
	"GET /v1/producer/deliveries/{deliveryID}": {
		build: func(a, b *tenant) (string, any) {
			return fmt.Sprintf("/v1/producer/deliveries/%d?env=live", b.DeliveryID), nil
		},
		want: http.StatusNotFound,
	},
	"POST /v1/producer/deliveries/replay": {
		build: func(a, b *tenant) (string, any) {
			// Aimed at B's endpoint with a key of its own: the selection is empty
			// in A's scope, so this must create nothing anywhere (FR-1106).
			return "/v1/producer/deliveries/replay?env=live", map[string]any{
				"endpoint_id": b.EndpointID, "idempotency_key": "cp-probe-replay",
			}
		},
		want: http.StatusOK,
		after: func(t *testing.T, e *env, created []byte) {
			a, b := e.tenantFor(t, "cp-alpha"), e.tenantFor(t, "cp-beta")
			res := decode[replayJSONBody](t, created)
			if res.Matched != 0 || res.Created != 0 || len(res.DeliveryIDs) != 0 {
				t.Fatalf("a replay aimed at another tenant's endpoint = %+v, want an empty selection", res)
			}
			// Neither tenant gained a replay, and B's delivery is untouched.
			if n := e.countRows(t,
				`select count(*) from deliveries where producer_id in ($1, $2) and replay_of_delivery_id is not null`,
				a.ID, b.ID); n != 0 {
				t.Fatalf("the cross-producer replay created %d deliveries", n)
			}
			e.expectDeliveryUntouched(t, b.DeliveryID)
		},
	},
	"GET /v1/producer/events/{eventID}": {
		build: func(a, b *tenant) (string, any) {
			return "/v1/producer/events/" + b.DeliveryEventID + "?env=live", nil
		},
		want: http.StatusNotFound,
	},
	"GET /v1/consumer/deliveries": {
		// A consumer session carries its own consumer: this case presents B's and
		// proves the answer holds only B's rows (FR-1105), never A's.
		credential: func(a, b *tenant) string { return b.ConsumerToken },
		build:      func(a, b *tenant) (string, any) { return "/v1/consumer/deliveries", nil },
		want:       http.StatusOK,
		leaks: func(a, b *tenant) []string {
			return []string{a.EndpointID, a.DeliveryEventID, fmt.Sprintf("\"id\":%d", a.DeliveryID)}
		},
	},
	"GET /v1/consumer/deliveries/{deliveryID}": {
		credential: func(a, b *tenant) string { return b.ConsumerToken },
		build: func(a, b *tenant) (string, any) {
			return fmt.Sprintf("/v1/consumer/deliveries/%d", a.DeliveryID), nil
		},
		want: http.StatusNotFound,
	},
	"POST /v1/consumer/deliveries/replay": {
		credential: func(a, b *tenant) string { return b.ConsumerToken },
		build: func(a, b *tenant) (string, any) {
			// The body names A's consumer explicitly; the session's claim must win
			// (FR-110), so this replays B's own deliveries and nothing of A's.
			return "/v1/consumer/deliveries/replay", map[string]any{
				"consumer_id": a.ConsumerID, "status": "delivered", "idempotency_key": "cp-probe-consumer-replay",
			}
		},
		want: http.StatusOK,
		after: func(t *testing.T, e *env, created []byte) {
			a := e.tenantFor(t, "cp-alpha")
			res := decode[replayJSONBody](t, created)
			for _, id := range res.DeliveryIDs {
				if id == a.DeliveryID {
					t.Fatalf("a consumer replay reached producer A's delivery %d", id)
				}
			}
			e.expectDeliveryUntouched(t, a.DeliveryID)
		},
	},
}

// notProducerScoped classifies every route that is not part of the producer-scoped
// surface, with the reason. The coverage test below requires each registered route
// to appear in exactly one of the two tables, so "every resource has a case" is a
// property of the build rather than a claim in a document — and a route that is
// added without a classification fails instead of being skipped silently (which is
// how /v1/whoami and /v1/consumer/me escaped the first version of this suite).
var notProducerScoped = map[string]string{
	"GET /health":                                   "liveness: no credential and no tenant",
	"GET /health/ready":                             "readiness: no credential and no tenant",
	"POST /v1/session":                              "login: obtains the credential, so it cannot require one",
	"GET /v1/session":                               "self-description: scoped to the caller's own claims, no tenant id in the request",
	"DELETE /v1/session":                            "logout: acts on the presented token only",
	"POST /v1/session/password":                     "FR-101 forced change: acts on the caller's own account only",
	"POST /v1/invites/accept":                       "accepts an invitation: the token identifies the tenant, and an existing account must present its own session",
	"POST /v1/admin/producers":                      "instance-admin surface: no producer scope by design (FR-102)",
	"GET /v1/admin/producers":                       "instance-admin surface: lists tenants, so it cannot be tenant-scoped",
	"GET /v1/admin/producers/{producerID}":          "instance-admin surface: the admin may address any tenant (FR-102)",
	"POST /v1/admin/producers/{producerID}/suspend": "instance-admin surface: suspends any tenant (FR-107)",
	"POST /v1/admin/producers/{producerID}/resume":  "instance-admin surface: resumes any tenant (FR-107)",
	"GET /v1/admin/producers/{producerID}/audit":    "instance-admin surface: the audit log is not tenant data (MT-5)",
	"GET /metrics":                                  "platform telemetry: aggregate non-delivery counters over every tenant, so it is operator (admin) telemetry, not tenant-scoped (FR-508)",
	"GET /v1/meta/usage":                            "platform telemetry: reports the deployment's disk usage and table sizes, so it is operator (admin) telemetry, not tenant-scoped (SB-6)",
	"POST /v1/producers":                            "FR-102 self-registration: exists only when enabled, and by definition has no tenant yet",
}

// TestCrossProducerRequestsAreNotFound is AC-15 and AC-20: every producer-scoped
// route, addressed with another tenant's identifier, answers 404 and never 403,
// and the refusal is logged.
func TestCrossProducerRequestsAreNotFound(t *testing.T) {
	e, logs := e0(t)
	a := e.seedTenant(t, "cp-alpha")
	b := e.seedTenant(t, "cp-beta")

	// A pending invitation in B gives the invitation routes a real id to aim at.
	b.PendingInviteEmail = "cp-beta-pending@example.test"
	code, raw := e.do(http.MethodPost, "/v1/producer/invites", b.OwnerToken, map[string]string{
		"email": b.PendingInviteEmail, "role": "developer",
	})
	mustStatus(t, code, raw, http.StatusCreated)
	b.PendingInviteID = decode[inviteJSON](t, raw).ID
	e.remember(t, b)

	// Each producer registers one live event type, so the ingest and event-type
	// routes have a real scoped contract to act on. The name is derived from the
	// slug so the two tenants' types are distinct and the leak check has a name to
	// prove is absent from the other tenant.
	seedCrossProducerEventType(t, e, a)
	seedCrossProducerEventType(t, e, b)

	// And one delivered delivery each, on a consumer-routed type of its own, so
	// the inspect and replay routes (FR-1101..FR-1111) have a real delivery id
	// and event id in every tenant to aim across the tenant boundary.
	seedCrossProducerDelivery(t, e, a)
	seedCrossProducerDelivery(t, e, b)

	// A's owner session, aimed at every one of B's identifiers — except on the two
	// routes that take no identifier, where the case presents B's own credential
	// and proves the answer is scoped to it.
	for _, rt := range e.routes() {
		if !producerScoped(rt.Kind) {
			continue
		}
		key := rt.Method + " " + rt.Path
		tc, ok := crossProducerCases[key]
		if !ok {
			t.Fatalf("route %s has no cross-producer case; AC-20 says every resource (add it to crossProducerCases)", key)
		}
		t.Run(strings.ReplaceAll(key, " ", "_"), func(t *testing.T) {
			path, body := tc.build(a, b)
			token := a.OwnerToken
			if tc.credential != nil {
				token = tc.credential(a, b)
			}
			code, raw := e.do(rt.Method, path, token, body)
			mustStatus(t, code, raw, tc.want)
			if tc.want == http.StatusNotFound {
				if got := errorCode(t, raw); got != "not_found" {
					t.Fatalf("cross-producer error code = %q, want not_found (never forbidden)", got)
				}
			}
			if tc.leaks != nil {
				for _, leak := range tc.leaks(a, b) {
					if leak != "" && strings.Contains(string(raw), leak) {
						t.Fatalf("producer A's response contains producer B's %q: %s", leak, raw)
					}
				}
			}
			if tc.after != nil {
				tc.after(t, e, raw)
			}
		})
	}

	// AC-15: the refusal is logged, so enumeration is visible rather than
	// indistinguishable from a typo (MT-2's intent).
	logged := logs.String()
	if !strings.Contains(logged, "cross-producer attempt") {
		t.Fatalf("no cross-producer log line after the suite; log was:\n%s", logged)
	}
	if !strings.Contains(logged, b.ConsumerID) {
		t.Fatalf("the log line does not name the requested identifier; log was:\n%s", logged)
	}
}

// producerScoped reports whether a route's credential names a tenant. These are
// exactly the routes AC-20 has to cover: a credential that resolves to one
// producer must never reach another's rows.
func producerScoped(k routeKind) bool {
	switch k {
	case kindProducerKey, kindMember, kindOwner, kindConsumer:
		return true
	}
	return false
}

// TestCrossProducerSuiteCoversEveryRoute is the anti-rot half of AC-20: every
// route in the table is classified, either as producer-scoped (and then it must
// have a cross-producer case) or as not producer-scoped (and then the exclusion
// has to be written down with a reason in notProducerScoped).
//
// The first version of this test skipped every route that was neither kindMember
// nor kindOwner, so /v1/whoami and /v1/consumer/me were never exercised while the
// documentation claimed a route without a case fails the build. The classification
// is asserted over the *superset* of routes (self-registration enabled), so a
// conditional route cannot escape it either.
func TestCrossProducerSuiteCoversEveryRoute(t *testing.T) {
	s := newAuthServer(t, nil, nil, true)
	seen := map[string]bool{}
	scoped := 0

	for _, rt := range s.routes() {
		key := rt.Method + " " + rt.Path
		if seen[key] {
			t.Errorf("route %s is classified twice", key)
		}
		seen[key] = true

		if producerScoped(rt.Kind) {
			scoped++
			if _, ok := crossProducerCases[key]; !ok {
				t.Errorf("producer-scoped route %s has no cross-producer case (AC-20)", key)
			}
			continue
		}
		if reason, ok := notProducerScoped[key]; !ok || strings.TrimSpace(reason) == "" {
			t.Errorf("route %s is not producer-scoped and has no recorded reason in notProducerScoped (AC-20 classifies every route)", key)
		}
	}
	if scoped == 0 {
		t.Fatal("no producer-scoped routes found; the route table is empty or the kinds changed")
	}

	for key := range crossProducerCases {
		if !seen[key] {
			t.Errorf("cross-producer case %s names no registered producer route", key)
		}
	}
	for key := range notProducerScoped {
		if !seen[key] {
			t.Errorf("notProducerScoped %s names no registered route", key)
		}
	}
	if len(seen) != len(s.routes()) {
		t.Fatalf("classified %d routes, table has %d: a route is unclassified", len(seen), len(s.routes()))
	}
	t.Logf("AC-20: %d routes classified, %d producer-scoped with a cross-producer case", len(seen), scoped)
}

// --- small helpers the table needs -----------------------------------------

// e0 is setup plus a log buffer.
func e0(t *testing.T) (*env, *logBuffer) {
	t.Helper()
	return setup(t).withLog(t)
}

// remember records a fixture so other tenants' hooks can find it.
func (e *env) remember(t *testing.T, tn *tenant) {
	t.Helper()
	if e.tenants == nil {
		e.tenants = map[string]*tenant{}
	}
	e.tenants[tn.Slug] = tn
}

// tenantFor returns the fixture this test created for a slug, so the "after"
// hooks can act as the other tenant.
func (e *env) tenantFor(t *testing.T, slug string) *tenant {
	t.Helper()
	if tn, ok := e.tenants[slug]; ok {
		return tn
	}
	t.Fatalf("no fixture recorded for %s", slug)
	return nil
}

// seedCrossProducerEventType registers one live event type for a producer so the
// ingest and event-type routes have a scoped contract to act on. The name is the
// slug with hyphens dropped plus ".order" - the only form the name regex accepts
// from a slug - so the two tenants' types never collide and each is absent from
// the other (they are what the leak checks prove).
func seedCrossProducerEventType(t *testing.T, e *env, tn *tenant) {
	t.Helper()
	tn.EventTypeName = strings.ReplaceAll(tn.Slug, "-", "") + ".order"
	// Event types and events are ingest-credential routes (FR-201): they take the
	// producer API key, not an owner session.
	code, raw := e.do(http.MethodPost, "/v1/event-types", tn.KeyPlain, map[string]any{
		"name":           tn.EventTypeName,
		"payload_schema": map[string]any{"type": "object"},
	})
	mustStatus(t, code, raw, http.StatusCreated)
}
