package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/m7jay/pigeon/internal/ssrf"
)

// TestEndpointSecretIsStandardWebhooksForm: FR-1001 issues secrets as "whsec_" +
// base64 in the STANDARD alphabet. The alphabet is not cosmetic — a spec verifier
// decodes the value after the prefix with a standard decoder, which does not fail
// on the URL-safe "-"/"_": it drops those bytes and derives a short key, so a
// URL-safe secret silently verifies for only about one endpoint in four. Minting
// and signing therefore have to agree on the alphabet, and this is the mint's half.
func TestEndpointSecretIsStandardWebhooksForm(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 200; i++ {
		secret, hint, err := createEndpointSecret()
		if err != nil {
			t.Fatalf("createEndpointSecret: %v", err)
		}
		if !strings.HasPrefix(secret, whsecPrefix) {
			t.Fatalf("secret %q is missing the whsec_ prefix (FR-1001)", secret)
		}
		payload := strings.TrimPrefix(secret, whsecPrefix)
		if strings.ContainsAny(payload, "-_") {
			t.Fatalf("secret %q uses the URL-safe alphabet, which a spec verifier mis-decodes", secret)
		}
		// The mint is unpadded; a spec verifier restores the padding before
		// decoding, and so must this assertion.
		enc := payload
		if pad := len(enc) % 4; pad != 0 {
			enc += strings.Repeat("=", 4-pad)
		}
		raw, err := base64.StdEncoding.DecodeString(enc)
		if err != nil {
			t.Fatalf("secret %q is not standard base64: %v", secret, err)
		}
		if len(raw) != 32 {
			t.Fatalf("secret decodes to %d bytes, want 32", len(raw))
		}
		if hint != secret[:16] {
			t.Fatalf("hint = %q, want the first 16 characters of the secret", hint)
		}
		if seen[secret] {
			t.Fatal("createEndpointSecret repeated a secret")
		}
		seen[secret] = true
	}
}

// AC-19 / FR-402: an endpoint URL pointing at a private or metadata address, or
// http in live, is rejected at registration before any row is written. Fail
// closed: the store never persists a URL it cannot prove is a public https host.
func TestCreateEndpointRejectsSSRF(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	st := New(appPool(t, admin))
	ctx := context.Background()
	pid := seedRoutingProducer(t, admin, "ssrf")
	sc := Scope{ProducerID: pid, Env: "live"}
	con := mustConsumer(t, st, sc, "school_1", "School 1")

	cases := []struct {
		name, url string
	}{
		{"live requires https", "http://public.example.com/hook"},
		{"cloud metadata", "https://169.254.169.254/latest/meta-data/"},
		{"loopback", "https://127.0.0.1/hook"},
		{"private range", "https://10.0.0.5/hook"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := st.CreateEndpoint(ctx, sc, NewEndpoint{ConsumerID: con.ID, Name: "x", URL: c.url})
			if err == nil || !errors.Is(err, ssrf.ErrBlocked) {
				t.Fatalf("CreateEndpoint(%q): err = %v, want ssrf.ErrBlocked", c.url, err)
			}
		})
	}
	// Nothing was written: a blocked registration is a no-op.
	var n int
	if err := admin.QueryRow(ctx, `select count(*) from endpoints where producer_id=$1`, pid).Scan(&n); err != nil {
		t.Fatalf("count endpoints: %v", err)
	}
	if n != 0 {
		t.Fatalf("%d endpoints persisted from blocked URLs, want 0", n)
	}
}

// FR-402 escape hatch: an operator-owned endpoint on a private network may be
// marked internal, which is the only way a private/metadata address is accepted.
func TestCreateEndpointInternalAllowsPrivate(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	st := New(appPool(t, admin))
	ctx := context.Background()
	pid := seedRoutingProducer(t, admin, "ssrf-int")
	sc := Scope{ProducerID: pid, Env: "live"}
	con := mustConsumer(t, st, sc, "school_1", "School 1")

	created, err := st.CreateEndpoint(ctx, sc, NewEndpoint{
		ConsumerID: con.ID, Name: "internal", URL: "https://10.0.0.5/hook", Internal: true,
	})
	if err != nil {
		t.Fatalf("CreateEndpoint(internal): %v", err)
	}
	if !created.Endpoint.Internal {
		t.Fatalf("endpoint not marked internal")
	}
}

// mustEndpointWithFlag registers and verifies an endpoint with FR-402's flag set
// as asked. The shared mustVerifiedEndpoint always registers public, and a private
// URL cannot be registered without the flag (FR-402 refuses it before the row
// exists), so the flag is the only variable here.
func mustEndpointWithFlag(t *testing.T, st *Store, sc Scope, consumerID, name, url string, internal bool) Endpoint {
	t.Helper()
	created, err := st.CreateEndpoint(context.Background(), sc, NewEndpoint{
		ConsumerID: consumerID, Name: name, URL: url, Internal: internal,
	})
	if err != nil {
		t.Fatalf("CreateEndpoint(%s): %v", name, err)
	}
	if err := st.VerifyEndpoint(context.Background(), sc, created.Endpoint.ID, created.VerificationToken); err != nil {
		t.Fatalf("VerifyEndpoint(%s): %v", name, err)
	}
	return created.Endpoint
}

// TestClaimCarriesTheEndpointInternalFlag is FR-402's escape hatch at the boundary
// FR-1005's request-time check reads it from (0022): the claim reports the
// endpoint's own opt-in, so the worker never has to read `endpoints` itself.
//
// That read cannot work and is no longer attempted. This store is on the runtime
// pool, exactly as the worker's is, and the endpoints policies are scoped by
// producer and env - the worker holds no scope when it judges an attempt, so a
// read by id finds no row. It then fails in the direction that silently disables
// the hatch: the flag reads false, an endpoint the operator opted into is treated
// as public, and every attempt to it ends terminally at the SSRF check before a
// socket is opened.
//
// Both directions are asserted, because a claim that answered only one of them
// would satisfy a single-direction test while breaking a requirement: the `true`
// is what lets the opted-in endpoint be dialled at all, and the `false` is what
// keeps AC-19's refusal in place.
func TestClaimCarriesTheEndpointInternalFlag(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	st := New(appPool(t, admin))
	ctx := context.Background()
	pid := seedRoutingProducer(t, admin, "fr402claim")
	sc := Scope{ProducerID: pid, Env: "test"}

	et := mustConsumerEvent(t, st, sc, "probe.created")
	con := mustConsumer(t, st, sc, "loop", "Loopback")

	// The public endpoint is registered at a public literal rather than at a
	// private one with the flag cleared: FR-402 refuses a private URL at
	// registration without the flag, so the flag is the only thing that differs.
	optIn := mustEndpointWithFlag(t, st, sc, con.ID, "sink", "http://127.0.0.1:9001/hook", true)
	public := mustEndpointWithFlag(t, st, sc, con.ID, "receiver", "https://8.8.8.8/hook", false)
	mustSubscribe(t, st, sc, optIn.ID, et.Name)
	mustSubscribe(t, st, sc, public.ID, et.Name)

	pub, err := st.PublishEvent(ctx, sc, PublishInput{
		EventType:   et.Name,
		Payload:     json.RawMessage(`{}`),
		ConsumerRef: ptr(con.Ref),
	})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	if _, err := st.FanOutEvent(ctx, sc, pub.ID); err != nil {
		t.Fatalf("FanOutEvent: %v", err)
	}

	claimed := map[string]bool{}
	for i := 0; i < 2; i++ {
		d, ok, err := st.ClaimDelivery(ctx, 40*time.Second, 72*time.Hour, AttemptClassAny)
		if err != nil {
			t.Fatalf("claim %d: %v", i+1, err)
		}
		if !ok {
			t.Fatalf("claim %d found nothing, want one delivery per subscribed endpoint", i+1)
		}
		claimed[d.EndpointID] = d.EndpointInternal
	}

	got, ok := claimed[optIn.ID]
	if !ok {
		t.Fatalf("no delivery was claimed for the opted-in endpoint %s", optIn.ID)
	}
	if !got {
		t.Error("the claim reported the opted-in endpoint as public, so FR-402's hatch cannot reach FR-1005's request-time check")
	}
	got, ok = claimed[public.ID]
	if !ok {
		t.Fatalf("no delivery was claimed for endpoint %s", public.ID)
	}
	if got {
		t.Error("the claim reported an endpoint without the flag as internal, which widens what the worker may dial (FR-402/AC-19)")
	}
}

// FR-406 / FR-407: pause/resume flips status and stamps the transition; soft
// delete is terminal and retains the row (never orphaning deliveries).
func TestEndpointPauseAndSoftDelete(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	st := New(appPool(t, admin))
	ctx := context.Background()
	pid := seedRoutingProducer(t, admin, "ep-lifecycle")
	sc := Scope{ProducerID: pid, Env: "live"}
	con := mustConsumer(t, st, sc, "school_1", "School 1")
	ep := mustVerifiedEndpoint(t, st, sc, con.ID, "e1", "https://8.8.8.8/hook")

	paused, err := st.SetEndpointPaused(ctx, sc, ep.ID, true)
	if err != nil {
		t.Fatalf("pause: %v", err)
	}
	if paused.Status != "paused" {
		t.Fatalf("pause: status = %q, want paused", paused.Status)
	}
	resumed, err := st.SetEndpointPaused(ctx, sc, ep.ID, false)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if resumed.Status != "active" {
		t.Fatalf("resume: status = %q, want active", resumed.Status)
	}

	if err := st.DeleteEndpoint(ctx, sc, ep.ID); err != nil {
		t.Fatalf("soft delete: %v", err)
	}
	// The row is retained (soft delete), and invisible to a normal get.
	if _, err := st.GetEndpoint(ctx, sc, ep.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get after delete: err = %v, want ErrNotFound", err)
	}
	var stillThere bool
	if err := admin.QueryRow(ctx,
		`select true from endpoints where producer_id=$1 and id=$2 and status='deleted'`, pid, ep.ID).Scan(&stillThere); err != nil {
		t.Fatalf("row not retained after soft delete: %v", err)
	}
	if !stillThere {
		t.Fatalf("soft delete did not retain the row")
	}
}

// FR-506: a consumer ref is immutable everywhere but RekeyConsumer, which rejects
// a duplicate ref (conflict) and an unknown consumer (not found).
func TestRekeyConsumer(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	st := New(appPool(t, admin))
	ctx := context.Background()
	pid := seedRoutingProducer(t, admin, "rekey")
	sc := Scope{ProducerID: pid, Env: "live"}
	a := mustConsumer(t, st, sc, "school_A", "School A")
	mustConsumer(t, st, sc, "school_B", "School B")

	// Move A to a new ref.
	moved, err := st.RekeyConsumer(ctx, sc, a.ID, "school_1")
	if err != nil {
		t.Fatalf("rekey: %v", err)
	}
	if moved.Ref != "school_1" {
		t.Fatalf("rekey: ref = %q, want school_1", moved.Ref)
	}
	// The old ref no longer resolves to A.
	if _, err := st.GetConsumerByRef(ctx, sc, "school_A"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old ref still resolves: err = %v", err)
	}
	// A duplicate (B still holds school_B) is a conflict.
	if _, err := st.RekeyConsumer(ctx, sc, a.ID, "school_B"); !errors.Is(err, ErrConflict) {
		t.Fatalf("rekey to a held ref: err = %v, want ErrConflict", err)
	}
	// An unknown consumer is not found.
	if _, err := st.RekeyConsumer(ctx, sc, "con_nope", "school_x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rekey unknown: err = %v, want ErrNotFound", err)
	}
}

// FR-501: re-subscribing an existing (endpoint, event_type) pair is a conflict,
// and a soft-deleted subscription can be recreated.
func TestSubscriptionLifecycle(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	st := New(appPool(t, admin))
	ctx := context.Background()
	pid := seedRoutingProducer(t, admin, "sub")
	sc := Scope{ProducerID: pid, Env: "live"}
	et := mustConsumerEvent(t, st, sc, "signup.completed")
	con := mustConsumer(t, st, sc, "school_1", "School 1")
	ep := mustVerifiedEndpoint(t, st, sc, con.ID, "e1", "https://8.8.8.8/hook")

	first, err := st.CreateSubscription(ctx, sc, NewSubscription{EndpointID: ep.ID, EventType: et.Name})
	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	if first.Status != "active" {
		t.Fatalf("subscription status = %q, want active", first.Status)
	}
	// Duplicate pair is a conflict.
	if _, err := st.CreateSubscription(ctx, sc, NewSubscription{EndpointID: ep.ID, EventType: et.Name}); !errors.Is(err, ErrConflict) {
		t.Fatalf("re-subscribe: err = %v, want ErrConflict", err)
	}
	// Soft delete then recreate.
	if err := st.DeleteSubscription(ctx, sc, ep.ID, et.Name); err != nil {
		t.Fatalf("delete subscription: %v", err)
	}
	if _, err := st.CreateSubscription(ctx, sc, NewSubscription{EndpointID: ep.ID, EventType: et.Name}); err != nil {
		t.Fatalf("recreate after soft delete: %v", err)
	}
}
