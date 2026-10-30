package httpapi

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/m7jay/pigeon/internal/envelope"
	"github.com/m7jay/pigeon/internal/identity"
	"github.com/m7jay/pigeon/internal/oidc"
	"github.com/m7jay/pigeon/internal/ratelimit"
	"github.com/m7jay/pigeon/internal/store"
)

// This file is S9a's API acceptance suite for the three surfaces the slice added
// and S9a is the first to reach over HTTP:
//
//   - the producer portal's member routes (producerportal.go): event types, the
//     synthetic publish, the usage panel, one consumer's unroutable events and
//     the producer-side secret rotation;
//   - FR-111's anonymous password reset (passwordreset.go) - both routes, whose
//     whole design is that asking cannot enumerate accounts and accepting grants
//     a credential rather than a claim;
//   - FR-106's optional OIDC login (oidc.go).
//
// Everything runs through the shared harness in api_harness_test.go against a
// real PostgreSQL, as the non-superuser application role. The member routes are
// the point of the exercise: they are the same operations as the key routes, but
// the producer and the user come from the session claim, so the tests here aim a
// session at another tenant's identifiers and prove the claim wins.

// childServer builds a second API over the same database and store as e, with
// one extra dependency wired. It is how a test injects a notifier or an OIDC
// verifier without disturbing the shared harness (sync.Once) that every other
// test uses.
func (e *env) childServer(t *testing.T, extra Deps) *env {
	t.Helper()
	extra.Lookup = e.st.LookupAPIKey
	extra.Sessions = e.st.LookupSession
	extra.Data = e.st
	extra.TouchKey = e.st.TouchKey
	if extra.Version == "" {
		extra.Version = "test-s9a"
	}
	if extra.SessionTTL <= 0 {
		extra.SessionTTL = time.Hour
	}
	extra.KeyLimiter = ratelimit.New(1000, 1000)
	extra.ProducerLimiter = ratelimit.New(1000, 1000)
	api := New(extra, slog.New(slog.NewTextHandler(io.Discard, nil)))
	out := &env{admin: e.admin, app: e.app, st: e.st, api: api, adminToken: e.adminToken, tenants: e.tenants}
	out.srv = httptest.NewServer(api.Handler())
	t.Cleanup(out.srv.Close)
	return out
}

// TestProducerRoutesRefuseAMachineKeyAndAConsumerToken is the credential half of
// the member surface: the /v1/producer/... routes are session-only (FR-112's
// portal session, not FR-201's machine key), and a consumer session - a real
// session that names no producer role - does not find the route at all.
func TestProducerRoutesRefuseAMachineKeyAndAConsumerToken(t *testing.T) {
	e := setup(t)
	tn := e.seedTenant(t, "s9a-refuse")

	// The event-type list and the publish route are the two the slice is for.
	routes := []struct {
		method, path string
		body         any
	}{
		{http.MethodGet, "/v1/producer/event-types?env=live", nil},
		{http.MethodPost, "/v1/producer/event-types?env=live", map[string]any{
			"name": "refuse.event", "payload_schema": map[string]any{"type": "object"},
		}},
		{http.MethodPost, "/v1/producer/events?env=live", map[string]any{
			"event_type": "refuse.event", "payload": map[string]any{},
		}},
	}
	for _, rt := range routes {
		// A producer API key is a machine credential: the session middleware
		// refuses it by prefix before any handler runs (FR-208's separation).
		code, raw := e.do(rt.method, rt.path, tn.KeyPlain, rt.body)
		if code != http.StatusUnauthorized && code != http.StatusForbidden {
			t.Fatalf("machine key on %s %s answered %d (%s), want 401 or 403", rt.method, rt.path, code, raw)
		}
		// A consumer session authenticates, but it names no producer role, so
		// the route does not exist for it: 404, never a 200 and never a 403 that
		// would admit the route exists (FR-108).
		code, raw = e.do(rt.method, rt.path, tn.ConsumerToken, rt.body)
		switch code {
		case http.StatusNotFound, http.StatusForbidden:
		default:
			t.Fatalf("consumer token on %s %s answered %d (%s), want 404 or 403, never a success", rt.method, rt.path, code, raw)
		}
	}
}

// TestProducerPublishTakesTheProducerFromTheSessionClaim is the member publish's
// scoping rule: the producer comes from the session claim, so a body field that
// names another producer - even its consumer's real ref - cannot move the row.
// The foreign ref resolves to no consumer in the caller's environment, which is
// FR-508's unroutable outcome rather than a delivery to the sibling.
func TestProducerPublishTakesTheProducerFromTheSessionClaim(t *testing.T) {
	e := setup(t)
	a := e.seedInspectTenant(t, "s9a-claim-a")
	b := e.seedTenant(t, "s9a-claim-b")
	ctx := context.Background()

	code, raw := e.do(http.MethodPost, "/v1/producer/events?env=live", a.OwnerToken, map[string]any{
		"event_type":   a.EventTypeName,
		"consumer_ref": b.ConsumerRef,
		"payload":      map[string]any{"note": "session-claim-probe"},
	})
	mustStatus(t, code, raw, http.StatusAccepted)
	pub := decode[publishResponse](t, raw)

	var producerID string
	if err := e.admin.QueryRow(ctx, `select producer_id from events where id = $1`, pub.ID).Scan(&producerID); err != nil {
		t.Fatalf("read the published event: %v", err)
	}
	if producerID != a.ID {
		t.Fatalf("member publish landed under producer %q, want the session's producer %q", producerID, a.ID)
	}
	if n := e.countRows(t, `select count(*) from events where id = $1 and producer_id = $2`, pub.ID, b.ID); n != 0 {
		t.Fatalf("the event is visible in producer B (%d rows); a body field redirected the publish", n)
	}

	// Fan-out is what resolves the ref, and it runs under A's scope: B's ref is
	// a miss, so the outcome is recorded and nothing is written to a delivery.
	res := e.fanOutRaw(t, a, pub.ID)
	if res.EventReason != store.ReasonUnroutableNotRegistered || res.Deliveries != 0 {
		t.Fatalf("fan-out of a foreign consumer_ref = %+v, want %q and zero deliveries", res, store.ReasonUnroutableNotRegistered)
	}
	view := decode[eventViewJSONBody](t, bodyOf(t, e, http.MethodGet,
		"/v1/producer/events/"+pub.ID+"?env=live", a.OwnerToken, nil))
	if view.Event.RoutingState == nil || *view.Event.RoutingState != "unroutable" {
		t.Fatalf("event routing_state = %v, want unroutable", view.Event.RoutingState)
	}
	if view.Event.NonDeliveryReason == nil || *view.Event.NonDeliveryReason != store.ReasonUnroutableNotRegistered {
		t.Fatalf("event non_delivery_reason = %v, want %q", view.Event.NonDeliveryReason, store.ReasonUnroutableNotRegistered)
	}
	if n := e.countRows(t, `select count(*) from deliveries where producer_id = $1 and event_id = $2`, b.ID, pub.ID); n != 0 {
		t.Fatalf("producer B has %d deliveries for producer A's event", n)
	}

	// A body field that names a producer directly is not a field the publish
	// accepts at all, so it can never move the scope either.
	code, raw = e.do(http.MethodPost, "/v1/producer/events?env=live", a.OwnerToken, map[string]any{
		"event_type":  a.EventTypeName,
		"producer_id": b.ID,
		"payload":     map[string]any{},
	})
	mustStatus(t, code, raw, http.StatusBadRequest)
}

// TestTestPublishIsMarkedSyntheticEverywhere is UI-4's API leg: a `test: true`
// publish is answered 202, and the synthetic marker is durable on the row, so it
// is visible on the event view and in the producer delivery list - not just on
// the page that sent it. A normal publish is not marked.
func TestTestPublishIsMarkedSyntheticEverywhere(t *testing.T) {
	e := setup(t)
	tn := e.seedInspectTenant(t, "s9a-test-publish")

	publish := func(test bool) (publishResponse, []byte) {
		t.Helper()
		body := map[string]any{
			"event_type":   tn.EventTypeName,
			"consumer_ref": tn.ConsumerRef,
			"payload":      map[string]any{"note": "synthetic-probe"},
		}
		if test {
			body["test"] = true
		}
		code, raw := e.do(http.MethodPost, "/v1/producer/events?env=live", tn.OwnerToken, body)
		mustStatus(t, code, raw, http.StatusAccepted)
		return decode[publishResponse](t, raw), raw
	}

	synth, synthRaw := publish(true)
	if !synth.IsTest {
		t.Fatalf("202 for a test publish says is_test=false: %s", synthRaw)
	}
	normal, normalRaw := publish(false)
	if normal.IsTest {
		t.Fatalf("a normal publish was answered is_test=true: %s", normalRaw)
	}

	// Both events fan out so the delivery list has a row of each to compare.
	e.fanOut(t, tn, synth.ID)
	e.fanOut(t, tn, normal.ID)

	sv := decode[eventViewJSONBody](t, bodyOf(t, e, http.MethodGet,
		"/v1/producer/events/"+synth.ID+"?env=live", tn.OwnerToken, nil))
	if !sv.Event.IsTest {
		t.Fatalf("the event view does not mark a test event as is_test: %+v", sv.Event)
	}
	nv := decode[eventViewJSONBody](t, bodyOf(t, e, http.MethodGet,
		"/v1/producer/events/"+normal.ID+"?env=live", tn.OwnerToken, nil))
	if nv.Event.IsTest {
		t.Fatalf("the event view marks a normal event as is_test: %+v", nv.Event)
	}

	list := decode[deliveryListJSON](t, bodyOf(t, e, http.MethodGet,
		"/v1/producer/deliveries?env=live&endpoint_id="+tn.EndpointID, tn.OwnerToken, nil))
	marked := map[string]bool{}
	for _, d := range list.Deliveries {
		marked[d.EventID] = d.IsTest
	}
	if _, ok := marked[synth.ID]; !ok {
		t.Fatalf("the producer delivery list has no delivery for the test event %s: %+v", synth.ID, list.Deliveries)
	}
	if !marked[synth.ID] {
		t.Fatalf("the producer delivery list does not mark the test event's delivery as is_test")
	}
	if marked[normal.ID] {
		t.Fatalf("the producer delivery list marks a normal event's delivery as is_test")
	}
}

// endpointRotatedWithWindowBody is the wire shape of the producer rotation. It is
// declared here rather than reusing an unexported handler type so the test
// decodes exactly the documented contract and will fail if a field is renamed.
type endpointRotatedWithWindowBody struct {
	SigningSecret           string  `json:"signing_secret"`
	SigningSecretHint       string  `json:"signing_secret_hint"`
	PreviousSecretExpiresAt *string `json:"previous_secret_expires_at"`
	OverlapSeconds          int     `json:"overlap_seconds"`
}

// verifyStandardWebhook independently recomputes a Standard Webhooks signature
// the way a receiver does (FR-1001/FR-1002): strip the `whsec_` wrapper, pad,
// standard base64, HMAC-SHA256 over "<id>.<timestamp>.<body>", `v1,` prefix. It is
// the same construction internal/delivery's verifySignature uses, reimplemented
// here because that helper is unexported and in another package - sharing the
// signer's decode would let both sides be wrong together.
func verifyStandardWebhook(t *testing.T, secret, id, ts, body, sig string) bool {
	t.Helper()
	enc := strings.TrimPrefix(secret, "whsec_")
	if enc == secret || enc == "" {
		t.Fatalf("secret %q is not a whsec_ value (FR-1001)", secret)
	}
	if pad := len(enc) % 4; pad != 0 {
		enc += strings.Repeat("=", 4-pad)
	}
	key, err := base64.StdEncoding.DecodeString(enc)
	if err != nil || len(key) == 0 {
		t.Fatalf("secret is not standard base64: %v", err)
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(id + "." + ts + "."))
	_, _ = mac.Write([]byte(body))
	want := "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(sig), []byte(want))
}

// TestProducerRotateEndpointSecretOpensAnOverlapWindow is UI-6's producer-side
// rotation: the route returns the new secret, its hint, the overlap it opened and
// when the previous secret stops verifying, and the outgoing secret survives the
// window on the row, so a receiver still keyed on it verifies (FR-1004). A member
// of another producer cannot aim it at their consumer or endpoint id: 404.
func TestProducerRotateEndpointSecretOpensAnOverlapWindow(t *testing.T) {
	e := setup(t)
	tn := e.seedTenant(t, "s9a-rotate-a")
	other := e.seedTenant(t, "s9a-rotate-b")
	ctx := context.Background()

	// Register an endpoint through the producer path so the old secret is known
	// to this test the way it is known to the producer: exactly once, at creation.
	code, raw := e.do(http.MethodPost, "/v1/producer/consumers/"+tn.ConsumerID+"/endpoints?env=live", tn.OwnerToken,
		map[string]string{"name": "rotate-probe", "url": "https://8.8.8.8/hook"})
	mustStatus(t, code, raw, http.StatusCreated)
	created := decode[endpointCreatedJSON](t, raw)
	oldSecret := created.SigningSecret
	if !strings.HasPrefix(oldSecret, "whsec_") {
		t.Fatalf("registration did not reveal a whsec_ secret: %+v", created)
	}

	before := time.Now()
	code, raw = e.do(http.MethodPost,
		"/v1/producer/consumers/"+tn.ConsumerID+"/endpoints/"+created.ID+"/rotate?env=live", tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusCreated)
	rot := decode[endpointRotatedWithWindowBody](t, raw)

	if rot.SigningSecret == "" || rot.SigningSecret == oldSecret {
		t.Fatalf("rotation returned %q, want a secret different from the outgoing one", rot.SigningSecret)
	}
	if !strings.HasPrefix(rot.SigningSecret, "whsec_") || rot.SigningSecretHint == "" {
		t.Fatalf("rotation did not return a whsec_ secret and its hint: %s", raw)
	}
	if want := int(store.SecretOverlapTTL / time.Second); rot.OverlapSeconds != want {
		t.Fatalf("overlap_seconds = %d, want %d (FR-1004)", rot.OverlapSeconds, want)
	}
	if rot.PreviousSecretExpiresAt == nil {
		t.Fatalf("rotation of an endpoint with an outgoing secret reported no previous_secret_expires_at: %s", raw)
	}
	expires, err := time.Parse(time.RFC3339, *rot.PreviousSecretExpiresAt)
	if err != nil {
		t.Fatalf("previous_secret_expires_at %q is not RFC 3339: %v", *rot.PreviousSecretExpiresAt, err)
	}
	if d := expires.Sub(before); d < store.SecretOverlapTTL-time.Minute || d > store.SecretOverlapTTL+time.Minute {
		t.Fatalf("the overlap expires in %s, want about %s (FR-1004)", d, store.SecretOverlapTTL)
	}

	// The row carries the outgoing secret and an expiry still in the future -
	// which is what makes it verifiable during the window.
	var previous *string
	var rowExpiry *time.Time
	var current string
	if err := e.admin.QueryRow(ctx,
		`select previous_signing_secret, previous_secret_expires_at, signing_secret from endpoints where id = $1`,
		created.ID).Scan(&previous, &rowExpiry, &current); err != nil {
		t.Fatalf("read the endpoint row: %v", err)
	}
	if previous == nil || *previous != oldSecret {
		t.Fatal("the rotation did not carry the outgoing secret forward; a receiver still on it could not verify (FR-1004)")
	}
	if rowExpiry == nil || !rowExpiry.After(time.Now()) {
		t.Fatalf("previous_secret_expires_at = %v, want a window still open", rowExpiry)
	}
	if current != rot.SigningSecret {
		t.Fatalf("the row's signing_secret is not the newly returned one")
	}

	// A receiver holding the OLD secret verifies a delivery signed by the
	// signer's construction, and the NEW secret is a different key - so the
	// overlap is real rather than the two strings merely differing.
	ev := envelope.Envelope{
		ID: "evt_s9a_rotate_probe", Type: "probe.rotate", Version: "1.0",
		ProducerID: tn.ID, ConsumerRef: tn.ConsumerRef, Attempt: 1,
		CreatedAt: time.Now(), Data: json.RawMessage(`{"note":"rotate"}`),
	}
	ts := envelope.WebhookTimestamp(time.Now())
	body := string(envelope.MustMarshal(ev))
	oldKey, err := envelope.SecretBytes(oldSecret)
	if err != nil {
		t.Fatalf("decode the outgoing secret: %v", err)
	}
	newKey, err := envelope.SecretBytes(rot.SigningSecret)
	if err != nil {
		t.Fatalf("decode the rotated secret: %v", err)
	}
	oldSig := ev.Signature(oldKey, ts, body)
	newSig := ev.Signature(newKey, ts, body)
	if oldSig == newSig {
		t.Fatal("the outgoing and rotated secrets sign identically; the rotation did not change the key")
	}
	if !verifyStandardWebhook(t, oldSecret, ev.ID, ts, body, oldSig) {
		t.Fatal("a receiver holding the outgoing secret cannot verify during the overlap window")
	}
	if !verifyStandardWebhook(t, rot.SigningSecret, ev.ID, ts, body, newSig) {
		t.Fatal("a receiver holding the rotated secret cannot verify")
	}
	if verifyStandardWebhook(t, rot.SigningSecret, ev.ID, ts, body, oldSig) {
		t.Fatal("the rotated secret verified a signature made with the outgoing one; the two keys are not distinct")
	}

	// Cross-producer: A's owner aiming the rotation at B's consumer/endpoint is
	// a miss, not a 403 that admits the resource exists (FR-108, AC-39).
	code, raw = e.do(http.MethodPost,
		"/v1/producer/consumers/"+other.ConsumerID+"/endpoints/"+other.EndpointID+"/rotate?env=live", tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusNotFound)
}

// recordingResetNotifier is the FR-111 delivery seam under test: it captures the
// tokens the API hands out so the accept flow (and the "exactly once" assertion
// in the enumeration test) can see them, since the token never appears in an HTTP
// response.
type recordingResetNotifier struct {
	mu    sync.Mutex
	mails []resetMail
}

type resetMail struct {
	Email string
	Token string
}

func (n *recordingResetNotifier) notify(_ context.Context, email, token string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.mails = append(n.mails, resetMail{Email: email, Token: token})
	return nil
}

// mailsFor returns the tokens this notifier was handed for one address, in order.
func (n *recordingResetNotifier) mailsFor(email string) []resetMail {
	n.mu.Lock()
	defer n.mu.Unlock()
	var out []resetMail
	for _, m := range n.mails {
		if m.Email == email {
			out = append(out, m)
		}
	}
	return out
}

func (n *recordingResetNotifier) count() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.mails)
}

// TestPasswordResetRequestCannotEnumerateAccounts is FR-111's first property: the
// public request endpoint answers 202 with a byte-identical body whether or not
// the address has an account, the body carries no token, and the notifier is
// called exactly for the address that exists.
func TestPasswordResetRequestCannotEnumerateAccounts(t *testing.T) {
	e := setup(t)
	notifier := &recordingResetNotifier{}
	pe := e.childServer(t, Deps{
		NotifyPasswordReset: notifier.notify,
		PasswordResetTTL:    time.Hour,
	})
	tn := pe.seedTenant(t, "s9a-reset-enum")

	const missing = "s9a-reset-missing@example.test"
	codeExisting, bodyExisting := pe.do(http.MethodPost, "/v1/password-resets", "", map[string]string{"email": tn.OwnerEmail})
	mustStatus(t, codeExisting, bodyExisting, http.StatusAccepted)
	codeMissing, bodyMissing := pe.do(http.MethodPost, "/v1/password-resets", "", map[string]string{"email": missing})
	mustStatus(t, codeMissing, bodyMissing, http.StatusAccepted)

	if !bytes.Equal(bodyExisting, bodyMissing) {
		t.Fatalf("the response differs for an existing vs a missing address; that is an enumeration oracle:\n exists: %s\n missing: %s", bodyExisting, bodyMissing)
	}
	if strings.Contains(string(bodyExisting), store.ResetPrefix) {
		t.Fatalf("the request response carries a reset token: %s", bodyExisting)
	}

	if got := notifier.count(); got != 1 {
		t.Fatalf("the notifier was called %d times for one existing and one missing address, want exactly 1 (the existing one): %+v", got, notifier.mails)
	}
	mail := notifier.mailsFor(tn.OwnerEmail)
	if len(mail) != 1 {
		t.Fatalf("the notifier was not called for the existing address %s", tn.OwnerEmail)
	}
	if !strings.HasPrefix(mail[0].Token, store.ResetPrefix) {
		t.Fatalf("the delivered token %q does not carry the reset namespace %q", mail[0].Token, store.ResetPrefix)
	}
	if got := notifier.mailsFor(missing); len(got) != 0 {
		t.Fatalf("the notifier was called for an address with no account: %+v", got)
	}
}

// TestPasswordResetAcceptRewritesOnlyTheCredential is FR-111's second property:
// accepting a token is 204, rewrites exactly one account's password, revokes that
// account's sessions, burns the token, and grants no producer role.
//
// The reset acts on a member account with no producer to claim (an instance
// admin), which makes "no producer role" directly observable: the new session
// still carries no producer_id claim. A reset token minted for a producer
// member's address is shown to authenticate nothing on a producer route.
func TestPasswordResetAcceptRewritesOnlyTheCredential(t *testing.T) {
	e := setup(t)
	notifier := &recordingResetNotifier{}
	pe := e.childServer(t, Deps{
		NotifyPasswordReset: notifier.notify,
		PasswordResetTTL:    time.Hour,
	})
	ctx := context.Background()

	const (
		oldPassword = "s9a-reset-old-password-1"
		newPassword = "s9a-reset-new-password-1"
		email       = "s9a-reset-account@example.test"
		userID      = "usr_s9a_reset_account"
	)
	hash, err := identity.HashPassword(oldPassword)
	if err != nil {
		t.Fatalf("hash the fixture password: %v", err)
	}
	if _, err := e.admin.Exec(ctx,
		`insert into users (id, email, password_hash, is_instance_admin) values ($1, $2, $3, true)`,
		userID, email, hash); err != nil {
		t.Fatalf("seed the reset account: %v", err)
	}
	t.Cleanup(func() {
		_, _ = e.admin.Exec(context.Background(), `delete from users where id = $1`, userID)
	})

	// Two live sessions, so revocation has something to revoke beyond the one
	// the accept flow would otherwise be measured against.
	first := pe.login(email, oldPassword, "")
	second := pe.login(email, oldPassword, "")

	code, raw := pe.do(http.MethodPost, "/v1/password-resets", "", map[string]string{"email": email})
	mustStatus(t, code, raw, http.StatusAccepted)
	mails := notifier.mailsFor(email)
	if len(mails) != 1 {
		t.Fatalf("the request did not hand exactly one token to the notifier: %+v", mails)
	}
	token := mails[0].Token

	code, raw = pe.do(http.MethodPost, "/v1/password-resets/accept", "",
		map[string]string{"token": token, "new_password": newPassword})
	mustStatus(t, code, raw, http.StatusNoContent)

	// The OLD password no longer authenticates.
	code, raw = pe.do(http.MethodPost, "/v1/session", "", map[string]string{"email": email, "password": oldPassword})
	mustStatus(t, code, raw, http.StatusUnauthorized)

	// Every session the account had is dead.
	for name, tok := range map[string]string{"first": first.Token, "second": second.Token} {
		if code, _ := pe.do(http.MethodGet, "/v1/session", tok, nil); code != http.StatusUnauthorized {
			t.Fatalf("the %s session still authenticates after the reset: %d", name, code)
		}
	}

	// The NEW password authenticates, and the session it mints carries no
	// producer role: a reset granted a credential, never a claim.
	sess := pe.login(email, newPassword, "")
	if !sess.Claims.InstanceAdmin {
		t.Fatalf("the reset account lost its admin claim: %+v", sess.Claims)
	}
	if sess.Claims.ProducerID != "" || sess.Claims.Role != "" {
		t.Fatalf("the new session carries a producer claim after a reset: producer_id=%q role=%q", sess.Claims.ProducerID, sess.Claims.Role)
	}
	if n := e.countRows(t, `select count(*) from producer_members where user_id = $1`, userID); n != 0 {
		t.Fatalf("a password reset added %d producer memberships to the account", n)
	}

	// The same token is refused on a second use: one answer for a spent token,
	// with no hint that it was once valid.
	code, raw = pe.do(http.MethodPost, "/v1/password-resets/accept", "",
		map[string]string{"token": token, "new_password": "s9a-reset-third-password-1"})
	mustStatus(t, code, raw, http.StatusNotFound)
	if got := errorCode(t, raw); got != "not_found" {
		t.Fatalf("a spent reset token answered error code %q, want not_found", got)
	}

	// A reset token minted for a producer member's address is not a credential
	// for anything: presented as a bearer it is not a session and hits no
	// producer route.
	member := pe.seedTenant(t, "s9a-reset-member")
	code, raw = pe.do(http.MethodPost, "/v1/password-resets", "", map[string]string{"email": member.DeveloperEmail})
	mustStatus(t, code, raw, http.StatusAccepted)
	memberMails := notifier.mailsFor(member.DeveloperEmail)
	if len(memberMails) != 1 {
		t.Fatalf("no reset token was minted for the producer member %s: %+v", member.DeveloperEmail, memberMails)
	}
	memberToken := memberMails[0].Token
	if code, _ := pe.do(http.MethodGet, "/v1/producer/members", memberToken, nil); code != http.StatusUnauthorized {
		t.Fatalf("a reset token reached a producer route: %d", code)
	}
	if code, _ := pe.do(http.MethodGet, "/v1/session", memberToken, nil); code != http.StatusUnauthorized {
		t.Fatalf("a reset token authenticated as a session: %d", code)
	}
}

// fakeOIDCVerifier implements OIDCVerifier with fixed claims (or a fixed error),
// so every rejection path in handleOIDCLogin can be driven without a live issuer.
type fakeOIDCVerifier struct {
	claims oidc.Claims
	err    error
}

func (f *fakeOIDCVerifier) VerifyIDToken(context.Context, string) (oidc.Claims, error) {
	if f.err != nil {
		return oidc.Claims{}, f.err
	}
	return f.claims, nil
}

// TestOIDCLoginMintsASessionForAMappedIdentity is FR-106's happy path and its
// three refusals: a verified id_token whose nonce matches mints a session for the
// EXISTING account the verified address maps to; a nonce mismatch, an unknown
// address or a verifier error is a 401 that mints nothing.
func TestOIDCLoginMintsASessionForAMappedIdentity(t *testing.T) {
	e := setup(t)
	tn := e.seedTenant(t, "s9a-oidc-map")
	verifier := &fakeOIDCVerifier{claims: oidc.Claims{
		Email: tn.OwnerEmail, Nonce: "nonce-1", EmailVerified: true, Subject: "sub-1",
	}}
	oe := e.childServer(t, Deps{OIDC: verifier})

	sessionCount := func() int {
		return e.countRows(t, `select count(*) from sessions where user_id = (select id from users where email = $1)`, tn.OwnerEmail)
	}

	code, raw := oe.do(http.MethodPost, "/v1/session/oidc", "",
		map[string]string{"id_token": "signed.jwt.value", "nonce": "nonce-1"})
	mustStatus(t, code, raw, http.StatusCreated)
	created := decode[sessionPayload](t, raw)
	if created.Token == "" {
		t.Fatalf("OIDC login did not mint a session token: %s", raw)
	}
	if created.Claims.ProducerID != tn.ID {
		t.Fatalf("OIDC session producer_id = %q, want the mapped account's %q", created.Claims.ProducerID, tn.ID)
	}
	// The minted token actually authenticates.
	code, raw = oe.do(http.MethodGet, "/v1/session", created.Token, nil)
	mustStatus(t, code, raw, http.StatusOK)

	// A nonce mismatch is refused and mints nothing.
	before := sessionCount()
	code, raw = oe.do(http.MethodPost, "/v1/session/oidc", "",
		map[string]string{"id_token": "signed.jwt.value", "nonce": "not-the-nonce"})
	mustStatus(t, code, raw, http.StatusUnauthorized)
	if after := sessionCount(); after != before {
		t.Fatalf("a nonce mismatch minted a session (%d -> %d)", before, after)
	}

	// An unknown address is refused (no account, so no session is created).
	verifier.claims = oidc.Claims{Email: "s9a-oidc-nobody@example.test", Nonce: "nonce-1", EmailVerified: true}
	code, raw = oe.do(http.MethodPost, "/v1/session/oidc", "",
		map[string]string{"id_token": "signed.jwt.value", "nonce": "nonce-1"})
	mustStatus(t, code, raw, http.StatusUnauthorized)

	// A verifier error is refused.
	verifier.err = errors.New("bad signature")
	code, raw = oe.do(http.MethodPost, "/v1/session/oidc", "",
		map[string]string{"id_token": "signed.jwt.value", "nonce": "nonce-1"})
	mustStatus(t, code, raw, http.StatusUnauthorized)

	// A missing token or nonce is the caller's error, told apart from a refusal.
	verifier.err = nil
	verifier.claims = oidc.Claims{Email: tn.OwnerEmail, Nonce: "", EmailVerified: true}
	code, raw = oe.do(http.MethodPost, "/v1/session/oidc", "",
		map[string]string{"id_token": "signed.jwt.value"})
	mustStatus(t, code, raw, http.StatusBadRequest)
}

// TestOIDCLoginIsAbsentWithoutAProvider is FR-106's "optional means absent": with
// no provider configured the route answers 404, and every other route behaves
// exactly as it does with a provider wired.
func TestOIDCLoginIsAbsentWithoutAProvider(t *testing.T) {
	e := setup(t) // the shared harness wires no OIDC provider.

	code, raw := e.do(http.MethodPost, "/v1/session/oidc", "",
		map[string]string{"id_token": "signed.jwt.value", "nonce": "nonce-1"})
	mustStatus(t, code, raw, http.StatusNotFound)
	if got := errorCode(t, raw); got != "not_found" {
		t.Fatalf("the absent OIDC route answered error code %q, want not_found", got)
	}

	// No other route changes behaviour: password login still works and a member
	// route still answers for the session it mints.
	tn := e.seedTenant(t, "s9a-oidc-off")
	sess := e.login(tn.OwnerEmail, memberPassword, "")
	if code, raw := e.do(http.MethodGet, "/v1/producer/members", sess.Token, nil); code != http.StatusOK {
		t.Fatalf("a producer route answered %d with OIDC unconfigured: %s", code, raw)
	}
}
