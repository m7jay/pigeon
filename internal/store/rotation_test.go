package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// TestRotateKeepsThePreviousSecretForTheOverlap is FR-1004's storage half: a
// rotation moves the outgoing secret into previous_signing_secret with an expiry
// SecretOverlapTTL out, and the next claim carries both so the worker can sign
// with both.
//
// The claim is asserted through the real pigeon_claim_delivery rather than by
// reading the row: the value has to travel in the claim or the signer cannot see
// it, and a test that read the table would pass while the function dropped the
// column.
func TestRotateKeepsThePreviousSecretForTheOverlap(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	ctx := context.Background()
	pid := seedRoutingProducer(t, admin, "s10rot")
	sc := Scope{ProducerID: pid, Env: "live"}
	st := New(appPool(t, admin))

	et := mustConsumerEvent(t, st, sc, "order.created")
	con := mustConsumer(t, st, sc, "c1", "Consumer One")
	// A public literal: verification re-checks the address (FR-1005), and a name
	// that does not resolve would be refused for a reason unrelated to rotation.
	ep := mustVerifiedEndpoint(t, st, sc, con.ID, "hook", "https://8.8.8.8/hook")
	mustSubscribe(t, st, sc, ep.ID, et.Name)

	first, err := st.RotateConsumerEndpointSecret(ctx, sc, con.ID, ep.ID)
	if err != nil {
		t.Fatalf("first rotation: %v", err)
	}
	second, err := st.RotateConsumerEndpointSecret(ctx, sc, con.ID, ep.ID)
	if err != nil {
		t.Fatalf("second rotation: %v", err)
	}
	if first.Secret == second.Secret {
		t.Fatal("two rotations minted the same secret")
	}

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

	d, got, err := st.ClaimDelivery(ctx, 40*time.Second, 72*time.Hour, AttemptClassAny)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if !got {
		t.Fatal("no delivery was claimable after fan-out")
	}
	if d.SigningSecret != second.Secret {
		t.Errorf("claim carries signing_secret %q, want the newest rotation %q",
			hintOf(d.SigningSecret), hintOf(second.Secret))
	}
	if d.PreviousSigningSecret != first.Secret {
		t.Errorf("claim carries previous_signing_secret %q, want the secret the second rotation replaced (%q)",
			hintOf(d.PreviousSigningSecret), hintOf(first.Secret))
	}
	if d.PreviousSecretExpiresAt == nil {
		t.Fatal("the claim carries no previous_secret_expires_at, so the signer cannot honour \"up to 24 h\"")
	}
	until := time.Until(*d.PreviousSecretExpiresAt)
	if until < SecretOverlapTTL-time.Minute || until > SecretOverlapTTL+time.Minute {
		t.Errorf("the overlap expires in %s, want %s (FR-1004)", until, SecretOverlapTTL)
	}
}

// TestClaimScansAnEndpointThatHasNeverRotated guards the claim against the NULL
// that FR-1004's columns hold by default. It is a regression test with a live
// origin: pgx will not scan NULL into a *string, so passing the address of the
// plain-string field made every claim fail with "cannot scan NULL into *string" on
// any endpoint that had not rotated - which is all of them, on a fresh install.
// The store test for the rotation itself could not catch it, because a rotated
// endpoint's previous_signing_secret is not NULL.
func TestClaimScansAnEndpointThatHasNeverRotated(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	ctx := context.Background()
	pid := seedRoutingProducer(t, admin, "s10null")
	sc := Scope{ProducerID: pid, Env: "live"}
	st := New(appPool(t, admin))

	et := mustConsumerEvent(t, st, sc, "order.created")
	con := mustConsumer(t, st, sc, "c1", "Consumer One")
	ep := mustVerifiedEndpoint(t, st, sc, con.ID, "hook", "https://8.8.8.8/hook")
	mustSubscribe(t, st, sc, ep.ID, et.Name)

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
	// Clear the lease so the row is claimable now rather than in 40 s.
	if _, err := admin.Exec(ctx, `update deliveries set lease_expires_at = now() - interval '1 minute'`); err != nil {
		t.Fatalf("clear lease: %v", err)
	}

	d, got, err := st.ClaimDelivery(ctx, 40*time.Second, 72*time.Hour, AttemptClassAny)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if !got {
		t.Fatal("no delivery was claimable")
	}
	if d.PreviousSigningSecret != "" {
		t.Errorf("previous secret = %q, want empty for an endpoint that never rotated", hintOf(d.PreviousSigningSecret))
	}
	if d.PreviousSecretExpiresAt != nil {
		t.Errorf("previous expiry = %v, want nil", d.PreviousSecretExpiresAt)
	}
	if d.SigningSecret == "" {
		t.Fatal("the claim lost the endpoint's current signing secret")
	}
}

// TestExpiredPreviousSecretsAreCleared is the other end of FR-1004: the overlap
// ends. Without this the endpoint would carry two live credentials forever, and
// rotating would not have removed anything.
func TestExpiredPreviousSecretsAreCleared(t *testing.T) {
	admin := testPool(t)
	resetRoutingTables(t, admin)
	ctx := context.Background()
	pid := seedRoutingProducer(t, admin, "s10rotclear")
	sc := Scope{ProducerID: pid, Env: "live"}
	st := New(appPool(t, admin))

	con := mustConsumer(t, st, sc, "c1", "Consumer One")
	ep := mustVerifiedEndpoint(t, st, sc, con.ID, "hook", "https://8.8.8.8/hook")
	if _, err := st.RotateConsumerEndpointSecret(ctx, sc, con.ID, ep.ID); err != nil {
		t.Fatalf("rotate: %v", err)
	}

	// Nothing to clear while the window is open.
	var cleared int
	if err := admin.QueryRow(ctx, `select pigeon_clear_expired_previous_secrets()`).Scan(&cleared); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if cleared != 0 {
		t.Errorf("cleared %d rows inside the overlap; the window is not over", cleared)
	}

	// Age the expiry, as 24 hours of wall clock would.
	if _, err := admin.Exec(ctx,
		`update endpoints set previous_secret_expires_at = now() - interval '1 second' where id = $1`, ep.ID); err != nil {
		t.Fatalf("age the expiry: %v", err)
	}
	if err := admin.QueryRow(ctx, `select pigeon_clear_expired_previous_secrets()`).Scan(&cleared); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if cleared != 1 {
		t.Fatalf("cleared %d rows, want 1", cleared)
	}

	var previous, current *string
	var expiry *time.Time
	if err := admin.QueryRow(ctx,
		`select previous_signing_secret, previous_secret_expires_at, signing_secret from endpoints where id = $1`, ep.ID).
		Scan(&previous, &expiry, &current); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if previous != nil || expiry != nil {
		t.Fatalf("the expired pair survived: secret=%v expiry=%v", previous, expiry)
	}
	// Clearing an expired overlap must not un-sign the endpoint.
	if current == nil || *current == "" {
		t.Fatal("the current signing secret was cleared along with the previous one")
	}
	// Idempotent: a second call finds nothing and changes nothing.
	if err := admin.QueryRow(ctx, `select pigeon_clear_expired_previous_secrets()`).Scan(&cleared); err != nil {
		t.Fatalf("second clear: %v", err)
	}
	if cleared != 0 {
		t.Errorf("the second call cleared %d rows; the sweep must be idempotent", cleared)
	}
}

// hintOf renders a secret as its safe prefix so a failure message never carries a
// credential (FR-1008) while still telling two secrets apart.
func hintOf(secret string) string {
	if secret == "" {
		return "(empty)"
	}
	if len(secret) <= 12 {
		return secret[:4] + "..."
	}
	return secret[:12] + "..."
}
