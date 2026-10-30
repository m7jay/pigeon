package httpapi

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/m7jay/pigeon/internal/store"
)

// memberPassword is used for every invited human in the suite. Invitations set
// the password, so it only has to satisfy the length floor.
const memberPassword = "member-password-1"

// tenant is one fully provisioned producer: an owner, a developer, a consumer, a
// consumer user and a live API key. Every test starts from this because the
// cross-producer suite needs a second tenant with real ids to aim at.
type tenant struct {
	Slug string
	ID   string

	OwnerEmail string
	OwnerToken string

	DeveloperEmail string
	DeveloperToken string

	ConsumerID    string
	ConsumerRef   string
	ConsumerEmail string
	ConsumerUser  string
	ConsumerToken string

	// EndpointID is a live endpoint under the tenant's consumer, so the routing
	// (endpoint/subscription) routes have a real id to aim at cross-producer.
	EndpointID          string
	EndpointVerifyToken string

	KeyID    string
	KeyPlain string

	// PendingInviteID is a live, unaccepted invitation in this producer, used to
	// give the invitation routes a real id to aim at.
	PendingInviteID    string
	PendingInviteEmail string

	// EventTypeName is a live event type this producer registered, so the ingest
	// and event-type routes have a real, scoped contract to publish against and a
	// name the other tenant must not see.
	EventTypeName string

	// DeliveryID and DeliveryEventID are a delivered delivery and the event it
	// came from, registered on a consumer-routed type and fanned out, so S7's
	// id-addressed routes (FR-1101..FR-1111) have real identifiers in each tenant
	// to aim at cross-producer.
	DeliveryID            int64
	DeliveryEventID       string
	DeliveryEventTypeName string
}

type createdProducer struct {
	Producer struct {
		ID   string `json:"id"`
		Slug string `json:"slug"`
		Name string `json:"name"`
	} `json:"producer"`
	OwnerInvite struct {
		ID    string `json:"id"`
		Email string `json:"email"`
		Role  string `json:"role"`
		Token string `json:"token"`
	} `json:"owner_invite"`
}

// seedTenant creates a producer through the instance-admin API (FR-102), accepts
// the owner invitation, then builds one of everything an owner can build. Doing it
// over HTTP rather than with SQL is deliberate: the fixtures are the same paths
// the tests are about.
func (e *env) seedTenant(t *testing.T, slug string) *tenant {
	t.Helper()
	tn := &tenant{Slug: slug, OwnerEmail: slug + "-owner@example.test"}

	code, raw := e.do(http.MethodPost, "/v1/admin/producers", e.adminToken, map[string]string{
		"slug":        slug,
		"name":        "Producer " + slug,
		"owner_email": tn.OwnerEmail,
	})
	mustStatus(t, code, raw, http.StatusCreated)
	created := decode[createdProducer](t, raw)
	tn.ID = created.Producer.ID
	if created.OwnerInvite.Role != "owner" || created.OwnerInvite.Token == "" {
		t.Fatalf("producer creation did not return an owner invitation: %s", raw)
	}
	e.acceptInvite(t, created.OwnerInvite.Token)
	tn.OwnerToken = e.login(tn.OwnerEmail, memberPassword, "").Token

	// A developer, invited by the owner (FR-104).
	tn.DeveloperEmail = slug + "-dev@example.test"
	code, raw = e.do(http.MethodPost, "/v1/producer/invites", tn.OwnerToken, map[string]string{
		"email": tn.DeveloperEmail, "role": "developer",
	})
	mustStatus(t, code, raw, http.StatusCreated)
	e.acceptInvite(t, decode[inviteJSON](t, raw).TokenValue())
	tn.DeveloperToken = e.login(tn.DeveloperEmail, memberPassword, "").Token

	// A consumer in the live plane, and a consumer user bound to it (FR-109).
	tn.ConsumerRef = "ref-" + slug
	code, raw = e.do(http.MethodPost, "/v1/producer/consumers?env=live", tn.OwnerToken, map[string]string{
		"ref": tn.ConsumerRef, "name": "Consumer of " + slug,
	})
	mustStatus(t, code, raw, http.StatusCreated)
	tn.ConsumerID = decode[consumerJSON](t, raw).ID

	// An endpoint for the consumer (FR-401), so the routing routes have a real id
	// to aim at cross-producer. The create response carries the one-time
	// verification token, which the verify case needs.
	code, raw = e.do(http.MethodPost, "/v1/producer/consumers/"+tn.ConsumerID+"/endpoints?env=live", tn.OwnerToken, map[string]string{
		"name": "endpoint of " + slug, "url": "https://8.8.8.8/hook",
	})
	mustStatus(t, code, raw, http.StatusCreated)
	createdEP := decode[endpointCreatedJSON](t, raw)
	tn.EndpointID = createdEP.ID
	tn.EndpointVerifyToken = createdEP.VerificationToken

	tn.ConsumerEmail = slug + "-consumer@example.test"
	code, raw = e.do(http.MethodPost, "/v1/producer/consumers/"+tn.ConsumerID+"/invites?env=live",
		tn.OwnerToken, map[string]string{"email": tn.ConsumerEmail})
	mustStatus(t, code, raw, http.StatusCreated)
	e.acceptInvite(t, decode[inviteJSON](t, raw).TokenValue())

	consumerSession := e.login(tn.ConsumerEmail, memberPassword, "")
	if consumerSession.Claims.ConsumerID != tn.ConsumerID {
		t.Fatalf("consumer session claims %q, want %q (FR-110)", consumerSession.Claims.ConsumerID, tn.ConsumerID)
	}
	tn.ConsumerToken = consumerSession.Token

	// The bound user's id, so the cross-producer suite can aim at a real one.
	code, raw = e.do(http.MethodGet, "/v1/producer/consumers/"+tn.ConsumerID+"/users?env=live", tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusOK)
	if users := decode[struct {
		Users []consumerUserJSON `json:"users"`
	}](t, raw); len(users.Users) == 1 {
		tn.ConsumerUser = users.Users[0].UserID
	} else {
		t.Fatalf("consumer %s has %d bound users, want 1", tn.ConsumerID, len(users.Users))
	}

	// A live API key (FR-203).
	code, raw = e.do(http.MethodPost, "/v1/producer/keys?env=live", tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusCreated)
	issued := decode[keyJSON](t, raw)
	tn.KeyID = issued.ID
	if issued.Key == nil || *issued.Key == "" {
		t.Fatalf("key issuance did not reveal the plaintext once: %s", raw)
	}
	tn.KeyPlain = *issued.Key

	e.remember(t, tn)
	return tn
}

// TokenValue is a small helper so the fixtures can read the one-time token out of
// the JSON shape without a pointer dance in every call site.
func (i inviteJSON) TokenValue() string {
	if i.Token == nil {
		return ""
	}
	return *i.Token
}

func (e *env) acceptInvite(t *testing.T, token string) {
	t.Helper()
	code, raw := e.do(http.MethodPost, "/v1/invites/accept", "", map[string]string{
		"token": token, "password": memberPassword,
	})
	mustStatus(t, code, raw, http.StatusOK)
}

// TestInstanceAdminCreatesProducersAndIsAudited is FR-102 and MT-5: the admin
// creates a tenant, the tenant is manageable immediately (owner invitation), the
// action is audited with the actor, and nobody else can reach the admin surface.
func TestInstanceAdminCreatesProducersAndIsAudited(t *testing.T) {
	e := setup(t)
	tn := e.seedTenant(t, "acme-audit")

	// The admin can read it back.
	code, raw := e.do(http.MethodGet, "/v1/admin/producers/"+tn.ID, e.adminToken, nil)
	mustStatus(t, code, raw, http.StatusOK)
	if got := decode[producerJSON](t, raw); got.Slug != "acme-audit" || got.Suspended {
		t.Fatalf("producer = %+v, want slug acme-audit and not suspended", got)
	}

	// MT-5: actor, action, timestamp, and the request id that ties it to the log.
	code, raw = e.do(http.MethodGet, "/v1/admin/producers/"+tn.ID+"/audit", e.adminToken, nil)
	mustStatus(t, code, raw, http.StatusOK)
	audit := decode[struct {
		Audit []auditJSON `json:"audit"`
	}](t, raw)
	if len(audit.Audit) == 0 {
		t.Fatal("no audit rows for a producer created through the admin API (MT-5)")
	}
	first := audit.Audit[len(audit.Audit)-1]
	if first.Action != "producer.created" {
		t.Fatalf("audit action = %q, want producer.created", first.Action)
	}
	if first.ActorUserID == "" {
		t.Fatal("audit row has no actor (MT-5)")
	}

	// FR-108: the admin surface does not exist for a producer owner.
	code, raw = e.do(http.MethodGet, "/v1/admin/producers", tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusNotFound)
	code, raw = e.do(http.MethodPost, "/v1/admin/producers", tn.DeveloperToken, map[string]string{
		"slug": "sneaky", "name": "Sneaky", "owner_email": "sneaky@example.test",
	})
	mustStatus(t, code, raw, http.StatusNotFound)

	// FR-111/FR-102: self-registration is off by default, so the route is absent
	// rather than forbidden - there is no surface to probe.
	code, raw = e.do(http.MethodPost, "/v1/producers", "", map[string]string{
		"slug": "selfserve", "name": "Self", "email": "s@example.test", "password": memberPassword,
	})
	mustStatus(t, code, raw, http.StatusNotFound)

	// And a duplicate slug is a conflict, not a second tenant.
	code, raw = e.do(http.MethodPost, "/v1/admin/producers", e.adminToken, map[string]string{
		"slug": "acme-audit", "name": "Again", "owner_email": "again@example.test",
	})
	mustStatus(t, code, raw, http.StatusConflict)
}

// TestSuspensionStopsIngestionAndIsReversible is FR-107 with MT-5's reason.
func TestSuspensionStopsIngestionAndIsReversible(t *testing.T) {
	e := setup(t)
	tn := e.seedTenant(t, "suspend-co")

	// The key works before the suspension.
	code, raw := e.do(http.MethodGet, "/v1/whoami", tn.KeyPlain, nil)
	mustStatus(t, code, raw, http.StatusOK)

	code, raw = e.do(http.MethodPost, "/v1/admin/producers/"+tn.ID+"/suspend", e.adminToken, map[string]string{
		"reason": "unpaid invoice",
	})
	mustStatus(t, code, raw, http.StatusOK)
	if !decode[producerJSON](t, raw).Suspended {
		t.Fatal("suspend response does not report the producer as suspended")
	}

	// FR-107: ingestion stops. The key still authenticates, and the caller is
	// told why - a 401 would send them to re-issue a valid key.
	code, raw = e.do(http.MethodGet, "/v1/whoami", tn.KeyPlain, nil)
	mustStatus(t, code, raw, http.StatusForbidden)
	if got := errorCode(t, raw); got != "producer_suspended" {
		t.Fatalf("error code = %q, want producer_suspended", got)
	}

	// The reason is on the record (MT-5).
	code, raw = e.do(http.MethodGet, "/v1/admin/producers/"+tn.ID+"/audit", e.adminToken, nil)
	mustStatus(t, code, raw, http.StatusOK)
	audit := decode[struct {
		Audit []auditJSON `json:"audit"`
	}](t, raw)
	if !hasAudit(audit.Audit, "producer.suspended", "unpaid invoice") {
		t.Fatalf("no producer.suspended audit row with the reason; got %+v", audit.Audit)
	}

	// Resume is reversible, and the key works again with no re-issuance.
	code, raw = e.do(http.MethodPost, "/v1/admin/producers/"+tn.ID+"/resume", e.adminToken, nil)
	mustStatus(t, code, raw, http.StatusOK)
	if decode[producerJSON](t, raw).Suspended {
		t.Fatal("resume response still reports suspended")
	}
	code, raw = e.do(http.MethodGet, "/v1/whoami", tn.KeyPlain, nil)
	mustStatus(t, code, raw, http.StatusOK)
}

func hasAudit(rows []auditJSON, action, reason string) bool {
	for _, r := range rows {
		if r.Action == action && r.Reason != nil && *r.Reason == reason {
			return true
		}
	}
	return false
}

// TestMembershipSpansProducersAndRolesAreEnforced is FR-104 plus FR-105.
func TestMembershipSpansProducersAndRolesAreEnforced(t *testing.T) {
	e := setup(t)
	one := e.seedTenant(t, "multi-one")
	two := e.seedTenant(t, "multi-two")

	// FR-104: the owner sees both humans in the producer.
	code, raw := e.do(http.MethodGet, "/v1/producer/members", one.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusOK)
	members := decode[struct {
		Members []memberJSON `json:"members"`
	}](t, raw)
	if len(members.Members) != 2 {
		t.Fatalf("members = %+v, want the owner and the developer", members.Members)
	}

	// A developer is not an owner: inviting and key issuance are 404, not 403
	// (FR-108 - the route does not exist for them).
	code, raw = e.do(http.MethodPost, "/v1/producer/invites", one.DeveloperToken, map[string]string{
		"email": "another@example.test", "role": "developer",
	})
	mustStatus(t, code, raw, http.StatusNotFound)
	code, raw = e.do(http.MethodPost, "/v1/producer/keys?env=live", one.DeveloperToken, nil)
	mustStatus(t, code, raw, http.StatusNotFound)
	code, raw = e.do(http.MethodGet, "/v1/producer/keys?env=live", one.DeveloperToken, nil)
	mustStatus(t, code, raw, http.StatusOK)

	// FR-105: the same human in two producers. The developer already has an
	// account, and an invitation for an existing account is not a credential on
	// its own (the inviter holds the token), so it is redeemed with the invitee's
	// own session. That is the shape every second membership has until there is a
	// mailer.
	code, raw = e.do(http.MethodPost, "/v1/producer/invites", two.OwnerToken, map[string]string{
		"email": one.DeveloperEmail, "role": "developer",
	})
	mustStatus(t, code, raw, http.StatusCreated)
	code, raw = e.do(http.MethodPost, "/v1/invites/accept", one.DeveloperToken, map[string]string{
		"token": decode[inviteJSON](t, raw).TokenValue(),
	})
	mustStatus(t, code, raw, http.StatusOK)

	// A login that names no producer is now ambiguous, and the server refuses to
	// guess which tenant the session is for (FR-105).
	code, raw = e.do(http.MethodPost, "/v1/session", "", map[string]string{
		"email": one.DeveloperEmail, "password": memberPassword,
	})
	mustStatus(t, code, raw, http.StatusBadRequest)
	if got := errorCode(t, raw); got != "producer_required" {
		t.Fatalf("error code = %q, want producer_required", got)
	}

	for _, tc := range []struct {
		slug     string
		other    *tenant
		ownOwner string
	}{{one.Slug, two, one.OwnerEmail}, {two.Slug, one, two.OwnerEmail}} {
		sess := e.login(one.DeveloperEmail, memberPassword, tc.slug)
		if sess.Claims.Role != "developer" {
			t.Fatalf("session for %s has role %q, want developer", tc.slug, sess.Claims.Role)
		}
		// Each session sees exactly its own producer's members. The developer
		// legitimately appears in both (FR-105 is the point), so the assertion is
		// about the other tenant's owner, which nothing shared can explain.
		code, raw := e.do(http.MethodGet, "/v1/producer/members", sess.Token, nil)
		mustStatus(t, code, raw, http.StatusOK)
		if strings.Contains(string(raw), tc.other.OwnerEmail) {
			t.Fatalf("session scoped to %s sees %s from the other producer: %s", tc.slug, tc.other.OwnerEmail, raw)
		}
		for _, own := range []string{tc.ownOwner, one.DeveloperEmail} {
			if !strings.Contains(string(raw), own) {
				t.Fatalf("session scoped to %s does not list %s: %s", tc.slug, own, raw)
			}
		}
	}
}

// TestConsumerUserIsBoundToOneConsumerAndRevocable is FR-109, FR-110 and FR-111.
func TestConsumerUserIsBoundToOneConsumerAndRevocable(t *testing.T) {
	e := setup(t)
	tn := e.seedTenant(t, "consumer-co")

	// FR-110: the session's claim is the consumer id, and the API filters by it.
	code, raw := e.do(http.MethodGet, "/v1/consumer/me", tn.ConsumerToken, nil)
	mustStatus(t, code, raw, http.StatusOK)
	if got := decode[consumerJSON](t, raw); got.ID != tn.ConsumerID {
		t.Fatalf("consumer/me returned %q, want %q", got.ID, tn.ConsumerID)
	}
	if got := decode[consumerJSON](t, raw); got.Env != "live" {
		t.Fatalf("consumer/me returned env %q, want live", got.Env)
	}

	// A consumer session has no producer scope at all: every producer route is
	// absent for it, including the ones that would list consumers.
	for _, path := range []string{
		"/v1/producer/members",
		"/v1/producer/consumers?env=live",
		"/v1/producer/keys?env=live",
		"/v1/producer/consumers/" + tn.ConsumerID + "/users?env=live",
	} {
		code, raw := e.do(http.MethodGet, path, tn.ConsumerToken, nil)
		mustStatus(t, code, raw, http.StatusNotFound)
	}

	// FR-109: a second consumer in the same producer cannot claim the same user.
	code, raw = e.do(http.MethodPost, "/v1/producer/consumers?env=live", tn.OwnerToken, map[string]string{
		"ref": "ref-second", "name": "Second",
	})
	mustStatus(t, code, raw, http.StatusCreated)
	second := decode[consumerJSON](t, raw).ID

	code, raw = e.do(http.MethodPost, "/v1/producer/consumers/"+second+"/invites?env=live",
		tn.OwnerToken, map[string]string{"email": tn.ConsumerEmail})
	mustStatus(t, code, raw, http.StatusCreated)
	// The consumer user already has an account, so the redemption carries their
	// session; the conflict is then about the binding (one consumer per user), not
	// about proving who they are.
	code, raw = e.do(http.MethodPost, "/v1/invites/accept", tn.ConsumerToken, map[string]string{
		"token": decode[inviteJSON](t, raw).TokenValue(),
	})
	mustStatus(t, code, raw, http.StatusConflict)

	// FR-109: revoking the binding leaves the consumer and its data alone.
	code, raw = e.do(http.MethodGet, "/v1/producer/consumers/"+tn.ConsumerID+"/users?env=live",
		tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusOK)
	users := decode[struct {
		Users []consumerUserJSON `json:"users"`
	}](t, raw)
	if len(users.Users) != 1 {
		t.Fatalf("consumer users = %+v, want exactly the invited one", users.Users)
	}
	userID := users.Users[0].UserID

	code, raw = e.do(http.MethodDelete, "/v1/producer/consumers/"+tn.ConsumerID+"/users/"+userID+"?env=live",
		tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusNoContent)

	// The consumer record survives, and the session stops resolving.
	code, raw = e.do(http.MethodGet, "/v1/producer/consumers/"+tn.ConsumerID+"?env=live", tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusOK)
	code, raw = e.do(http.MethodGet, "/v1/consumer/me", tn.ConsumerToken, nil)
	mustStatus(t, code, raw, http.StatusUnauthorized)

	// FR-111: there is no route by which a user adds themselves to a producer.
	// The only anonymous writes are login, invitation acceptance and (when
	// enabled) producer self-registration - plus S9a's credential checks:
	// FR-106's optional OIDC login (another way to obtain a session) and
	// FR-111's reset request/accept, which mint and redeem a token for an
	// ALREADY-existing account. Accepting a reset rewrites one password, revokes
	// that account's sessions and grants no producer claim (FR-111), so none of
	// these is a way into a tenant.
	for _, rt := range e.routes() {
		if rt.Kind != kindAnonymous || rt.Method != http.MethodPost {
			continue
		}
		switch rt.Path {
		case "/v1/session", "/v1/invites/accept", "/v1/producers",
			"/v1/session/oidc", "/v1/password-resets", "/v1/password-resets/accept":
		default:
			t.Fatalf("anonymous write route %s %s exists; every producer write must need a credential", rt.Method, rt.Path)
		}
	}
}

// TestAPIKeyLifecycleAndRevocation is FR-201 to FR-206.
func TestAPIKeyLifecycleAndRevocation(t *testing.T) {
	e := setup(t)
	tn := e.seedTenant(t, "keys-co")

	// FR-201/FR-203: the key authenticates as its producer and environment.
	code, raw := e.do(http.MethodGet, "/v1/whoami", tn.KeyPlain, nil)
	mustStatus(t, code, raw, http.StatusOK)
	who := decode[struct {
		KeyID      string `json:"key_id"`
		ProducerID string `json:"producer_id"`
		Env        string `json:"env"`
	}](t, raw)
	if who.ProducerID != tn.ID || who.Env != "live" || who.KeyID != tn.KeyID {
		t.Fatalf("whoami = %+v, want key %s of %s/live", who, tn.KeyID, tn.ID)
	}
	if !strings.HasPrefix(tn.KeyPlain, "pgn_live_") {
		t.Fatalf("key %q is not namespaced by environment (FR-203)", tn.KeyPlain)
	}

	// FR-202: the plaintext is never readable again; only the hint is.
	code, raw = e.do(http.MethodGet, "/v1/producer/keys?env=live", tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusOK)
	keys := decode[struct {
		Keys []keyJSON `json:"keys"`
	}](t, raw)
	var found *keyJSON
	for i := range keys.Keys {
		if keys.Keys[i].ID == tn.KeyID {
			found = &keys.Keys[i]
		}
	}
	if found == nil {
		t.Fatalf("issued key %s not in the list", tn.KeyID)
	}
	if found.Key != nil {
		t.Fatal("key list returned the plaintext; it must be shown once (FR-202)")
	}
	if len(found.Hint) != 12 || !strings.HasPrefix(tn.KeyPlain, found.Hint) {
		t.Fatalf("hint %q is not the 12-character prefix of the key", found.Hint)
	}

	// FR-206: last_used_at is recorded. It is an off-path evidence write in
	// production, so the test waits for it rather than assuming it is synchronous.
	deadline := time.Now().Add(3 * time.Second)
	for {
		code, raw = e.do(http.MethodGet, "/v1/producer/keys/"+tn.KeyID+"?env=live", tn.OwnerToken, nil)
		mustStatus(t, code, raw, http.StatusOK)
		if decode[keyJSON](t, raw).LastUsedAt != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("last_used_at was never recorded for a key that authenticated (FR-206)")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// FR-205: revocation takes effect on the next request, well inside 60 s.
	revokedAt := time.Now()
	code, raw = e.do(http.MethodDelete, "/v1/producer/keys/"+tn.KeyID+"?env=live", tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusNoContent)

	code, raw = e.do(http.MethodGet, "/v1/whoami", tn.KeyPlain, nil)
	mustStatus(t, code, raw, http.StatusUnauthorized)
	if elapsed := time.Since(revokedAt); elapsed > 60*time.Second {
		t.Fatalf("revocation took %v, over the 60 s budget", elapsed)
	}
	t.Logf("FR-205: the next request after revocation was refused %v after the DELETE", elapsedSince(revokedAt))

	// Revoking again is a 404: there is no unrevoked key under this id anymore.
	code, raw = e.do(http.MethodDelete, "/v1/producer/keys/"+tn.KeyID+"?env=live", tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusNotFound)

	// The revoked key is still listed, with its revocation time: "which key was
	// used last Tuesday" is a question about history.
	code, raw = e.do(http.MethodGet, "/v1/producer/keys/"+tn.KeyID+"?env=live", tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusOK)
	if revoked := decode[keyJSON](t, raw); revoked.RevokedAt == nil {
		t.Fatal("revoked key has no revoked_at")
	}

	// FR-204: rotation needs no downtime - a second key works while the first is
	// revoked, and the two are independent.
	code, raw = e.do(http.MethodPost, "/v1/producer/keys?env=live", tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusCreated)
	second := decode[keyJSON](t, raw)
	if second.Key == nil || *second.Key == tn.KeyPlain {
		t.Fatal("rotation did not mint a distinct key")
	}
	code, raw = e.do(http.MethodGet, "/v1/whoami", *second.Key, nil)
	mustStatus(t, code, raw, http.StatusOK)
}

// elapsedSince is a named helper so the FR-205 log line reads as a measurement
// rather than an arithmetic expression.
func elapsedSince(t0 time.Time) time.Duration { return time.Since(t0) }

// TestRateLimitsArePerKeyAndPerProducer is FR-207. It builds a second server with
// tight limits so the functional server is never affected.
func TestRateLimitsArePerKeyAndPerProducer(t *testing.T) {
	e := setup(t)
	tn := e.seedTenant(t, "ratelimit-co")
	other := e.seedTenant(t, "ratelimit-other")

	// Per key: one key's burst is its own.
	perKey := e.withLimits(t, 0.0001, 1, 0, 0)
	code, raw := perKey.do(http.MethodGet, "/v1/whoami", tn.KeyPlain, nil)
	mustStatus(t, code, raw, http.StatusOK)
	code, raw = perKey.do(http.MethodGet, "/v1/whoami", tn.KeyPlain, nil)
	mustStatus(t, code, raw, http.StatusTooManyRequests)
	if got := errorCode(t, raw); got != "rate_limited" {
		t.Fatalf("error code = %q, want rate_limited", got)
	}
	if got := perKey.lastRetryAfter(); got == "" {
		t.Fatal("429 response has no Retry-After header (FR-207)")
	} else if got == "0" {
		t.Fatal("Retry-After: 0 would invite the retry storm the limit prevents")
	}
	// A different key of another producer is unaffected.
	code, raw = perKey.do(http.MethodGet, "/v1/whoami", other.KeyPlain, nil)
	mustStatus(t, code, raw, http.StatusOK)

	// Per producer: two keys of one producer share a budget.
	perProducer := e.withLimits(t, 0, 0, 0.0001, 1)
	code, raw = perProducer.do(http.MethodGet, "/v1/whoami", tn.KeyPlain, nil)
	mustStatus(t, code, raw, http.StatusOK)
	code, raw = perProducer.do(http.MethodPost, "/v1/producer/keys?env=live", tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusCreated)
	secondKey := decode[keyJSON](t, raw)
	code, raw = perProducer.do(http.MethodGet, "/v1/whoami", *secondKey.Key, nil)
	mustStatus(t, code, raw, http.StatusTooManyRequests)
	code, raw = perProducer.do(http.MethodGet, "/v1/whoami", other.KeyPlain, nil)
	mustStatus(t, code, raw, http.StatusOK)
}

// TestTestAndLivePlanesAreSeparate is FR-103 at the HTTP layer.
func TestTestAndLivePlanesAreSeparate(t *testing.T) {
	e := setup(t)
	tn := e.seedTenant(t, "envsep-co")

	// The same consumer ref is legal in both planes, and they are different rows.
	code, raw := e.do(http.MethodPost, "/v1/producer/consumers?env=test", tn.OwnerToken, map[string]string{
		"ref": tn.ConsumerRef, "name": "Test-plane twin",
	})
	mustStatus(t, code, raw, http.StatusCreated)
	testConsumer := decode[consumerJSON](t, raw).ID
	if testConsumer == tn.ConsumerID {
		t.Fatal("a test consumer and a live consumer with the same ref are the same row; FR-103 is broken")
	}

	// Each plane lists only its own consumers.
	for _, tc := range []struct {
		env     string
		want    string
		notWant string
	}{
		{"live", tn.ConsumerID, testConsumer},
		{"test", testConsumer, tn.ConsumerID},
	} {
		code, raw := e.do(http.MethodGet, "/v1/producer/consumers?env="+tc.env, tn.OwnerToken, nil)
		mustStatus(t, code, raw, http.StatusOK)
		body := string(raw)
		if !strings.Contains(body, tc.want) {
			t.Fatalf("%s listing does not contain its own consumer %s: %s", tc.env, tc.want, body)
		}
		if strings.Contains(body, tc.notWant) {
			t.Fatalf("%s listing contains the other plane's consumer %s: %s", tc.env, tc.notWant, body)
		}
	}

	// Asking for the other plane's row by id is a 404, not a 403 (FR-108 + FR-103).
	code, raw = e.do(http.MethodGet, "/v1/producer/consumers/"+testConsumer+"?env=live", tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusNotFound)
	code, raw = e.do(http.MethodGet, "/v1/producer/consumers/"+tn.ConsumerID+"?env=test", tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusNotFound)

	// A request that names no plane is refused rather than defaulted: guessing
	// live from a test-shaped request is the failure FR-103 exists to prevent.
	code, raw = e.do(http.MethodGet, "/v1/producer/consumers", tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusBadRequest)
	if got := errorCode(t, raw); got != "env_required" {
		t.Fatalf("error code = %q, want env_required", got)
	}
	code, raw = e.do(http.MethodGet, "/v1/producer/consumers?env=staging", tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusBadRequest)
	if got := errorCode(t, raw); got != "invalid_env" {
		t.Fatalf("error code = %q, want invalid_env", got)
	}

	// A key is scoped to one environment and cannot address the other.
	code, raw = e.do(http.MethodGet, "/v1/whoami", tn.KeyPlain, nil)
	mustStatus(t, code, raw, http.StatusOK)
	if who := decode[struct {
		Env string `json:"env"`
	}](t, raw); who.Env != "live" {
		t.Fatalf("key env = %q, want live", who.Env)
	}
}

// TestAC54ReIssueEndpointChallenge is AC-54's acceptance test, asserted at the
// acceptance layer: a member re-issues the ownership challenge for an endpoint
// that has not yet been verified. Each call mints a fresh token and hint and
// answers 201 with the creation response's own field names. The fresh token is
// the only value that verifies; the registration token is retired. A foreign,
// unknown or deleted id is 404 (AC-39); a verified endpoint is 409. No read
// route returns the token or its hash, and an unverified endpoint keeps its
// not_delivered:endpoint_unverified delivery row.
func TestAC54ReIssueEndpointChallenge(t *testing.T) {
	e := setup(t)
	tn := e.seedTenant(t, "ac54-reissue")
	seedCrossProducerEventType(t, e, tn)

	// Register an endpoint through the producer path: the token is shown exactly
	// once, at registration.
	code, raw := e.do(http.MethodPost,
		"/v1/producer/consumers/"+tn.ConsumerID+"/endpoints?env=live", tn.OwnerToken,
		map[string]string{"name": "ac54", "url": "https://8.8.8.8/hook"})
	mustStatus(t, code, raw, http.StatusCreated)
	created := decode[endpointCreatedJSON](t, raw)
	regToken := created.VerificationToken
	if regToken == "" {
		t.Fatalf("registration did not reveal a token: %s", raw)
	}

	// A member re-issues the challenge: 201 with the token and hint.
	code, raw = e.do(http.MethodPost,
		"/v1/producer/consumers/"+tn.ConsumerID+"/endpoints/"+created.ID+"/challenge?env=live",
		tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusCreated)
	frac := decode[endpointReissuedJSON](t, raw)
	if frac.VerificationToken == "" || frac.VerificationHint == "" {
		t.Fatalf("re-issue did not return the token and hint: %s", raw)
	}
	if frac.VerificationToken == regToken {
		t.Fatal("re-issue minted the registration token again; the new value is stale")
	}
	if !strings.HasPrefix(frac.VerificationToken, frac.VerificationHint) || len(frac.VerificationHint) != 8 {
		t.Fatalf("hint %q is not the first 8 characters of the token", frac.VerificationHint)
	}
	// The endpoint is subscribed to the event type and the event is published
	// while the endpoint is still unverified: fan-out records a skip delivery
	// row with the endpoint-unverified reason (FR-403). The re-issue in the
	// block above arms the endpoint; it does not re-deliver the window, so the
	// skip row is expected to survive the re-issue and everything that follows.
	code, raw = e.do(http.MethodPost,
		"/v1/producer/endpoints/"+created.ID+"/subscriptions?env=live",
		tn.OwnerToken, map[string]string{"event_type": tn.EventTypeName})
	mustStatus(t, code, raw, http.StatusCreated)

	code, raw = e.do(http.MethodPost, "/v1/events", tn.KeyPlain, map[string]any{
		"event_type":   tn.EventTypeName,
		"consumer_ref": tn.ConsumerRef,
		"payload":      map[string]any{"note": "reissue-window"},
	})
	mustStatus(t, code, raw, http.StatusAccepted)
	evt := decode[publishResponse](t, raw)
	if evt.ID == "" {
		t.Fatalf("publish returned no event id: %s", raw)
	}

	// The endpoint is still unverified: verified_at is untouched by the reissue.
	var nullFlag bool
	if err := e.admin.QueryRow(context.Background(),
		"select verified_at is null from endpoints where id = $1", created.ID).Scan(&nullFlag); err != nil {
		t.Fatalf("read verified_at: %v", err)
	}
	if !nullFlag {
		t.Fatal("re-issue changed verified_at")
	}

	// The fresh token verifies; the registration token does not (the hash was
	// replaced, so the old value is 422).
	code, raw = e.do(http.MethodPost,
		"/v1/producer/consumers/"+tn.ConsumerID+"/endpoints/"+created.ID+"/verify?env=live",
		tn.OwnerToken, map[string]string{"token": frac.VerificationToken})
	mustStatus(t, code, raw, http.StatusNoContent)
	code, raw = e.do(http.MethodPost,
		"/v1/producer/consumers/"+tn.ConsumerID+"/endpoints/"+created.ID+"/verify?env=live",
		tn.OwnerToken, map[string]string{"token": regToken})
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("registration token still verifies after reissue: %d (%s)", code, raw)
	}

	// A consumer re-issues its own endpoint: 201.
	tc := e.seedTenant(t, "ac54-consumer")
	seedCrossProducerEventType(t, e, tc)
	code, raw = e.do(http.MethodPost,
		"/v1/producer/consumers/"+tc.ConsumerID+"/endpoints?env=live", tc.OwnerToken,
		map[string]string{"name": "cc", "url": "https://8.8.8.8/hook"})
	mustStatus(t, code, raw, http.StatusCreated)
	cc := decode[endpointCreatedJSON](t, raw)
	code, raw = e.do(http.MethodPost,
		"/v1/consumer/endpoints/"+cc.ID+"/challenge", tc.ConsumerToken, nil)
	mustStatus(t, code, raw, http.StatusCreated)
	frac = decode[endpointReissuedJSON](t, raw)
	if frac.VerificationToken == "" || frac.VerificationHint == "" {
		t.Fatalf("consumer re-issue did not return the token and hint: %s", raw)
	}

	// 404 for a foreign, unknown and deleted id, on both routes.
	tn2 := e.seedTenant(t, "ac54-other")
	seedCrossProducerEventType(t, e, tn2)
	code, raw = e.do(http.MethodPost,
		"/v1/producer/consumers/"+tn2.ConsumerID+"/endpoints?env=live", tn2.OwnerToken,
		map[string]string{"name": "other", "url": "https://8.8.8.8/hook"})
	mustStatus(t, code, raw, http.StatusCreated)
	code, raw = e.do(http.MethodPost,
		"/v1/producer/consumers/"+tn2.ConsumerID+"/endpoints/"+created.ID+"/challenge?env=live",
		tn2.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusNotFound)
	code, raw = e.do(http.MethodPost,
		"/v1/producer/consumers/"+tn.ConsumerID+"/endpoints/nope999/challenge?env=live",
		tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusNotFound)
	e.admin.Exec(context.Background(),
		"update endpoints set status = 'deleted' where id = $1", created.ID)
	code, raw = e.do(http.MethodPost,
		"/v1/producer/consumers/"+tn.ConsumerID+"/endpoints/"+created.ID+"/challenge?env=live",
		tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusNotFound)
	e.admin.Exec(context.Background(), "update endpoints set status = 'active' where id = $1", created.ID)

	// 409 on an already-verified endpoint, with verified_at unchanged.
	code, raw = e.do(http.MethodPost,
		"/v1/producer/consumers/"+tn.ConsumerID+"/endpoints/"+created.ID+"/challenge?env=live",
		tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusConflict)
	code, raw = e.do(http.MethodPost,
		"/v1/producer/consumers/"+tn.ConsumerID+"/endpoints/"+created.ID+"/challenge?env=live",
		tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusConflict)

	// No read route returns the token or its hash.
	code, raw = e.do(http.MethodGet,
		"/v1/producer/consumers/"+tn.ConsumerID+"/endpoints/"+created.ID+"?env=live",
		tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusOK)
	if strings.Contains(string(raw), regToken) || strings.Contains(string(raw), frac.VerificationToken) {
		t.Fatalf("read route returns a challenge token: %s", raw)
	}

	// A delivery row recorded while the endpoint was unverified survives the
	// re-issue: the re-issue arms the endpoint, it does not re-deliver the
	// window. The skip row is terminal (status 'skipped'), so nothing re-
	// delivered the event and nothing has touched it since.
	var skipRows int
	if err := e.admin.QueryRow(context.Background(),
		`select count(*) from deliveries
		where event_id = $1 and endpoint_id = $2 and producer_id = $3 and env = $4
		  and consumer_id = $5 and status = 'skipped'
		  and non_delivery_reason = 'not_delivered:endpoint_unverified'`,
		evt.ID, created.ID, tn.ID, store.EnvLive, tn.ConsumerID).Scan(&skipRows); err != nil {
		t.Fatalf("read delivery rows: %v", err)
	}
	if skipRows != 1 {
		t.Fatalf("expected one not_delivered:endpoint_unverified skip row after the re-issue, got %d", skipRows)
	}

	// The instance admin has no such route: the re-issue route lives on the
	// member and consumer planes, never on the admin surface.
	code, raw = e.do(http.MethodPost,
		"/v1/admin/producers/"+tn.ID+"/endpoints/"+created.ID+"/challenge", e.adminToken, nil)
	if code != http.StatusNotFound {
		t.Fatalf("instance admin should not have a challenge re-issue route: %d", code)
	}
}
