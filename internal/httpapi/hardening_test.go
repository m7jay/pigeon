package httpapi

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestInvitationAcceptanceRequiresTheInviteesOwnSession is the HTTP-level
// regression test for the account-takeover defect.
//
// The chain that was reproduced against the first S1 head: an owner invites
// admin@pigeon.local, receives the invitation token (there is no mailer, so the
// API returns it to the inviter), accepts it with a password of their choosing,
// and the acceptance overwrote the instance admin's password. Two fixes are
// asserted together here: acceptance never touches an existing account's
// credentials, and a token for an existing account is only redeemable by that
// account's own authenticated session.
func TestInvitationAcceptanceRequiresTheInviteesOwnSession(t *testing.T) {
	e := setup(t)
	tn := e.seedTenant(t, "takeover-co")

	// The attack's first step: invite the instance admin's address.
	code, raw := e.do(http.MethodPost, "/v1/producer/invites", tn.OwnerToken, map[string]string{
		"email": adminEmail, "role": "developer",
	})
	mustStatus(t, code, raw, http.StatusCreated)
	token := decode[inviteJSON](t, raw).TokenValue()
	if token == "" {
		t.Fatalf("no invitation token returned, so this test is not exercising the attack: %s", raw)
	}

	const stolen = "attacker-password-1"

	// Step two: redeem it anonymously with a chosen password. The token alone is
	// not proof of account control, so this must be refused.
	code, raw = e.do(http.MethodPost, "/v1/invites/accept", "", map[string]string{
		"token": token, "password": stolen,
	})
	if code == http.StatusOK {
		t.Fatalf("an invitation for an existing account was redeemed with only the token: %s", raw)
	}
	mustStatus(t, code, raw, http.StatusForbidden)
	if got := errorCode(t, raw); got != "session_required" {
		t.Fatalf("error code = %q, want session_required", got)
	}

	// The takeover's payoff must not exist: the chosen password does not work and
	// the bootstrap password still does.
	code, raw = e.do(http.MethodPost, "/v1/session", "", map[string]string{
		"email": adminEmail, "password": stolen,
	})
	mustStatus(t, code, raw, http.StatusUnauthorized)
	sess := e.login(adminEmail, adminRotated, "")
	if !sess.Claims.InstanceAdmin {
		t.Fatal("the instance admin lost its admin claim; the acceptance rewrote the account")
	}

	// Nor was the admin silently enrolled in the attacker's tenant.
	if members := e.members(t, tn); strings.Contains(members, adminEmail) {
		t.Fatalf("the invitation bound the instance admin into a producer without proof of account control: %s", members)
	}

	// A third party holding the token — including the inviter — cannot redeem it.
	code, raw = e.do(http.MethodPost, "/v1/invites/accept", tn.OwnerToken, map[string]string{
		"token": token, "password": stolen,
	})
	mustStatus(t, code, raw, http.StatusNotFound)
	if members := e.members(t, tn); strings.Contains(members, adminEmail) {
		t.Fatalf("a different user's session redeemed an invitation addressed to %s: %s", adminEmail, members)
	}

	// The account holder may redeem it, and only the membership changes. No
	// password is required for an account that already has one.
	code, raw = e.do(http.MethodPost, "/v1/invites/accept", e.adminToken, map[string]string{
		"token": token,
	})
	mustStatus(t, code, raw, http.StatusOK)
	if members := e.members(t, tn); !strings.Contains(members, adminEmail) {
		t.Fatalf("the account holder could not accept their own invitation: %s", members)
	}
	code, raw = e.do(http.MethodPost, "/v1/session", "", map[string]string{
		"email": adminEmail, "password": stolen,
	})
	mustStatus(t, code, raw, http.StatusUnauthorized)
	if sess := e.login(adminEmail, adminRotated, ""); !sess.Claims.InstanceAdmin {
		t.Fatal("accepting an invitation stripped the instance admin's admin claim")
	}

	// The shared harness must not be left with the admin inside a tenant: every
	// later test would inherit an ambiguous login.
	if _, err := e.admin.Exec(context.Background(),
		`delete from producer_members where producer_id = $1
		   and user_id = (select id from users where email = $2)`, tn.ID, adminEmail); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
}

// members lists a producer's members as its owner.
func (e *env) members(t *testing.T, tn *tenant) string {
	t.Helper()
	code, raw := e.do(http.MethodGet, "/v1/producer/members", tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusOK)
	return string(raw)
}

// TestReplayScopedKeyCanBeIssued is FR-208: a key may be issued with the optional
// replay scope, and the scope is visible on every later read. A column that no
// issuance path can set is not a feature.
func TestReplayScopedKeyCanBeIssued(t *testing.T) {
	e := setup(t)
	tn := e.seedTenant(t, "replay-co")

	code, raw := e.do(http.MethodPost, "/v1/producer/keys?env=live", tn.OwnerToken, map[string]bool{
		"replay_scope": true,
	})
	mustStatus(t, code, raw, http.StatusCreated)
	replay := decode[keyJSON](t, raw)
	if !replay.ReplayScope {
		t.Fatalf("a key issued with replay_scope=true is reported with replay_scope=false: %s", raw)
	}
	if replay.Key == nil || *replay.Key == "" {
		t.Fatalf("a replay-scoped key was not shown once at issuance (FR-202): %s", raw)
	}

	// The authentication path carries the scope to the request (FR-208's
	// separation): whoami reports it, so a publish-only credential is
	// distinguishable from an administrative one.
	code, raw = e.do(http.MethodGet, "/v1/whoami", *replay.Key, nil)
	mustStatus(t, code, raw, http.StatusOK)
	if who := decode[struct {
		ReplayOnly bool `json:"replay_only"`
	}](t, raw); !who.ReplayOnly {
		t.Fatalf("whoami does not report the replay scope: %s", raw)
	}

	// It survives a read back from the database, and the default is still false.
	code, raw = e.do(http.MethodGet, "/v1/producer/keys/"+replay.ID+"?env=live", tn.OwnerToken, nil)
	mustStatus(t, code, raw, http.StatusOK)
	if !decode[keyJSON](t, raw).ReplayScope {
		t.Fatal("replay_scope was not persisted")
	}

	code, raw = e.do(http.MethodPost, "/v1/producer/keys?env=live", tn.OwnerToken, map[string]bool{
		"replay_scope": false,
	})
	mustStatus(t, code, raw, http.StatusCreated)
	if decode[keyJSON](t, raw).ReplayScope {
		t.Fatal("a key issued with replay_scope=false is reported as replay-scoped")
	}
}

// TestSuspendIsIdempotentAndReportsTheStoredTimestamp pins the response to state.
// A retried suspend used to answer 200 with a fresh `now` while the row kept the
// original value: a client reconciling the two would see a suspension that moved
// every time it was retried.
func TestSuspendIsIdempotentAndReportsTheStoredTimestamp(t *testing.T) {
	e := setup(t)
	tn := e.seedTenant(t, "idem-co")

	code, raw := e.do(http.MethodPost, "/v1/admin/producers/"+tn.ID+"/suspend", e.adminToken,
		map[string]string{"reason": "first"})
	mustStatus(t, code, raw, http.StatusOK)
	first := decode[producerJSON](t, raw)
	if first.SuspendedAt == nil {
		t.Fatalf("a suspended producer has no suspended_at: %s", raw)
	}

	// The timestamps the API emits have second resolution (RFC 3339), so wait
	// long enough that a freshly-computed value would differ.
	time.Sleep(1100 * time.Millisecond)

	code, raw = e.do(http.MethodPost, "/v1/admin/producers/"+tn.ID+"/suspend", e.adminToken,
		map[string]string{"reason": "retry"})
	mustStatus(t, code, raw, http.StatusOK)
	second := decode[producerJSON](t, raw)
	if second.SuspendedAt == nil || *second.SuspendedAt != *first.SuspendedAt {
		t.Fatalf("a no-op suspend returned suspended_at %v, want the stored %q",
			second.SuspendedAt, *first.SuspendedAt)
	}

	// And the response agrees with what a read returns.
	code, raw = e.do(http.MethodGet, "/v1/admin/producers/"+tn.ID, e.adminToken, nil)
	mustStatus(t, code, raw, http.StatusOK)
	if got := decode[producerJSON](t, raw); got.SuspendedAt == nil || *got.SuspendedAt != *first.SuspendedAt {
		t.Fatalf("stored suspended_at = %v, want %q", got.SuspendedAt, *first.SuspendedAt)
	}

	// Resume is idempotent in the other direction.
	for i := 0; i < 2; i++ {
		code, raw = e.do(http.MethodPost, "/v1/admin/producers/"+tn.ID+"/resume", e.adminToken, nil)
		mustStatus(t, code, raw, http.StatusOK)
		if p := decode[producerJSON](t, raw); p.Suspended || p.SuspendedAt != nil {
			t.Fatalf("resume %d returned suspended=%v suspended_at=%v", i, p.Suspended, p.SuspendedAt)
		}
	}
}
