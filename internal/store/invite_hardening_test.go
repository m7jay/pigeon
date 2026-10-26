package store

import (
	"context"
	"strings"
	"testing"

	"github.com/m7jay/pigeon/internal/identity"
)

// longPassword is longer than bcrypt accepts (72 bytes). HashPassword rejects it,
// so the error a caller sees tells us *when* hashing happened: ErrInvalid means
// the password was hashed before the invitation was looked up, ErrNotFound means
// the (unknown) token was rejected first.
func longPassword() string { return strings.Repeat("p", 100) }

// TestInviteAcceptLooksUpTheInviteBeforeHashing is the CPU-exhaustion fix.
//
// POST /v1/invites/accept is the only anonymous write in the API, and bcrypt at
// cost 12 costs ~250 ms. Hashing before the invite lookup turns a random
// well-formed token plus any 8-character password into a cheap way to burn a CPU:
// the token does not have to exist for the work to happen.
func TestInviteAcceptLooksUpTheInviteBeforeHashing(t *testing.T) {
	admin := testPool(t)
	app := appPool(t, admin)
	st := New(app)
	ctx := context.Background()

	// Well-formed, correctly prefixed, and not in the table.
	unknown := InvitePrefix + strings.Repeat("A", 43)
	if _, err := st.AcceptInvite(ctx, unknown, longPassword(), ""); err != ErrNotFound {
		t.Fatalf("accepting an unknown token returned %v; want ErrNotFound, which proves the lookup happened before bcrypt (a hash-first path answers ErrInvalid)", err)
	}
}

// TestInviteAcceptanceCannotRewriteAnExistingAccount is the regression test for
// the account-takeover defect: `pigeon_invite_accept` used to overwrite the
// password of whatever user already owned the invited address, and the invitation
// token is handed to the *inviter* (there is no mailer yet). Together those two
// facts let any producer owner take over the instance admin.
//
// The rule the schema now enforces: an invitation addressed to an address that
// already has an account never touches that account's credentials, and it can only
// be redeemed by that account's own authenticated session.
func TestInviteAcceptanceCannotRewriteAnExistingAccount(t *testing.T) {
	admin := testPool(t)
	app := appPool(t, admin)
	st := New(app)
	ctx := context.Background()

	const (
		email       = "existing-account@example.test"
		oldPassword = "original-password-1"
		newPassword = "attacker-password-1"
		userID      = "usr_existing_account"
		producerID  = "prd_invite_takeover"
		inviteID    = "inv_takeover"
	)

	if _, err := admin.Exec(ctx,
		`insert into producers (id, slug, name) values ($1,'invite-takeover','Invite takeover') on conflict do nothing`,
		producerID); err != nil {
		t.Fatalf("seed producer: %v", err)
	}

	hash, err := identity.HashPassword(oldPassword)
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	if _, err := admin.Exec(ctx,
		`insert into users (id, email, password_hash) values ($1,$2,$3)
		 on conflict (id) do update set password_hash = excluded.password_hash`,
		userID, email, hash); err != nil {
		t.Fatalf("seed user: %v", err)
	}

	token := InvitePrefix + strings.Repeat("B", 43)
	if _, err := admin.Exec(ctx, `
		insert into invites (id, producer_id, env, email, role, token_hash, invited_by, expires_at)
		values ($1, $2, null, $3, 'developer', $4, $5, now() + interval '1 day')
		on conflict (id) do nothing`,
		inviteID, producerID, email, hashToken(token), userID); err != nil {
		t.Fatalf("seed invite: %v", err)
	}

	// 1. Possession of the token alone must not be enough: the invitee has to
	//    prove they are the account holder.
	if _, err := st.AcceptInvite(ctx, token, newPassword, ""); err == nil {
		t.Fatal("an invitation for an existing account was redeemed anonymously; the token alone must not be a credential")
	}

	// 2. Someone else's session must not be enough either.
	if _, err := st.AcceptInvite(ctx, token, newPassword, "usr_someone_else"); err == nil {
		t.Fatal("an invitation for an existing account was redeemed by a different user")
	}

	// 3. The account's own session may redeem it, and it still must not touch
	//    the stored password.
	acc, err := st.AcceptInvite(ctx, token, newPassword, userID)
	if err != nil {
		t.Fatalf("the account holder could not accept their own invitation: %v", err)
	}
	if acc.UserID != userID || acc.Role != "developer" {
		t.Fatalf("accepted invite = %+v, want user %s as developer", acc, userID)
	}

	var stored string
	if err := admin.QueryRow(ctx, `select password_hash from users where id = $1`, userID).Scan(&stored); err != nil {
		t.Fatalf("read password: %v", err)
	}
	if stored != hash {
		t.Fatal("accepting an invitation rewrote an existing account's password; that is the account-takeover defect")
	}
	var mustChange bool
	if err := admin.QueryRow(ctx, `select must_change_password from users where id = $1`, userID).Scan(&mustChange); err != nil {
		t.Fatalf("read must_change_password: %v", err)
	}
	if mustChange {
		t.Fatal("accepting an invitation set must_change_password on an existing account")
	}

	// 4. The membership is the only thing that changed.
	var role string
	if err := admin.QueryRow(ctx,
		`select role from producer_members where producer_id = $1 and user_id = $2`,
		producerID, userID).Scan(&role); err != nil {
		t.Fatalf("the accepted invitation did not add the membership: %v", err)
	}
	if role != "developer" {
		t.Fatalf("membership role = %q, want developer", role)
	}

	// 5. The token is burned.
	if _, err := st.AcceptInvite(ctx, token, newPassword, userID); err == nil {
		t.Fatal("an invitation was accepted twice")
	}

	_, _ = admin.Exec(ctx, `delete from producer_members where producer_id = $1`, producerID)
	_, _ = admin.Exec(ctx, `delete from invites where id = $1`, inviteID)
	_, _ = admin.Exec(ctx, `delete from producers where id = $1`, producerID)
	_, _ = admin.Exec(ctx, `delete from users where id = $1`, userID)
}

// TestTenantTablesCannotBeBypassedByTheRuntimeRole is the deployment half of
// MT-1: RLS only constrains a role that does not own the table. The runtime role
// must either not own the tenant tables, or the tables must carry
// `force row level security`. This asserts it from the runtime role itself, so a
// migration that creates a table owned by the app role (or a deployment that runs
// the app as the owner) fails here rather than in a review comment.
func TestTenantTablesCannotBeBypassedByTheRuntimeRole(t *testing.T) {
	admin := testPool(t)
	app := appPool(t, admin)
	ctx := context.Background()

	tables := []string{
		"producers", "producer_members", "consumers", "consumer_users",
		"api_keys", "invites", "sessions", "admin_audit_log",
	}

	rows, err := app.Query(ctx, `
		select c.relname,
		       c.relrowsecurity,
		       c.relforcerowsecurity,
		       pg_get_userbyid(c.relowner) = current_user
		  from pg_class c
		  join pg_namespace n on n.oid = c.relnamespace
		 where n.nspname = 'public' and c.relkind = 'r' and c.relname = any($1)`,
		tables)
	if err != nil {
		t.Fatalf("read pg_class: %v", err)
	}
	defer rows.Close()

	seen := map[string]bool{}
	for rows.Next() {
		var name string
		var rls, forced, ownerIsRuntime bool
		if err := rows.Scan(&name, &rls, &forced, &ownerIsRuntime); err != nil {
			t.Fatalf("scan: %v", err)
		}
		seen[name] = true
		if !rls {
			t.Errorf("table %s has row level security disabled; a tenant table without RLS is a cross-tenant read waiting to happen", name)
		}
		if ownerIsRuntime && !forced {
			t.Errorf("table %s is owned by the runtime role and does not force row level security, so every policy on it is inert in the deployed topology", name)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	for _, name := range tables {
		if !seen[name] {
			t.Errorf("expected tenant table %s does not exist or is not a table", name)
		}
	}

	// The owner must not be the runtime role at all: that is the property the
	// deployment has to preserve (compose.yaml runs migrations as the owner and
	// serves as pigeon_app).
	var owner, runtimeRole string
	if err := app.QueryRow(ctx, `
		select distinct pg_get_userbyid(c.relowner)
		  from pg_class c
		  join pg_namespace n on n.oid = c.relnamespace
		 where n.nspname = 'public' and c.relkind = 'r' and c.relname = any($1)`,
		tables).Scan(&owner); err != nil {
		t.Fatalf("read table owners: %v", err)
	}
	if err := app.QueryRow(ctx, `select current_user`).Scan(&runtimeRole); err != nil {
		t.Fatalf("read current_user: %v", err)
	}
	if owner == runtimeRole {
		t.Fatalf("the runtime role %q owns the tenant tables; every RLS policy is bypassed for it", runtimeRole)
	}
}
