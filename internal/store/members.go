package store

import (
	"context"
	"fmt"
	"net/mail"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/m7jay/pigeon/internal/id"
)

// InviteTTL is how long an invitation link stays usable. Seven days is long
// enough for a holiday backlog and short enough that a forwarded email is not a
// permanent credential; the token is burned on first use either way.
const InviteTTL = 7 * 24 * time.Hour

// Member is one producer membership row (FR-104).
type Member struct {
	UserID    string
	Email     string
	Role      string
	CreatedAt time.Time
}

// ConsumerUser is FR-109's binding: a user attached to exactly one consumer.
type ConsumerUser struct {
	UserID     string
	Email      string
	ConsumerID string
	CreatedAt  time.Time
}

// Invite is the one-time credential handed to an invitee. Token is the
// plaintext and exists only in this value (FR-202's rule applied to invitations).
type Invite struct {
	ID         string
	Email      string
	Role       string
	ConsumerID string
	Env        string
	Token      string
	ExpiresAt  time.Time
}

// InviteMeta is an invitation without its token, for listing.
type InviteMeta struct {
	ID         string
	Email      string
	Role       string
	ConsumerID string
	Env        string
	CreatedAt  time.Time
	ExpiresAt  time.Time
}

// ListMembers returns the humans in the producer, with their roles.
func (s *Store) ListMembers(ctx context.Context, sc Scope) ([]Member, error) {
	var out []Member
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			select pm.user_id, u.email, pm.role, pm.created_at
			  from producer_members pm
			  join users u on u.id = pm.user_id
			 where pm.producer_id = $1
			 order by u.email`, sc.ProducerID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var m Member
			if err := rows.Scan(&m.UserID, &m.Email, &m.Role, &m.CreatedAt); err != nil {
				return err
			}
			out = append(out, m)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// InviteMember is FR-104: an owner invites a user by email as owner or
// developer. The invitation is producer-wide, so it carries no environment.
func (s *Store) InviteMember(ctx context.Context, sc Scope, actor Actor, email, role string) (Invite, error) {
	if err := validateEmail(email); err != nil {
		return Invite{}, err
	}
	if role != "owner" && role != "developer" {
		return Invite{}, fmt.Errorf("%w: role must be owner or developer", ErrInvalid)
	}
	if actor.UserID == "" {
		return Invite{}, fmt.Errorf("%w: invitation needs an inviter", ErrInvalid)
	}

	token := newToken(InvitePrefix)
	expires := time.Now().Add(InviteTTL)
	inv := Invite{ID: id.New("inv"), Email: strings.TrimSpace(email), Role: role, Token: token, ExpiresAt: expires}

	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		// Re-inviting the same address for the same role answers 409 rather than
		// quietly minting a second live token: two valid links for one person is
		// a support incident, not a convenience.
		_, err := tx.Exec(ctx, `
			insert into invites (id, producer_id, env, email, role, token_hash, invited_by, expires_at)
			values ($1, $2, null, $3, $4, $5, $6, $7)`,
			inv.ID, sc.ProducerID, inv.Email, role, hashToken(token), actor.UserID, expires)
		return mapPgError(err, ErrConflict, ErrNotFound)
	})
	if err != nil {
		return Invite{}, err
	}
	return inv, nil
}

// InviteConsumerUser is FR-109: bind a consumer user to exactly one consumer.
// A consumer id outside the caller's (producer, environment) is ErrNotFound -
// the invitation flow must not become a way to probe for another tenant's
// consumers.
func (s *Store) InviteConsumerUser(ctx context.Context, sc Scope, actor Actor, consumerID, email string) (Invite, error) {
	if err := validateEmail(email); err != nil {
		return Invite{}, err
	}
	if actor.UserID == "" {
		return Invite{}, fmt.Errorf("%w: invitation needs an inviter", ErrInvalid)
	}

	token := newToken(InvitePrefix)
	expires := time.Now().Add(InviteTTL)
	inv := Invite{
		ID: id.New("inv"), Email: strings.TrimSpace(email), Role: "consumer",
		ConsumerID: consumerID, Env: sc.Env, Token: token, ExpiresAt: expires,
	}

	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		var exists bool
		if err := tx.QueryRow(ctx, `
			select exists (select 1 from consumers
			                where producer_id = $1 and env = $2 and id = $3)`,
			sc.ProducerID, sc.Env, consumerID).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return ErrNotFound
		}
		_, err := tx.Exec(ctx, `
			insert into invites (id, producer_id, env, email, role, consumer_id, token_hash, invited_by, expires_at)
			values ($1, $2, $3, $4, 'consumer', $5, $6, $7, $8)`,
			inv.ID, sc.ProducerID, sc.Env, inv.Email, consumerID, hashToken(token), actor.UserID, expires)
		return mapPgError(err, ErrConflict, ErrNotFound)
	})
	if err != nil {
		return Invite{}, err
	}
	return inv, nil
}

// ListInvites shows the pending invitations in the caller's scope. Member
// invitations (no environment) are producer-wide, so they appear in both
// planes; consumer invitations appear only in the plane they belong to.
func (s *Store) ListInvites(ctx context.Context, sc Scope) ([]InviteMeta, error) {
	var out []InviteMeta
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			select id, email, role, coalesce(consumer_id, ''), coalesce(env, ''), created_at, expires_at
			  from invites
			 where producer_id = $1
			   and (env is null or env = $2)
			   and accepted_at is null and revoked_at is null
			 order by created_at desc`, sc.ProducerID, sc.Env)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var m InviteMeta
			if err := rows.Scan(&m.ID, &m.Email, &m.Role, &m.ConsumerID, &m.Env, &m.CreatedAt, &m.ExpiresAt); err != nil {
				return err
			}
			out = append(out, m)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RevokeInvite burns a pending invitation. An invite id from another tenant is
// ErrNotFound (FR-108), and so is one that is already accepted or revoked.
func (s *Store) RevokeInvite(ctx context.Context, sc Scope, inviteID string) error {
	return s.scoped(ctx, sc, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			update invites set revoked_at = now()
			 where id = $1 and producer_id = $2
			   and (env is null or env = $3)
			   and accepted_at is null and revoked_at is null`,
			inviteID, sc.ProducerID, sc.Env)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// ListConsumerUsers lists who can see one consumer's data.
func (s *Store) ListConsumerUsers(ctx context.Context, sc Scope, consumerID string) ([]ConsumerUser, error) {
	var out []ConsumerUser
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			select cu.user_id, u.email, cu.consumer_id, cu.created_at
			  from consumer_users cu
			  join users u on u.id = cu.user_id
			 where cu.producer_id = $1 and cu.env = $2 and cu.consumer_id = $3
			 order by u.email`, sc.ProducerID, sc.Env, consumerID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var cu ConsumerUser
			if err := rows.Scan(&cu.UserID, &cu.Email, &cu.ConsumerID, &cu.CreatedAt); err != nil {
				return err
			}
			out = append(out, cu)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RevokeConsumerUser is the "independently of the consumer record" half of
// FR-109: it deletes the binding and leaves the consumer, its endpoints and its
// delivery history untouched. Existing sessions holding the claim stop working
// on their next request (0004's pigeon_session_lookup joins the binding).
func (s *Store) RevokeConsumerUser(ctx context.Context, sc Scope, consumerID, userID string) error {
	return s.scoped(ctx, sc, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			delete from consumer_users
			 where consumer_id = $1 and user_id = $2 and producer_id = $3 and env = $4`,
			consumerID, userID, sc.ProducerID, sc.Env)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// validateEmail rejects what cannot be an address without trying to be a mail
// server: one @, no spaces, a dotted domain. Deliverability is the mailer's
// problem; shape is ours.
func validateEmail(email string) error {
	email = strings.TrimSpace(email)
	if email == "" {
		return fmt.Errorf("%w: email is required", ErrInvalid)
	}
	addr, err := mail.ParseAddress(email)
	if err != nil || addr.Address != email {
		return fmt.Errorf("%w: %q is not an email address", ErrInvalid, email)
	}
	return nil
}
