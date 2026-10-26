package store

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/m7jay/pigeon/internal/db"
	"github.com/m7jay/pigeon/internal/id"
	"github.com/m7jay/pigeon/internal/identity"
)

// Audit actions. They are the vocabulary of MT-5's log: one constant per thing
// the instance admin can do, so a query for "who suspended anything last week"
// is exact rather than a LIKE over prose.
const (
	ActionProducerCreated   = "producer.created"
	ActionProducerSuspended = "producer.suspended"
	ActionProducerResumed   = "producer.resumed"
)

// Producer is a tenant. SuspendedAt non-nil means FR-107 suspension is in
// effect: ingestion credentials are refused and, from S3, non-terminal
// deliveries stop.
type Producer struct {
	ID          string
	Slug        string
	Name        string
	SuspendedAt *time.Time
	CreatedAt   time.Time
}

// Suspended reports FR-107 state without the caller having to nil-check.
func (p Producer) Suspended() bool { return p.SuspendedAt != nil }

// slugPattern mirrors the producers_slug_format check in 0001. Validating in Go
// as well as in SQL turns a 500 (constraint violation) into a 422 with the
// reason, which is the difference between an actionable error and a bug report.
var slugPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,62}$`)

// ValidateSlug is exported so the handler can reject a bad slug before opening a
// transaction.
func ValidateSlug(slug string) error {
	if !slugPattern.MatchString(slug) {
		return fmt.Errorf("%w: slug must match %s", ErrInvalid, slugPattern)
	}
	return nil
}

// NewProducer is the admin's create request. OwnerEmail is required: a producer
// with no owner has nobody who can invite, manage or recover it, and the admin
// API has no "adopt a member" route by design (FR-104 gives that power to
// owners). Creating the tenant and its first owner invitation together is what
// keeps that invariant true from the first second.
type NewProducer struct {
	Slug       string
	Name       string
	OwnerEmail string
}

// CreateProducer is FR-102: the instance admin creates a tenant, and the tenant
// arrives with an owner invitation so it is manageable immediately.
//
// Self-service registration is a configuration flag (see config), not a second
// code path in here.
func (s *Store) CreateProducer(ctx context.Context, actor Actor, in NewProducer) (Producer, Invite, error) {
	if err := ValidateSlug(in.Slug); err != nil {
		return Producer{}, Invite{}, err
	}
	name := strings.TrimSpace(in.Name)
	if name == "" {
		return Producer{}, Invite{}, fmt.Errorf("%w: name is required", ErrInvalid)
	}
	if err := validateEmail(in.OwnerEmail); err != nil {
		return Producer{}, Invite{}, err
	}

	token := newToken(InvitePrefix)
	expires := time.Now().Add(InviteTTL)
	inv := Invite{
		ID: id.New("inv"), Email: strings.TrimSpace(in.OwnerEmail), Role: "owner",
		Token: token, ExpiresAt: expires,
	}

	var p Producer
	err := s.admin(ctx, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			insert into producers (id, slug, name) values ($1, $2, $3)
			returning id, slug, name, suspended_at, created_at`,
			id.New("prd"), in.Slug, name,
		).Scan(&p.ID, &p.Slug, &p.Name, &p.SuspendedAt, &p.CreatedAt)
		if err != nil {
			return mapPgError(err, ErrConflict, ErrNotFound)
		}
		// The invitation row is written under the tenant policy, so the new
		// producer's scope is set on this transaction too.
		if err := db.SetProducerScope(ctx, tx, p.ID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			insert into invites (id, producer_id, env, email, role, token_hash, invited_by, expires_at)
			values ($1, $2, null, $3, 'owner', $4, $5, $6)`,
			inv.ID, p.ID, inv.Email, hashToken(token), actor.UserID, expires); err != nil {
			return mapPgError(err, ErrConflict, ErrNotFound)
		}
		return insertAudit(ctx, tx, actor, ActionProducerCreated, p.ID,
			"created via instance-admin API; first owner invited")
	})
	if err != nil {
		return Producer{}, Invite{}, err
	}
	return p, inv, nil
}

// ListProducers is the admin's tenant list.
func (s *Store) ListProducers(ctx context.Context) ([]Producer, error) {
	var out []Producer
	err := s.admin(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			select id, slug, name, suspended_at, created_at
			  from producers order by slug`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var p Producer
			if err := rows.Scan(&p.ID, &p.Slug, &p.Name, &p.SuspendedAt, &p.CreatedAt); err != nil {
				return err
			}
			out = append(out, p)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetProducer reads one producer on the admin path. A producer id that does not
// exist is ErrNotFound; so is an id the caller may not see, which for the admin
// is the same thing (there is no "another admin's producer").
func (s *Store) GetProducer(ctx context.Context, producerID string) (Producer, error) {
	var p Producer
	err := s.admin(ctx, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			select id, slug, name, suspended_at, created_at
			  from producers where id = $1`, producerID,
		).Scan(&p.ID, &p.Slug, &p.Name, &p.SuspendedAt, &p.CreatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	})
	if err != nil {
		return Producer{}, err
	}
	return p, nil
}

// SetProducerSuspended is FR-107: suspend and resume, reversibly, with the
// reason recorded against the actor (MT-5). It is idempotent so that a retried
// suspend is not an error; the audit row records the transition that actually
// happened, not the request.
func (s *Store) SetProducerSuspended(ctx context.Context, actor Actor, producerID string, suspended bool, reason string) (Producer, error) {
	var p Producer
	err := s.admin(ctx, func(tx pgx.Tx) error {
		var previous *time.Time
		err := tx.QueryRow(ctx, `
			select id, slug, name, suspended_at, created_at
			  from producers where id = $1 for update`, producerID,
		).Scan(&p.ID, &p.Slug, &p.Name, &p.SuspendedAt, &p.CreatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		previous = p.SuspendedAt

		if suspended {
			if previous == nil {
				if _, err := tx.Exec(ctx, `update producers set suspended_at = now() where id = $1`, producerID); err != nil {
					return err
				}
			}
		} else if previous != nil {
			if _, err := tx.Exec(ctx, `update producers set suspended_at = null where id = $1`, producerID); err != nil {
				return err
			}
		}

		action := ActionProducerResumed
		if suspended {
			action = ActionProducerSuspended
		}
		// A no-op request still writes an audit row: an admin asking twice is
		// information (an automated caller is stuck in a loop, or a human is
		// unsure) and the actor trail should show it.
		if err := insertAudit(ctx, tx, actor, action, producerID, reason); err != nil {
			return err
		}

		// Re-read rather than compute the new value: an idempotent suspend keeps
		// the original suspended_at, and a response carrying a fresh `now` would
		// disagree with the row every later read returns.
		return tx.QueryRow(ctx, `
			select id, slug, name, suspended_at, created_at
			  from producers where id = $1`, producerID,
		).Scan(&p.ID, &p.Slug, &p.Name, &p.SuspendedAt, &p.CreatedAt)
	})
	if err != nil {
		return Producer{}, err
	}
	return p, nil
}

// insertAudit writes one MT-5 row. It runs in the caller's transaction on
// purpose: an admin action that committed without its audit row would be
// exactly the gap the requirement exists to close.
func insertAudit(ctx context.Context, tx pgx.Tx, actor Actor, action, targetProducerID, reason string) error {
	if actor.UserID == "" {
		// Every admin route is a session route today, so an empty actor is a
		// wiring bug rather than a case to support.
		return fmt.Errorf("store: audit %s without an actor", action)
	}
	_, err := tx.Exec(ctx, `
		insert into admin_audit_log (id, actor_user_id, action, target_producer_id, reason, request_id)
		values ($1, $2, $3, nullif($4, ''), nullif($5, ''), nullif($6, ''))`,
		id.New("aud"), actor.UserID, action, targetProducerID, reason, actor.RequestID)
	if err != nil {
		return fmt.Errorf("insert audit: %w", err)
	}
	return nil
}

// AuditEntry is one MT-5 row as the API shows it.
type AuditEntry struct {
	ID               string
	ActorUserID      string
	Action           string
	TargetProducerID *string
	Reason           *string
	RequestID        *string
	CreatedAt        time.Time
}

// ListAudit returns the audit trail for one producer, newest first. It reads on
// the instance-admin path because the log is not tenant data: a producer owner
// reading the record of their own suspension would be reasonable, but it is not
// what S1 promises and the policy in 0004 says so.
func (s *Store) ListAudit(ctx context.Context, producerID string) ([]AuditEntry, error) {
	var out []AuditEntry
	err := s.admin(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			select id, actor_user_id, action, target_producer_id, reason, request_id, created_at
			  from admin_audit_log
			 where target_producer_id = $1
			 order by created_at desc, id desc`, producerID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var e AuditEntry
			if err := rows.Scan(&e.ID, &e.ActorUserID, &e.Action, &e.TargetProducerID,
				&e.Reason, &e.RequestID, &e.CreatedAt); err != nil {
				return err
			}
			out = append(out, e)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// SelfRegisterProducer is FR-102's opt-in path: a tenant and its first owner are
// created together, so a self-registered producer is never ownerless (which would
// make it unmanageable).
//
// It is not an instance-admin action, so it writes no MT-5 row: the actor is the
// new owner, and the row that records them is the membership itself.
func (s *Store) SelfRegisterProducer(ctx context.Context, slug, name, email, password string) (Producer, error) {
	if err := ValidateSlug(slug); err != nil {
		return Producer{}, err
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return Producer{}, fmt.Errorf("%w: name is required", ErrInvalid)
	}
	if err := validateEmail(email); err != nil {
		return Producer{}, err
	}
	hash, err := identity.HashPassword(password)
	if err != nil {
		return Producer{}, fmt.Errorf("%w: %s", ErrInvalid, err.Error())
	}

	var p Producer
	err = s.admin(ctx, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			insert into producers (id, slug, name) values ($1, $2, $3)
			returning id, slug, name, suspended_at, created_at`,
			id.New("prd"), slug, name,
		).Scan(&p.ID, &p.Slug, &p.Name, &p.SuspendedAt, &p.CreatedAt)
		if err != nil {
			return mapPgError(err, ErrConflict, ErrNotFound)
		}
		// The membership insert below runs under the tenant policy, so the new
		// producer's scope has to be set on this transaction as well.
		if err := db.SetProducerScope(ctx, tx, p.ID); err != nil {
			return err
		}
		userID := id.New("usr")
		if _, err := tx.Exec(ctx, `
			insert into users (id, email, password_hash) values ($1, $2, $3)`,
			userID, strings.TrimSpace(email), hash); err != nil {
			return mapPgError(err, ErrConflict, ErrNotFound)
		}
		_, err = tx.Exec(ctx, `
			insert into producer_members (producer_id, user_id, role) values ($1, $2, 'owner')`,
			p.ID, userID)
		return mapPgError(err, ErrConflict, ErrNotFound)
	})
	if err != nil {
		return Producer{}, err
	}
	return p, nil
}
