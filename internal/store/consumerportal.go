package store

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/m7jay/pigeon/internal/id"
	"github.com/m7jay/pigeon/internal/ssrf"
)

// Consumer portal self-service (M2b, UI-13/UI-14, AC-38, AC-39).
//
// These are the same operations the producer-facing endpoints.go and
// subscriptions.go expose, but the caller's consumer id is part of every WHERE
// clause rather than an argument the client supplies. A consumer user's session
// names exactly one consumer (FR-110), so the handler passes that id straight
// through and the store filters by it: a request aimed at another consumer's
// endpoint or subscription - by guessing the id - answers ErrNotFound, which the
// handler renders as 404 (AC-39). "The UI hiding a control is not the control":
// the boundary is here, in SQL, on top of the producer+env RLS scope.
//
// The producer-facing methods filter by (producer, env) only, because a producer
// member legitimately spans all of its consumers. These add the consumer_id
// predicate on purpose and are the one place the consumer boundary is enforced,
// so it is greppable.

func errInvalidField(field string) error {
	return fmt.Errorf("%w: %s is required", ErrInvalid, field)
}

// CreateConsumerEndpoint registers one of the caller's own endpoints (UI-13).
// consumerID is the caller's, resolved from the session by the handler and never
// taken from the request body, so a consumer cannot register an endpoint under a
// consumer they do not own. FR-402's `internal` escape hatch is an operator
// decision a consumer does not have, so it is always false here.
func (s *Store) CreateConsumerEndpoint(ctx context.Context, sc Scope, consumerID, name, url string) (EndpointCreated, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return EndpointCreated{}, errInvalidField("name")
	}
	url = strings.TrimSpace(url)
	if url == "" {
		return EndpointCreated{}, errInvalidField("url")
	}
	// FR-402 / AC-19: reject the URL before the row exists. Fail closed, and never
	// let a consumer mark an address internal.
	if err := ssrf.CheckEndpointURL(url, sc.Env, false); err != nil {
		return EndpointCreated{}, err
	}

	secret, hint, err := createEndpointSecret()
	if err != nil {
		return EndpointCreated{}, err
	}
	token := id.New("vrf") // high-entropy challenge, shown once
	var ep Endpoint
	err = s.scoped(ctx, sc, func(tx pgx.Tx) error {
		scanErr := tx.QueryRow(ctx, `
			insert into endpoints
				(id, producer_id, env, consumer_id, name, url, internal,
				 verification_token_hash, verification_hint, signing_secret, signing_secret_hint)
			values ($1,$2,$3,$4,$5,$6,false, $7, $8, $9, $10)
			returning id, consumer_id, name, url, internal, status,
				verified_at is not null, coalesce(verified_at, now()),
				coalesce(paused_at, now()), coalesce(deleted_at, now()), created_at`,
			id.New("end"), sc.ProducerID, sc.Env, consumerID, name, url,
			tokenHash(token), token[:8], secret, hint,
		).Scan(&ep.ID, &ep.ConsumerID, &ep.Name, &ep.URL, &ep.Internal, &ep.Status,
			&ep.Verified, &ep.VerifiedAt, &ep.PausedAt, &ep.DeletedAt, &ep.CreatedAt)
		return mapPgError(scanErr, ErrConflict, ErrNotFound)
	})
	if err != nil {
		return EndpointCreated{}, err
	}
	return EndpointCreated{
		Endpoint:          ep,
		Secret:            secret,
		SecretHint:        hint,
		VerificationToken: token,
		VerificationHint:  token[:8],
	}, nil
}

// GetConsumerEndpoint reads one of the caller's endpoints. An id that is not one
// of the caller's own - wrong consumer, deleted, or cross-environment - is
// ErrNotFound (AC-39).
func (s *Store) GetConsumerEndpoint(ctx context.Context, sc Scope, consumerID, endpointID string) (Endpoint, error) {
	var ep Endpoint
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `select `+endpointSelectCols+`
			from endpoints
			where producer_id = $1 and env = $2 and consumer_id = $3 and id = $4 and status <> 'deleted'`,
			sc.ProducerID, sc.Env, consumerID, endpointID).Scan(
			&ep.ID, &ep.ConsumerID, &ep.Name, &ep.URL, &ep.Internal, &ep.Status,
			&ep.Verified, &ep.VerifiedAt, &ep.PausedAt, &ep.DeletedAt, &ep.CreatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	})
	if err != nil {
		return Endpoint{}, err
	}
	return ep, nil
}

// consumerEndpointOwnedTx reports whether endpointID belongs to this consumer and
// is not deleted, inside an already-open transaction. The subscription writes use
// it so a consumer cannot subscribe or unsubscribe against an endpoint that is
// not theirs (AC-39).
func consumerEndpointOwnedTx(ctx context.Context, tx pgx.Tx, sc Scope, consumerID, endpointID string) (bool, error) {
	var owned bool
	err := tx.QueryRow(ctx, `select exists (
		select 1 from endpoints
		where producer_id = $1 and env = $2 and id = $3 and consumer_id = $4 and status <> 'deleted'
	)`, sc.ProducerID, sc.Env, endpointID, consumerID).Scan(&owned)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrNotFound
	}
	if err != nil {
		return false, err
	}
	return owned, nil
}

// SetConsumerEndpointPaused pauses or resumes one of the caller's endpoints
// (UI-13). A deleted or foreign id is ErrNotFound.
func (s *Store) SetConsumerEndpointPaused(ctx context.Context, sc Scope, consumerID, endpointID string, paused bool) (Endpoint, error) {
	status := "active"
	if paused {
		status = "paused"
	}
	var ep Endpoint
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		var res pgconn.CommandTag
		var err error
		if status == "deleted" {
			res, err = tx.Exec(ctx, `
				update endpoints set status = 'deleted', deleted_at = now()
				where producer_id = $1 and env = $2 and consumer_id = $3 and id = $4 and status <> 'deleted'`,
				sc.ProducerID, sc.Env, consumerID, endpointID)
		} else {
			pausedAt := "coalesce(paused_at, now())"
			if paused {
				pausedAt = "now()"
			}
			res, err = tx.Exec(ctx, fmt.Sprintf(`
				update endpoints set status = $5, paused_at = %s
				where producer_id = $1 and env = $2 and consumer_id = $3 and id = $4 and status <> 'deleted'`, pausedAt),
				sc.ProducerID, sc.Env, consumerID, endpointID, status)
		}
		if err != nil {
			return mapPgError(err, ErrConflict, ErrNotFound)
		}
		if res.RowsAffected() == 0 {
			return ErrNotFound
		}
		return tx.QueryRow(ctx, `select `+endpointSelectCols+`
			from endpoints where producer_id = $1 and env = $2 and consumer_id = $3 and id = $4`,
			sc.ProducerID, sc.Env, consumerID, endpointID).Scan(
			&ep.ID, &ep.ConsumerID, &ep.Name, &ep.URL, &ep.Internal, &ep.Status,
			&ep.Verified, &ep.VerifiedAt, &ep.PausedAt, &ep.DeletedAt, &ep.CreatedAt)
	})
	if err != nil {
		return Endpoint{}, err
	}
	return ep, nil
}

// DeleteConsumerEndpoint is FR-407's soft delete, consumer-scoped: the row is
// retained (history, the FK from deliveries) and never hard-removed. A deleted or
// foreign id is ErrNotFound.
func (s *Store) DeleteConsumerEndpoint(ctx context.Context, sc Scope, consumerID, endpointID string) error {
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		res, err := tx.Exec(ctx, `
			update endpoints set status = 'deleted', deleted_at = now()
			where producer_id = $1 and env = $2 and consumer_id = $3 and id = $4 and status <> 'deleted'`,
			sc.ProducerID, sc.Env, consumerID, endpointID)
		if err != nil {
			return mapPgError(err, ErrConflict, ErrNotFound)
		}
		if res.RowsAffected() == 0 {
			return ErrNotFound
		}
		return nil
	})
	return err
}

// VerifyConsumerEndpoint answers the ownership challenge (FR-403) for one of the
// caller's endpoints. A correct token stamps verified_at; a wrong token is
// ErrInvalid; an unknown or foreign endpoint is ErrNotFound.
func (s *Store) VerifyConsumerEndpoint(ctx context.Context, sc Scope, consumerID, endpointID, token string) error {
	if strings.TrimSpace(token) == "" {
		return errInvalidField("token")
	}
	return s.scoped(ctx, sc, func(tx pgx.Tx) error {
		var hash []byte
		err := tx.QueryRow(ctx, `select verification_token_hash
			from endpoints where producer_id = $1 and env = $2 and consumer_id = $3 and id = $4 and status <> 'deleted'`,
			sc.ProducerID, sc.Env, consumerID, endpointID).Scan(&hash)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if subtle.ConstantTimeCompare(hash, tokenHash(token)) != 1 {
			return fmt.Errorf("%w: token does not match", ErrInvalid)
		}
		res, err := tx.Exec(ctx, `
			update endpoints set verified_at = now()
			where producer_id = $1 and env = $2 and consumer_id = $3 and id = $4 and status <> 'deleted'
			  and verified_at is null`,
			sc.ProducerID, sc.Env, consumerID, endpointID)
		if err != nil {
			return err
		}
		if res.RowsAffected() == 0 {
			return nil // already verified is idempotent
		}
		return nil
	})
}

// ListConsumerSubscriptions returns the caller's subscriptions for one of their
// own endpoints, newest pair first. An endpoint id that is not the caller's
// yields an empty list, never another consumer's pairs (AC-39).
func (s *Store) ListConsumerSubscriptions(ctx context.Context, sc Scope, consumerID, endpointID string) ([]Subscription, error) {
	var out []Subscription
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `select s.id, s.endpoint_id, s.event_type, s.status, s.created_at
			from subscriptions s
			where s.producer_id = $1 and s.env = $2 and s.endpoint_id = $3 and s.status <> 'deleted'
			  and exists (
				select 1 from endpoints e
				where e.producer_id = s.producer_id and e.env = s.env and e.id = s.endpoint_id
				  and e.consumer_id = $4 and e.status <> 'deleted')
			order by s.created_at desc`,
			sc.ProducerID, sc.Env, endpointID, consumerID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var sub Subscription
			if err := rows.Scan(&sub.ID, &sub.EndpointID, &sub.EventType, &sub.Status, &sub.CreatedAt); err != nil {
				return err
			}
			out = append(out, sub)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// CreateConsumerSubscription opts one of the caller's endpoints into an event
// type (UI-14). The endpoint must belong to this consumer, or it is ErrNotFound
// (AC-39); an unknown event type is ErrNotFound through the FK; re-adding an
// active pair is ErrConflict.
func (s *Store) CreateConsumerSubscription(ctx context.Context, sc Scope, consumerID, endpointID, eventType string) (Subscription, error) {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return Subscription{}, errInvalidField("event_type")
	}
	var sub Subscription
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		owned, err := consumerEndpointOwnedTx(ctx, tx, sc, consumerID, endpointID)
		if err != nil {
			return err
		}
		if !owned {
			return ErrNotFound
		}
		return tx.QueryRow(ctx, `
			insert into subscriptions (id, producer_id, env, endpoint_id, event_type)
			values ($1,$2,$3,$4,$5)
			returning id, endpoint_id, event_type, status, created_at`,
			id.New("sub"), sc.ProducerID, sc.Env, endpointID, eventType,
		).Scan(&sub.ID, &sub.EndpointID, &sub.EventType, &sub.Status, &sub.CreatedAt)
	})
	if err != nil {
		return Subscription{}, mapPgError(err, ErrConflict, ErrNotFound)
	}
	return sub, nil
}

// DeleteConsumerSubscription opts one of the caller's endpoints out of an event
// type (UI-14). A pair against an endpoint that is not the caller's is
// ErrNotFound, so the unsubscribe cannot touch another consumer's subscription.
func (s *Store) DeleteConsumerSubscription(ctx context.Context, sc Scope, consumerID, endpointID, eventType string) error {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return errInvalidField("event_type")
	}
	return s.scoped(ctx, sc, func(tx pgx.Tx) error {
		owned, err := consumerEndpointOwnedTx(ctx, tx, sc, consumerID, endpointID)
		if err != nil {
			return err
		}
		if !owned {
			return ErrNotFound
		}
		res, err := tx.Exec(ctx, `
			update subscriptions set status = 'deleted'
			where producer_id = $1 and env = $2 and endpoint_id = $3 and event_type = $4
			  and status <> 'deleted'
			  and exists (select 1 from endpoints e
				where e.producer_id = subscriptions.producer_id and e.env = subscriptions.env
				  and e.id = subscriptions.endpoint_id and e.consumer_id = $5 and e.status <> 'deleted')`,
			sc.ProducerID, sc.Env, endpointID, eventType, consumerID)
		if err != nil {
			return mapPgError(err, ErrConflict, ErrNotFound)
		}
		if res.RowsAffected() == 0 {
			return ErrNotFound
		}
		return nil
	})
}
