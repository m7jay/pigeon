package store

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

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

// errNotPubliclySubscribable is UI-14's refusal: the producer has not allowed this
// event type to be publicly subscribable. It wraps ErrInvalid, so the API answers
// 422 with the reason rather than a bare refusal. Naming the reason leaks nothing:
// the consumer user is inside the producer's own tenant, and the alternative - a
// 404 - would tell them their type name was wrong.
var errNotPubliclySubscribable = fmt.Errorf(
	"%w: the producer has not made this event type publicly subscribable", ErrInvalid)

// errEventTypeDisabled is FR-305 seen from the consumer side: a disabled type can
// never be published, so a subscription to it would set up a delivery that can
// never happen.
var errEventTypeDisabled = fmt.Errorf("%w: this event type is disabled", ErrInvalid)

// NewConsumerEndpoint is a consumer user's POST to register one of their own
// endpoints (UI-13). The owning consumer is not a field: it is the session claim
// the handler passes in (FR-110).
type NewConsumerEndpoint struct {
	Name              string
	URL               string
	NotificationEmail string // optional; "" leaves it unset
}

// ConsumerEndpointPatch is a PATCH to one of the caller's own endpoints (UI-13):
// the fields a consumer user may change about their own endpoint. A nil field is
// left unchanged, so an empty body is a no-op. Paused is FR-406's toggle (status
// and paused_at together); Name, URL and NotificationEmail are the edit UI-13
// asks for, with "" clearing the email. There is deliberately no way to move an
// endpoint to another consumer: the owning consumer is the session claim and it
// is in the WHERE clause, so that request is a 404 rather than a 422.
type ConsumerEndpointPatch struct {
	Name              *string
	URL               *string
	NotificationEmail *string
	Paused            *bool
}

// notifyEmail normalises and validates UI-13's notification address. "" is legal
// (the consumer has not set one); anything else must look like an address, judged
// by the same helper an invitation uses.
func notifyEmail(email string) (any, error) {
	email = strings.TrimSpace(email)
	if email == "" {
		return nil, nil
	}
	if err := validateEmail(email); err != nil {
		return nil, fmt.Errorf("%w: notification_email %s", ErrInvalid, strings.TrimPrefix(err.Error(), ErrInvalid.Error()+": "))
	}
	return email, nil
}

// CreateConsumerEndpoint registers one of the caller's own endpoints (UI-13).
// consumerID is the caller's, resolved from the session by the handler and never
// taken from the request body, so a consumer cannot register an endpoint under a
// consumer they do not own. FR-402's `internal` escape hatch is an operator
// decision a consumer does not have, so it is always false here.
func (s *Store) CreateConsumerEndpoint(ctx context.Context, sc Scope, consumerID string, in NewConsumerEndpoint) (EndpointCreated, error) {
	name := strings.TrimSpace(in.Name)
	if name == "" {
		return EndpointCreated{}, errInvalidField("name")
	}
	url := strings.TrimSpace(in.URL)
	if url == "" {
		return EndpointCreated{}, errInvalidField("url")
	}
	email, err := notifyEmail(in.NotificationEmail)
	if err != nil {
		return EndpointCreated{}, err
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
				(id, producer_id, env, consumer_id, name, url, notification_email, internal,
				 verification_token_hash, verification_hint, signing_secret, signing_secret_hint)
			values ($1,$2,$3,$4,$5,$6,$7,false, $8, $9, $10, $11)
			returning id, consumer_id, name, url, notification_email, internal, status,
				verified_at is not null, coalesce(verified_at, now()),
				coalesce(paused_at, now()), coalesce(deleted_at, now()), created_at`,
			id.New("end"), sc.ProducerID, sc.Env, consumerID, name, url, email,
			tokenHash(token), token[:8], secret, hint,
		).Scan(&ep.ID, &ep.ConsumerID, &ep.Name, &ep.URL, &ep.NotificationEmail, &ep.Internal, &ep.Status,
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
			&ep.ID, &ep.ConsumerID, &ep.Name, &ep.URL, &ep.NotificationEmail, &ep.Internal, &ep.Status,
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

// UpdateConsumerEndpoint applies a ConsumerEndpointPatch to one of the caller's
// own endpoints and returns the updated row (UI-13: edit, set the notification
// email, pause/resume). Every field is written in one UPDATE whose predicate
// carries consumer_id, so a foreign, deleted or unknown id is ErrNotFound and the
// write cannot touch another consumer's row (AC-39). A URL edit re-runs FR-402's
// check before the row is touched: registering a public address and then moving
// the endpoint onto a private one is the same SSRF in two steps.
func (s *Store) UpdateConsumerEndpoint(ctx context.Context, sc Scope, consumerID, endpointID string, p ConsumerEndpointPatch) (Endpoint, error) {
	var name, url string
	var email any
	haveEmail := false
	if p.Name != nil {
		name = strings.TrimSpace(*p.Name)
		if name == "" {
			return Endpoint{}, errInvalidField("name")
		}
	}
	if p.URL != nil {
		url = strings.TrimSpace(*p.URL)
		if url == "" {
			return Endpoint{}, errInvalidField("url")
		}
		// FR-402 / AC-19: same pre-flight as registration, before any write.
		if err := ssrf.CheckEndpointURL(url, sc.Env, false); err != nil {
			return Endpoint{}, err
		}
	}
	if p.NotificationEmail != nil {
		var err error
		email, err = notifyEmail(*p.NotificationEmail)
		if err != nil {
			return Endpoint{}, err
		}
		haveEmail = true
	}

	// An empty patch is a read: it is the same answer as a PATCH that changed
	// nothing, and it still 404s a foreign id.
	if p.Name == nil && p.URL == nil && !haveEmail && p.Paused == nil {
		return s.GetConsumerEndpoint(ctx, sc, consumerID, endpointID)
	}

	var ep Endpoint
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		sets := make([]string, 0, 4)
		args := make([]any, 0, 8)
		add := func(col string, v any) {
			args = append(args, v)
			sets = append(sets, fmt.Sprintf("%s = $%d", col, len(args)))
		}
		if p.Name != nil {
			add("name", name)
		}
		if p.URL != nil {
			add("url", url)
		}
		if haveEmail {
			// Setting "" clears the address, exactly as the consumer asked.
			add("notification_email", email)
		}
		if p.Paused != nil {
			status := "active"
			pausedAt := "coalesce(paused_at, now())"
			if *p.Paused {
				status, pausedAt = "paused", "now()"
			}
			add("status", status)
			sets = append(sets, "paused_at = "+pausedAt) // literal, no placeholder
		}
		args = append(args, sc.ProducerID, sc.Env, consumerID, endpointID)
		q := fmt.Sprintf(`update endpoints set %s
			where producer_id = $%d and env = $%d and consumer_id = $%d and id = $%d and status <> 'deleted'`,
			strings.Join(sets, ", "), len(args)-3, len(args)-2, len(args)-1, len(args))
		res, err := tx.Exec(ctx, q, args...)
		if err != nil {
			return mapPgError(err, ErrConflict, ErrNotFound)
		}
		if res.RowsAffected() == 0 {
			return ErrNotFound
		}
		return tx.QueryRow(ctx, `select `+endpointSelectCols+`
			from endpoints where producer_id = $1 and env = $2 and consumer_id = $3 and id = $4`,
			sc.ProducerID, sc.Env, consumerID, endpointID).Scan(
			&ep.ID, &ep.ConsumerID, &ep.Name, &ep.URL, &ep.NotificationEmail, &ep.Internal, &ep.Status,
			&ep.Verified, &ep.VerifiedAt, &ep.PausedAt, &ep.DeletedAt, &ep.CreatedAt)
	})
	if err != nil {
		return Endpoint{}, err
	}
	return ep, nil
}

// RotateConsumerEndpointSecret mints a fresh signing secret for one of the
// caller's own endpoints and returns the plaintext exactly once (UI-13's "rotate
// their own secret"). The predicate carries consumer_id, so a foreign, deleted or
// unknown id is ErrNotFound and the write cannot touch another consumer's row
// (AC-39). Only the endpoint's own secret moves: the ownership challenge, the URL
// and the verified state are untouched, so rotating needs no re-verification.
//
// FR-1004's dual-signature overlap window (both secrets in webhook-signature for
// up to 24h) belongs with the signer, not here: the delivery worker is a separate
// slice and reads endpoints.signing_secret, so today the new secret simply takes
// effect for the next delivery. Recorded as the FR-1004 card rather than implied.
func (s *Store) RotateConsumerEndpointSecret(ctx context.Context, sc Scope, consumerID, endpointID string) (EndpointSecret, error) {
	secret, hint, err := createEndpointSecret()
	if err != nil {
		return EndpointSecret{}, err
	}
	err = s.scoped(ctx, sc, func(tx pgx.Tx) error {
		res, err := tx.Exec(ctx, `
			update endpoints set signing_secret = $5, signing_secret_hint = $6
			where producer_id = $1 and env = $2 and consumer_id = $3 and id = $4 and status <> 'deleted'`,
			sc.ProducerID, sc.Env, consumerID, endpointID, secret, hint)
		if err != nil {
			return mapPgError(err, ErrConflict, ErrNotFound)
		}
		if res.RowsAffected() == 0 {
			return ErrNotFound
		}
		return nil
	})
	if err != nil {
		return EndpointSecret{}, err
	}
	return EndpointSecret{Secret: secret, Hint: hint}, nil
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

// ListSubscribableEventTypes returns the names of the event types this producer
// has left open to public subscription in the caller's environment (UI-14), so the
// consumer portal can offer a list instead of a free-text field. It is the read
// side of the same gate CreateConsumerSubscription enforces; a disabled type is
// omitted because it can never be published (FR-305).
func (s *Store) ListSubscribableEventTypes(ctx context.Context, sc Scope) ([]string, error) {
	var out []string
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			select name from event_types
			where producer_id = $1 and env = $2 and publicly_subscribable and enabled
			order by name`, sc.ProducerID, sc.Env)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return err
			}
			out = append(out, name)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ListConsumerSubscriptions returns the caller's subscriptions for one of their
// own endpoints, newest pair first. An endpoint id that is not the caller's is
// ErrNotFound - the same 404 its sibling single-endpoint reads give (AC-39: "404
// for every resource outside their consumer_id, including by guessing ids"). It
// used to answer an empty 200 for a foreign id, which disagreed with the criterion
// the slice is judged on and with every other route in this file, even though it
// leaked nothing.
func (s *Store) ListConsumerSubscriptions(ctx context.Context, sc Scope, consumerID, endpointID string) ([]Subscription, error) {
	var out []Subscription
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		owned, err := consumerEndpointOwnedTx(ctx, tx, sc, consumerID, endpointID)
		if err != nil {
			return err
		}
		if !owned {
			return ErrNotFound
		}
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
//
// UI-14's gate is enforced here, not in the portal: the type must be enabled
// (FR-305) and the producer must have left it publicly subscribable. Both
// refusals are ErrInvalid, so the API answers 422 naming the reason, and the same
// answer comes back whether the request was made through the portal or by a script
// holding the consumer's own token (§14).
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
		var subscribable, enabled bool
		err = tx.QueryRow(ctx, `select publicly_subscribable, enabled from event_types
			where producer_id = $1 and env = $2 and name = $3`,
			sc.ProducerID, sc.Env, eventType).Scan(&subscribable, &enabled)
		if errors.Is(err, pgx.ErrNoRows) {
			// The type does not exist in this producer+env: the same answer the FK
			// would give (404), decided before the insert so the reason is explicit.
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if !subscribable {
			return errNotPubliclySubscribable
		}
		if !enabled {
			return errEventTypeDisabled
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
