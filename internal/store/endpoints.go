package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/m7jay/pigeon/internal/id"
	"github.com/m7jay/pigeon/internal/ssrf"
)

// Endpoints are a consumer's addresses (FR-401). A producer registers them under
// one of its consumers; it never names a URL at a consumer itself (FR-7.5a). Each
// carries its own signing secret (FR-404), an ownership-challenge token it must
// answer before it receives live events (FR-403), and a status that models pause
// (FR-406) and soft delete (FR-407).
type Endpoint struct {
	ID         string
	ConsumerID string
	Name       string
	URL        string
	Internal   bool
	Status     string // active | paused | deleted
	Verified   bool
	VerifiedAt *time.Time // null until the ownership challenge is answered
	PausedAt   time.Time
	DeletedAt  time.Time
	CreatedAt  time.Time
}

// NewEndpoint is a POST to register an endpoint. Internal is FR-402's escape hatch
// for an operator-owned endpoint on a private network.
type NewEndpoint struct {
	Name       string
	URL        string
	ConsumerID string
	Internal   bool
}

// EndpointSecret is the plaintext signing secret, returned once at creation and
// never read back (FR-404). The database keeps only the usable secret; the
// delivery worker (S4) reads it to sign, which is why it is not hashed like an
// api_key.
type EndpointSecret struct {
	Secret string // the whsec_... value, shown once
	Hint   string // safe-to-display prefix of the secret
}

// EndpointCreated is the whole creation response: the row, the signing secret,
// and the ownership-challenge token. The secret and the token are both shown
// once (FR-403, FR-404) and never read back, so they travel together out of the
// store and straight into the API's one-time response body.
type EndpointCreated struct {
	Endpoint          Endpoint
	Secret            string
	SecretHint        string
	VerificationToken string // FR-403: the challenge, shown once
	VerificationHint  string // safe-to-display prefix of the token
}

// whsecPrefix is the Standard Webhooks secret namespace (FR-1001).
const whsecPrefix = "whsec_"

// createEndpointSecret mints a fresh signing secret and its hint. 32 bytes of
// entropy, base64 with the STANDARD alphabet, prefixed whsec_ so secret scanners
// can find a leaked one. The alphabet matters: a spec verifier decodes the value
// after "whsec_" with a standard decoder, which does not fail on the URL-safe
// "-"/"_" — it drops those bytes and derives a short key, so a URL-safe secret
// would verify for roughly one endpoint in four. Unpadded, because the padding
// carries no entropy and SecretBytes restores it.
func createEndpointSecret() (secret, hint string, err error) {
	raw := make([]byte, 32)
	if _, err = rand.Read(raw); err != nil {
		return "", "", fmt.Errorf("read entropy: %w", err)
	}
	secret = whsecPrefix + base64.RawStdEncoding.EncodeToString(raw)
	if len(secret) > 16 {
		hint = secret[:16]
	} else {
		hint = secret
	}
	return secret, hint, nil
}

// tokenHash is the ownership-challenge secret stored hashed, exactly as api_keys
// store their hash: the token is shown once, and verification is a constant-time
// compare against the stored digest.
func tokenHash(token string) []byte {
	sum := sha256.Sum256([]byte(token))
	return sum[:]
}

// CreateEndpoint registers an endpoint for a consumer (FR-401). It rejects a
// non-https live URL or an internal-range address before any row is written
// (FR-402, AC-19), generates the endpoint's own signing secret (FR-404) and an
// ownership-challenge token (FR-403), and returns the plaintext secret exactly
// once. An unknown consumer id is ErrNotFound (FR-108), enforced by the FK.
func (s *Store) CreateEndpoint(ctx context.Context, sc Scope, in NewEndpoint) (EndpointCreated, error) {
	name := strings.TrimSpace(in.Name)
	if name == "" {
		return EndpointCreated{}, fmt.Errorf("%w: name is required", ErrInvalid)
	}
	in.Name = name
	in.URL = strings.TrimSpace(in.URL)
	if in.URL == "" {
		return EndpointCreated{}, fmt.Errorf("%w: url is required", ErrInvalid)
	}
	// FR-402 / AC-19: reject the URL before the row exists. Fail closed.
	if err := ssrf.CheckEndpointURL(in.URL, sc.Env, in.Internal); err != nil {
		return EndpointCreated{}, err
	}

	secret, hint, err := createEndpointSecret()
	if err != nil {
		return EndpointCreated{}, err
	}
	token := id.New("vrf") // high-entropy challenge, shown once
	var ep Endpoint
	err = s.scoped(ctx, sc, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			insert into endpoints
				(id, producer_id, env, consumer_id, name, url, internal,
				 verification_token_hash, verification_hint, signing_secret, signing_secret_hint)
			values ($1,$2,$3,$4,$5,$6,$7, $8, $9, $10, $11)
			returning id, consumer_id, name, url, internal, status,
				verified_at is not null, coalesce(verified_at, now()),
				coalesce(paused_at, now()), coalesce(deleted_at, now()), created_at`,
			id.New("end"), sc.ProducerID, sc.Env, in.ConsumerID, in.Name, in.URL, in.Internal,
			tokenHash(token), token[:8], secret, hint,
		).Scan(&ep.ID, &ep.ConsumerID, &ep.Name, &ep.URL, &ep.Internal, &ep.Status,
			&ep.Verified, &ep.VerifiedAt, &ep.PausedAt, &ep.DeletedAt, &ep.CreatedAt)
		return mapPgError(err, ErrConflict, ErrNotFound)
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

const endpointSelectCols = `id, consumer_id, name, url, internal, status,
	verified_at is not null as verified, verified_at,
	coalesce(paused_at, now()) as paused_at, coalesce(deleted_at, now()) as deleted_at, created_at`

// GetEndpoint reads one endpoint in the caller's scope. A foreign or
// cross-environment id is ErrNotFound (FR-108 + FR-103).
func (s *Store) GetEndpoint(ctx context.Context, sc Scope, endpointID string) (Endpoint, error) {
	var ep Endpoint
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `select `+endpointSelectCols+`
			from endpoints where producer_id = $1 and env = $2 and id = $3 and status <> 'deleted'`,
			sc.ProducerID, sc.Env, endpointID).Scan(
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

// ListEndpointsForConsumer returns a consumer's non-deleted endpoints, active
// first, then by name. Fan-out and the consumer portal both read this shape.
func (s *Store) ListEndpointsForConsumer(ctx context.Context, sc Scope, consumerID string) ([]Endpoint, error) {
	var out []Endpoint
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `select `+endpointSelectCols+`
			from endpoints
			where producer_id = $1 and env = $2 and consumer_id = $3 and status <> 'deleted'
			order by (status = 'active') desc, name`,
			sc.ProducerID, sc.Env, consumerID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var ep Endpoint
			if err := rows.Scan(&ep.ID, &ep.ConsumerID, &ep.Name, &ep.URL, &ep.Internal, &ep.Status,
				&ep.Verified, &ep.VerifiedAt, &ep.PausedAt, &ep.DeletedAt, &ep.CreatedAt); err != nil {
				return err
			}
			out = append(out, ep)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// SetEndpointPaused is FR-406: pause or resume an endpoint. A paused endpoint
// collects no new deliveries (enforced at fan-out) and holds in-flight ones
// without consuming retry budget. It refuses to touch a deleted endpoint.
func (s *Store) SetEndpointPaused(ctx context.Context, sc Scope, endpointID string, paused bool) (Endpoint, error) {
	status := "active"
	if paused {
		status = "paused"
	}
	return s.setEndpointStatus(ctx, sc, endpointID, status, paused)
}

// setEndpointStatus is the shared write for pause/resume and delete: flip status
// and stamp the transition time in one UPDATE, then re-read.
func (s *Store) setEndpointStatus(ctx context.Context, sc Scope, endpointID, status string, paused bool) (Endpoint, error) {
	var ep Endpoint
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		var res pgconn.CommandTag
		var err error
		if status == "deleted" {
			res, err = tx.Exec(ctx, `
				update endpoints set status = 'deleted', deleted_at = now()
				where producer_id = $1 and env = $2 and id = $3 and status <> 'deleted'`,
				sc.ProducerID, sc.Env, endpointID)
		} else {
			// Resuming clears nothing that matters; pausing stamps paused_at.
			pausedAt := "coalesce(paused_at, now())"
			if paused {
				pausedAt = "now()"
			}
			res, err = tx.Exec(ctx, fmt.Sprintf(`
				update endpoints set status = $4, paused_at = %s
				where producer_id = $1 and env = $2 and id = $3 and status <> 'deleted'`, pausedAt),
				sc.ProducerID, sc.Env, endpointID, status)
		}
		if err != nil {
			return mapPgError(err, ErrConflict, ErrNotFound)
		}
		if res.RowsAffected() == 0 {
			return ErrNotFound
		}
		return tx.QueryRow(ctx, `select `+endpointSelectCols+`
			from endpoints where producer_id = $1 and env = $2 and id = $3`,
			sc.ProducerID, sc.Env, endpointID).Scan(
			&ep.ID, &ep.ConsumerID, &ep.Name, &ep.URL, &ep.Internal, &ep.Status,
			&ep.Verified, &ep.VerifiedAt, &ep.PausedAt, &ep.DeletedAt, &ep.CreatedAt)
	})
	if err != nil {
		return Endpoint{}, err
	}
	return ep, nil
}

// DeleteEndpoint is FR-407's soft delete: the row is retained (history, the FK
// from deliveries) and never hard-removed, so no delivery is orphaned.
func (s *Store) DeleteEndpoint(ctx context.Context, sc Scope, endpointID string) error {
	_, err := s.setEndpointStatus(ctx, sc, endpointID, "deleted", false)
	return err
}

// VerifyEndpoint answers the ownership challenge (FR-403). A correct token for a
// non-deleted endpoint stamps verified_at; from then it may receive live events.
// A wrong token is ErrInvalid (422); an unknown endpoint is ErrNotFound. It never
// reveals whether a token was right beyond the status code.
func (s *Store) VerifyEndpoint(ctx context.Context, sc Scope, endpointID, token string) error {
	if strings.TrimSpace(token) == "" {
		return fmt.Errorf("%w: verification token is required", ErrInvalid)
	}
	return s.scoped(ctx, sc, func(tx pgx.Tx) error {
		var hash []byte
		err := tx.QueryRow(ctx, `select verification_token_hash
			from endpoints where producer_id = $1 and env = $2 and id = $3 and status <> 'deleted'`,
			sc.ProducerID, sc.Env, endpointID).Scan(&hash)
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
			where producer_id = $1 and env = $2 and id = $3 and status <> 'deleted'
			  and verified_at is null`,
			sc.ProducerID, sc.Env, endpointID)
		if err != nil {
			return err
		}
		if res.RowsAffected() == 0 {
			// Already verified is not an error: verification is idempotent.
			return nil
		}
		return nil
	})
}
