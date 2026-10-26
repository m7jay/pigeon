package store

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/m7jay/pigeon/internal/id"
)

// Consumer is the producer's own customer (§7.5a). `Ref` is the producer's
// identifier for it, unique per (producer, environment); `ID` is ours.
type Consumer struct {
	ID        string
	Ref       string
	Name      string
	Env       string
	CreatedAt time.Time
}

// refPattern is deliberately permissive about the content (§7.5a says the
// producer's own identifier - a school id, an account number) and strict about
// the shape: no leading or trailing whitespace, bounded length, no control
// characters. Case is preserved and significant.
var refPattern = regexp.MustCompile(`^[^\s\x00-\x1f][^\x00-\x1f]{0,127}$`)

// CreateConsumer registers a consumer in the caller's environment. The
// environment comes from the Scope, never from the request body: a live key
// cannot create a test consumer and a member session cannot quietly address the
// other plane (FR-103).
func (s *Store) CreateConsumer(ctx context.Context, sc Scope, ref, name string) (Consumer, error) {
	if !refPattern.MatchString(ref) {
		return Consumer{}, fmt.Errorf("%w: ref must be 1-128 characters with no surrounding whitespace", ErrInvalid)
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return Consumer{}, fmt.Errorf("%w: name is required", ErrInvalid)
	}

	var c Consumer
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		// The producer_id and env predicates are in the SQL as well as in RLS:
		// the policy is the backstop, not the only guard (MT-1).
		err := tx.QueryRow(ctx, `
			insert into consumers (id, producer_id, env, ref, name)
			values ($1, $2, $3, $4, $5)
			returning id, ref, name, env, created_at`,
			id.New("con"), sc.ProducerID, sc.Env, ref, name,
		).Scan(&c.ID, &c.Ref, &c.Name, &c.Env, &c.CreatedAt)
		if err != nil {
			return mapPgError(err, ErrConflict, ErrNotFound)
		}
		return nil
	})
	if err != nil {
		return Consumer{}, err
	}
	return c, nil
}

// ListConsumers returns the caller's environment only.
func (s *Store) ListConsumers(ctx context.Context, sc Scope) ([]Consumer, error) {
	var out []Consumer
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			select id, ref, name, env, created_at
			  from consumers
			 where producer_id = $1 and env = $2
			 order by ref`, sc.ProducerID, sc.Env)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var c Consumer
			if err := rows.Scan(&c.ID, &c.Ref, &c.Name, &c.Env, &c.CreatedAt); err != nil {
				return err
			}
			out = append(out, c)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetConsumer reads one consumer in the caller's scope. A consumer id from
// another producer *or the other environment* is ErrNotFound, which is FR-108's
// 404 rule applied to the environment boundary as well.
func (s *Store) GetConsumer(ctx context.Context, sc Scope, consumerID string) (Consumer, error) {
	var c Consumer
	err := s.scoped(ctx, sc, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			select id, ref, name, env, created_at
			  from consumers
			 where producer_id = $1 and env = $2 and id = $3`,
			sc.ProducerID, sc.Env, consumerID,
		).Scan(&c.ID, &c.Ref, &c.Name, &c.Env, &c.CreatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	})
	if err != nil {
		return Consumer{}, err
	}
	return c, nil
}
