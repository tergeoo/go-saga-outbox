package outbox

import (
	"context"
	"fmt"
	"go-saga-outbox/pkg/trx"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type Repo struct {
	trx *trx.Transaction
}

func NewOutboxRepo(trx *trx.Transaction) *Repo {
	return &Repo{
		trx: trx,
	}
}

func (r *Repo) Append(ctx context.Context, msg Outbox) error {
	tx := r.trx.FromContext(ctx)

	query := `
	INSERT INTO outbox (id, aggregate_type, aggregate_id, topic, key, payload, headers, created_at)
	VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8)`

	_, err := tx.ExecContext(ctx, query, msg.ID, msg.AggregateType, msg.AggregateID, msg.Topic, msg.Key, msg.Payload, msg.Headers, msg.CreatedAt)
	if err != nil {
		return fmt.Errorf("failed to add outbox: %w", err)
	}

	return nil
}

// FetchUnpublished вызывать только внутри транзакции
func (r *Repo) FetchUnpublished(ctx context.Context, limit int) ([]Outbox, error) {
	tx := r.trx.FromContext(ctx)

	query := `
	SELECT id, aggregate_type, aggregate_id, topic, key, payload, headers, created_at, published_at
	FROM outbox
	WHERE published_at IS NULL
	ORDER BY created_at
	LIMIT $1
	FOR UPDATE SKIP LOCKED`

	var msgs []Outbox

	err := tx.SelectContext(ctx, &msgs, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch unpublished outbox: %w", err)
	}

	return msgs, nil
}

func (r *Repo) MarkPublished(ctx context.Context, ids []uuid.UUID, at time.Time) error {
	if len(ids) == 0 {
		return nil
	}

	tx := r.trx.FromContext(ctx)

	query := `UPDATE outbox SET published_at = ? WHERE id IN (?)`

	query, args, err := sqlx.In(query, at, ids)
	if err != nil {
		return fmt.Errorf("failed to mark published outbox: %w", err)
	}

	query = sqlx.Rebind(sqlx.DOLLAR, query)

	_, err = tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to mark published outbox: %w", err)
	}

	return nil
}
