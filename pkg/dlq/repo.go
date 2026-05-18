package dlq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"go-saga-outbox/pkg/trx"
	"time"

	"github.com/google/uuid"
)

type Repo struct {
	trx *trx.Transaction
}

func NewRepo(trx *trx.Transaction) *Repo {
	return &Repo{trx}
}

func (r *Repo) Insert(ctx context.Context, msg DeadMessage) error {
	tx := r.trx.FromContext(ctx)

	query := `
		INSERT INTO dead_message (id, saga_id, message_id, topic, payload, headers, reason, consumer, created_at)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9)
		ON CONFLICT (consumer, message_id) DO NOTHING`

	if _, err := tx.ExecContext(ctx, query, msg.ID, msg.SagaID, msg.MessageID, msg.Topic, msg.Payload, msg.Headers, msg.Reason, msg.Consumer, msg.CreatedAt); err != nil {
		return fmt.Errorf("insert dead_message: %w", err)
	}

	return nil
}

func (r *Repo) FindByID(ctx context.Context, id uuid.UUID) (DeadMessage, error) {
	tx := r.trx.FromContext(ctx)

	query := `
		SELECT id, saga_id, message_id, topic, payload, headers, reason, replayed_at, created_at, consumer
		FROM dead_message
		WHERE id = $1`

	var msg DeadMessage
	err := tx.GetContext(ctx, &msg, query, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return DeadMessage{}, ErrDeadMessageNotFound
		}
		return DeadMessage{}, err
	}

	return msg, nil
}

func (r *Repo) MarkReplayed(ctx context.Context, id uuid.UUID, replayedAt time.Time) error {
	tx := r.trx.FromContext(ctx)
	query := `UPDATE dead_message SET replayed_at = $1 WHERE id = $2`

	_, err := tx.ExecContext(ctx, query, replayedAt, id)
	if err != nil {
		return fmt.Errorf("mark replayed: %w", err)
	}

	return nil
}

func (r *Repo) ListUnreplayed(ctx context.Context, limit int) ([]DeadMessage, error) {
	tx := r.trx.FromContext(ctx)

	query := `
		SELECT id, saga_id, message_id, topic, payload, headers, reason, replayed_at, created_at, consumer
		FROM dead_message
		WHERE replayed_at IS NULL
		ORDER BY created_at
		LIMIT $1`

	var msgs []DeadMessage
	err := tx.SelectContext(ctx, &msgs, query, limit)
	if err != nil {
		return nil, err
	}

	return msgs, nil
}
