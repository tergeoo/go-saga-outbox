package repo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"go-saga-outbox/orchestrator/internal/model"
	"go-saga-outbox/pkg/trx"
	"time"

	"github.com/google/uuid"
)

var ErrSagaNotFound = errors.New("saga not found")

type SagaRepo struct {
	trx *trx.Transaction
}

func NewSagaRepo(trx *trx.Transaction) *SagaRepo {
	return &SagaRepo{
		trx: trx,
	}
}

func (r *SagaRepo) Upsert(ctx context.Context, saga model.Saga) error {
	tx := r.trx.FromContext(ctx)

	query := `
	INSERT INTO saga (id, type, state, current_step, payload, context, attempts, next_attempt_at, created_at, updated_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	ON CONFLICT (id) DO UPDATE 
		SET state = EXCLUDED.state,
		current_step = EXCLUDED.current_step,
		context = EXCLUDED.context,
		attempts = EXCLUDED.attempts,
		next_attempt_at = EXCLUDED.next_attempt_at,
		updated_at = EXCLUDED.updated_at`

	_, err := tx.ExecContext(ctx, query, saga.ID, saga.Type, saga.State, saga.CurrentStep, saga.Payload, saga.Context, saga.Attempts, saga.NextAttemptAt, saga.CreatedAt, saga.UpdatedAt)
	if err != nil {
		return fmt.Errorf("failed to insert saga: %w", err)
	}

	return nil
}

func (r *SagaRepo) FindByID(ctx context.Context, id uuid.UUID) (model.Saga, error) {
	tx := r.trx.FromContext(ctx)

	query := `
	SELECT id, type, state, current_step, payload, context, attempts, next_attempt_at, created_at, updated_at
	FROM saga 
	WHERE saga.id = $1`

	var saga model.Saga
	err := tx.GetContext(ctx, &saga, query, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Saga{}, ErrSagaNotFound
		}
		return model.Saga{}, fmt.Errorf("failed to query saga: %w", err)
	}

	return saga, nil
}

func (r *SagaRepo) FindExpired(ctx context.Context, now time.Time, limit int) ([]model.Saga, error) {
	tx := r.trx.FromContext(ctx)
	const q = `
          SELECT id, type, state, current_step, payload, context, attempts, next_attempt_at, created_at, updated_at
          FROM saga
          WHERE state IN ('running', 'compensating')
            AND next_attempt_at IS NOT NULL
            AND next_attempt_at <= $1
          ORDER BY next_attempt_at
          FOR UPDATE SKIP LOCKED
          LIMIT $2
      `
	var sagas []model.Saga
	if err := tx.SelectContext(ctx, &sagas, q, now, limit); err != nil {
		return nil, fmt.Errorf("find expired sagas: %w", err)
	}
	return sagas, nil
}
