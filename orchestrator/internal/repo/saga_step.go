package repo

import (
	"context"
	"fmt"
	"go-saga-outbox/orchestrator/internal/model"
	"go-saga-outbox/pkg/trx"
)

type SagaStepRepo struct {
	tx *trx.Transaction
}

func NewSagaStepRepo(tx *trx.Transaction) *SagaStepRepo {
	return &SagaStepRepo{tx: tx}
}

func (r *SagaStepRepo) Append(ctx context.Context, step model.SagaStep) error {
	tx := r.tx.FromContext(ctx)

	query := `
	INSERT INTO saga_step (id, saga_id, step_name, direction, status, command_message_id, reply_message_id, error, created_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

	_, err := tx.ExecContext(ctx, query, step.ID, step.SagaID, step.StepName, step.Direction, step.Status, step.CommandMessageID, step.ReplyMessageID, step.Error, step.CreatedAt)
	if err != nil {
		return fmt.Errorf("error adding saga step: %w", err)
	}

	return nil
}
