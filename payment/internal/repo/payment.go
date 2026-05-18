package repo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"go-saga-outbox/payment/internal/model"
	"go-saga-outbox/pkg/trx"

	"github.com/google/uuid"
)

var ErrPaymentNotFound = errors.New("payment not found")

type PaymentRepo struct{ trx *trx.Transaction }

func NewPaymentRepo(trx *trx.Transaction) *PaymentRepo {
	return &PaymentRepo{trx: trx}
}

func (r *PaymentRepo) Create(ctx context.Context, p model.Payment) error {
	tx := r.trx.FromContext(ctx)

	query := `
	INSERT INTO payment (id, saga_id, amount_cents, status, external_id, user_id)
	VALUES ($1, $2, $3, $4, $5, $6)`

	_, err := tx.ExecContext(ctx, query, p.ID, p.SagaID, p.AmountCents, p.Status, p.ExternalID, p.UserID)
	if err != nil {
		return fmt.Errorf("failed to insert payment: %w", err)
	}

	return nil
}

func (r *PaymentRepo) FindBySagaID(ctx context.Context, sagaID uuid.UUID) (model.Payment, error) {
	tx := r.trx.FromContext(ctx)

	query := `SELECT id, saga_id, amount_cents, status, external_id, user_id FROM payment WHERE saga_id = $1`

	var p model.Payment
	err := tx.GetContext(ctx, &p, query, sagaID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return p, ErrPaymentNotFound
		}
		return model.Payment{}, fmt.Errorf("failed to find payment: %w", err)
	}

	return p, nil
}

func (r *PaymentRepo) Update(ctx context.Context, p model.Payment) error {
	tx := r.trx.FromContext(ctx)

	query := `UPDATE payment SET status = $1, external_id = $2, user_id = $3 WHERE id = $4`

	_, err := tx.ExecContext(ctx, query, p.Status, p.ExternalID, p.UserID, p.ID)
	if err != nil {
		return fmt.Errorf("failed to update payment status: %w", err)
	}

	return nil
}
