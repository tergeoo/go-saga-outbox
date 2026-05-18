package model

import (
	"errors"
	uuidv7 "go-saga-outbox/pkg/uuid"

	"github.com/google/uuid"
)

var ErrNegativeAmount = errors.New("payment: negative amount")

type Payment struct {
	ID          uuid.UUID     `db:"id"`
	SagaID      uuid.UUID     `db:"saga_id"`
	UserID      uuid.UUID     `db:"user_id"`
	AmountCents int64         `db:"amount_cents"`
	Status      PaymentStatus `db:"status"`
	ExternalID  *string       `db:"external_id"`
}

func NewPayment(sagaID, userID uuid.UUID, amountCents int64) (Payment, error) {
	p := Payment{
		ID:          uuidv7.V7(),
		SagaID:      sagaID,
		UserID:      userID,
		AmountCents: amountCents,
		ExternalID:  nil,
	}

	if amountCents < 0 {
		p.Status = PaymentStatusFailed
		return p, ErrNegativeAmount
	}

	p.Status = PaymentStatusCharged
	return p, nil
}

func (p *Payment) IsCharged() bool {
	return p.Status == PaymentStatusCharged
}
