package payment

import "github.com/google/uuid"

const (
	TopicCommands   = "saga.payment.commands"
	EventTypeCharge = "charge.command"
	EventTypeRefund = "refund.command"
)

type Charge struct {
	SagaID uuid.UUID `json:"saga_id"`
	UserID uuid.UUID `json:"user_id"`
	Amount int64     `json:"amount"`
}

func NewCharge(sagaID, userID uuid.UUID, amount int64) Charge {
	return Charge{
		SagaID: sagaID,
		UserID: userID,
		Amount: amount,
	}
}

type Refund struct {
	SagaID    uuid.UUID `json:"saga_id"`
	PaymentID uuid.UUID `json:"payment_id"`
}

func NewRefund(sagaID, paymentID uuid.UUID) Refund {
	return Refund{
		SagaID:    sagaID,
		PaymentID: paymentID,
	}
}
