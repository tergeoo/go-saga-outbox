package payment

import "github.com/google/uuid"

const (
	TopicEvents       = "saga.payment.events"
	EventTypeCharged  = "payment.charged"
	EventTypeFailed   = "payment.failed"
	EventTypeRefunded = "payment.refunded"
)

type Charged struct {
	PaymentID uuid.UUID `json:"payment_id"`
	SagaID    uuid.UUID `json:"saga_id"`
	Amount    int64     `json:"amount"`
}

func NewCharged(paymentID, sagaID uuid.UUID, amount int64) Charged {
	return Charged{PaymentID: paymentID, SagaID: sagaID, Amount: amount}
}

type Failed struct {
	SagaID uuid.UUID `json:"saga_id"`
	Reason string    `json:"reason"`
}

func NewFailed(sagaID uuid.UUID, reason string) Failed {
	return Failed{SagaID: sagaID, Reason: reason}
}

type Refunded struct {
	SagaID    uuid.UUID `json:"saga_id"`
	PaymentID uuid.UUID `json:"payment_id"`
}

func NewRefunded(sagaID, paymentID uuid.UUID) Refunded {
	return Refunded{
		SagaID:    sagaID,
		PaymentID: paymentID,
	}
}
