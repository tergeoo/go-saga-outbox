package notification

import "github.com/google/uuid"

const (
	TopicCommands   = "saga.notification.commands"
	EventTypeSend   = "send.command"
	EventTypeRefund = "refund.command"
)

type Send struct {
	SagaID  uuid.UUID `json:"saga_id"`
	UserID  uuid.UUID `json:"user_id"`
	Channel string    `json:"channel"`
}

func NewSend(
	sagaID uuid.UUID,
	userID uuid.UUID,
	channel string,
) Send {
	return Send{
		SagaID:  sagaID,
		UserID:  userID,
		Channel: channel,
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
