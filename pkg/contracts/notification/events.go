package notification

import "github.com/google/uuid"

const (
	TopicEvents       = "saga.notification.events"
	EventTypeSent     = "notification.sent"
	EventTypeFailed   = "notification.failed"
	EventTypeRefunded = "payment.refunded"
)

type Sent struct {
	NotificationID uuid.UUID `json:"notification_id"`
	SagaID         uuid.UUID `json:"saga_id"`
}

func NewSent(notificationID uuid.UUID, sagaID uuid.UUID) *Sent {
	return &Sent{
		NotificationID: notificationID,
		SagaID:         sagaID,
	}
}

type Failed struct {
	SagaID         uuid.UUID `json:"saga_id"`
	NotificationID uuid.UUID `json:"notification_id"`
	Reason         string    `json:"reason"`
}

func NewFailed(sagaID, notificationID uuid.UUID, reason string) Failed {
	return Failed{
		SagaID:         sagaID,
		NotificationID: notificationID,
		Reason:         reason,
	}
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
