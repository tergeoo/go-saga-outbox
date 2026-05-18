package model

import (
	"time"

	"github.com/google/uuid"
)

type Notification struct {
	ID      uuid.UUID          `db:"id"`
	SagaID  uuid.UUID          `db:"saga_id"`
	Channel string             `db:"channel"`
	Status  NotificationStatus `db:"status"`
	SentAt  time.Time          `db:"sent_at"`
}

func NewNotification(
	id uuid.UUID,
	sagaID uuid.UUID,
	channel string,
	status NotificationStatus,
	sentAt time.Time,
) Notification {
	return Notification{
		ID:      id,
		SagaID:  sagaID,
		Channel: channel,
		Status:  status,
		SentAt:  sentAt,
	}
}
