package dlq

import (
	"errors"
	uuidV7 "go-saga-outbox/pkg/uuid"
	"time"

	"github.com/google/uuid"
)

var (
	ErrDeadMessageAlreadyReplayed = errors.New("dead message already replayed")
	ErrDeadMessageCannotReplay    = errors.New("dead message can not replay")
)

type DeadMessage struct {
	ID         uuid.UUID  `db:"id" json:"id"`
	SagaID     *uuid.UUID `db:"saga_id" json:"saga_id"`
	MessageID  uuid.UUID  `db:"message_id" json:"message_id"`
	Topic      string     `db:"topic" json:"topic"`
	Payload    []byte     `db:"payload" json:"payload"`
	Headers    []byte     `db:"headers" json:"headers"`
	Reason     string     `db:"reason" json:"reason"`
	Consumer   string     `db:"consumer" json:"consumer"`
	CreatedAt  time.Time  `db:"created_at" json:"created_at"`
	ReplayedAt *time.Time `db:"replayed_at" json:"replayed_at"`
}

func NewDeadMessage(messageID uuid.UUID, sagaID *uuid.UUID,
	topic, reason, consumer string,
	payload, headers []byte, now time.Time) DeadMessage {
	return DeadMessage{
		ID:        uuidV7.V7(),
		SagaID:    sagaID,
		MessageID: messageID,
		Topic:     topic,
		Payload:   payload,
		Headers:   headers,
		Reason:    reason,
		Consumer:  consumer,
		CreatedAt: now,
	}
}

func (dm DeadMessage) CanReplay() error {
	if dm.ReplayedAt != nil {
		return ErrDeadMessageAlreadyReplayed
	}

	if dm.SagaID == nil {
		return ErrDeadMessageCannotReplay
	}

	return nil
}
