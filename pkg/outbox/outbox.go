package outbox

import (
	"encoding/json"
	"fmt"
	"go-saga-outbox/pkg/messaging"
	"time"

	"github.com/google/uuid"
)

type Outbox struct {
	ID            uuid.UUID       `db:"id"`
	AggregateType string          `db:"aggregate_type"`
	AggregateID   uuid.UUID       `db:"aggregate_id"`
	Topic         string          `db:"topic"`
	Key           uuid.UUID       `db:"key"`
	Payload       json.RawMessage `db:"payload"`
	Headers       json.RawMessage `db:"headers"`
	CreatedAt     time.Time       `db:"created_at"`
	PublishedAt   *time.Time      `db:"published_at"`
}

func NewOutbox(
	id uuid.UUID,
	aggregateID uuid.UUID,
	sagaID uuid.UUID,
	aggregateType string,
	topic string,
	payload any,
	headers messaging.MessageHeaders,
	now time.Time,
) (Outbox, error) {
	payloadJson, err := json.Marshal(payload)
	if err != nil {
		return Outbox{}, fmt.Errorf("failed to marshal payload: %w", err)
	}
	headersJson, err := json.Marshal(headers)
	if err != nil {
		return Outbox{}, fmt.Errorf("failed to marshal headers payload: %w", err)
	}

	return Outbox{
		ID:            id,
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		Topic:         topic,
		Key:           sagaID,
		Payload:       payloadJson,
		Headers:       headersJson,
		CreatedAt:     now,
	}, nil
}
