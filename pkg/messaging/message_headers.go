package messaging

import "github.com/google/uuid"

type MessageHeaders struct {
	MessageID     uuid.UUID  `json:"message_id"`
	SagaID        uuid.UUID  `json:"saga_id"`
	CausationID   *uuid.UUID `json:"causation_id,omitempty"`
	EventType     string     `json:"event_type"`
	SchemaVersion int        `json:"schema_version"`
}

func NewMessageHeaders(messageID, sagaID uuid.UUID, causation *uuid.UUID, eventType string) MessageHeaders {
	return MessageHeaders{
		MessageID:     messageID,
		SagaID:        sagaID,
		CausationID:   causation,
		EventType:     eventType,
		SchemaVersion: 1,
	}
}
