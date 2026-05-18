package inbox

import (
	"time"

	"github.com/google/uuid"
)

type Inbox struct {
	MessageID   uuid.UUID
	Consumer    string
	ProcessedAt time.Time
}

func NewInbox(messageID uuid.UUID, consumer string, now time.Time) Inbox {
	return Inbox{
		MessageID:   messageID,
		Consumer:    consumer,
		ProcessedAt: now,
	}
}
