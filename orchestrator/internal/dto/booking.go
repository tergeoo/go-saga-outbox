package dto

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type CreateBookingRequest struct {
	EventID uuid.UUID `json:"event_id"`
	UserID  uuid.UUID `json:"user_id"`
	Amount  int64     `json:"amount"`
	Channel string    `json:"channel"`
}

type CreateBookingResponse struct {
	SagaID uuid.UUID `json:"saga_id"`
}

type BookingResponse struct {
	SagaID      uuid.UUID       `json:"saga_id"`
	Type        string          `json:"type"`
	State       string          `json:"state"`
	CurrentStep string          `json:"current_step"`
	Payload     json.RawMessage `json:"payload"`
	Context     json.RawMessage `json:"context"`
	Attempts    int             `json:"attempts"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}
