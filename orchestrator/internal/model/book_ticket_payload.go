package model

import "github.com/google/uuid"

type BookTicketPayload struct {
	EventID uuid.UUID `json:"event_id"`
	UserID  uuid.UUID `json:"user_id"`
	Amount  int64     `json:"amount"`
	Channel string    `json:"channel"`
}

func NewBookTicketPayload(
	eventID uuid.UUID,
	userID uuid.UUID,
	amount int64,
	channel string,
) BookTicketPayload {
	return BookTicketPayload{
		EventID: eventID,
		UserID:  userID,
		Amount:  amount,
		Channel: channel,
	}
}
