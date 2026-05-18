package model

import "github.com/google/uuid"

type Seat struct {
	ID      uuid.UUID  `db:"id"`
	EventID uuid.UUID  `db:"event_id"`
	Status  SeatStatus `db:"status"`
}
