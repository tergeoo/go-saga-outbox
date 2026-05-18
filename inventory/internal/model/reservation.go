package model

import (
	"time"

	uuidv7 "go-saga-outbox/pkg/uuid"

	"github.com/google/uuid"
)

type Reservation struct {
	ID        uuid.UUID         `db:"id"`
	SagaID    uuid.UUID         `db:"saga_id"`
	SeatID    uuid.UUID         `db:"seat_id"`
	Status    ReservationStatus `db:"status"`
	CreatedAt time.Time         `db:"created_at"`
}

func NewReservation(sagaID uuid.UUID, seatID uuid.UUID, createdAt time.Time) Reservation {
	return Reservation{
		ID:        uuidv7.V7(),
		SagaID:    sagaID,
		SeatID:    seatID,
		Status:    ReservationStatusReserved,
		CreatedAt: createdAt,
	}
}
