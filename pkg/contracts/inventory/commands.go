package inventory

import "github.com/google/uuid"

const (
	TopicCommands        = "saga.inventory.commands"
	EventTypeReserveSeat = "reserve.command"
	EventTypeReleaseSeat = "release.command"
)

type ReserveSeat struct {
	SagaID  uuid.UUID `json:"saga_id"`
	EventID uuid.UUID `json:"event_id"`
	UserID  uuid.UUID `json:"user_id"`
}

func NewReserveSeat(sagaID, eventID, userID uuid.UUID) ReserveSeat {
	return ReserveSeat{
		SagaID:  sagaID,
		EventID: eventID,
		UserID:  userID,
	}
}

type ReleaseSeat struct {
	SagaID        uuid.UUID `json:"saga_id"`
	ReservationID uuid.UUID `json:"reservation_id"`
	SeatID        uuid.UUID `json:"seat_id"`
}

func NewReleaseSeat(sagaID, reservationID, seatID uuid.UUID) ReleaseSeat {
	return ReleaseSeat{
		SagaID:        sagaID,
		ReservationID: reservationID,
		SeatID:        seatID,
	}
}
