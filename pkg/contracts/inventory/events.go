package inventory

import "github.com/google/uuid"

const (
	TopicEvents                    = "saga.inventory.events"
	EventTypeSeatReserved          = "seat.reserved"
	EventTypeSeatReservationFailed = "seat.reservation_failed"
	EventTypeSeatReleased          = "seat.released"
)

type SeatReserved struct {
	ReservationID uuid.UUID `json:"reservation_id"`
	SeatID        uuid.UUID `json:"seat_id"`
	EventID       uuid.UUID `json:"event_id"`
	UserID        uuid.UUID `json:"user_id"`
}

func NewSeatReserved(reservationID, seatID, eventID, userID uuid.UUID) SeatReserved {
	return SeatReserved{
		ReservationID: reservationID,
		SeatID:        seatID,
		EventID:       eventID,
		UserID:        userID,
	}
}

type SeatReservationFailed struct {
	SagaID  uuid.UUID `json:"saga_id"`
	EventID uuid.UUID `json:"event_id"`
	UserID  uuid.UUID `json:"user_id"`
	Reason  string    `json:"reason"`
}

func NewSeatReservationFailed(sagaID, eventID, userID uuid.UUID, reason string) SeatReservationFailed {
	return SeatReservationFailed{
		SagaID:  sagaID,
		EventID: eventID,
		UserID:  userID,
		Reason:  reason,
	}
}

type SeatReleased struct {
	SagaID        uuid.UUID `json:"saga_id"`
	SeatID        uuid.UUID `json:"seat_id"`
	ReservationID uuid.UUID `json:"reservation_id"`
}

func NewSeatReleased(
	sagaID uuid.UUID,
	seatID uuid.UUID,
	reservationID uuid.UUID,
) SeatReleased {
	return SeatReleased{
		SagaID:        sagaID,
		SeatID:        seatID,
		ReservationID: reservationID,
	}
}
