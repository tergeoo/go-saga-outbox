package model

import (
	"errors"

	"github.com/google/uuid"
)

var (
	ErrSagaHasNoSeatID        = errors.New("saga has no seatID")
	ErrSagaHasNoReservationID = errors.New("saga has no reservationID")
	ErrSagaHasNoPaymentID     = errors.New("saga has no paymentID")
	ErrSagaHasNoSagatID       = errors.New("saga has no id")
)

type BookTicketContext struct {
	ReservationID  *uuid.UUID `json:"reservation_id,omitempty"`
	SeatID         *uuid.UUID `json:"seat_id,omitempty"`
	PaymentID      *uuid.UUID `json:"payment_id,omitempty"`
	NotificationID *uuid.UUID `json:"notification_id,omitempty"`
}

func (b *BookTicketContext) ComparePaymentID(paymentID uuid.UUID) bool {
	if b.PaymentID == nil {
		return false
	}

	return *b.PaymentID == paymentID
}

func (b *BookTicketContext) ValidateReservationID() error {
	if b.ReservationID == nil {
		return ErrSagaHasNoReservationID
	}

	return nil
}

func (b *BookTicketContext) ValidateSagaID() error {
	if b.SeatID == nil {
		return ErrSagaHasNoSeatID
	}

	return nil
}

func (b *BookTicketContext) ValidatePaymentID() error {
	if b.PaymentID == nil {
		return ErrSagaHasNoPaymentID
	}

	return nil
}

func (b *BookTicketContext) ValidateSeatID() error {
	if b.SeatID == nil {
		return ErrSagaHasNoSeatID
	}

	return nil
}
