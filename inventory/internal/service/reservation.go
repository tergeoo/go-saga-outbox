package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go-saga-outbox/inventory/internal/model"
	"go-saga-outbox/inventory/internal/repo"
	"go-saga-outbox/pkg/contracts/inventory"
	"go-saga-outbox/pkg/inbox"
	"go-saga-outbox/pkg/kafka"
	"go-saga-outbox/pkg/messaging"
	"go-saga-outbox/pkg/outbox"
	"go-saga-outbox/pkg/trx"
	uuidv7 "go-saga-outbox/pkg/uuid"
	"time"

	"github.com/google/uuid"
)

const (
	consumerName             = "inventory"
	aggregateTypeReservation = "reservation"
)

var (
	ErrSeatsMismatch = errors.New("seat id does not match")
)

type ReservationService struct {
	trx             *trx.Transaction
	inboxRepo       *inbox.Repo
	seatRepo        *repo.SeatRepo
	reservationRepo *repo.ReservationRepo
	outboxRepo      *outbox.Repo
	now             func() time.Time
}

func NewReservationService(
	trx *trx.Transaction,
	inboxRepo *inbox.Repo,
	seatRepo *repo.SeatRepo,
	reservationRepo *repo.ReservationRepo,
	outboxRepo *outbox.Repo,
	now func() time.Time,
) *ReservationService {
	return &ReservationService{
		trx:             trx,
		inboxRepo:       inboxRepo,
		seatRepo:        seatRepo,
		reservationRepo: reservationRepo,
		outboxRepo:      outboxRepo,
		now:             now,
	}
}

func (s *ReservationService) HandleReserveSeatCommand(ctx context.Context, msg kafka.Message) error {
	return s.trx.Run(ctx, func(ctx context.Context) error {
		inserted, err := s.inboxRepo.Dedup(ctx, msg.MessageID, consumerName, s.now())
		if err != nil {
			return err
		}
		if !inserted {
			return nil
		}

		var cmd inventory.ReserveSeat
		if err = json.Unmarshal(msg.Value, &cmd); err != nil {
			return fmt.Errorf("unmarshal reserve.command: %w", err)
		}

		seat, err := s.seatRepo.FindFreeSeat(ctx, cmd.EventID)
		if errors.Is(err, repo.ErrNoFreeSeats) {
			payload := inventory.NewSeatReservationFailed(cmd.SagaID, cmd.EventID, cmd.UserID, "no seats available")
			return s.emit(ctx, uuid.Nil, cmd.SagaID, msg.MessageID, payload, inventory.EventTypeSeatReservationFailed)
		}
		if err != nil {
			return fmt.Errorf("find free seat: %w", err)
		}

		if err = s.seatRepo.MarkSeatReserved(ctx, seat.ID); err != nil {
			return fmt.Errorf("mark seat reserved: %w", err)
		}

		now := s.now()
		reservation := model.NewReservation(cmd.SagaID, seat.ID, now)
		if err = s.reservationRepo.Create(ctx, reservation); err != nil {
			return fmt.Errorf("create reservation: %w", err)
		}

		payload := inventory.NewSeatReserved(reservation.ID, seat.ID, cmd.EventID, cmd.UserID)
		return s.emit(ctx, reservation.ID, cmd.SagaID, msg.MessageID, payload, inventory.EventTypeSeatReserved)
	})
}

func (s *ReservationService) HandleReleaseSeatCommand(ctx context.Context, msg kafka.Message) error {
	return s.trx.Run(ctx, func(ctx context.Context) error {
		inserted, err := s.inboxRepo.Dedup(ctx, msg.MessageID, consumerName, s.now())
		if err != nil {
			return err
		}
		if !inserted {
			return nil
		}

		var cmd inventory.ReleaseSeat
		if err = json.Unmarshal(msg.Value, &cmd); err != nil {
			return fmt.Errorf("unmarshal release.command: %w", err)
		}

		reservation, err := s.reservationRepo.FindBySagaID(ctx, cmd.SagaID)
		if err != nil {
			if errors.Is(err, repo.ErrReservationNotFound) {
				return nil
			}
			return fmt.Errorf("find reservation: %w", err)
		}

		if reservation.Status == model.ReservationStatusReleased {
			return nil
		}

		if reservation.SeatID != cmd.SeatID {
			return ErrSeatsMismatch
		}

		err = s.seatRepo.MarkSeatReleased(ctx, cmd.SeatID)
		if err != nil {
			return fmt.Errorf("cannot release seat: %w", err)
		}

		reservation.Status = model.ReservationStatusReleased

		if err = s.reservationRepo.Update(ctx, reservation); err != nil {
			return fmt.Errorf("update reservation: %w", err)
		}

		payload := inventory.NewSeatReleased(cmd.SagaID, cmd.SeatID, cmd.ReservationID)
		return s.emit(ctx, reservation.ID, cmd.SagaID, msg.MessageID, payload, inventory.EventTypeSeatReleased)
	})
}

func (s *ReservationService) emit(
	ctx context.Context,
	aggregateID uuid.UUID,
	sagaID uuid.UUID,
	causationID uuid.UUID,
	payload any,
	eventType string,
) error {
	msgID := uuidv7.V7()
	headers := messaging.NewMessageHeaders(msgID, sagaID, &causationID, eventType)
	outboxMsg, err := outbox.NewOutbox(
		msgID,
		aggregateID,
		sagaID,
		aggregateTypeReservation,
		inventory.TopicEvents,
		payload,
		headers,
		s.now(),
	)
	if err != nil {
		return fmt.Errorf("create outbox message: %w", err)
	}

	return s.outboxRepo.Append(ctx, outboxMsg)
}
