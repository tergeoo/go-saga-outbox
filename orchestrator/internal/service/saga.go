package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go-saga-outbox/orchestrator/internal/model"
	"go-saga-outbox/orchestrator/internal/repo"
	"go-saga-outbox/pkg/contracts/inventory"
	"go-saga-outbox/pkg/contracts/notification"
	"go-saga-outbox/pkg/contracts/payment"
	"go-saga-outbox/pkg/dlq"
	"go-saga-outbox/pkg/inbox"
	"go-saga-outbox/pkg/kafka"
	"go-saga-outbox/pkg/messaging"
	"go-saga-outbox/pkg/metrics"
	"go-saga-outbox/pkg/outbox"
	"go-saga-outbox/pkg/trx"
	uuidv7 "go-saga-outbox/pkg/uuid"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

const (
	consumerName  = "orchestrator"
	sagaType      = "book_ticket"
	aggregateType = "saga"
)

var (
	ErrPayloadContextMismatch = errors.New("payload does not match saga context")
	ErrDecodeSagaCtx          = errors.New("could not decode saga context")
)

type SagaService struct {
	trx          *trx.Transaction
	inboxRepo    *inbox.Repo
	sagaRepo     *repo.SagaRepo
	sagaStepRepo *repo.SagaStepRepo
	outboxRepo   *outbox.Repo
	dlqRepo      *dlq.Repo
	now          func() time.Time
}

func NewSagaService(
	trx *trx.Transaction,
	inboxRepo *inbox.Repo,
	sagaRepo *repo.SagaRepo,
	sagaStepRepo *repo.SagaStepRepo,
	outboxRepo *outbox.Repo,
	dlqRepo *dlq.Repo,
	now func() time.Time,
) *SagaService {
	return &SagaService{
		trx:          trx,
		inboxRepo:    inboxRepo,
		sagaRepo:     sagaRepo,
		sagaStepRepo: sagaStepRepo,
		outboxRepo:   outboxRepo,
		dlqRepo:      dlqRepo,
		now:          now,
	}
}

func (s *SagaService) StartBooking(ctx context.Context, eventID, userID uuid.UUID, channel string, amount int64) (uuid.UUID, error) {
	sagaID := uuidv7.V7()

	err := s.trx.Run(ctx, func(ctx context.Context) error {
		payload := model.NewBookTicketPayload(eventID, userID, amount, channel)
		saga, err := model.NewSaga(sagaID, sagaType, payload, s.now())
		if err != nil {
			return fmt.Errorf("create saga: %w", err)
		}

		saga.CurrentStep = model.SagaStepNameReserveSeat
		saga.ScheduleNext(s.now())

		if err = s.sagaRepo.Upsert(ctx, saga); err != nil {
			return fmt.Errorf("upsert saga: %w", err)
		}

		cmd := inventory.NewReserveSeat(sagaID, eventID, userID)
		msgID := uuidv7.V7()
		headers := messaging.NewMessageHeaders(msgID, sagaID, nil, inventory.EventTypeReserveSeat)
		outboxMsg, err := outbox.NewOutbox(msgID, sagaID, sagaID, aggregateType, inventory.TopicCommands, cmd, headers, s.now())
		if err != nil {
			return fmt.Errorf("create outbox message: %w", err)
		}

		return s.outboxRepo.Append(ctx, outboxMsg)
	})
	if err != nil {
		return uuid.Nil, err
	}

	return sagaID, nil
}

func (s *SagaService) GetBooking(ctx context.Context, sagaID uuid.UUID) (model.Saga, error) {
	return s.sagaRepo.FindByID(ctx, sagaID)
}

func (s *SagaService) HandleSeatReserved(ctx context.Context, msg kafka.Message) error {
	return s.trx.Run(ctx, func(ctx context.Context) error {
		inserted, err := s.inboxRepo.Dedup(ctx, msg.MessageID, consumerName, s.now())
		if err != nil {
			return err
		}
		if !inserted {
			return nil
		}

		var p inventory.SeatReserved
		if err = json.Unmarshal(msg.Value, &p); err != nil {
			return fmt.Errorf("unmarshal seat.reserved: %w", err)
		}

		sagaID, err := msg.GetSagaIDFromHeaders()
		if err != nil {
			return fmt.Errorf("parse saga id: %w", err)
		}

		saga, err := s.sagaRepo.FindByID(ctx, sagaID)
		if err != nil {
			return fmt.Errorf("find saga: %w", err)
		}

		sagaCtx, err := saga.DecodeContext()
		if err != nil {
			return fmt.Errorf("decode context: %w", err)
		}
		sagaCtx.ReservationID = &p.ReservationID
		sagaCtx.SeatID = &p.SeatID
		if err = saga.SetContext(sagaCtx); err != nil {
			return fmt.Errorf("set context: %w", err)
		}

		saga.State = model.SagaStateRunning
		saga.CurrentStep = model.SagaStepNameChargePayment
		saga.UpdatedAt = s.now()
		saga.Attempts = 0
		next := s.now().Add(model.StepTimeout(0))
		saga.NextAttemptAt = &next
		if err = s.sagaRepo.Upsert(ctx, saga); err != nil {
			return fmt.Errorf("upsert saga: %w", err)
		}

		step := model.NewSagaStep(uuidv7.V7(), sagaID, model.SagaStepNameReserveSeat, model.DirectionForward, s.now())
		step.Status = model.StepStatusSucceeded
		if err = s.sagaStepRepo.Append(ctx, step); err != nil {
			return fmt.Errorf("append step: %w", err)
		}

		sagaPayload, err := saga.DecodePayload()
		if err != nil {
			return fmt.Errorf("decode payload: %w", err)
		}

		cmd := payment.NewCharge(sagaID, sagaPayload.UserID, sagaPayload.Amount)
		msgID := uuidv7.V7()
		causation := msg.MessageID
		headers := messaging.NewMessageHeaders(msgID, sagaID, &causation, payment.EventTypeCharge)
		outboxMsg, err := outbox.NewOutbox(msgID, sagaID, sagaID, aggregateType, payment.TopicCommands, cmd, headers, s.now())
		if err != nil {
			return fmt.Errorf("create outbox message: %w", err)
		}

		return s.outboxRepo.Append(ctx, outboxMsg)
	})
}

func (s *SagaService) HandlePaymentRefunded(ctx context.Context, msg kafka.Message) error {
	return s.trx.Run(ctx, func(ctx context.Context) error {
		inserted, err := s.inboxRepo.Dedup(ctx, msg.MessageID, consumerName, s.now())
		if err != nil {
			return err
		}
		if !inserted {
			return nil
		}

		var refunded payment.Refunded
		if err = json.Unmarshal(msg.Value, &refunded); err != nil {
			return fmt.Errorf("unmarshal payment.refunded: %w", err)
		}

		sagaID, err := msg.GetSagaIDFromHeaders()
		if err != nil {
			return fmt.Errorf("parse saga id: %w", err)
		}

		saga, err := s.sagaRepo.FindByID(ctx, sagaID)
		if err != nil {
			return fmt.Errorf("find saga: %w", err)
		}

		if !saga.IsCompensating() {
			return nil
		}

		sagaCtx, err := saga.DecodeContext()
		if err != nil {
			return ErrDecodeSagaCtx
		}

		if err = sagaCtx.ValidateReservationID(); err != nil {
			return err
		}

		if err = sagaCtx.ValidateSeatID(); err != nil {
			return err
		}

		if !sagaCtx.ComparePaymentID(refunded.PaymentID) {
			return ErrPayloadContextMismatch
		}

		saga.UpdatedAt = s.now()
		if err = s.sagaRepo.Upsert(ctx, saga); err != nil {
			return fmt.Errorf("upsert saga: %w", err)
		}

		step := model.NewSagaStep(
			uuidv7.V7(),
			sagaID,
			model.SagaStepNameChargePayment,
			model.DirectionCompensate,
			s.now(),
		)
		step.Status = model.StepStatusSucceeded

		if err = s.sagaStepRepo.Append(ctx, step); err != nil {
			return fmt.Errorf("append step: %w", err)
		}

		cmd := inventory.NewReleaseSeat(sagaID, *sagaCtx.ReservationID, *sagaCtx.SeatID)
		msgID := uuidv7.V7()
		causation := msg.MessageID
		headers := messaging.NewMessageHeaders(msgID, sagaID, &causation, inventory.EventTypeReleaseSeat)
		outboxMsg, err := outbox.NewOutbox(msgID, sagaID, sagaID, aggregateType, inventory.TopicCommands, cmd, headers, s.now())
		if err != nil {
			return fmt.Errorf("create outbox message: %w", err)
		}

		return s.outboxRepo.Append(ctx, outboxMsg)
	})
}

func (s *SagaService) HandlePaymentCharged(ctx context.Context, msg kafka.Message) error {
	return s.trx.Run(ctx, func(ctx context.Context) error {
		inserted, err := s.inboxRepo.Dedup(ctx, msg.MessageID, consumerName, s.now())
		if err != nil {
			return err
		}
		if !inserted {
			return nil
		}

		sagaID, err := msg.GetSagaIDFromHeaders()
		if err != nil {
			return fmt.Errorf("parse saga id: %w", err)
		}

		var pc payment.Charged
		if err = json.Unmarshal(msg.Value, &pc); err != nil {
			return fmt.Errorf("failed to unmarshal payment.charge: %w", err)
		}

		saga, err := s.sagaRepo.FindByID(ctx, sagaID)
		if err != nil {
			return fmt.Errorf("find saga: %w", err)
		}

		sagaCtx, err := saga.DecodeContext()
		if err != nil {
			return fmt.Errorf("decode context: %w", err)
		}
		sagaCtx.PaymentID = &pc.PaymentID
		if err = saga.SetContext(sagaCtx); err != nil {
			return fmt.Errorf("set context: %w", err)
		}

		saga.State = model.SagaStateRunning
		saga.CurrentStep = model.SagaStepNameSendNotification
		saga.UpdatedAt = s.now()
		saga.Attempts = 0
		next := s.now().Add(model.StepTimeout(0))
		saga.NextAttemptAt = &next
		if err = s.sagaRepo.Upsert(ctx, saga); err != nil {
			return fmt.Errorf("upsert saga: %w", err)
		}

		step := model.NewSagaStep(uuidv7.V7(), sagaID, model.SagaStepNameChargePayment, model.DirectionForward, s.now())
		step.Status = model.StepStatusSucceeded
		if err = s.sagaStepRepo.Append(ctx, step); err != nil {
			return fmt.Errorf("append step: %w", err)
		}

		sagaPayload, err := saga.DecodePayload()
		if err != nil {
			return fmt.Errorf("decode payload: %w", err)
		}

		cmd := notification.NewSend(sagaID, sagaPayload.UserID, sagaPayload.Channel)
		msgID := uuidv7.V7()
		causation := msg.MessageID
		headers := messaging.NewMessageHeaders(msgID, sagaID, &causation, notification.EventTypeSend)
		outboxMsg, err := outbox.NewOutbox(msgID, sagaID, sagaID, aggregateType, notification.TopicCommands, cmd, headers, s.now())
		if err != nil {
			return fmt.Errorf("create outbox message: %w", err)
		}
		return s.outboxRepo.Append(ctx, outboxMsg)
	})
}

func (s *SagaService) HandlePaymentFailed(ctx context.Context, msg kafka.Message) error {
	return s.trx.Run(ctx, func(ctx context.Context) error {
		inserted, err := s.inboxRepo.Dedup(ctx, msg.MessageID, consumerName, s.now())
		if err != nil {
			return err
		}
		if !inserted {
			return nil
		}

		sagaID, err := msg.GetSagaIDFromHeaders()
		if err != nil {
			return fmt.Errorf("parse saga id: %w", err)
		}

		var payload payment.Failed
		if err = json.Unmarshal(msg.Value, &payload); err != nil {
			return fmt.Errorf("payload to unmarshal payment.failed: %w", err)
		}

		saga, err := s.sagaRepo.FindByID(ctx, sagaID)
		if err != nil {
			return fmt.Errorf("find saga: %w", err)
		}

		if saga.IsCompensated() || saga.IsCompensating() {
			return nil
		}

		sagaCtx, err := saga.DecodeContext()
		if err != nil {
			return fmt.Errorf("decode context: %w", err)
		}

		saga.State = model.SagaStateCompensating
		saga.UpdatedAt = s.now()
		if err = s.sagaRepo.Upsert(ctx, saga); err != nil {
			return fmt.Errorf("upsert saga: %w", err)
		}

		step := model.NewSagaStep(uuidv7.V7(), sagaID, model.SagaStepNameChargePayment, model.DirectionForward, s.now())
		step.Status = model.StepStatusFailed
		if payload.Reason != "" {
			step.Error = &payload.Reason
		}
		if err = s.sagaStepRepo.Append(ctx, step); err != nil {
			return fmt.Errorf("append step: %w", err)
		}
		if err = sagaCtx.ValidateReservationID(); err != nil {
			return err
		}
		if err = sagaCtx.ValidateSeatID(); err != nil {
			return err
		}

		cmd := inventory.NewReleaseSeat(sagaID, *sagaCtx.ReservationID, *sagaCtx.SeatID)
		msgID := uuidv7.V7()
		causation := msg.MessageID
		headers := messaging.NewMessageHeaders(msgID, sagaID, &causation, inventory.EventTypeReleaseSeat)
		outboxMsg, err := outbox.NewOutbox(msgID, sagaID, sagaID, aggregateType, inventory.TopicCommands, cmd, headers, s.now())
		if err != nil {
			return fmt.Errorf("create outbox message: %w", err)
		}
		return s.outboxRepo.Append(ctx, outboxMsg)
	})
}

func (s *SagaService) HandleSeatReservationFailed(ctx context.Context, msg kafka.Message) error {
	return s.trx.Run(ctx, func(ctx context.Context) error {
		inserted, err := s.inboxRepo.Dedup(ctx, msg.MessageID, consumerName, s.now())
		if err != nil {
			return err
		}
		if !inserted {
			return nil
		}

		sagaID, err := msg.GetSagaIDFromHeaders()
		if err != nil {
			return fmt.Errorf("parse saga id: %w", err)
		}

		saga, err := s.sagaRepo.FindByID(ctx, sagaID)
		if err != nil {
			return fmt.Errorf("find saga: %w", err)
		}

		var payload inventory.SeatReservationFailed
		if err = json.Unmarshal(msg.Value, &payload); err != nil {
			return fmt.Errorf("unmarshal seat.reservation_failed: %w", err)
		}

		saga.State = model.SagaStateFailed
		saga.UpdatedAt = s.now()
		if err = s.sagaRepo.Upsert(ctx, saga); err != nil {
			return fmt.Errorf("upsert saga: %w", err)
		}
		metrics.SagaFailedTotal.Inc()

		step := model.NewSagaStep(
			uuidv7.V7(),
			sagaID,
			model.SagaStepNameReserveSeat,
			model.DirectionForward,
			s.now(),
		)
		step.Status = model.StepStatusFailed
		if payload.Reason != "" {
			step.Error = &payload.Reason
		}

		return s.sagaStepRepo.Append(ctx, step)
	})
}

func (s *SagaService) HandleSeatReleased(ctx context.Context, msg kafka.Message) error {
	return s.trx.Run(ctx, func(ctx context.Context) error {
		inserted, err := s.inboxRepo.Dedup(ctx, msg.MessageID, consumerName, s.now())
		if err != nil {
			return err
		}
		if !inserted {
			return nil
		}

		var payload inventory.SeatReleased
		if err = json.Unmarshal(msg.Value, &payload); err != nil {
			return fmt.Errorf("unmarshal seat.released: %w", err)
		}

		sagaID, err := msg.GetSagaIDFromHeaders()
		if err != nil {
			return fmt.Errorf("parse saga id: %w", err)
		}

		saga, err := s.sagaRepo.FindByID(ctx, sagaID)
		if err != nil {
			return fmt.Errorf("find saga: %w", err)
		}

		if !saga.IsCompensating() {
			return nil
		}

		sagaCtx, err := saga.DecodeContext()
		if err != nil {
			return ErrDecodeSagaCtx
		}
		if err = sagaCtx.ValidateReservationID(); err != nil {
			return err
		}
		if err = sagaCtx.ValidateSeatID(); err != nil {
			return err
		}
		if *sagaCtx.ReservationID != payload.ReservationID || *sagaCtx.SeatID != payload.SeatID {
			return ErrPayloadContextMismatch
		}

		saga.State = model.SagaStateCompensated
		saga.UpdatedAt = s.now()
		if err = s.sagaRepo.Upsert(ctx, saga); err != nil {
			return fmt.Errorf("upsert saga: %w", err)
		}
		metrics.SagaCompensatedTotal.Inc()

		step := model.NewSagaStep(
			uuidv7.V7(),
			sagaID,
			model.SagaStepNameReserveSeat,
			model.DirectionCompensate,
			s.now(),
		)
		step.Status = model.StepStatusSucceeded

		return s.sagaStepRepo.Append(ctx, step)
	})
}

func (s *SagaService) HandleNotificationSent(ctx context.Context, msg kafka.Message) error {
	return s.trx.Run(ctx, func(ctx context.Context) error {
		inserted, err := s.inboxRepo.Dedup(ctx, msg.MessageID, consumerName, s.now())
		if err != nil {
			return err
		}
		if !inserted {
			return nil
		}

		sagaID, err := msg.GetSagaIDFromHeaders()
		if err != nil {
			return fmt.Errorf("parse saga id: %w", err)
		}

		var payload notification.Sent
		if err = json.Unmarshal(msg.Value, &payload); err != nil {
			return fmt.Errorf("unmarshal notification.sent: %w", err)
		}

		saga, err := s.sagaRepo.FindByID(ctx, sagaID)
		if err != nil {
			return fmt.Errorf("find saga: %w", err)
		}

		sagaCtx, err := saga.DecodeContext()
		if err != nil {
			return ErrDecodeSagaCtx
		}

		sagaCtx.NotificationID = &payload.NotificationID
		err = saga.SetContext(sagaCtx)
		if err != nil {
			return fmt.Errorf("set context: %w", err)
		}

		saga.State = model.SagaStateCompleted
		saga.UpdatedAt = s.now()
		saga.ResetSchedule(s.now())
		if err = s.sagaRepo.Upsert(ctx, saga); err != nil {
			return fmt.Errorf("upsert saga: %w", err)
		}
		metrics.SagaCompletedTotal.Inc()

		step := model.NewSagaStep(
			uuidv7.V7(),
			sagaID,
			model.SagaStepNameSendNotification,
			model.DirectionForward,
			s.now(),
		)
		step.Status = model.StepStatusSucceeded

		return s.sagaStepRepo.Append(ctx, step)
	})
}

func (s *SagaService) HandleNotificationFailed(ctx context.Context, msg kafka.Message) error {
	return s.trx.Run(ctx, func(ctx context.Context) error {
		inserted, err := s.inboxRepo.Dedup(ctx, msg.MessageID, consumerName, s.now())
		if err != nil {
			return err
		}
		if !inserted {
			return nil
		}

		sagaID, err := msg.GetSagaIDFromHeaders()
		if err != nil {
			return fmt.Errorf("parse saga id: %w", err)
		}

		var failed notification.Failed
		if err = json.Unmarshal(msg.Value, &failed); err != nil {
			return fmt.Errorf("unmarshal notification.failed: %w", err)
		}

		saga, err := s.sagaRepo.FindByID(ctx, sagaID)
		if err != nil {
			return fmt.Errorf("find saga: %w", err)
		}
		if saga.IsCompensated() || saga.IsCompensating() {
			return nil
		}

		sagaCtx, err := saga.DecodeContext()
		if err != nil {
			return ErrDecodeSagaCtx
		}
		if err = sagaCtx.ValidatePaymentID(); err != nil {
			return err
		}

		sagaCtx.NotificationID = &failed.NotificationID
		if err = saga.SetContext(sagaCtx); err != nil {
			return fmt.Errorf("set context: %w", err)
		}

		saga.State = model.SagaStateCompensating
		saga.UpdatedAt = s.now()
		if err = s.sagaRepo.Upsert(ctx, saga); err != nil {
			return fmt.Errorf("upsert saga: %w", err)
		}

		step := model.NewSagaStep(uuidv7.V7(), sagaID, model.SagaStepNameSendNotification, model.DirectionForward, s.now())
		step.Status = model.StepStatusFailed
		if failed.Reason != "" {
			step.Error = &failed.Reason
		}
		if err = s.sagaStepRepo.Append(ctx, step); err != nil {
			return fmt.Errorf("append step: %w", err)
		}

		cmd := payment.NewRefund(sagaID, *sagaCtx.PaymentID)
		msgID := uuidv7.V7()
		causation := msg.MessageID
		headers := messaging.NewMessageHeaders(msgID, sagaID, &causation, payment.EventTypeRefund)
		outboxMsg, err := outbox.NewOutbox(msgID, sagaID, sagaID, aggregateType, payment.TopicCommands, cmd, headers, s.now())
		if err != nil {
			return fmt.Errorf("create outbox: %w", err)
		}
		return s.outboxRepo.Append(ctx, outboxMsg)
	})
}

func (s *SagaService) ReplayDeadMessage(ctx context.Context, id uuid.UUID) error {
	return s.trx.Run(ctx, func(ctx context.Context) error {
		dm, err := s.dlqRepo.FindByID(ctx, id)
		if err != nil {
			return fmt.Errorf("find dead message: %w", err)
		}

		if err = dm.CanReplay(); err != nil {
			return err
		}

		var headers messaging.MessageHeaders
		if err := json.Unmarshal(dm.Headers, &headers); err != nil {
			return fmt.Errorf("unmarshal headers: %w", err)
		}
		newMessageID := uuidv7.V7()
		headers.MessageID = newMessageID
		causation := dm.MessageID
		headers.CausationID = &causation

		sagaID := *dm.SagaID
		aggregateID := sagaID

		outboxMsg, err := outbox.NewOutbox(
			newMessageID,
			aggregateID,
			sagaID,
			aggregateType,
			dm.Topic,
			json.RawMessage(dm.Payload),
			headers,
			s.now(),
		)
		if err != nil {
			return fmt.Errorf("create outbox: %w", err)
		}

		if err = s.outboxRepo.Append(ctx, outboxMsg); err != nil {
			return fmt.Errorf("outbox append: %w", err)
		}

		return s.dlqRepo.MarkReplayed(ctx, id, s.now())
	})
}

func (s *SagaService) ProcessExpired(ctx context.Context) error {
	return s.trx.Run(ctx, func(ctx context.Context) error {
		sagas, err := s.sagaRepo.FindExpired(ctx, s.now(), 10)
		if err != nil {
			return fmt.Errorf("find expired: %w", err)
		}
		for i := range sagas {
			if err := s.tickSaga(ctx, &sagas[i]); err != nil {
				slog.Error("tick saga failed", "saga_id", sagas[i].ID, "err", err)
			}
		}
		return nil
	})
}

func (s *SagaService) tickSaga(ctx context.Context, saga *model.Saga) error {
	saga.Attempts++

	if saga.Attempts >= model.MaxSagaAttempts {
		return s.escalateSaga(ctx, saga)
	}

	return s.rePublishCurrentCommand(ctx, saga)
}

func (s *SagaService) rePublishCurrentCommand(ctx context.Context, saga *model.Saga) error {
	_, err := saga.DecodeContext()
	if err != nil {
		return fmt.Errorf("decode context: %w", err)
	}

	sagaPayload, err := saga.DecodePayload()
	if err != nil {
		return fmt.Errorf("decode payload: %w", err)
	}

	var (
		eventType string
		topic     string
		cmd       any
	)
	switch {
	case saga.State == model.SagaStateRunning && saga.CurrentStep == model.SagaStepNameReserveSeat:
		eventType = inventory.EventTypeReserveSeat
		topic = inventory.TopicCommands
		cmd = inventory.NewReserveSeat(saga.ID, sagaPayload.EventID, sagaPayload.UserID)
	case saga.State == model.SagaStateRunning && saga.CurrentStep == model.SagaStepNameChargePayment:
		eventType = payment.EventTypeCharge
		topic = payment.TopicCommands
		cmd = payment.NewCharge(saga.ID, sagaPayload.UserID, sagaPayload.Amount)
	case saga.State == model.SagaStateRunning && saga.CurrentStep == model.SagaStepNameSendNotification:
		eventType = notification.EventTypeSend
		topic = notification.TopicCommands
		cmd = notification.NewSend(saga.ID, sagaPayload.UserID, sagaPayload.Channel)
	default:
		return nil
	}

	msgID := uuidv7.V7()
	headers := messaging.NewMessageHeaders(msgID, saga.ID, nil, eventType)
	outboxMsg, err := outbox.NewOutbox(msgID, saga.ID, saga.ID, aggregateType, topic, cmd, headers, s.now())
	if err != nil {
		return fmt.Errorf("create outbox: %w", err)
	}
	if err := s.outboxRepo.Append(ctx, outboxMsg); err != nil {
		return fmt.Errorf("outbox append: %w", err)
	}
	metrics.SagaRetriesTotal.WithLabelValues(string(saga.CurrentStep)).Inc()

	next := s.now().Add(model.StepTimeout(saga.Attempts))
	saga.NextAttemptAt = &next
	saga.UpdatedAt = s.now()

	return s.sagaRepo.Upsert(ctx, *saga)
}

func (s *SagaService) escalateSaga(ctx context.Context, saga *model.Saga) error {
	if saga.State == model.SagaStateRunning {
		sagaCtx, err := saga.DecodeContext()
		if err != nil {
			return err
		}

		saga.State = model.SagaStateCompensating
		saga.ResetSchedule(s.now())
		if err := s.sagaRepo.Upsert(ctx, *saga); err != nil {
			return err
		}
		metrics.SagaStuckTotal.Inc()

		reason := fmt.Sprintf("max attempts exceeded (%d)", model.MaxSagaAttempts)
		step := model.NewSagaStep(uuidv7.V7(), saga.ID, saga.CurrentStep, model.DirectionForward, s.now())
		step.Status = model.StepStatusFailed
		step.Error = &reason
		if err := s.sagaStepRepo.Append(ctx, step); err != nil {
			return err
		}

		if sagaCtx.ReservationID == nil || sagaCtx.SeatID == nil {
			saga.State = model.SagaStateFailed
			metrics.SagaFailedTotal.Inc()
			return s.sagaRepo.Upsert(ctx, *saga)
		}

		cmd := inventory.NewReleaseSeat(saga.ID, *sagaCtx.ReservationID, *sagaCtx.SeatID)
		msgID := uuidv7.V7()
		headers := messaging.NewMessageHeaders(msgID, saga.ID, nil, inventory.EventTypeReleaseSeat)
		outboxMsg, err := outbox.NewOutbox(msgID, saga.ID, saga.ID, aggregateType, inventory.TopicCommands, cmd, headers, s.now())
		if err != nil {
			return err
		}
		return s.outboxRepo.Append(ctx, outboxMsg)
	}

	if saga.State == model.SagaStateCompensating {
		saga.State = model.SagaStateFailed
		saga.ResetSchedule(s.now())
		metrics.SagaStuckTotal.Inc()
		metrics.SagaFailedTotal.Inc()
		// TODO: можно положить в dead_message для оператора
		return s.sagaRepo.Upsert(ctx, *saga)
	}

	return nil
}

func IsPermanent(err error) bool {
	return dlq.IsBasePermanent(err) ||
		errors.Is(err, kafka.ErrMalformedHeaders) ||
		errors.Is(err, ErrPayloadContextMismatch) ||
		errors.Is(err, model.ErrSagaHasNoReservationID) ||
		errors.Is(err, model.ErrSagaHasNoPaymentID) ||
		errors.Is(err, model.ErrSagaHasNoSeatID)
}
