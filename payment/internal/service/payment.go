package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go-saga-outbox/payment/internal/model"
	"go-saga-outbox/payment/internal/repo"
	"go-saga-outbox/pkg/contracts/payment"
	"go-saga-outbox/pkg/inbox"
	"go-saga-outbox/pkg/kafka"
	"go-saga-outbox/pkg/messaging"
	"go-saga-outbox/pkg/outbox"
	"go-saga-outbox/pkg/trx"
	uuidV7 "go-saga-outbox/pkg/uuid"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

const (
	consumerName         = "payment"
	aggregateTypePayment = "payment"
)

var (
	ErrPaymentNotFound           = errors.New("payment not found")
	ErrCannotRefundNotCharged    = errors.New("cannot refund not charged")
	ErrCannotUpdatePaymentStatus = errors.New("cannot update payment status")
)

type PaymentService struct {
	trx         *trx.Transaction
	inboxRepo   *inbox.Repo
	outboxRepo  *outbox.Repo
	paymentRepo *repo.PaymentRepo
	now         func() time.Time
}

func NewPaymentService(
	trx *trx.Transaction,
	inboxRepo *inbox.Repo,
	outboxRepo *outbox.Repo,
	paymentRepo *repo.PaymentRepo,
	now func() time.Time,
) *PaymentService {
	return &PaymentService{
		trx:         trx,
		inboxRepo:   inboxRepo,
		paymentRepo: paymentRepo,
		outboxRepo:  outboxRepo,
		now:         now,
	}
}

func (s *PaymentService) HandleChargeCommand(ctx context.Context, msg kafka.Message) error {
	return s.trx.Run(ctx, func(ctx context.Context) error {
		inserted, err := s.inboxRepo.Dedup(ctx, msg.MessageID, consumerName, s.now())
		if err != nil {
			return err
		}
		if !inserted {
			return nil
		}

		var cmd payment.Charge
		if err := json.Unmarshal(msg.Value, &cmd); err != nil {
			return fmt.Errorf("unmarshal charge: %w", err)
		}

		if _, err = s.paymentRepo.FindBySagaID(ctx, cmd.SagaID); err == nil {
			return nil
		} else if !errors.Is(err, repo.ErrPaymentNotFound) {
			return fmt.Errorf("find by saga: %w", err)
		}

		mPayment, domainErr := model.NewPayment(cmd.SagaID, cmd.UserID, cmd.Amount)
		if err = s.paymentRepo.Create(ctx, mPayment); err != nil {
			return fmt.Errorf("create payment: %w", err)
		}

		eventType, payload := s.contractFromDomain(mPayment, domainErr)
		return s.emit(ctx, mPayment.ID, cmd.SagaID, eventType, payment.TopicEvents, payload, msg.MessageID)
	})
}

func (s *PaymentService) HandleRefundCommand(ctx context.Context, msg kafka.Message) error {
	return s.trx.Run(ctx, func(ctx context.Context) error {
		inserted, err := s.inboxRepo.Dedup(ctx, msg.MessageID, consumerName, s.now())
		if err != nil {
			return err
		}
		if !inserted {
			return nil
		}

		var cmd payment.Refund
		if err := json.Unmarshal(msg.Value, &cmd); err != nil {
			return fmt.Errorf("unmarshal refund.command: %w", err)
		}

		mPayment, err := s.paymentRepo.FindBySagaID(ctx, cmd.SagaID)
		if err != nil {
			if errors.Is(err, repo.ErrPaymentNotFound) {
				slog.Warn("refund for missing payment, skipping",
					"saga_id", cmd.SagaID, "payment_id", cmd.PaymentID)
				return nil
			}
			return fmt.Errorf("find payment: %w", err)
		}

		if mPayment.Status == model.PaymentStatusRefunded {
			return nil
		}

		if !mPayment.IsCharged() {
			return ErrCannotRefundNotCharged
		}

		mPayment.Status = model.PaymentStatusRefunded
		if err := s.paymentRepo.Update(ctx, mPayment); err != nil {
			return fmt.Errorf("update payment: %w", err)
		}

		payload := payment.NewRefunded(cmd.SagaID, mPayment.ID)
		return s.emit(ctx, mPayment.ID, cmd.SagaID,
			payment.EventTypeRefunded, payment.TopicEvents, payload, msg.MessageID)
	})
}

func (s *PaymentService) emit(
	ctx context.Context,
	aggregateID uuid.UUID,
	sagaID uuid.UUID,
	eventType string,
	topic string,
	payload any,
	causationID uuid.UUID,
) error {
	msgID := uuidV7.V7()
	headers := messaging.NewMessageHeaders(msgID, sagaID, &causationID, eventType)
	outboxMsg, err := outbox.NewOutbox(msgID, aggregateID, sagaID, aggregateTypePayment, topic, payload, headers, s.now())
	if err != nil {
		return fmt.Errorf("create outbox: %w", err)
	}
	return s.outboxRepo.Append(ctx, outboxMsg)
}

func (s *PaymentService) contractFromDomain(p model.Payment, domainErr error) (string, any) {
	switch {
	case errors.Is(domainErr, model.ErrNegativeAmount):
		return payment.EventTypeFailed, payment.NewFailed(p.SagaID, domainErr.Error())
	case domainErr != nil:
		return payment.EventTypeFailed, payment.NewFailed(p.SagaID, domainErr.Error())
	default:
		return payment.EventTypeCharged, payment.NewCharged(p.ID, p.SagaID, p.AmountCents)
	}
}
