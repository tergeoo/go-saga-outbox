package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go-saga-outbox/notification/internal/model"
	"go-saga-outbox/notification/internal/repo"
	"go-saga-outbox/pkg/contracts/notification"
	"go-saga-outbox/pkg/inbox"
	"go-saga-outbox/pkg/kafka"
	"go-saga-outbox/pkg/messaging"
	"go-saga-outbox/pkg/outbox"
	"go-saga-outbox/pkg/trx"
	uuidV7 "go-saga-outbox/pkg/uuid"
	"time"

	"github.com/google/uuid"
)

const (
	consumerName              = "notification"
	aggregateTypeNotification = "notification"
)

type NotificationService struct {
	trx              *trx.Transaction
	inboxRepo        *inbox.Repo
	outboxRepo       *outbox.Repo
	notificationRepo *repo.NotificationRepo
	now              func() time.Time
}

func NewNotificationService(
	trx *trx.Transaction,
	inboxRepo *inbox.Repo,
	outboxRepo *outbox.Repo,
	notificationRepo *repo.NotificationRepo,
	now func() time.Time,
) *NotificationService {
	return &NotificationService{
		trx:              trx,
		inboxRepo:        inboxRepo,
		outboxRepo:       outboxRepo,
		notificationRepo: notificationRepo,
		now:              now,
	}
}

func (s *NotificationService) HandleSendCommand(ctx context.Context, msg kafka.Message) error {
	return s.trx.Run(ctx, func(ctx context.Context) error {
		inserted, err := s.inboxRepo.Dedup(ctx, msg.MessageID, consumerName, s.now())
		if err != nil {
			return err
		}
		if !inserted {
			return nil
		}

		var cmd notification.Send
		if err = json.Unmarshal(msg.Value, &cmd); err != nil {
			return fmt.Errorf("unmarshal notification: %w", err)
		}

		n, err := s.notificationRepo.FindBySagaID(ctx, cmd.SagaID)
		if err != nil && !errors.Is(err, repo.ErrNotificationNotFound) {
			return fmt.Errorf("find notification: %w", err)
		}
		if err == nil {
			return nil
		}

		if cmd.Channel == "broken" {
			n = model.NewNotification(uuidV7.V7(), cmd.SagaID, cmd.Channel, model.NotificationStatusFailed, s.now())
			if err := s.notificationRepo.Create(ctx, n); err != nil {
				return fmt.Errorf("create notification: %w", err)
			}

			payload := notification.NewFailed(cmd.SagaID, n.ID, "channel is broken")
			return s.emit(ctx, cmd.SagaID, notification.EventTypeFailed, n.ID, msg.MessageID, payload)
		}

		n = model.NewNotification(uuidV7.V7(), cmd.SagaID, cmd.Channel, model.NotificationStatusSent, s.now())
		err = s.notificationRepo.Create(ctx, n)
		if err != nil {
			return fmt.Errorf("create notification: %w", err)
		}

		payload := notification.NewSent(n.ID, cmd.SagaID)
		return s.emit(ctx, cmd.SagaID, notification.EventTypeSent, n.ID, msg.MessageID, payload)
	})
}

func (s *NotificationService) emit(
	ctx context.Context,
	sagaID uuid.UUID,
	eventType string,
	notificationID uuid.UUID,
	messageID uuid.UUID,
	payload any,
) error {
	msgID := uuidV7.V7()
	causation := messageID
	headers := messaging.NewMessageHeaders(msgID, sagaID, &causation, eventType)
	outboxMsg, err := outbox.NewOutbox(
		msgID,
		notificationID,
		sagaID,
		aggregateTypeNotification,
		notification.TopicEvents,
		payload,
		headers,
		s.now(),
	)
	if err != nil {
		return fmt.Errorf("create outbox: %w", err)
	}
	return s.outboxRepo.Append(ctx, outboxMsg)
}
