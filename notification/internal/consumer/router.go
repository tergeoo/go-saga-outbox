package consumer

import (
	"context"
	"go-saga-outbox/notification/internal/service"
	"go-saga-outbox/pkg/contracts/notification"
	"go-saga-outbox/pkg/kafka"
	"log/slog"
)

func Route(svc *service.NotificationService) kafka.Handler {
	return func(ctx context.Context, msg kafka.Message) error {
		switch msg.Headers["event_type"] {
		case notification.EventTypeSend:
			return svc.HandleSendCommand(ctx, msg)
		default:
			slog.Warn("unknown event type", "type", msg.Headers["event_type"], "topic", msg.Topic)
			return nil
		}
	}
}
