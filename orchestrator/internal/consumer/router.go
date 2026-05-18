package consumer

import (
	"context"
	"go-saga-outbox/orchestrator/internal/service"
	"go-saga-outbox/pkg/contracts/inventory"
	"go-saga-outbox/pkg/contracts/notification"
	"go-saga-outbox/pkg/contracts/payment"
	"go-saga-outbox/pkg/kafka"
	"log/slog"
)

func Route(svc *service.SagaService) kafka.Handler {
	return func(ctx context.Context, msg kafka.Message) error {
		slog.Info("Starting go-saga-outbox", "msg", msg)
		switch msg.Headers["event_type"] {
		case inventory.EventTypeSeatReserved:
			return svc.HandleSeatReserved(ctx, msg)
		case inventory.EventTypeSeatReservationFailed:
			return svc.HandleSeatReservationFailed(ctx, msg)
		case inventory.EventTypeSeatReleased:
			return svc.HandleSeatReleased(ctx, msg)

		case payment.EventTypeCharged:
			return svc.HandlePaymentCharged(ctx, msg)
		case payment.EventTypeFailed:
			return svc.HandlePaymentFailed(ctx, msg)
		case payment.EventTypeRefunded:
			return svc.HandlePaymentRefunded(ctx, msg)

		case notification.EventTypeSent:
			return svc.HandleNotificationSent(ctx, msg)
		case notification.EventTypeFailed:
			return svc.HandleNotificationFailed(ctx, msg)
		default:
			slog.Warn("unknown event type", "type", msg.Headers["event_type"], "topic", msg.Topic)
			return nil
		}
	}
}
