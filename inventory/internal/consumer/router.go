package consumer

import (
	"context"
	"go-saga-outbox/inventory/internal/service"
	"go-saga-outbox/pkg/contracts/inventory"
	"go-saga-outbox/pkg/kafka"
	"log/slog"
)

func Route(svc *service.ReservationService) kafka.Handler {
	return func(ctx context.Context, msg kafka.Message) error {
		switch msg.Headers["event_type"] {
		case inventory.EventTypeReserveSeat:
			return svc.HandleReserveSeatCommand(ctx, msg)
		case inventory.EventTypeReleaseSeat:
			return svc.HandleReleaseSeatCommand(ctx, msg)
		default:
			slog.Warn("unknown event type", "type", msg.Headers["event_type"], "topic", msg.Topic)
			return nil
		}
	}
}
