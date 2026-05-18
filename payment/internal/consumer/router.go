package consumer

import (
	"context"
	"go-saga-outbox/payment/internal/service"
	"go-saga-outbox/pkg/contracts/payment"
	"go-saga-outbox/pkg/kafka"
	"log/slog"
)

func Route(svc *service.PaymentService) kafka.Handler {
	return func(ctx context.Context, msg kafka.Message) error {
		switch msg.Headers["event_type"] {
		case payment.EventTypeCharge:
			return svc.HandleChargeCommand(ctx, msg)
		case payment.EventTypeRefund:
			return svc.HandleRefundCommand(ctx, msg)
		default:
			slog.Warn("unknown event type", "type", msg.Headers["event_type"], "topic", msg.Topic)
			return nil
		}
	}
}
