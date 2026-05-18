package dlq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go-saga-outbox/pkg/kafka"
	"go-saga-outbox/pkg/metrics"
	"go-saga-outbox/pkg/trx"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
)

func Wrap(
	consumer string,
	dlqRepo *Repo,
	trx *trx.Transaction,
	now func() time.Time,
	isPermanent PermanentChecker,
	inner kafka.Handler,
) kafka.Handler {
	return func(ctx context.Context, msg kafka.Message) (returnErr error) {
		defer func() {
			if r := recover(); r != nil {
				reason := fmt.Sprintf("panic: %v\n%s", r, debug.Stack())
				returnErr = errors.Join(kafka.ErrPermanent, fmt.Errorf("panic: %v", r))
				writeDLQ(ctx, dlqRepo, trx, consumer, now(), msg, reason)
			}
		}()

		err := inner(ctx, msg)
		if err == nil {
			return nil
		}

		if isPermanent(err) {
			writeDLQ(ctx, dlqRepo, trx, consumer, now(), msg, err.Error())
			return errors.Join(kafka.ErrPermanent, err)
		}
		return err
	}
}

func writeDLQ(
	ctx context.Context,
	repo *Repo,
	trx *trx.Transaction,
	consumer string,
	now time.Time,
	msg kafka.Message,
	reason string,
) {
	headersBytes, _ := json.Marshal(msg.Headers)
	sagaID, _ := msg.GetSagaIDFromHeaders()
	var sagaIDPtr *uuid.UUID
	if sagaID != uuid.Nil {
		sagaIDPtr = &sagaID
	}
	dm := NewDeadMessage(msg.MessageID, sagaIDPtr, msg.Topic, reason, consumer,
		msg.Value, headersBytes, now)
	if err := trx.Run(ctx, func(ctx context.Context) error {
		return repo.Insert(ctx, dm)
	}); err != nil {
		slog.Error("failed to write DLQ", "err", err, "message_id", msg.MessageID)
		return
	}
	metrics.DLQMessagesTotal.WithLabelValues(consumer).Inc()
}
