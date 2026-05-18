package repo

import (
	"context"
	"database/sql"
	"errors"
	"go-saga-outbox/notification/internal/model"
	"go-saga-outbox/pkg/trx"

	"github.com/google/uuid"
)

var ErrNotificationNotFound = errors.New("notification not found")

type NotificationRepo struct {
	trx *trx.Transaction
}

func NewNotificationRepo(trx *trx.Transaction) *NotificationRepo {
	return &NotificationRepo{trx}
}

func (r *NotificationRepo) Create(ctx context.Context, notification model.Notification) error {
	tx := r.trx.FromContext(ctx)

	query := `
	INSERT INTO notification (id, saga_id, channel, status, sent_at) VALUES ($1, $2, $3, $4, $5)`

	_, err := tx.ExecContext(ctx, query, notification.ID, notification.SagaID, notification.Channel, notification.Status, notification.SentAt)
	if err != nil {
		return err
	}

	return nil
}

func (r *NotificationRepo) FindBySagaID(ctx context.Context, id uuid.UUID) (model.Notification, error) {
	tx := r.trx.FromContext(ctx)

	query := `SELECT id, saga_id, channel, status, sent_at FROM notification WHERE saga_id = $1`

	var notification model.Notification
	err := tx.GetContext(ctx, &notification, query, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Notification{}, ErrNotificationNotFound
		}
		return notification, err
	}

	return notification, nil
}
