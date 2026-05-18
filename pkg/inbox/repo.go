package inbox

import (
	"context"
	"fmt"
	"go-saga-outbox/pkg/metrics"
	"go-saga-outbox/pkg/trx"
	"time"

	"github.com/google/uuid"
)

type Repo struct {
	trx *trx.Transaction
}

func NewInboxRepo(trx *trx.Transaction) *Repo {
	return &Repo{trx: trx}
}

func (r *Repo) Insert(ctx context.Context, msg Inbox) (bool, error) {
	tx := r.trx.FromContext(ctx)

	query := `                                                  
      INSERT INTO inbox (message_id, consumer, processed_at)
      VALUES ($1, $2, $3)                                                                                                                                                                                        
      ON CONFLICT (consumer, message_id) DO NOTHING`

	res, err := tx.ExecContext(ctx, query, msg.MessageID, msg.Consumer, msg.ProcessedAt)
	if err != nil {
		return false, fmt.Errorf("insert inbox: %w", err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("rows affected: %w", err)
	}

	return n == 1, nil
}

func (r *Repo) Dedup(ctx context.Context, messageID uuid.UUID, consumer string, now time.Time) (bool, error) {
	inserted, err := r.Insert(ctx, NewInbox(messageID, consumer, now))
	if err != nil {
		return false, fmt.Errorf("inbox dedup: %w", err)
	}
	if !inserted {
		metrics.InboxDuplicatesTotal.WithLabelValues(consumer).Inc()
	}
	return inserted, nil
}
