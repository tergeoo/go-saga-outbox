package outbox

import (
	"context"
	"fmt"
	"go-saga-outbox/pkg/trx"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

type RelayConfig struct {
	PollInterval time.Duration `env:"POLL_INTERVAL" envDefault:"1s"`
	BatchSize    int           `env:"BATCH_SIZE" envDefault:"50"`
}

type outboxRepo interface {
	Append(ctx context.Context, msg Outbox) error
	// FetchUnpublished вызывать только внутри транзакции
	FetchUnpublished(ctx context.Context, limit int) ([]Outbox, error)
	MarkPublished(ctx context.Context, ids []uuid.UUID, at time.Time) error
}

type Relay struct {
	cfg        RelayConfig
	trx        *trx.Transaction
	outboxRepo outboxRepo
	producer   *Producer
	now        func() time.Time
}

func NewRelay(
	cfg RelayConfig,
	trx *trx.Transaction,
	outboxRepo outboxRepo,
	producer *Producer,
	now func() time.Time,
) *Relay {
	return &Relay{
		cfg:        cfg,
		trx:        trx,
		outboxRepo: outboxRepo,
		producer:   producer,
		now:        now,
	}
}

func (r *Relay) Start(ctx context.Context) {
	go func() {
		if err := r.run(ctx); err != nil {
			slog.Error("failed to start relay", "err", err)
		}
	}()
}

func (r *Relay) run(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := r.processBatch(ctx); err != nil {
				slog.Error("relay batch failed", "err", err)
			}
		}
	}
}

func (r *Relay) processBatch(ctx context.Context) error {
	err := r.trx.Run(ctx, func(ctx context.Context) error {
		results, err := r.outboxRepo.FetchUnpublished(ctx, r.cfg.BatchSize)
		if err != nil {
			slog.Error("fetch unpublished failed", "err", err)
			return fmt.Errorf("fetch unpublished failed: %w", err)
		}

		publishedIDs := make([]uuid.UUID, 0, len(results))

		for _, result := range results {
			publishErr := r.producer.Publish(ctx, result)
			if publishErr != nil {
				slog.Error("publish failed", "err", publishErr)
				continue
			}

			publishedIDs = append(publishedIDs, result.ID)
		}

		if len(publishedIDs) == 0 {
			return nil
		}

		err = r.outboxRepo.MarkPublished(ctx, publishedIDs, r.now())
		if err != nil {
			slog.Error("mark published failed", "err", err)
			return fmt.Errorf("mark published failed: %w", err)
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}
