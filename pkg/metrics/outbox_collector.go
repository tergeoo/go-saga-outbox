package metrics

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/jmoiron/sqlx"
)

func StartOutboxCollector(ctx context.Context, db *sqlx.DB, service string, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				collectOutboxStats(ctx, db, service)
			}
		}
	}()
}

func collectOutboxStats(ctx context.Context, db *sqlx.DB, service string) {
	var count int
	if err := db.GetContext(ctx, &count,
		`SELECT COUNT(*) FROM outbox WHERE published_at IS NULL`); err != nil {
		slog.Debug("outbox collector: count query failed", "err", err, "service", service)
		return
	}
	OutboxUnpublishedCount.WithLabelValues(service).Set(float64(count))

	if count == 0 {
		OutboxOldestAgeSeconds.WithLabelValues(service).Set(0)
		return
	}

	var age sql.NullFloat64
	if err := db.GetContext(ctx, &age,
		`SELECT EXTRACT(EPOCH FROM (now() - MIN(created_at))) FROM outbox WHERE published_at IS NULL`); err != nil {
		slog.Debug("outbox collector: age query failed", "err", err, "service", service)
		return
	}
	if age.Valid {
		OutboxOldestAgeSeconds.WithLabelValues(service).Set(age.Float64)
	}
}
