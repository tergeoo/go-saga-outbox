package metrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/jmoiron/sqlx"
)

func StartSagaCollector(ctx context.Context, db *sqlx.DB, interval time.Duration) {
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
				collectSagaStates(ctx, db)
				collectDLQUnreplayed(ctx, db)
			}
		}
	}()
}

func collectSagaStates(ctx context.Context, db *sqlx.DB) {
	rows, err := db.QueryContext(ctx, `SELECT state, COUNT(*) FROM saga GROUP BY state`)
	if err != nil {
		slog.Debug("saga collector: state query failed", "err", err)
		return
	}
	defer rows.Close()

	seen := make(map[string]struct{})
	for rows.Next() {
		var state string
		var count int
		if err := rows.Scan(&state, &count); err != nil {
			slog.Debug("saga collector: scan failed", "err", err)
			return
		}
		SagaStateCount.WithLabelValues(state).Set(float64(count))
		seen[state] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		slog.Debug("saga collector: rows err", "err", err)
		return
	}

	for _, state := range []string{"running", "compensating", "completed", "compensated", "failed"} {
		if _, ok := seen[state]; !ok {
			SagaStateCount.WithLabelValues(state).Set(0)
		}
	}
}

func collectDLQUnreplayed(ctx context.Context, db *sqlx.DB) {
	rows, err := db.QueryContext(ctx,
		`SELECT consumer, COUNT(*) FROM dead_message WHERE replayed_at IS NULL GROUP BY consumer`)
	if err != nil {
		slog.Debug("saga collector: dlq query failed", "err", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var consumer string
		var count int
		if err := rows.Scan(&consumer, &count); err != nil {
			slog.Debug("saga collector: dlq scan failed", "err", err)
			return
		}
		DLQUnreplayedCount.WithLabelValues(consumer).Set(float64(count))
	}
	if err := rows.Err(); err != nil {
		slog.Debug("saga collector: dlq rows err", "err", err)
	}
}
