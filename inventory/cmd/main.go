package main

import (
	"context"
	"go-saga-outbox/inventory/internal"
	"go-saga-outbox/inventory/internal/config"
	"log/slog"
)

func main() {
	cfg, err := config.NewConfig()
	if err != nil {
		slog.Error("config", "err", err)
		panic(err)
	}

	ctx := context.Background()

	app, err := internal.NewApp(cfg)
	if err != nil {
		slog.Error("new app", "err", err)
		panic(err)
	}
	defer func() {
		if err := app.Shutdown(context.Background()); err != nil {
			slog.Error("shutdown", "err", err)
		}
	}()

	if err = app.Run(ctx); err != nil {
		slog.Error("run", "err", err)
		panic(err)
	}
}
