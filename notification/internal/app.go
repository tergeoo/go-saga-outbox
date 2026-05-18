package internal

import (
	"context"
	"errors"
	"go-saga-outbox/notification/internal/config"
	"go-saga-outbox/notification/internal/consumer"
	"go-saga-outbox/notification/internal/migrations"
	"go-saga-outbox/notification/internal/repo"
	"go-saga-outbox/notification/internal/service"
	"go-saga-outbox/pkg/db"
	"go-saga-outbox/pkg/inbox"
	"go-saga-outbox/pkg/kafka"
	"go-saga-outbox/pkg/metrics"
	"go-saga-outbox/pkg/outbox"
	"go-saga-outbox/pkg/trx"
	"log/slog"
	"os/signal"
	"syscall"
	"time"

	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/errgroup"
)

type App struct {
	config config.Config
	db     *sqlx.DB

	trx   *trx.Transaction
	nowFn func() time.Time

	inboxRepo        *inbox.Repo
	outboxRepo       *outbox.Repo
	notificationRepo *repo.NotificationRepo

	outboxProducer   *outbox.Producer
	outboxRelay      *outbox.Relay
	commandsConsumer *kafka.Consumer

	notificationService *service.NotificationService
}

func NewApp(cfg config.Config) (*App, error) {
	app := &App{
		config: cfg,
	}

	nowFn := func() time.Time {
		return time.Now().UTC()
	}
	app.nowFn = nowFn

	database, err := db.PostgresSQLX(cfg.DS)
	if err != nil {
		return nil, err
	}
	app.db = database

	if err = db.Migrate(app.db.DB, migrations.FS, cfg.Name, cfg.MigrationsDir); err != nil {
		return nil, err
	}

	app.trx = trx.NewTransaction(app.db)

	app.inboxRepo = inbox.NewInboxRepo(app.trx)
	app.outboxRepo = outbox.NewOutboxRepo(app.trx)
	app.notificationRepo = repo.NewNotificationRepo(app.trx)

	app.notificationService = service.NewNotificationService(
		app.trx,
		app.inboxRepo,
		app.outboxRepo,
		app.notificationRepo,
		app.nowFn,
	)

	app.outboxProducer = outbox.NewOutboxProducer(cfg.OutboxProducer)
	app.outboxRelay = outbox.NewRelay(cfg.OutboxRelay, app.trx, app.outboxRepo, app.outboxProducer, app.nowFn)

	app.commandsConsumer = kafka.NewConsumer(cfg.CommandsConsumer)

	return app, nil
}

func (a *App) Run(ctx context.Context) error {
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	a.outboxRelay.Start(ctx)
	metrics.StartOutboxCollector(ctx, a.db, "notification", 5*time.Second)

	g, gctx := errgroup.WithContext(ctx)
	handler := consumer.Route(a.notificationService)

	g.Go(func() error { return a.commandsConsumer.Run(gctx, handler) })
	g.Go(func() error { return metrics.StartServer(gctx, a.config.MetricsPort) })

	slog.Info("notification running", "app", a.config.Name)
	return g.Wait()
}

func (a *App) Shutdown(ctx context.Context) error {
	slog.Info("app shutting down")

	var shutdownErr error
	addErr := func(err error) {
		if err == nil {
			return
		}
		shutdownErr = errors.Join(shutdownErr, err)
	}

	if a.db != nil {
		addErr(a.db.Close())
	}
	if a.commandsConsumer != nil {
		addErr(a.commandsConsumer.Close())
	}
	if a.outboxProducer != nil {
		addErr(a.outboxProducer.Shutdown(ctx))
	}

	return shutdownErr
}
