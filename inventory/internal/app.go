package internal

import (
	"context"
	"errors"
	"go-saga-outbox/inventory/internal/config"
	"go-saga-outbox/inventory/internal/consumer"
	"go-saga-outbox/inventory/internal/migrations"
	"go-saga-outbox/inventory/internal/repo"
	"go-saga-outbox/inventory/internal/service"
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

	inboxRepo       *inbox.Repo
	seatRepo        *repo.SeatRepo
	reservationRepo *repo.ReservationRepo
	outboxRepo      *outbox.Repo

	outboxProducer   *outbox.Producer
	outboxRelay      *outbox.Relay
	commandsConsumer *kafka.Consumer

	reservationService *service.ReservationService
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
	app.seatRepo = repo.NewSeatRepo(app.trx)
	app.reservationRepo = repo.NewReservationRepo(app.trx)
	app.outboxRepo = outbox.NewOutboxRepo(app.trx)

	app.reservationService = service.NewReservationService(
		app.trx,
		app.inboxRepo,
		app.seatRepo,
		app.reservationRepo,
		app.outboxRepo,
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
	metrics.StartOutboxCollector(ctx, a.db, "inventory", 5*time.Second)

	g, gctx := errgroup.WithContext(ctx)
	handler := consumer.Route(a.reservationService)

	g.Go(func() error { return a.commandsConsumer.Run(gctx, handler) })
	g.Go(func() error { return metrics.StartServer(gctx, a.config.MetricsPort) })

	slog.Info("inventory running", "app", a.config.Name)
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
