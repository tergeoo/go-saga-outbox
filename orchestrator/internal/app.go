package internal

import (
	"context"
	"errors"
	"fmt"
	"go-saga-outbox/orchestrator/internal/config"
	"go-saga-outbox/orchestrator/internal/consumer"
	"go-saga-outbox/orchestrator/internal/handler"
	"go-saga-outbox/orchestrator/internal/repo"
	"go-saga-outbox/orchestrator/internal/service"
	"go-saga-outbox/orchestrator/migrations"
	"go-saga-outbox/pkg/db"
	"go-saga-outbox/pkg/dlq"
	"go-saga-outbox/pkg/inbox"
	"go-saga-outbox/pkg/kafka"
	"go-saga-outbox/pkg/metrics"
	"go-saga-outbox/pkg/outbox"
	"go-saga-outbox/pkg/trx"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v4"
	echoMdw "github.com/labstack/echo/v4/middleware"
	"golang.org/x/sync/errgroup"
)

type App struct {
	config config.Config

	router     *echo.Echo
	httpServer *http.Server

	db    *sqlx.DB
	trx   *trx.Transaction
	nowFn func() time.Time

	outboxProducer       *outbox.Producer
	inventoryConsumer    *kafka.Consumer
	paymentConsumer      *kafka.Consumer
	notificationConsumer *kafka.Consumer
	outboxRelay          *outbox.Relay

	inboxRepo    *inbox.Repo
	outboxRepo   *outbox.Repo
	sagaRepo     *repo.SagaRepo
	sagaStepRepo *repo.SagaStepRepo
	dlqRepo      *dlq.Repo

	sagaService *service.SagaService

	bookingHandler *handler.BookingHandler
	dlqHandler     *handler.DLQHandler
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
	app.dlqRepo = dlq.NewRepo(app.trx)
	app.sagaRepo = repo.NewSagaRepo(app.trx)
	app.sagaStepRepo = repo.NewSagaStepRepo(app.trx)

	app.sagaService = service.NewSagaService(
		app.trx,
		app.inboxRepo,
		app.sagaRepo,
		app.sagaStepRepo,
		app.outboxRepo,
		app.dlqRepo,
		app.nowFn,
	)

	app.inventoryConsumer = kafka.NewConsumer(cfg.InventoryConsumer)
	app.paymentConsumer = kafka.NewConsumer(cfg.PaymentConsumer)
	app.notificationConsumer = kafka.NewConsumer(cfg.NotificationConsumer)

	app.outboxProducer = outbox.NewOutboxProducer(cfg.OutboxProducer)
	app.outboxRelay = outbox.NewRelay(cfg.OutboxRelay, app.trx, app.outboxRepo, app.outboxProducer, app.nowFn)

	app.bookingHandler = handler.NewBookingHandler(app.sagaService)
	app.dlqHandler = handler.NewDLQHandler(app.sagaService)

	router := echo.New()
	router.HideBanner = true
	router.HidePort = true
	router.Use(echoMdw.Recover())
	router.Use(echoMdw.RequestID())
	router.Use(echoMdw.Logger())
	app.bookingHandler.RegisterRoutes(router)
	app.dlqHandler.RegisterRoutes(router)
	router.GET("/metrics", echo.WrapHandler(metrics.Handler()))
	app.router = router

	app.httpServer = &http.Server{
		Addr:           fmt.Sprintf(":%d", cfg.Server.Port),
		Handler:        router,
		ReadTimeout:    cfg.Server.Timeout,
		WriteTimeout:   cfg.Server.Timeout,
		IdleTimeout:    cfg.Server.Timeout,
		MaxHeaderBytes: http.DefaultMaxHeaderBytes,
	}

	return app, nil
}

func (a *App) Run(ctx context.Context) error {
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	a.outboxRelay.Start(ctx)
	metrics.StartOutboxCollector(ctx, a.db, "orchestrator", 5*time.Second)
	metrics.StartSagaCollector(ctx, a.db, 5*time.Second)

	g, gctx := errgroup.WithContext(ctx)
	h := dlq.Wrap("orchestrator", a.dlqRepo, a.trx, a.nowFn, service.IsPermanent, consumer.Route(a.sagaService))

	g.Go(func() error { return a.inventoryConsumer.Run(gctx, h) })
	g.Go(func() error { return a.paymentConsumer.Run(gctx, h) })
	g.Go(func() error { return a.notificationConsumer.Run(gctx, h) })
	g.Go(func() error { return a.runScheduler(gctx) })

	g.Go(func() error {
		slog.Info("http server listening", "port", a.config.Server.Port, "app", a.config.Name)
		if err := a.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})

	g.Go(func() error {
		<-gctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return a.httpServer.Shutdown(shutdownCtx)
	})

	slog.Info("orchestrator running")
	return g.Wait()
}

func (a *App) runScheduler(ctx context.Context) error {
	ticker := time.NewTicker(a.config.RepublisherInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := a.sagaService.ProcessExpired(ctx); err != nil {
				slog.Error("scheduler: process expired", "err", err)
			}
		}
	}
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

	addErr(a.db.Close())
	addErr(a.inventoryConsumer.Close())
	addErr(a.paymentConsumer.Close())
	addErr(a.notificationConsumer.Close())
	addErr(a.outboxProducer.Shutdown(ctx))

	return shutdownErr
}
