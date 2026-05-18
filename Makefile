SHELL := /bin/bash

COMPOSE      := docker compose -f deploy/docker-compose.yml
KAFKA_BROKER := localhost:19092

GOOSE        := goose
GOOSE_DRIVER := postgres

DSN_ORCH   := postgres://postgres:postgres@localhost:5439/orchestrator?sslmode=disable
DSN_INV    := postgres://postgres:postgres@localhost:5434/inventory?sslmode=disable
DSN_PAY    := postgres://postgres:postgres@localhost:5435/payment?sslmode=disable
DSN_NOTIF  := postgres://postgres:postgres@localhost:5436/notification?sslmode=disable

SERVICES := orchestrator inventory payment notification
TOPICS   := saga.inventory.commands saga.inventory.events \
            saga.payment.commands   saga.payment.events   \
            saga.notification.commands saga.notification.events \
            saga.dlq

SEED_EVENT_ID ?= 11111111-1111-1111-1111-111111111111

.PHONY: help up down restart wipe ps logs \
        topics topics-list \
        metrics metrics-reload \
        migrate migrate-down migrate-reset migrate-status migrate-create \
        psql-orchestrator psql-inventory psql-payment psql-notification \
        seed-inventory \
        run-inventory run-orchestrator run-payment run-notification \
        test test-integration tidy fmt vet

help:
	@echo "Targets:"
	@echo "  up                 — поднять весь стек (postgres x4 + redpanda + prometheus + grafana)"
	@echo "  down               — остановить (volume не трогает)"
	@echo "  wipe               — down + удалить volume-ы (полный сброс)"
	@echo "  restart            — down + up"
	@echo "  ps / logs          — статус и хвост логов"
	@echo "  topics             — создать все Kafka-топики"
	@echo "  topics-list        — посмотреть существующие топики"
	@echo "  migrate            — goose up по всем 4 БД"
	@echo "  migrate-down       — goose down (1 шаг назад) по всем 4 БД"
	@echo "  migrate-reset      — goose reset (откатить всё) по всем 4 БД"
	@echo "  migrate-status     — goose status по всем 4 БД"
	@echo "  migrate-create N=… SVC=…  — создать .sql миграцию goose"
	@echo "  psql-<service>     — открыть psql к нужной БД"
	@echo "  seed-inventory     — вставить одно free-seat под SEED_EVENT_ID"
	@echo "  run-inventory      — запустить inventory с inventory/.env.dist"
	@echo "  run-orchestrator   — запустить orchestrator с orchestrator/.env.dist"
	@echo "  metrics            — открыть Grafana dashboard в браузере"
	@echo "  metrics-reload     — перечитать grafana-dashboard.json и prometheus.yml"
	@echo "  test               — go test -race ./..."
	@echo "  test-integration   — testcontainers-based integration tests"
	@echo "  fmt / vet / tidy   — стандартные Go-проверки"

up:
	$(COMPOSE) up -d
	@echo
	@echo "Postgres orchestrator : localhost:5439  (db=orchestrator)"
	@echo "Postgres inventory    : localhost:5434  (db=inventory)"
	@echo "Postgres payment      : localhost:5435  (db=payment)"
	@echo "Postgres notification : localhost:5436  (db=notification)"
	@echo "Kafka (Redpanda)      : localhost:19092"
	@echo "Redpanda Console      : http://localhost:8080"
	@echo "Prometheus            : http://localhost:9090"
	@echo "Grafana               : http://localhost:3000  (anonymous Editor)"

down:
	$(COMPOSE) down

restart: down up

wipe:
	$(COMPOSE) down -v

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=100

topics:
	@for t in $(TOPICS); do \
		echo "Creating topic $$t"; \
		$(COMPOSE) exec -T redpanda rpk topic create $$t -p 3 -r 1 || true; \
	done

topics-list:
	$(COMPOSE) exec -T redpanda rpk topic list

# ---------- migrations (goose) ----------

# Каталоги миграций различаются: orchestrator/payment держат их на верхнем уровне,
# inventory/notification — внутри internal/. Маппинг для goose CLI:
MIG_DIR_orchestrator := orchestrator/migrations
MIG_DIR_payment      := payment/migrations
MIG_DIR_inventory    := inventory/internal/migrations
MIG_DIR_notification := notification/internal/migrations

DSN_orchestrator := $(DSN_ORCH)
DSN_payment      := $(DSN_PAY)
DSN_inventory    := $(DSN_INV)
DSN_notification := $(DSN_NOTIF)

define goose_run_one
	@echo "==> [$(1)] goose $(2)"
	@GOOSE_DRIVER=$(GOOSE_DRIVER) GOOSE_DBSTRING="$(DSN_$(1))" \
		$(GOOSE) -dir $(MIG_DIR_$(1)) -table migrations_$(1) $(2)
endef

migrate:
	$(call goose_run_one,orchestrator,up)
	$(call goose_run_one,payment,up)
	$(call goose_run_one,inventory,up)
	$(call goose_run_one,notification,up)

migrate-down:
	$(call goose_run_one,orchestrator,down)
	$(call goose_run_one,payment,down)
	$(call goose_run_one,inventory,down)
	$(call goose_run_one,notification,down)

migrate-reset:
	$(call goose_run_one,orchestrator,reset)
	$(call goose_run_one,payment,reset)
	$(call goose_run_one,inventory,reset)
	$(call goose_run_one,notification,reset)

migrate-status:
	$(call goose_run_one,orchestrator,status)
	$(call goose_run_one,payment,status)
	$(call goose_run_one,inventory,status)
	$(call goose_run_one,notification,status)

# Создать новую goose-миграцию. Пример:
#   make migrate-create SVC=orchestrator N=add_index
migrate-create:
	@test -n "$(SVC)" || (echo "SVC required (orchestrator|payment|inventory|notification)" && exit 1)
	@test -n "$(N)"   || (echo "N (migration name) required" && exit 1)
	GOOSE_DRIVER=$(GOOSE_DRIVER) $(GOOSE) -dir $(MIG_DIR_$(SVC)) -table migrations_$(SVC) create $(N) sql

# ---------- psql shortcuts ----------

psql-orchestrator:
	$(COMPOSE) exec -it postgres-orchestrator psql -U postgres -d orchestrator

psql-inventory:
	$(COMPOSE) exec -it postgres-inventory psql -U postgres -d inventory

psql-payment:
	$(COMPOSE) exec -it postgres-payment psql -U postgres -d payment

psql-notification:
	$(COMPOSE) exec -it postgres-notification psql -U postgres -d notification

# ---------- metrics / dashboards ----------

# Открыть готовый dashboard. Provisioning файлов сделает Prometheus default-источником
# и подгрузит deploy/grafana-dashboard.json при первом старте Grafana.
metrics:
	@open http://localhost:3000/d/go-saga-outbox 2>/dev/null || \
		xdg-open http://localhost:3000/d/go-saga-outbox 2>/dev/null || \
		echo "Open http://localhost:3000/d/go-saga-outbox manually"

# Применить изменения в grafana-dashboard.json или prometheus.yml без полного wipe.
# Prometheus читает конфиг при рестарте; Grafana provisioning переcканирует папки по таймеру (10s).
metrics-reload:
	$(COMPOSE) restart prometheus grafana
	@echo "prometheus + grafana restarted; dashboard will be re-scanned in <10s"

test:
	go test -race -count=1 ./...

test-integration:
	go test -race -count=1 -tags=integration ./test/integration/...

tidy:
	go mod tidy

fmt:
	gofmt -w .
	@command -v goimports >/dev/null && goimports -w . || true

vet:
	go vet ./...

seed-inventory:
	@echo "Seeding seat for event_id=$(SEED_EVENT_ID)"
	$(COMPOSE) exec -T postgres-inventory psql -U postgres -d inventory -c \
		"INSERT INTO seat (id, event_id, status) VALUES (gen_random_uuid(), '$(SEED_EVENT_ID)', 'free'),(gen_random_uuid(), '$(SEED_EVENT_ID)', 'free'),(gen_random_uuid(), '$(SEED_EVENT_ID)', 'free'),(gen_random_uuid(), '$(SEED_EVENT_ID)', 'free'),(gen_random_uuid(), '$(SEED_EVENT_ID)', 'free'),(gen_random_uuid(), '$(SEED_EVENT_ID)', 'free'),(gen_random_uuid(), '$(SEED_EVENT_ID)', 'free'),(gen_random_uuid(), '$(SEED_EVENT_ID)', 'free'),(gen_random_uuid(), '$(SEED_EVENT_ID)', 'free'),(gen_random_uuid(), '$(SEED_EVENT_ID)', 'free');"

run-inventory:
	set -a && source inventory/.env && set +a && go run ./inventory/cmd

run-orchestrator:
	set -a && source orchestrator/.env && set +a && go run ./orchestrator/cmd

run-payment:
	set -a && source payment/.env && set +a && go run ./payment/cmd

run-notification:
	set -a && source notification/.env && set +a && go run ./notification/cmd

