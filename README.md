# go-saga-outbox

A reference implementation of the **Saga**, **Transactional Outbox** and **Inbox** patterns
on four Go microservices coordinated through Apache Kafka and PostgreSQL.

This is an educational project: every architectural decision is small, explicit, and explainable.
It is meant to be *read*, not just used.

---

## What it demonstrates

- **Saga orchestration** — a central `orchestrator` service drives a three-step booking flow.
- **Transactional outbox** — atomic *business write + event* in a single SQL transaction,
  later published to Kafka by a background relay.
- **Inbox idempotency** — at-least-once Kafka delivery is absorbed cleanly via a
  per-consumer `inbox` table with `(message_id) UNIQUE`.
- **Compensation in reverse order** — single-step rollback (payment fails) and
  multi-step rollback (notification fails → refund → release).
- **Dead Letter Queue** — poison messages are quarantined for manual replay via
  `POST /dead-messages/{id}/replay`.
- **Scheduler** — exponential-backoff retries of stuck commands, escalation to
  compensation after `MaxSagaAttempts`.
- **Observability** — Prometheus metrics + provisioned Grafana dashboard.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                              Kafka (Redpanda)                            │
│  saga.inventory.{commands,events}   saga.payment.{commands,events}       │
│  saga.notification.{commands,events}                                      │
└──┬────────────────┬─────────────────┬───────────────────┬────────────────┘
   ▼                ▼                 ▼                   ▼
 ┌──────────┐   ┌───────────┐   ┌───────────┐      ┌─────────────────┐
 │inventory │   │  payment  │   │notification│      │   orchestrator   │
 │   svc    │   │    svc    │   │    svc     │      │      svc         │
 │          │   │           │   │            │      │                  │
 │  Postgres│   │ Postgres  │   │ Postgres   │      │   Postgres       │
 │ ┌──────┐ │   │ ┌───────┐ │   │ ┌────────┐ │      │ ┌─────────────┐  │
 │ │ seat │ │   │ │payment│ │   │ │notif.  │ │      │ │ saga        │  │
 │ │reserv│ │   │ │       │ │   │ │        │ │      │ │ saga_step   │  │
 │ │outbox│ │   │ │outbox │ │   │ │outbox  │ │      │ │ outbox      │  │
 │ │inbox │ │   │ │inbox  │ │   │ │inbox   │ │      │ │ inbox       │  │
 │ │dlq   │ │   │ │dlq    │ │   │ │dlq     │ │      │ │ dead_message│  │
 │ └──────┘ │   │ └───────┘ │   │ └────────┘ │      │ └─────────────┘  │
 └──────────┘   └───────────┘   └────────────┘      └──────────────────┘
                                                            │
                                                            ▼
                                                       HTTP API
                                                  (POST /bookings,
                                                   GET  /bookings/:id,
                                                   POST /dead-messages/:id/replay)
```

Each service owns its database. Cross-service communication is **only** through Kafka.
The orchestrator owns the saga state machine and turns events into the next command.

---

## Happy path

```
HTTP POST /bookings
       │
       ▼
┌──────────────┐     reserve.command       ┌──────────────┐
│ orchestrator │──────────────────────────▶│   inventory  │
└──────┬───────┘                            └───────┬──────┘
       │            seat.reserved                   │
       │◀───────────────────────────────────────────┘
       │
       │            charge.command         ┌──────────────┐
       │──────────────────────────────────▶│   payment    │
       │                                   └───────┬──────┘
       │            payment.charged                │
       │◀───────────────────────────────────────────┘
       │
       │            send.command           ┌──────────────┐
       │──────────────────────────────────▶│ notification │
       │                                   └───────┬──────┘
       │            notification.sent              │
       │◀───────────────────────────────────────────┘
       │
       ▼
   state = completed
```

Three forward steps. Each step is a transactional unit:
`inbox.Insert + business write + outbox.Append` in a single SQL transaction.

---

## Compensation flows

### Single compensation — `amount < 0`

Payment fails on a negative amount; only the seat needs releasing.

```
                                  ┌──────────────┐
                                  │   payment    │
                                  └───────┬──────┘
            payment.failed                │
       ◀───────────────────────────────────┘
       │  state = compensating
       │
       │            release.command       ┌──────────────┐
       │──────────────────────────────────▶│  inventory   │
       │                                   └───────┬──────┘
       │            seat.released                  │
       │◀───────────────────────────────────────────┘
       │
       ▼
   state = compensated
```

### Double compensation — `channel == "broken"`

Notification fails; we must roll back both the charge **and** the seat,
in reverse order.

```
                                       ┌──────────────┐
                                       │ notification │
                                       └───────┬──────┘
            notification.failed                │
       ◀────────────────────────────────────────┘
       │  state = compensating
       │
       │            refund.command         ┌──────────────┐
       │──────────────────────────────────▶│   payment    │
       │                                   └───────┬──────┘
       │            payment.refunded              │
       │◀───────────────────────────────────────────┘
       │
       │            release.command       ┌──────────────┐
       │──────────────────────────────────▶│  inventory   │
       │                                   └───────┬──────┘
       │            seat.released                  │
       │◀───────────────────────────────────────────┘
       │
       ▼
   state = compensated
```

---

## Quick start

```bash
git clone https://github.com/<you>/go-saga-outbox
cd go-saga-outbox

# 1. Spin up infrastructure (4 Postgres + Redpanda + Console + Prometheus + Grafana)
make up

# 2. Apply migrations and create Kafka topics
make migrate
make topics

# 3. Seed one event with 10 free seats
make seed-inventory

# 4. Start each service in its own terminal
make run-inventory
make run-payment
make run-notification
make run-orchestrator

# 5. Trigger a saga
curl -X POST http://localhost:8086/bookings \
  -H "Content-Type: application/json" \
  -d '{
    "event_id":"11111111-1111-1111-1111-111111111111",
    "user_id":"22222222-2222-2222-2222-222222222222",
    "amount":1000,
    "channel":"email"
  }'

# Inspect (use the saga_id from the response)
curl http://localhost:8086/bookings/<saga_id>
```

Other UIs available out of the box:

- Redpanda Console — <http://localhost:8080> (topic / message inspection)
- Prometheus — <http://localhost:9090>
- Grafana — <http://localhost:3000> (anonymous Editor; `make metrics` opens the dashboard directly)

---

## Scenarios

| Scenario              | Request body                                        | Final state    |
|-----------------------|-----------------------------------------------------|----------------|
| Happy path            | `{"amount":1000,"channel":"email",...}`             | `completed`    |
| Payment fails         | `{"amount":-1000,"channel":"email",...}`            | `compensated`  |
| Notification fails    | `{"amount":1000,"channel":"broken",...}`            | `compensated`  |

Inspect `saga_step` after each run for the audit trail.

---

## Project structure

```
.
├── orchestrator/          saga coordinator + HTTP API
├── payment/               payment service
├── inventory/             seat reservation service
├── notification/          notification service
├── pkg/
│   ├── outbox/            transactional outbox (Repo, Producer, Relay)
│   ├── inbox/             inbox dedup helper
│   ├── dlq/               dead letter queue, Wrap, IsBasePermanent
│   ├── kafka/             kafka-go wrapper, manual commits, ErrPermanent
│   ├── trx/               transaction context, trx.Run / trx.FromContext
│   ├── messaging/         Kafka headers (message_id, saga_id, …)
│   ├── db/                sqlx datasource + goose migrations runner
│   ├── contracts/         payload + topic constants per domain
│   └── uuid/              UUIDv7 generator
├── deploy/
│   ├── docker-compose.yml
│   ├── prometheus.yml
│   ├── grafana-dashboard.json
│   └── grafana/provisioning/
├── test/integration/      testcontainers-based end-to-end tests
└── Makefile
```

---

## Key design decisions

1. **`inbox + business + outbox` in one transaction.** Splitting these into separate
   transactions lets a process crash between them, leaving the saga either in a
   "received but did nothing" state or "did the work but never told anyone".
   Phase 2 experiment E demonstrates this concretely.
2. **At-least-once delivery + idempotent inbox = effectively exactly-once.**
3. **`event_type` in Kafka headers, payload is typed JSON.** The router dispatches
   by header without touching the payload. Cheap, fast, no schema registry.
4. **`Key = saga_id`** for partitioning. Guarantees in-order delivery for a single
   saga across topics — but only within a single topic; cross-topic ordering
   is not guaranteed by Kafka.
5. **DLQ is hosted by the orchestrator only.** Worker services emit `WARN` logs
   and rely on Kafka redelivery for transient errors. The orchestrator is where
   saga state lives, so it is the natural place for poison-message inspection.
6. **No automatic DLQ replay worker.** DLQ is a *quarantine*, not a retry queue.
   Auto-replay creates pingpong loops for permanent errors. Replays are
   operator-triggered via `POST /dead-messages/:id/replay`.
7. **Compensation is keyed by saga state, not message contents.** Handlers
   check `saga.State == SagaStateCompensating` before applying compensating
   transitions. This protects against out-of-order delivery and replays.

---

## Tests

```bash
# Unit tests
make test

# Integration tests — requires Docker
make test-integration
```

Integration tests use [testcontainers-go](https://github.com/testcontainers/testcontainers-go)
to spin up four Postgres containers and one Redpanda broker, then run the four
services as subprocesses. Expect ~40 seconds end-to-end on a warm Go cache.

Five scenarios are covered today:

- `TestSagaHappyPath` — forward flow `Reserve → Charge → Notify → completed`
- `TestSagaPaymentFails` — single compensation `Charge fails → Release → compensated`
- `TestSagaNotificationFails` — double compensation `Notify fails → Refund → Release → compensated`
- `TestSchedulerRetriesAfterPaymentRestart` — chaos: stop payment subprocess, verify scheduler
  increments `attempts`, restart payment, verify saga reaches `completed` via re-published commands
- `TestSagaMetricsAfterHappyPath` — verifies `/metrics` exposes saga counters and outbox gauges

Set `SAGA_TEST_VERBOSE=1` to stream service logs to stderr while tests run.

---

## Metrics

Each service exposes Prometheus metrics on `/metrics`:

| Service       | Port |
|---------------|------|
| orchestrator  | 8086 |
| payment       | 9101 |
| inventory     | 9102 |
| notification  | 9103 |

Available metrics:

| Metric                                       | Type        | Labels       | Description                                              |
|----------------------------------------------|-------------|--------------|----------------------------------------------------------|
| `saga_completed_total`                       | counter     | —            | Sagas that reached the `completed` state                 |
| `saga_compensated_total`                     | counter     | —            | Sagas that completed compensation successfully           |
| `saga_failed_total`                          | counter     | —            | Sagas that ended in `failed`                             |
| `saga_stuck_total`                           | counter     | —            | Sagas escalated by the scheduler after max attempts      |
| `saga_retries_total`                         | counter vec | `step`       | Command re-publishes performed by the scheduler          |
| `outbox_unpublished_count`                   | gauge vec   | `service`    | Current size of the outbox backlog                       |
| `outbox_oldest_unpublished_age_seconds`      | gauge vec   | `service`    | Age of the oldest unpublished message                    |
| `dlq_messages_total`                         | counter vec | `consumer`   | Poison messages routed to DLQ                            |
| `inbox_duplicates_total`                     | counter vec | `consumer`   | Duplicate messages caught by inbox dedup                 |
| `saga_state_count`                           | gauge vec   | `state`      | Current saga count per state, sourced from DB            |
| `dlq_unreplayed_count`                       | gauge vec   | `consumer`   | Dead messages awaiting replay, sourced from DB           |

Counters reset on process restart — use `rate(...)` for trends.
The `saga_state_count` and `dlq_unreplayed_count` gauges are DB snapshots and
survive restarts, so they back the absolute-value panels on the Grafana dashboard.

Example PromQL:

```promql
# Currently in-flight sagas
sum(saga_state_count{state=~"running|compensating"})

# Forward-flow throughput
rate(saga_completed_total[5m])

# Outbox publish lag per service
outbox_oldest_unpublished_age_seconds
```

### Grafana

A provisioned dashboard at <http://localhost:3000/d/go-saga-outbox> ships with the stack
(`deploy/grafana-dashboard.json` is loaded automatically). Open it with `make metrics`,
reload after edits with `make metrics-reload`.

---

## License

[MIT](./LICENSE)
