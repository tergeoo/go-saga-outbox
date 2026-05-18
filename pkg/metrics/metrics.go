package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	SagaCompletedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "saga_completed_total",
		Help: "Number of sagas that reached the completed state.",
	})

	SagaCompensatedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "saga_compensated_total",
		Help: "Number of sagas that reached the compensated state.",
	})

	SagaFailedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "saga_failed_total",
		Help: "Number of sagas that ended in failed state.",
	})

	SagaStuckTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "saga_stuck_total",
		Help: "Number of sagas escalated by the scheduler after max attempts.",
	})

	SagaRetriesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "saga_retries_total",
		Help: "Number of command re-publishes performed by the scheduler.",
	}, []string{"step"})

	OutboxUnpublishedCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "outbox_unpublished_count",
		Help: "Current number of unpublished outbox messages.",
	}, []string{"service"})

	OutboxOldestAgeSeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "outbox_oldest_unpublished_age_seconds",
		Help: "Age in seconds of the oldest unpublished outbox message.",
	}, []string{"service"})

	DLQMessagesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dlq_messages_total",
		Help: "Number of messages routed to the dead-letter queue.",
	}, []string{"consumer"})

	InboxDuplicatesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "inbox_duplicates_total",
		Help: "Number of duplicate messages caught by the inbox dedup.",
	}, []string{"consumer"})

	SagaStateCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "saga_state_count",
		Help: "Current number of sagas grouped by state (DB snapshot).",
	}, []string{"state"})

	DLQUnreplayedCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dlq_unreplayed_count",
		Help: "Current number of dead messages awaiting replay.",
	}, []string{"consumer"})
)

func Handler() http.Handler {
	return promhttp.Handler()
}
