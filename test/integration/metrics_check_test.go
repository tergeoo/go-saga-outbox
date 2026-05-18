//go:build integration

package integration_test

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestSagaMetricsAfterHappyPath(t *testing.T) {
	c := sharedCluster
	if c == nil {
		t.Skip("shared cluster not initialised")
	}

	sagaID := c.postBooking(t, 1000, "email")
	c.waitForSagaState(t, sagaID, "completed", 30*time.Second)

	time.Sleep(6 * time.Second)

	resp, err := http.Get(c.orchestratorURL("/metrics"))
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	s := string(body)

	if !strings.Contains(s, "saga_completed_total") {
		t.Errorf("saga_completed_total missing")
	}
	if !strings.Contains(s, `outbox_unpublished_count{service="orchestrator"}`) {
		t.Errorf("outbox_unpublished_count{orchestrator} missing")
	}
}
