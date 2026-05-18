//go:build integration

package integration_test

import (
	"testing"
	"time"
)

func TestSchedulerRetriesAfterPaymentRestart(t *testing.T) {
	c := sharedCluster
	if c == nil {
		t.Skip("shared cluster not initialised")
	}

	if err := c.stopService("payment"); err != nil {
		t.Fatalf("stop payment: %v", err)
	}

	t.Cleanup(func() {
		if !c.isServiceRunning("payment") {
			if err := c.startService("payment"); err != nil {
				t.Logf("cleanup: failed to restart payment: %v", err)
				return
			}
			_ = c.waitForServiceReady("payment", 20*time.Second)
			time.Sleep(2 * time.Second)
		}
	})

	sagaID := c.postBooking(t, 1000, "email")

	deadline := time.Now().Add(15 * time.Second)
	var attempts int
	for time.Now().Before(deadline) {
		if err := c.dbs["orchestrator"].GetContext(c.ctx, &attempts,
			`SELECT attempts FROM saga WHERE id = $1`, sagaID); err != nil {
			t.Fatalf("read attempts: %v", err)
		}
		if attempts >= 1 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if attempts < 1 {
		c.dumpLogs()
		t.Fatalf("scheduler did not increment attempts; saga %s still has attempts=0", sagaID)
	}

	st, err := c.getSagaState(sagaID)
	if err != nil {
		t.Fatalf("get saga state: %v", err)
	}
	if st.State != "running" {
		t.Fatalf("expected state=running, got %s/%s", st.State, st.CurrentStep)
	}

	if err := c.startService("payment"); err != nil {
		t.Fatalf("restart payment: %v", err)
	}
	if err := c.waitForServiceReady("payment", 20*time.Second); err != nil {
		t.Fatalf("payment not ready after restart: %v", err)
	}

	c.waitForSagaState(t, sagaID, "completed", 45*time.Second)

	if got := c.scalarFromDB(t, "payment",
		`SELECT status FROM payment WHERE saga_id = $1`, sagaID); got != "charged" {
		t.Errorf("payment.status: got=%s want=charged", got)
	}
	if got := c.scalarFromDB(t, "inventory",
		`SELECT status FROM reservation WHERE saga_id = $1`, sagaID); got != "reserved" {
		t.Errorf("reservation.status: got=%s want=reserved", got)
	}
	if got := c.scalarFromDB(t, "notification",
		`SELECT status FROM notification WHERE saga_id = $1`, sagaID); got != "sent" {
		t.Errorf("notification.status: got=%s want=sent", got)
	}
}
