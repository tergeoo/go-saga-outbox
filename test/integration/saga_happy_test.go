//go:build integration

package integration_test

import (
	"testing"
	"time"
)

func TestSagaHappyPath(t *testing.T) {
	c := sharedCluster
	if c == nil {
		t.Skip("shared cluster not initialised")
	}

	sagaID := c.postBooking(t, 1000, "email")
	final := c.waitForSagaState(t, sagaID, "completed", 30*time.Second)
	if final.State != "completed" {
		t.Fatalf("expected state=completed, got=%s", final.State)
	}

	steps := c.sagaSteps(t, sagaID)
	assertStepsMatch(t, steps, []expectedStep{
		{Name: "ReserveSeat", Direction: "forward", Status: "succeeded"},
		{Name: "ChargePayment", Direction: "forward", Status: "succeeded"},
		{Name: "SendNotification", Direction: "forward", Status: "succeeded"},
	})

	paymentStatus := c.scalarFromDB(t, "payment",
		`SELECT status FROM payment WHERE saga_id = $1`, sagaID)
	if paymentStatus != "charged" {
		t.Errorf("payment.status: got=%s want=charged", paymentStatus)
	}

	reservationStatus := c.scalarFromDB(t, "inventory",
		`SELECT status FROM reservation WHERE saga_id = $1`, sagaID)
	if reservationStatus != "reserved" {
		t.Errorf("reservation.status: got=%s want=reserved", reservationStatus)
	}

	notificationStatus := c.scalarFromDB(t, "notification",
		`SELECT status FROM notification WHERE saga_id = $1`, sagaID)
	if notificationStatus != "sent" {
		t.Errorf("notification.status: got=%s want=sent", notificationStatus)
	}
}
