//go:build integration

package integration_test

import (
	"testing"
	"time"
)

func TestSagaPaymentFails(t *testing.T) {
	c := sharedCluster
	if c == nil {
		t.Skip("shared cluster not initialised")
	}

	sagaID := c.postBooking(t, -1000, "email")
	c.waitForSagaState(t, sagaID, "compensated", 30*time.Second)

	steps := c.sagaSteps(t, sagaID)
	assertStepsMatch(t, steps, []expectedStep{
		{Name: "ReserveSeat", Direction: "forward", Status: "succeeded"},
		{Name: "ChargePayment", Direction: "forward", Status: "failed", ExpectError: true},
		{Name: "ReserveSeat", Direction: "compensate", Status: "succeeded"},
	})

	if got := c.scalarFromDB(t, "payment",
		`SELECT status FROM payment WHERE saga_id = $1`, sagaID); got != "failed" {
		t.Errorf("payment.status: got=%s want=failed", got)
	}
	if got := c.scalarFromDB(t, "inventory",
		`SELECT status FROM reservation WHERE saga_id = $1`, sagaID); got != "released" {
		t.Errorf("reservation.status: got=%s want=released", got)
	}
	if got := c.scalarFromDB(t, "inventory",
		`SELECT status FROM seat
		 WHERE id = (SELECT seat_id FROM reservation WHERE saga_id = $1)`,
		sagaID); got != "free" {
		t.Errorf("seat.status: got=%s want=free", got)
	}
}

func TestSagaNotificationFails(t *testing.T) {
	c := sharedCluster
	if c == nil {
		t.Skip("shared cluster not initialised")
	}

	sagaID := c.postBooking(t, 1000, "broken")
	c.waitForSagaState(t, sagaID, "compensated", 45*time.Second)

	steps := c.sagaSteps(t, sagaID)
	assertStepsMatch(t, steps, []expectedStep{
		{Name: "ReserveSeat", Direction: "forward", Status: "succeeded"},
		{Name: "ChargePayment", Direction: "forward", Status: "succeeded"},
		{Name: "SendNotification", Direction: "forward", Status: "failed", ExpectError: true},
		{Name: "ChargePayment", Direction: "compensate", Status: "succeeded"},
		{Name: "ReserveSeat", Direction: "compensate", Status: "succeeded"},
	})

	if got := c.scalarFromDB(t, "payment",
		`SELECT status FROM payment WHERE saga_id = $1`, sagaID); got != "refunded" {
		t.Errorf("payment.status: got=%s want=refunded", got)
	}
	if got := c.scalarFromDB(t, "notification",
		`SELECT status FROM notification WHERE saga_id = $1`, sagaID); got != "failed" {
		t.Errorf("notification.status: got=%s want=failed", got)
	}
	if got := c.scalarFromDB(t, "inventory",
		`SELECT status FROM reservation WHERE saga_id = $1`, sagaID); got != "released" {
		t.Errorf("reservation.status: got=%s want=released", got)
	}
	if got := c.scalarFromDB(t, "inventory",
		`SELECT status FROM seat
		 WHERE id = (SELECT seat_id FROM reservation WHERE saga_id = $1)`,
		sagaID); got != "free" {
		t.Errorf("seat.status: got=%s want=free", got)
	}
}
