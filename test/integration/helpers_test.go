//go:build integration

package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/google/uuid"
)

func (c *cluster) postBooking(t *testing.T, amount int64, channel string) uuid.UUID {
	t.Helper()

	body := map[string]any{
		"event_id": testEventID,
		"user_id":  testUserID,
		"amount":   amount,
		"channel":  channel,
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	req, err := http.NewRequest(http.MethodPost, c.orchestratorURL("/bookings"), bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /bookings: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected status %d: %s", resp.StatusCode, string(raw))
	}

	var br bookingResponse
	if err := json.NewDecoder(resp.Body).Decode(&br); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	return br.SagaID
}

func (c *cluster) getSagaState(sagaID uuid.UUID) (bookingState, error) {
	url := c.orchestratorURL(fmt.Sprintf("/bookings/%s", sagaID))
	resp, err := http.Get(url)
	if err != nil {
		return bookingState{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return bookingState{}, fmt.Errorf("GET %s: status=%d body=%s", url, resp.StatusCode, string(raw))
	}
	var bs bookingState
	if err := json.NewDecoder(resp.Body).Decode(&bs); err != nil {
		return bookingState{}, err
	}
	return bs, nil
}

type expectedStep struct {
	Name        string
	Direction   string
	Status      string
	ExpectError bool
}

func assertStepsMatch(t *testing.T, got []sagaStep, want []expectedStep) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("saga_step count: got=%d want=%d (got=%+v)", len(got), len(want), got)
	}
	for i, w := range want {
		g := got[i]
		if g.StepName != w.Name || g.Direction != w.Direction || g.Status != w.Status {
			t.Fatalf("saga_step[%d]: got=(%s/%s/%s) want=(%s/%s/%s)",
				i, g.StepName, g.Direction, g.Status,
				w.Name, w.Direction, w.Status)
		}
		if w.ExpectError && (!g.Error.Valid || g.Error.String == "") {
			t.Fatalf("saga_step[%d]: expected non-empty error, got NULL/empty", i)
		}
	}
}

// scalarFromDB runs a query that returns a single scalar.
func (c *cluster) scalarFromDB(t *testing.T, db string, query string, args ...any) string {
	t.Helper()
	var s string
	if err := c.dbs[db].GetContext(c.ctx, &s, query, args...); err != nil {
		t.Fatalf("scalar query on %s: %v", db, err)
	}
	return s
}
