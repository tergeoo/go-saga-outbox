package model

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
)

const (
	MaxSagaAttempts       = 5
	InitialStepTimeoutSec = 5
	MaxStepTimeoutSec     = 60
)

type Saga struct {
	ID            uuid.UUID    `db:"id"`
	Type          string       `db:"type"`
	State         SagaState    `db:"state"`
	CurrentStep   SagaStepName `db:"current_step"`
	Payload       []byte       `db:"payload"`
	Context       []byte       `db:"context"`
	Attempts      int          `db:"attempts"`
	NextAttemptAt *time.Time   `db:"next_attempt_at"`
	CreatedAt     time.Time    `db:"created_at"`
	UpdatedAt     time.Time    `db:"updated_at"`
}

func NewSaga(id uuid.UUID, sagaType string, payload BookTicketPayload, now time.Time) (Saga, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return Saga{}, fmt.Errorf("error marshalling saga payload: %w", err)
	}
	nextAttempt := now.Add(StepTimeout(0))
	return Saga{
		ID:            id,
		Type:          sagaType,
		State:         SagaStateRunning,
		CurrentStep:   SagaStepNameInitial,
		Payload:       payloadBytes,
		Context:       nil,
		Attempts:      0,
		NextAttemptAt: &nextAttempt,
		CreatedAt:     now,
		UpdatedAt:     now,
	}, nil
}

func (s *Saga) SetContext(ctx BookTicketContext) error {
	bytes, err := json.Marshal(ctx)
	if err != nil {
		return err
	}

	s.Context = bytes
	return nil
}

func (s *Saga) DecodeContext() (BookTicketContext, error) {
	if len(s.Context) == 0 {
		return BookTicketContext{}, nil
	}

	var ctx BookTicketContext
	err := json.Unmarshal(s.Context, &ctx)
	return ctx, err
}

func (s *Saga) DecodePayload() (BookTicketPayload, error) {
	if len(s.Payload) == 0 {
		return BookTicketPayload{}, fmt.Errorf("saga payload is empty")
	}

	var p BookTicketPayload
	err := json.Unmarshal(s.Payload, &p)
	return p, err
}

func (s *Saga) IsCompensated() bool {
	return s.State == SagaStateCompensated
}

func (s *Saga) IsCompensating() bool {
	return s.State == SagaStateCompensating
}

func (s *Saga) ScheduleNext(now time.Time) {
	next := now.Add(StepTimeout(s.Attempts))
	s.NextAttemptAt = &next
	s.UpdatedAt = now
}

func (s *Saga) ResetSchedule(now time.Time) {
	s.Attempts = 0
	s.NextAttemptAt = nil
	s.UpdatedAt = now
}

func StepTimeout(attempts int) time.Duration {
	base := time.Duration(InitialStepTimeoutSec) * time.Second
	timeout := base << attempts
	c := time.Duration(MaxStepTimeoutSec) * time.Second
	if timeout > c {
		timeout = c
	}

	return timeout + time.Duration(rand.Intn(1000))*time.Millisecond
}
