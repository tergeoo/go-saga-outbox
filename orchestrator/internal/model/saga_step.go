package model

import (
	"time"

	"github.com/google/uuid"
)

type SagaStep struct {
	ID               uuid.UUID
	SagaID           uuid.UUID
	StepName         SagaStepName
	Direction        SagaStepDirection
	Status           SagaStepStatus
	CommandMessageID *uuid.UUID
	ReplyMessageID   *uuid.UUID
	Error            *string
	CreatedAt        time.Time
}

func NewSagaStep(id uuid.UUID, sagaID uuid.UUID, name SagaStepName, dir SagaStepDirection, now time.Time) SagaStep {
	return SagaStep{
		ID:               id,
		SagaID:           sagaID,
		StepName:         name,
		Direction:        dir,
		Status:           StepStatusPending,
		CommandMessageID: nil,
		ReplyMessageID:   nil,
		Error:            nil,
		CreatedAt:        now,
	}
}
