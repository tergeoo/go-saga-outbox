package model

type SagaState string

const (
	SagaStateRunning      SagaState = "running"
	SagaStateCompleted    SagaState = "completed"
	SagaStateCompensating SagaState = "compensating"
	SagaStateCompensated  SagaState = "compensated"
	SagaStateFailed       SagaState = "failed"
)
