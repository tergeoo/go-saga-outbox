package model

type SagaStepStatus string

const (
	StepStatusPending   SagaStepStatus = "pending"
	StepStatusSent      SagaStepStatus = "sent"
	StepStatusSucceeded SagaStepStatus = "succeeded"
	StepStatusFailed    SagaStepStatus = "failed"
)
