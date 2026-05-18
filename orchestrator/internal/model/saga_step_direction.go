package model

type SagaStepDirection string

const (
	DirectionForward    SagaStepDirection = "forward"
	DirectionCompensate SagaStepDirection = "compensate"
)
