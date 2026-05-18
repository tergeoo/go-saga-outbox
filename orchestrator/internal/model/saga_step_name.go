package model

type SagaStepName string

const (
	SagaStepNameInitial          SagaStepName = "Initial"
	SagaStepNameReserveSeat      SagaStepName = "ReserveSeat"
	SagaStepNameChargePayment    SagaStepName = "ChargePayment"
	SagaStepNameSendNotification SagaStepName = "SendNotification"
)

func (s SagaStepName) IsZero() bool {
	return s == ""
}
