package model

type PaymentStatus string

const (
	PaymentStatusAuthorized PaymentStatus = "authorized"
	PaymentStatusCharged    PaymentStatus = "charged"
	PaymentStatusRefunded   PaymentStatus = "refunded"
	PaymentStatusFailed     PaymentStatus = "failed"
)
