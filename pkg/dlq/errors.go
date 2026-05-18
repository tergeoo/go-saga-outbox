package dlq

import "errors"

var (
	ErrInvalidPayload      = errors.New("invalid payload")
	ErrDeadMessageNotFound = errors.New("dlq: dead message not found")
)
