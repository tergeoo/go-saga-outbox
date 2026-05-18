package dlq

import (
	"encoding/json"
	"errors"
)

type PermanentChecker func(error) bool

func IsBasePermanent(err error) bool {
	if err == nil {
		return false
	}
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	return errors.Is(err, ErrInvalidPayload) ||
		errors.As(err, &syntaxErr) ||
		errors.As(err, &typeErr)
}
