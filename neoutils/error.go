package neoutils

import (
	"fmt"
	"github.com/jmcvetta/neoism"
)

// ConstraintViolationError is a possible error the Service can return
type ConstraintViolationError struct {
	Msg string
	Err error
}

func (err *ConstraintViolationError) Error() string {
	return fmt.Sprintf("Neo4j ConstraintViolation %s", err.Msg)
}

// NewConstraintViolationError returns, as an error, a new NewConstraintViolationError
// with the given message and error details.
// As a convenience, if err is nil, NewSyscallError returns nil.
func NewConstraintViolationError(message string, err *neoism.NeoError) error {
	if err == nil {
		return nil
	}
	return &ConstraintViolationError{message, err}
}
