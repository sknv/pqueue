package pqueue

import "errors"

// unrecoverableError represents an error that should not be retried.
type unrecoverableError struct {
	error
}

func (e unrecoverableError) Error() string {
	if e.error == nil {
		return "unrecoverable error"
	}

	return e.error.Error()
}

func (e unrecoverableError) Unwrap() error {
	return e.error
}

// Unrecoverable wraps an error in unrecoverable error.
func Unrecoverable(err error) error {
	return unrecoverableError{error: err}
}

// IsUnrecoverable checks if error is an instance of unrecoverable error.
func IsUnrecoverable(err error) bool {
	_, ok := errors.AsType[unrecoverableError](err)

	return ok
}
