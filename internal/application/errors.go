package application

import (
	"errors"
	"fmt"

	"github.com/sentinel-engine/relay/internal/domain"
)

type ErrorCode string

const (
	CodeValidationError ErrorCode = "VALIDATION_ERROR"
	CodeNotFound        ErrorCode = "NOT_FOUND"
	CodeConflict        ErrorCode = "CONFLICT"
	CodePrecondition    ErrorCode = "PRECONDITION_FAILED"
	CodeInternal        ErrorCode = "INTERNAL_ERROR"
)

// AppError wraps domain or infrastructure errors with a machine-readable ErrorCode for transport handlers.
type AppError struct {
	Code    ErrorCode
	Message string
	Err     error
}

func (e *AppError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %s (%v)", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *AppError) Unwrap() error {
	return e.Err
}

// MapDomainError translates pure domain errors into structured application errors.
func MapDomainError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, domain.ErrInvalidTenantID),
		errors.Is(err, domain.ErrInvalidEndpointID),
		errors.Is(err, domain.ErrInvalidDeliveryID),
		errors.Is(err, domain.ErrInvalidIdempotencyKey),
		errors.Is(err, domain.ErrInvalidTargetURL),
		errors.Is(err, domain.ErrWeakSecretKey),
		errors.Is(err, domain.ErrEmptyPayload),
		errors.Is(err, domain.ErrInvalidEventType):
		return &AppError{Code: CodeValidationError, Message: err.Error(), Err: err}
	case errors.Is(err, domain.ErrDeliveryNotFound),
		errors.Is(err, domain.ErrEndpointNotFound):
		return &AppError{Code: CodeNotFound, Message: err.Error(), Err: err}
	case errors.Is(err, domain.ErrDuplicateDelivery):
		return &AppError{Code: CodeConflict, Message: err.Error(), Err: err}
	case errors.Is(err, domain.ErrEndpointDisabled),
		errors.Is(err, domain.ErrCircuitOpen),
		errors.Is(err, domain.ErrInvalidStateTransition):
		return &AppError{Code: CodePrecondition, Message: err.Error(), Err: err}
	default:
		return &AppError{Code: CodeInternal, Message: "internal operation failed", Err: err}
	}
}
