package domain

import "errors"

var (
	ErrInvalidTenantID       = errors.New("domain: tenant ID must be between 3 and 64 characters")
	ErrInvalidEndpointID     = errors.New("domain: endpoint ID must be between 3 and 64 characters")
	ErrInvalidDeliveryID     = errors.New("domain: delivery ID must be between 3 and 64 characters")
	ErrInvalidIdempotencyKey = errors.New("domain: idempotency key must be between 1 and 128 characters")
	ErrInvalidTargetURL      = errors.New("domain: target URL must be a valid http or https URL")
	ErrWeakSecretKey         = errors.New("domain: secret key must be at least 16 bytes")
	ErrEmptyPayload          = errors.New("domain: webhook payload cannot be empty or exceed 1 MiB")
	ErrInvalidEventType      = errors.New("domain: event type must be non-empty")
	ErrEndpointDisabled      = errors.New("domain: endpoint is disabled")
	ErrCircuitOpen           = errors.New("domain: endpoint circuit breaker is open")
	ErrDeliveryNotFound      = errors.New("domain: delivery not found")
	ErrEndpointNotFound      = errors.New("domain: endpoint not found")
	ErrDuplicateDelivery     = errors.New("domain: delivery with idempotency key already exists")
	ErrInvalidStateTransition = errors.New("domain: invalid delivery state transition")
)
