package domain

import (
	"context"
	"time"
)

// Clock abstracts time retrieval for deterministic testing of retry backoff and circuit breakers.
type Clock interface {
	Now() time.Time
}

// EndpointReader is a narrow read interface for Endpoint aggregates (ISP).
type EndpointReader interface {
	GetEndpoint(ctx context.Context, tenantID TenantID, endpointID EndpointID) (*Endpoint, error)
}

// EndpointWriter is a narrow write interface for Endpoint aggregates (ISP).
type EndpointWriter interface {
	UpsertEndpoint(ctx context.Context, endpoint *Endpoint) error
}

// EndpointRepository combines read and write operations for Endpoint storage adapters.
type EndpointRepository interface {
	EndpointReader
	EndpointWriter
}

// DeliveryCommandRepository handles state-mutating operations on Delivery aggregates (ISP).
type DeliveryCommandRepository interface {
	// Save persists a new Delivery or returns the existing Delivery if (TenantID, IdempotencyKey) already exists.
	// The boolean return value `created` is true when a new record was inserted, or false on idempotent replay.
	Save(ctx context.Context, delivery *Delivery) (existing *Delivery, created bool, err error)
	// ClaimDue atomically claims up to `limit` deliveries in PENDING or RETRYING state with NextAttemptAt <= now,
	// transitioning them to IN_FLIGHT so concurrent workers never double-dispatch.
	ClaimDue(ctx context.Context, now time.Time, limit int) ([]*Delivery, error)
	// Update persists state transitions (SUCCEEDED, RETRYING, DEAD_LETTERED, or PENDING on replay).
	Update(ctx context.Context, delivery *Delivery) error
}

// DeliveryQueryRepository handles read-only projections on Delivery aggregates (ISP).
type DeliveryQueryRepository interface {
	GetByID(ctx context.Context, tenantID TenantID, deliveryID DeliveryID) (*Delivery, error)
	ListDeadLetters(ctx context.Context, tenantID TenantID, limit int) ([]*Delivery, error)
}

// DeliveryRepository combines command and query operations for storage adapters.
type DeliveryRepository interface {
	DeliveryCommandRepository
	DeliveryQueryRepository
}

// DispatchResult captures the outcome of a single outbound HTTP webhook delivery attempt.
type DispatchResult struct {
	StatusCode int
	Duration   time.Duration
	Err        error
}

// WebhookDispatcher is the outbound port for executing signed HTTP POST requests.
type WebhookDispatcher interface {
	Dispatch(ctx context.Context, endpoint *Endpoint, delivery *Delivery, signature string, timestamp time.Time) DispatchResult
}
