package domain

import "time"

// DomainEvent represents a pure domain fact emitted by an Aggregate Root.
type DomainEvent interface {
	EventName() string
	OccurredAtTime() time.Time
}

type DeliveryEnqueuedEvent struct {
	DeliveryID DeliveryID
	TenantID   TenantID
	EndpointID EndpointID
	EventType  string
	OccurredAt time.Time
}

func (e DeliveryEnqueuedEvent) EventName() string         { return "delivery.enqueued" }
func (e DeliveryEnqueuedEvent) OccurredAtTime() time.Time { return e.OccurredAt }

type DeliverySucceededEvent struct {
	DeliveryID DeliveryID
	TenantID   TenantID
	EndpointID EndpointID
	Attempts   int
	OccurredAt time.Time
}

func (e DeliverySucceededEvent) EventName() string         { return "delivery.succeeded" }
func (e DeliverySucceededEvent) OccurredAtTime() time.Time { return e.OccurredAt }

type DeliveryRetryScheduledEvent struct {
	DeliveryID    DeliveryID
	TenantID      TenantID
	EndpointID    EndpointID
	Attempt       int
	NextAttemptAt time.Time
	OccurredAt    time.Time
}

func (e DeliveryRetryScheduledEvent) EventName() string         { return "delivery.retry_scheduled" }
func (e DeliveryRetryScheduledEvent) OccurredAtTime() time.Time { return e.OccurredAt }

type DeliveryDeadLetteredEvent struct {
	DeliveryID DeliveryID
	TenantID   TenantID
	EndpointID EndpointID
	Attempts   int
	Reason     string
	OccurredAt time.Time
}

func (e DeliveryDeadLetteredEvent) EventName() string         { return "delivery.dead_lettered" }
func (e DeliveryDeadLetteredEvent) OccurredAtTime() time.Time { return e.OccurredAt }
