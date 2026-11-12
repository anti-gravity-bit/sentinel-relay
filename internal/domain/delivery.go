package domain

import (
	"strings"
	"time"
)

type DeliveryStatus string

const (
	StatusPending      DeliveryStatus = "PENDING"
	StatusInFlight     DeliveryStatus = "IN_FLIGHT"
	StatusRetrying     DeliveryStatus = "RETRYING"
	StatusSucceeded    DeliveryStatus = "SUCCEEDED"
	StatusDeadLettered DeliveryStatus = "DEAD_LETTERED"
)

// Delivery is the Aggregate Root governing a single webhook dispatch lifecycle.
type Delivery struct {
	ID             DeliveryID
	TenantID       TenantID
	EndpointID     EndpointID
	IdempotencyKey IdempotencyKey
	EventType      string
	Payload        []byte
	Status         DeliveryStatus
	AttemptCount   int
	MaxAttempts    int
	NextAttemptAt  time.Time
	LastStatusCode int
	LastError      string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	events         []DomainEvent
}

func NewDelivery(
	id DeliveryID,
	tenantID TenantID,
	endpointID EndpointID,
	idempotencyKey IdempotencyKey,
	eventType string,
	payload []byte,
	maxAttempts int,
	now time.Time,
) (*Delivery, error) {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return nil, ErrInvalidEventType
	}
	if len(payload) == 0 || len(payload) > MaxPayloadBytes {
		return nil, ErrEmptyPayload
	}
	if maxAttempts <= 0 {
		maxAttempts = 5
	}
	 payloadCopy := make([]byte, len(payload))
	copy(payloadCopy, payload)

	ts := now.UTC()
	d := &Delivery{
		ID:             id,
		TenantID:       tenantID,
		EndpointID:     endpointID,
		IdempotencyKey: idempotencyKey,
		EventType:      eventType,
		Payload:        payloadCopy,
		Status:         StatusPending,
		AttemptCount:   0,
		MaxAttempts:    maxAttempts,
		NextAttemptAt:  ts,
		CreatedAt:      ts,
		UpdatedAt:      ts,
	}
	d.recordEvent(DeliveryEnqueuedEvent{
		DeliveryID: id,
		TenantID:   tenantID,
		EndpointID: endpointID,
		EventType:  eventType,
		OccurredAt: ts,
	})
	return d, nil
}

// MarkInFlight transitions a PENDING or RETRYING delivery to IN_FLIGHT when claimed by a worker.
func (d *Delivery) MarkInFlight(now time.Time) error {
	if d.Status != StatusPending && d.Status != StatusRetrying {
		return ErrInvalidStateTransition
	}
	d.Status = StatusInFlight
	d.UpdatedAt = now.UTC()
	return nil
}

// RecordSuccess transitions an IN_FLIGHT delivery to SUCCEEDED.
func (d *Delivery) RecordSuccess(statusCode int, now time.Time) error {
	if d.Status != StatusInFlight {
		return ErrInvalidStateTransition
	}
	ts := now.UTC()
	d.AttemptCount++
	d.Status = StatusSucceeded
	d.LastStatusCode = statusCode
	d.LastError = ""
	d.UpdatedAt = ts
	d.recordEvent(DeliverySucceededEvent{
		DeliveryID: d.ID,
		TenantID:   d.TenantID,
		EndpointID: d.EndpointID,
		Attempts:   d.AttemptCount,
		OccurredAt: ts,
	})
	return nil
}

// IsRetryableStatusCode returns true for network timeouts (0), 408 Request Timeout,
// 429 Too Many Requests, and all 5xx Server Errors. Other 4xx codes fail fast to DLQ.
func IsRetryableStatusCode(statusCode int) bool {
	if statusCode == 0 || statusCode == 408 || statusCode == 429 {
		return true
	}
	return statusCode >= 500 && statusCode <= 599
}

// RecordFailure evaluates retry eligibility and transitions the delivery to RETRYING or DEAD_LETTERED.
func (d *Delivery) RecordFailure(statusCode int, errMsg string, policy BackoffPolicy, now time.Time) error {
	if d.Status != StatusInFlight {
		return ErrInvalidStateTransition
	}
	ts := now.UTC()
	d.AttemptCount++
	d.LastStatusCode = statusCode
	d.LastError = errMsg
	d.UpdatedAt = ts

	if !IsRetryableStatusCode(statusCode) || d.AttemptCount >= d.MaxAttempts {
		d.Status = StatusDeadLettered
		d.recordEvent(DeliveryDeadLetteredEvent{
			DeliveryID: d.ID,
			TenantID:   d.TenantID,
			EndpointID: d.EndpointID,
			Attempts:   d.AttemptCount,
			Reason:     errMsg,
			OccurredAt: ts,
		})
		return nil
	}

	delay := policy.NextDelay(d.AttemptCount)
	d.Status = StatusRetrying
	d.NextAttemptAt = ts.Add(delay)
	d.recordEvent(DeliveryRetryScheduledEvent{
		DeliveryID:    d.ID,
		TenantID:      d.TenantID,
		EndpointID:    d.EndpointID,
		Attempt:       d.AttemptCount,
		NextAttemptAt: d.NextAttemptAt,
		OccurredAt:    ts,
	})
	return nil
}

// ReplayDeadLetter resets a DEAD_LETTERED delivery back to PENDING for manual operator replay.
func (d *Delivery) ReplayDeadLetter(now time.Time) error {
	if d.Status != StatusDeadLettered {
		return ErrInvalidStateTransition
	}
	ts := now.UTC()
	d.Status = StatusPending
	d.AttemptCount = 0
	d.LastError = ""
	d.LastStatusCode = 0
	d.NextAttemptAt = ts
	d.UpdatedAt = ts
	return nil
}

// PullEvents returns and clears accumulated domain events.
func (d *Delivery) PullEvents() []DomainEvent {
	out := d.events
	d.events = nil
	return out
}

func (d *Delivery) recordEvent(ev DomainEvent) {
	d.events = append(d.events, ev)
}

// Clone returns a deep copy of the Delivery aggregate to prevent shared-memory mutation in in-memory stores.
func (d *Delivery) Clone() *Delivery {
	cp := *d
	cp.Payload = make([]byte, len(d.Payload))
	copy(cp.Payload, d.Payload)
	cp.events = nil
	return &cp
}
