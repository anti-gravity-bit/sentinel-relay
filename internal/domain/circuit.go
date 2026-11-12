package domain

import "time"

type CircuitState string

const (
	CircuitClosed   CircuitState = "CLOSED"
	CircuitOpen     CircuitState = "OPEN"
	CircuitHalfOpen CircuitState = "HALF_OPEN"
)

// CircuitBreaker tracks consecutive failure counts for an Endpoint to prevent retry storms.
type CircuitBreaker struct {
	EndpointID          EndpointID
	State               CircuitState
	ConsecutiveFailures int
	FailureThreshold    int
	CooldownPeriod      time.Duration
	OpenedAt            time.Time
}

func NewCircuitBreaker(endpointID EndpointID, failureThreshold int, cooldown time.Duration) *CircuitBreaker {
	if failureThreshold <= 0 {
		failureThreshold = 5
	}
	if cooldown <= 0 {
		cooldown = 30 * time.Second
	}
	return &CircuitBreaker{
		EndpointID:       endpointID,
		State:            CircuitClosed,
		FailureThreshold: failureThreshold,
		CooldownPeriod:   cooldown,
	}
}

// Allow checks whether a delivery attempt is permitted at time `now`.
func (cb *CircuitBreaker) Allow(now time.Time) bool {
	switch cb.State {
	case CircuitClosed, CircuitHalfOpen:
		return true
	case CircuitOpen:
		if now.UTC().Sub(cb.OpenedAt) >= cb.CooldownPeriod {
			cb.State = CircuitHalfOpen
			return true
		}
		return false
	default:
		return true
	}
}

// OnSuccess resets the circuit breaker back to CLOSED.
func (cb *CircuitBreaker) OnSuccess() {
	cb.ConsecutiveFailures = 0
	cb.State = CircuitClosed
}

// OnFailure increments consecutive failures and trips the breaker to OPEN when threshold is reached.
func (cb *CircuitBreaker) OnFailure(now time.Time) {
	cb.ConsecutiveFailures++
	if cb.State == CircuitHalfOpen || cb.ConsecutiveFailures >= cb.FailureThreshold {
		cb.State = CircuitOpen
		cb.OpenedAt = now.UTC()
	}
}
