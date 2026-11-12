package domain_test

import (
	"testing"
	"time"

	"github.com/sentinel-engine/relay/internal/domain"
)

func TestValueObjectsAndSignature(t *testing.T) {
	t.Parallel()

	if _, err := domain.NewTenantID("ab"); err != domain.ErrInvalidTenantID {
		t.Fatalf("expected ErrInvalidTenantID, got %v", err)
	}
	tenantID, err := domain.NewTenantID("tenant_acme_01")
	if err != nil || tenantID != "tenant_acme_01" {
		t.Fatalf("unexpected tenantID result: %v, %v", tenantID, err)
	}

	if _, err := domain.NewSecretKey("short-secret"); err != domain.ErrWeakSecretKey {
		t.Fatalf("expected ErrWeakSecretKey, got %v", err)
	}
	secret, err := domain.NewSecretKey("whsec_0123456789abcdef0123456789")
	if err != nil {
		t.Fatalf("unexpected secret error: %v", err)
	}

	now := time.Unix(1700000000, 0).UTC()
	payload := []byte(`{"event":"invoice.paid","amount":4900}`)
	sig := secret.SignPayload(now, payload)
	if !secret.VerifySignature(now, payload, sig) {
		t.Fatalf("expected signature verification to succeed for %s", sig)
	}
	if secret.VerifySignature(now.Add(time.Second), payload, sig) {
		t.Fatalf("expected signature verification to fail on tampered timestamp")
	}
}

func TestDeliveryStateMachine(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	policy := domain.DefaultBackoffPolicy()

	tests := []struct {
		name           string
		maxAttempts    int
		statusCodes    []int
		wantStatus     domain.DeliveryStatus
		wantAttempts   int
		wantNextOffset time.Duration
	}{
		{
			name:         "immediate 200 OK transitions to SUCCEEDED",
			maxAttempts:  3,
			statusCodes:  []int{200},
			wantStatus:   domain.StatusSucceeded,
			wantAttempts: 1,
		},
		{
			name:           "transient 503 schedules retry with exponential backoff",
			maxAttempts:    3,
			statusCodes:    []int{503},
			wantStatus:     domain.StatusRetrying,
			wantAttempts:   1,
			wantNextOffset: 1 * time.Second,
		},
		{
			name:         "non-retryable 400 Bad Request transitions immediately to DEAD_LETTERED",
			maxAttempts:  5,
			statusCodes:  []int{400},
			wantStatus:   domain.StatusDeadLettered,
			wantAttempts: 1,
		},
		{
			name:         "429 Too Many Requests is treated as retryable until maxAttempts exhausted",
			maxAttempts:  2,
			statusCodes:  []int{429, 429},
			wantStatus:   domain.StatusDeadLettered,
			wantAttempts: 2,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d, err := domain.NewDelivery(
				"dlv_001",
				"tenant_01",
				"ep_01",
				"idem_01",
				"invoice.paid",
				[]byte(`{"ok":true}`),
				tc.maxAttempts,
				now,
			)
			if err != nil {
				t.Fatalf("NewDelivery failed: %v", err)
			}

			currTime := now
			for _, code := range tc.statusCodes {
				if err := d.MarkInFlight(currTime); err != nil {
					t.Fatalf("MarkInFlight failed: %v", err)
				}
				if code >= 200 && code < 300 {
					if err := d.RecordSuccess(code, currTime); err != nil {
						t.Fatalf("RecordSuccess failed: %v", err)
					}
				} else {
					if err := d.RecordFailure(code, "downstream error", policy, currTime); err != nil {
						t.Fatalf("RecordFailure failed: %v", err)
					}
				}
			}

			if d.Status != tc.wantStatus {
				t.Fatalf("status = %s, want %s", d.Status, tc.wantStatus)
			}
			if d.AttemptCount != tc.wantAttempts {
				t.Fatalf("attempts = %d, want %d", d.AttemptCount, tc.wantAttempts)
			}
			if tc.wantNextOffset > 0 {
				wantNext := currTime.Add(tc.wantNextOffset)
				if !d.NextAttemptAt.Equal(wantNext) {
					t.Fatalf("NextAttemptAt = %v, want %v", d.NextAttemptAt, wantNext)
				}
			}
		})
	}
}

func TestCircuitBreakerTransitions(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	cb := domain.NewCircuitBreaker("ep_01", 3, 15*time.Second)

	if !cb.Allow(now) {
		t.Fatal("expected CLOSED circuit to allow traffic")
	}

	cb.OnFailure(now)
	cb.OnFailure(now)
	if cb.State != domain.CircuitClosed {
		t.Fatalf("expected CLOSED after 2 failures, got %s", cb.State)
	}

	cb.OnFailure(now)
	if cb.State != domain.CircuitOpen {
		t.Fatalf("expected OPEN after 3rd failure, got %s", cb.State)
	}
	if cb.Allow(now.Add(5 * time.Second)) {
		t.Fatal("expected OPEN circuit to reject traffic before cooldown")
	}

	// Cooldown elapsed -> transitions to HALF_OPEN
	if !cb.Allow(now.Add(16 * time.Second)) {
		t.Fatal("expected circuit to transition to HALF_OPEN and allow probe after cooldown")
	}
	if cb.State != domain.CircuitHalfOpen {
		t.Fatalf("expected HALF_OPEN state, got %s", cb.State)
	}

	// Success in HALF_OPEN -> resets to CLOSED
	cb.OnSuccess()
	if cb.State != domain.CircuitClosed || cb.ConsecutiveFailures != 0 {
		t.Fatalf("expected CLOSED with 0 failures, got state=%s failures=%d", cb.State, cb.ConsecutiveFailures)
	}
}
