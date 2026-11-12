package observability_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sentinel-engine/relay/internal/domain"
	"github.com/sentinel-engine/relay/internal/infrastructure/observability"
)

func TestTenantRateLimiterAndPrometheusExposition(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 12, 15, 0, 0, 0, time.UTC)
	limiter := observability.NewTenantRateLimiter(10, 2)

	if !limiter.Allow("tenant_a", now) {
		t.Fatal("expected request 1 to be allowed")
	}
	if !limiter.Allow("tenant_a", now) {
		t.Fatal("expected request 2 (burst) to be allowed")
	}
	if limiter.Allow("tenant_a", now) {
		t.Fatal("expected request 3 at same instant to be rate-limited")
	}
	// Advance 100ms -> 1 token refilled at 10 RPS
	if !limiter.Allow("tenant_a", now.Add(100*time.Millisecond)) {
		t.Fatal("expected request after 100ms refill to be allowed")
	}

	metrics := observability.NewMetricsCollector()
	metrics.IncEnqueued()
	metrics.RecordDispatchAttempt("tenant_a", "ep_1", domain.StatusSucceeded, 200, 4*time.Millisecond)
	metrics.RecordCircuitTransition("ep_1", domain.CircuitOpen)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metrics.ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, "sentinel_deliveries_enqueued_total 1") {
		t.Fatalf("missing enqueued counter in metrics output:\n%s", body)
	}
	if !strings.Contains(body, "sentinel_deliveries_succeeded_total 1") {
		t.Fatalf("missing succeeded counter in metrics output:\n%s", body)
	}
	if !strings.Contains(body, `sentinel_circuit_breaker_open{endpoint_id="ep_1"} 1`) {
		t.Fatalf("missing circuit breaker gauge in metrics output:\n%s", body)
	}
}
