package observability

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sentinel-engine/relay/internal/domain"
)

// MetricsCollector implements application.TelemetryObserver and exposes Prometheus text-format metrics.
type MetricsCollector struct {
	enqueuedTotal     atomic.Uint64
	succeededTotal    atomic.Uint64
	retriedTotal      atomic.Uint64
	deadLetteredTotal atomic.Uint64
	rateLimitedTotal  atomic.Uint64
	dispatchCount     atomic.Uint64
	dispatchNanosSum  atomic.Uint64

	mu            sync.RWMutex
	circuitStates map[domain.EndpointID]domain.CircuitState
}

func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		circuitStates: make(map[domain.EndpointID]domain.CircuitState),
	}
}

func (m *MetricsCollector) IncEnqueued() {
	m.enqueuedTotal.Add(1)
}

func (m *MetricsCollector) IncRateLimited() {
	m.rateLimitedTotal.Add(1)
}

func (m *MetricsCollector) RecordDispatchAttempt(
	_ domain.TenantID,
	_ domain.EndpointID,
	status domain.DeliveryStatus,
	_ int,
	duration time.Duration,
) {
	m.dispatchCount.Add(1)
	m.dispatchNanosSum.Add(uint64(duration.Nanoseconds()))

	switch status {
	case domain.StatusSucceeded:
		m.succeededTotal.Add(1)
	case domain.StatusRetrying:
		m.retriedTotal.Add(1)
	case domain.StatusDeadLettered:
		m.deadLetteredTotal.Add(1)
	}
}

func (m *MetricsCollector) RecordCircuitTransition(endpointID domain.EndpointID, state domain.CircuitState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.circuitStates[endpointID] = state
}

// ServeHTTP renders metrics in standard Prometheus 0.0.4 plain-text exposition format.
func (m *MetricsCollector) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	var sb strings.Builder
	fmt.Fprintf(&sb, "# HELP sentinel_deliveries_enqueued_total Total webhooks accepted for delivery.\n")
	fmt.Fprintf(&sb, "# TYPE sentinel_deliveries_enqueued_total counter\n")
	fmt.Fprintf(&sb, "sentinel_deliveries_enqueued_total %d\n", m.enqueuedTotal.Load())

	fmt.Fprintf(&sb, "# HELP sentinel_deliveries_succeeded_total Total webhook deliveries succeeded.\n")
	fmt.Fprintf(&sb, "# TYPE sentinel_deliveries_succeeded_total counter\n")
	fmt.Fprintf(&sb, "sentinel_deliveries_succeeded_total %d\n", m.succeededTotal.Load())

	fmt.Fprintf(&sb, "# HELP sentinel_deliveries_retried_total Total webhook deliveries scheduled for retry.\n")
	fmt.Fprintf(&sb, "# TYPE sentinel_deliveries_retried_total counter\n")
	fmt.Fprintf(&sb, "sentinel_deliveries_retried_total %d\n", m.retriedTotal.Load())

	fmt.Fprintf(&sb, "# HELP sentinel_deliveries_dead_lettered_total Total webhook deliveries moved to DLQ.\n")
	fmt.Fprintf(&sb, "# TYPE sentinel_deliveries_dead_lettered_total counter\n")
	fmt.Fprintf(&sb, "sentinel_deliveries_dead_lettered_total %d\n", m.deadLetteredTotal.Load())

	fmt.Fprintf(&sb, "# HELP sentinel_http_rate_limited_total Total inbound API requests rejected by tenant rate limiter.\n")
	fmt.Fprintf(&sb, "# TYPE sentinel_http_rate_limited_total counter\n")
	fmt.Fprintf(&sb, "sentinel_http_rate_limited_total %d\n", m.rateLimitedTotal.Load())

	fmt.Fprintf(&sb, "# HELP sentinel_dispatch_duration_nanoseconds_sum Total nanoseconds spent in outbound webhook HTTP calls.\n")
	fmt.Fprintf(&sb, "# TYPE sentinel_dispatch_duration_nanoseconds_sum counter\n")
	fmt.Fprintf(&sb, "sentinel_dispatch_duration_nanoseconds_sum %d\n", m.dispatchNanosSum.Load())
	fmt.Fprintf(&sb, "sentinel_dispatch_duration_nanoseconds_count %d\n", m.dispatchCount.Load())

	m.mu.RLock()
	eps := make([]string, 0, len(m.circuitStates))
	for ep := range m.circuitStates {
		eps = append(eps, string(ep))
	}
	sort.Strings(eps)
	for _, ep := range eps {
		st := m.circuitStates[domain.EndpointID(ep)]
		val := 0
		if st == domain.CircuitOpen {
			val = 1
		}
		fmt.Fprintf(&sb, "sentinel_circuit_breaker_open{endpoint_id=%q} %d\n", ep, val)
	}
	m.mu.RUnlock()

	_, _ = w.Write([]byte(sb.String()))
}
