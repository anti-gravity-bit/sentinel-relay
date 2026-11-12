package application

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/sentinel-engine/relay/internal/domain"
)

// TelemetryObserver receives hooks for metrics and tracing without coupling the engine to Prometheus or OTel SDKs.
type TelemetryObserver interface {
	RecordDispatchAttempt(tenantID domain.TenantID, endpointID domain.EndpointID, status domain.DeliveryStatus, statusCode int, duration time.Duration)
	RecordCircuitTransition(endpointID domain.EndpointID, state domain.CircuitState)
}

type noopObserver struct{}

func (noopObserver) RecordDispatchAttempt(domain.TenantID, domain.EndpointID, domain.DeliveryStatus, int, time.Duration) {
}
func (noopObserver) RecordCircuitTransition(domain.EndpointID, domain.CircuitState) {}

type WorkerPoolConfig struct {
	Workers          int
	QueueSize        int
	BatchSize        int
	PollInterval     time.Duration
	FailureThreshold int
	CircuitCooldown  time.Duration
	BackoffPolicy    domain.BackoffPolicy
}

func DefaultWorkerPoolConfig() WorkerPoolConfig {
	return WorkerPoolConfig{
		Workers:          8,
		QueueSize:        256,
		BatchSize:        64,
		PollInterval:     100 * time.Millisecond,
		FailureThreshold: 5,
		CircuitCooldown:  30 * time.Second,
		BackoffPolicy:    domain.DefaultBackoffPolicy(),
	}
}

// WorkerPool orchestrates bounded concurrent webhook dispatch with per-endpoint circuit breaking.
type WorkerPool struct {
	cfg        WorkerPoolConfig
	endpoints  domain.EndpointReader
	deliveries domain.DeliveryCommandRepository
	dispatcher domain.WebhookDispatcher
	clock      domain.Clock
	observer   TelemetryObserver
	logger     *slog.Logger

	jobs chan *domain.Delivery

	cbMu     sync.Mutex
	breakers map[domain.EndpointID]*domain.CircuitBreaker
}

func NewWorkerPool(
	cfg WorkerPoolConfig,
	endpoints domain.EndpointReader,
	deliveries domain.DeliveryCommandRepository,
	dispatcher domain.WebhookDispatcher,
	clock domain.Clock,
	observer TelemetryObserver,
	logger *slog.Logger,
) *WorkerPool {
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 128
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 32
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	if clock == nil {
		clock = SystemClock{}
	}
	if observer == nil {
		observer = noopObserver{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &WorkerPool{
		cfg:        cfg,
		endpoints:  endpoints,
		deliveries: deliveries,
		dispatcher: dispatcher,
		clock:      clock,
		observer:   observer,
		logger:     logger,
		jobs:       make(chan *domain.Delivery, cfg.QueueSize),
		breakers:   make(map[domain.EndpointID]*domain.CircuitBreaker),
	}
}

// Run starts the worker goroutines and the scheduler polling loop until ctx is canceled,
// then drains all in-flight jobs before returning.
func (wp *WorkerPool) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for i := 0; i < wp.cfg.Workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for d := range wp.jobs {
				// Use a detached short-lived context so an in-flight delivery completes cleanly during shutdown.
				dispatchCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				wp.processDelivery(dispatchCtx, d)
				cancel()
			}
		}(i)
	}

	ticker := time.NewTicker(wp.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			close(wp.jobs)
			wg.Wait()
			return nil
		case <-ticker.C:
			wp.PollOnce(ctx)
		}
	}
}

// PollOnce claims due deliveries and dispatches them through the worker channel (or synchronously in tests).
func (wp *WorkerPool) PollOnce(ctx context.Context) int {
	claimed, err := wp.deliveries.ClaimDue(ctx, wp.clock.Now(), wp.cfg.BatchSize)
	if err != nil {
		wp.logger.ErrorContext(ctx, "failed to claim due deliveries", slog.String("error", err.Error()))
		return 0
	}
	dispatched := 0
	for _, d := range claimed {
		select {
		case wp.jobs <- d:
			dispatched++
		case <-ctx.Done():
			return dispatched
		}
	}
	return dispatched
}

// ProcessBatchSynchronously claims due deliveries and processes them immediately on the calling goroutine.
// Useful for deterministic unit and benchmark tests without ticker sleeps.
func (wp *WorkerPool) ProcessBatchSynchronously(ctx context.Context) (int, error) {
	claimed, err := wp.deliveries.ClaimDue(ctx, wp.clock.Now(), wp.cfg.BatchSize)
	if err != nil {
		return 0, err
	}
	for _, d := range claimed {
		wp.processDelivery(ctx, d)
	}
	return len(claimed), nil
}

func (wp *WorkerPool) processDelivery(ctx context.Context, d *domain.Delivery) {
	now := wp.clock.Now()
	ep, err := wp.endpoints.GetEndpoint(ctx, d.TenantID, d.EndpointID)
	if err != nil || !ep.Active {
		_ = d.RecordFailure(410, "endpoint missing or disabled", wp.cfg.BackoffPolicy, now)
		_ = wp.deliveries.Update(ctx, d)
		wp.observer.RecordDispatchAttempt(d.TenantID, d.EndpointID, d.Status, 410, 0)
		return
	}

	if !wp.allowEndpoint(ep.ID, now) {
		// Circuit is OPEN: reschedule retry without making a network call
		_ = d.RecordFailure(503, domain.ErrCircuitOpen.Error(), wp.cfg.BackoffPolicy, now)
		_ = wp.deliveries.Update(ctx, d)
		wp.observer.RecordDispatchAttempt(d.TenantID, d.EndpointID, d.Status, 503, 0)
		return
	}

	signature := ep.Secret.SignPayload(now, d.Payload)
	res := wp.dispatcher.Dispatch(ctx, ep, d, signature, now)

	if res.Err == nil && res.StatusCode >= 200 && res.StatusCode < 300 {
		wp.recordCircuitSuccess(ep.ID)
		_ = d.RecordSuccess(res.StatusCode, wp.clock.Now())
	} else {
		wp.recordCircuitFailure(ep.ID, wp.clock.Now())
		errMsg := "http status error"
		if res.Err != nil {
			errMsg = res.Err.Error()
		}
		_ = d.RecordFailure(res.StatusCode, errMsg, wp.cfg.BackoffPolicy, wp.clock.Now())
	}

	if err := wp.deliveries.Update(ctx, d); err != nil {
		wp.logger.ErrorContext(ctx, "failed to update delivery state",
			slog.String("delivery_id", string(d.ID)),
			slog.String("error", err.Error()),
		)
	}
	wp.observer.RecordDispatchAttempt(d.TenantID, d.EndpointID, d.Status, res.StatusCode, res.Duration)
}

func (wp *WorkerPool) getBreakerLocked(endpointID domain.EndpointID) *domain.CircuitBreaker {
	cb, ok := wp.breakers[endpointID]
	if !ok {
		cb = domain.NewCircuitBreaker(endpointID, wp.cfg.FailureThreshold, wp.cfg.CircuitCooldown)
		wp.breakers[endpointID] = cb
	}
	return cb
}

func (wp *WorkerPool) allowEndpoint(endpointID domain.EndpointID, now time.Time) bool {
	wp.cbMu.Lock()
	defer wp.cbMu.Unlock()
	cb := wp.getBreakerLocked(endpointID)
	prev := cb.State
	allowed := cb.Allow(now)
	if cb.State != prev {
		wp.observer.RecordCircuitTransition(endpointID, cb.State)
	}
	return allowed
}

func (wp *WorkerPool) recordCircuitSuccess(endpointID domain.EndpointID) {
	wp.cbMu.Lock()
	defer wp.cbMu.Unlock()
	cb := wp.getBreakerLocked(endpointID)
	prev := cb.State
	cb.OnSuccess()
	if cb.State != prev {
		wp.observer.RecordCircuitTransition(endpointID, cb.State)
	}
}

func (wp *WorkerPool) recordCircuitFailure(endpointID domain.EndpointID, now time.Time) {
	wp.cbMu.Lock()
	defer wp.cbMu.Unlock()
	cb := wp.getBreakerLocked(endpointID)
	prev := cb.State
	cb.OnFailure(now)
	if cb.State != prev {
		wp.observer.RecordCircuitTransition(endpointID, cb.State)
	}
}
