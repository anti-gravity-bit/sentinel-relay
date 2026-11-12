# Chapter 02: Core Engine, Use Cases & Concurrency Orchestration

[← Previous: 01. Domain & SOLID](./01-domain-and-solid.md) | [Hub](../README.md) | [Next: 03. Persistence, HTTP API & Scaling →](./03-persistence-and-api.md)

## Prerequisites

- Completed [Chapter 01](./01-domain-and-solid.md)
- Understanding of Go channels, `sync.WaitGroup`, `sync.Mutex`, and `context.Context`

## 1. Application Use Cases & Error Translation

The application layer (`internal/application/usecases.go`) coordinates domain objects and repositories without knowing whether requests arrived over HTTP, gRPC, or CLI.

To enforce **Interface Segregation** for upstream transport adapters, we define four narrow consumer ports:

```go
type EndpointRegistrar interface {
	RegisterEndpoint(ctx context.Context, cmd RegisterEndpointCommand) (*domain.Endpoint, error)
}

type WebhookEnqueuer interface {
	EnqueueWebhook(ctx context.Context, cmd EnqueueWebhookCommand) (*EnqueueWebhookResult, error)
}

type DeliveryQuerier interface {
	GetDelivery(ctx context.Context, tenantID, deliveryID string) (*domain.Delivery, error)
	ListDeadLetters(ctx context.Context, tenantID string, limit int) ([]*domain.Delivery, error)
}

type DeadLetterReplayer interface {
	ReplayDeadLetter(ctx context.Context, tenantID, deliveryID string) (*domain.Delivery, error)
}
```

When a domain validation or invariant fails, `MapDomainError` (`internal/application/errors.go`) translates sentinel domain errors (`domain.ErrInvalidTenantID`, `domain.ErrEndpointNotFound`, `domain.ErrInvalidStateTransition`) into a structured `*application.AppError` carrying a transport-agnostic `ErrorCode` (`VALIDATION_ERROR`, `NOT_FOUND`, `CONFLICT`, `PRECONDITION_FAILED`).

## 2. Bounded Worker Pool & Graceful Drain

Unbounded goroutine spawning (`go dispatch(d)` on every incoming HTTP request) causes memory exhaustion and file-descriptor starvation when downstream endpoints slow down.

`WorkerPool` (`internal/application/worker_pool.go`) decouples ingestion from dispatch using:
1. **A fixed-size buffered channel (`jobs chan *domain.Delivery`):** Bounds in-memory queue depth to `cfg.QueueSize`.
2. **A fixed number of worker goroutines (`cfg.Workers`):** Bounds concurrent outbound network sockets.
3. **Detached per-attempt timeouts during shutdown:** When `ctx.Done()` fires on `SIGTERM`, the scheduler stops polling, closes `wp.jobs`, and waits on `wg.Wait()` for every in-flight worker to finish its current HTTP attempt and persist its state update before exiting:

```go
func (wp *WorkerPool) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for i := 0; i < wp.cfg.Workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for d := range wp.jobs {
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
```

## 3. Per-Endpoint Circuit Breaking

If a customer endpoint suffers a database outage and starts timing out after 5 seconds on every call, retrying 1,000 queued webhooks against that endpoint will tie up all worker goroutines.

`WorkerPool` maintains a map of `domain.CircuitBreaker` instances keyed by `EndpointID` (`cbMu sync.Mutex`):
- Before making a network request, `allowEndpoint(ep.ID, now)` checks the breaker state.
- If `State == CircuitOpen` and `CooldownPeriod` has not elapsed, the worker skips the HTTP call entirely, records a fast `503` failure with exponential backoff, and frees the worker immediately for healthy tenants.
- Once `CooldownPeriod` elapses, a single probe transitions the breaker to `CircuitHalfOpen`. A `2xx` response closes the circuit; a failure re-opens it.

## 4. Deterministic Concurrency Testing without `time.Sleep`

Flaky concurrency tests almost always stem from `time.Sleep` calls waiting on wall-clock timers. In `internal/application/application_test.go`, we inject a thread-safe `fakeClock` implementing `domain.Clock` and drive the worker pool deterministically via `ProcessBatchSynchronously(ctx)`:

```go
// Attempt 1 fails with 502 -> transitions to RETRYING
n, err := pool.ProcessBatchSynchronously(ctx)

// Advance fake clock by 2 seconds so exponential backoff elapses immediately
clock.Advance(2 * time.Second)

// Attempt 2 fails with 502 -> transitions to DEAD_LETTERED
n, err = pool.ProcessBatchSynchronously(ctx)
```

This tests the entire retry-to-DLQ-to-replay pipeline in under 1 millisecond with zero wall-clock sleeps.

## 5. Verification Checkpoint

Run the application test suite with race detection:

```bash
go test -v -race ./internal/application/...
```

Expected output:

```text
=== RUN   TestApplicationRetryToDLQAndReplayLifecycle
--- PASS: TestApplicationRetryToDLQAndReplayLifecycle (0.00s)
PASS
ok  	github.com/sentinel-engine/relay/internal/application
```

[← Previous: 01. Domain & SOLID](./01-domain-and-solid.md) | [Hub](../README.md) | [Next: 03. Persistence, HTTP API & Scaling →](./03-persistence-and-api.md)
