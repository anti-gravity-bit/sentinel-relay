package application_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anti-gravity-bit/sentinel-relay/internal/application"
	"github.com/anti-gravity-bit/sentinel-relay/internal/domain"
	"github.com/anti-gravity-bit/sentinel-relay/internal/infrastructure/memory"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(t time.Time) *fakeClock {
	return &fakeClock{now: t.UTC()}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type fakeDispatcher struct {
	statusCode int32
	calls      atomic.Int64
}

func (f *fakeDispatcher) Dispatch(_ context.Context, _ *domain.Endpoint, _ *domain.Delivery, _ string, _ time.Time) domain.DispatchResult {
	f.calls.Add(1)
	code := int(atomic.LoadInt32(&f.statusCode))
	return domain.DispatchResult{
		StatusCode: code,
		Duration:   2 * time.Millisecond,
	}
}

func TestApplicationRetryToDLQAndReplayLifecycle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memory.NewStore()
	clock := newFakeClock(time.Date(2026, 11, 12, 14, 0, 0, 0, time.UTC))
	silentLogger := slog.New(slog.NewTextHandler(io.Discard, nil))

	svc := application.NewRelayService(store, store, clock, silentLogger)
	_, err := svc.RegisterEndpoint(ctx, application.RegisterEndpointCommand{
		TenantID:       "tenant_fin",
		EndpointID:     "ep_ledger",
		TargetURL:      "https://hooks.example.com/ledger",
		SecretKey:      "whsec_ledger_secret_key_0123456789",
		RateLimitRPS:   100,
		MaxConcurrency: 10,
	})
	if err != nil {
		t.Fatalf("RegisterEndpoint failed: %v", err)
	}

	enqRes, err := svc.EnqueueWebhook(ctx, application.EnqueueWebhookCommand{
		TenantID:       "tenant_fin",
		EndpointID:     "ep_ledger",
		IdempotencyKey: "evt_ledger_9001",
		EventType:      "ledger.entry.posted",
		Payload:        []byte(`{"entry_id":"le_9001","amount_cents":125000}`),
		MaxAttempts:    2,
	})
	if err != nil {
		t.Fatalf("EnqueueWebhook failed: %v", err)
	}
	if enqRes.IdempotentReplay {
		t.Fatal("expected first enqueue to not be an idempotent replay")
	}

	// Verify duplicate enqueue returns identical delivery ID
	dupRes, err := svc.EnqueueWebhook(ctx, application.EnqueueWebhookCommand{
		TenantID:       "tenant_fin",
		EndpointID:     "ep_ledger",
		IdempotencyKey: "evt_ledger_9001",
		EventType:      "ledger.entry.posted",
		Payload:        []byte(`{"entry_id":"le_9001","amount_cents":125000}`),
		MaxAttempts:    2,
	})
	if err != nil || !dupRes.IdempotentReplay || dupRes.Delivery.ID != enqRes.Delivery.ID {
		t.Fatalf("expected idempotent replay returning %s, got %+v (err=%v)", enqRes.Delivery.ID, dupRes, err)
	}

	dispatcher := &fakeDispatcher{statusCode: 502}
	cfg := application.DefaultWorkerPoolConfig()
	pool := application.NewWorkerPool(cfg, store, store, dispatcher, clock, nil, silentLogger)

	// Attempt 1 fails with 502 -> RETRYING
	n, err := pool.ProcessBatchSynchronously(ctx)
	if err != nil || n != 1 {
		t.Fatalf("expected 1 processed delivery on attempt 1, got %d (err=%v)", n, err)
	}

	// Advance clock by 2 seconds so backoff elapses -> Attempt 2 fails with 502 -> DEAD_LETTERED
	clock.Advance(2 * time.Second)
	n, err = pool.ProcessBatchSynchronously(ctx)
	if err != nil || n != 1 {
		t.Fatalf("expected 1 processed delivery on attempt 2, got %d (err=%v)", n, err)
	}

	dlq, err := svc.ListDeadLetters(ctx, "tenant_fin", 10)
	if err != nil || len(dlq) != 1 {
		t.Fatalf("expected 1 delivery in DLQ, got %d (err=%v)", len(dlq), err)
	}

	// Downstream endpoint recovers (200 OK) and operator replays the dead-lettered delivery
	atomic.StoreInt32(&dispatcher.statusCode, 200)
	if _, err := svc.ReplayDeadLetter(ctx, "tenant_fin", string(enqRes.Delivery.ID)); err != nil {
		t.Fatalf("ReplayDeadLetter failed: %v", err)
	}

	n, err = pool.ProcessBatchSynchronously(ctx)
	if err != nil || n != 1 {
		t.Fatalf("expected 1 processed delivery after replay, got %d (err=%v)", n, err)
	}

	finalState, err := svc.GetDelivery(ctx, "tenant_fin", string(enqRes.Delivery.ID))
	if err != nil || finalState.Status != domain.StatusSucceeded {
		t.Fatalf("expected SUCCEEDED status after replay, got %+v (err=%v)", finalState, err)
	}
}
