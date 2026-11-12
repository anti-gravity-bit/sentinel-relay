package memory_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sentinel-engine/relay/internal/domain"
	"github.com/sentinel-engine/relay/internal/infrastructure/memory"
)

func TestStoreConcurrentIdempotencyAndClaiming(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memory.NewStore()
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)

	// 1. Verify concurrent idempotent Save with identical (TenantID, IdempotencyKey) creates exactly 1 record
	const goroutines = 32
	var wg sync.WaitGroup
	createdCount := 0
	var mu sync.Mutex

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			d, err := domain.NewDelivery(
				domain.DeliveryID(fmt.Sprintf("dlv_%03d", idx)),
				"tenant_acme",
				"ep_primary",
				"idem_shared_key",
				"order.created",
				[]byte(`{"order_id":101}`),
				5,
				now,
			)
			if err != nil {
				t.Errorf("NewDelivery error: %v", err)
				return
			}
			_, created, err := store.Save(ctx, d)
			if err != nil {
				t.Errorf("Save error: %v", err)
				return
			}
			if created {
				mu.Lock()
				createdCount++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	if createdCount != 1 {
		t.Fatalf("expected exactly 1 created delivery across %d concurrent saves, got %d", goroutines, createdCount)
	}

	// 2. Verify concurrent ClaimDue never double-claims the same delivery
	totalClaimed := 0
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			claimed, err := store.ClaimDue(ctx, now, 10)
			if err != nil {
				t.Errorf("ClaimDue error: %v", err)
				return
			}
			mu.Lock()
			totalClaimed += len(claimed)
			mu.Unlock()
		}()
	}
	wg.Wait()

	if totalClaimed != 1 {
		t.Fatalf("expected exactly 1 claimed delivery across concurrent workers, got %d", totalClaimed)
	}
}
