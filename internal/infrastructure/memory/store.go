package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/anti-gravity-bit/sentinel-relay/internal/domain"
)

// Store is a thread-safe in-memory implementation of domain.EndpointRepository and domain.DeliveryRepository.
type Store struct {
	mu             sync.RWMutex
	endpoints      map[string]*domain.Endpoint
	deliveries     map[domain.DeliveryID]*domain.Delivery
	idempotencyIdx map[string]domain.DeliveryID
}

func NewStore() *Store {
	return &Store{
		endpoints:      make(map[string]*domain.Endpoint),
		deliveries:     make(map[domain.DeliveryID]*domain.Delivery),
		idempotencyIdx: make(map[string]domain.DeliveryID),
	}
}

func endpointKey(tenantID domain.TenantID, endpointID domain.EndpointID) string {
	return fmt.Sprintf("%s::%s", tenantID, endpointID)
}

func idempotencyMapKey(tenantID domain.TenantID, key domain.IdempotencyKey) string {
	return fmt.Sprintf("%s::%s", tenantID, key)
}

func (s *Store) UpsertEndpoint(ctx context.Context, ep *domain.Endpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	cp := *ep
	s.endpoints[endpointKey(ep.TenantID, ep.ID)] = &cp
	return nil
}

func (s *Store) GetEndpoint(ctx context.Context, tenantID domain.TenantID, endpointID domain.EndpointID) (*domain.Endpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	ep, ok := s.endpoints[endpointKey(tenantID, endpointID)]
	if !ok {
		return nil, domain.ErrEndpointNotFound
	}
	cp := *ep
	return &cp, nil
}

func (s *Store) Save(ctx context.Context, d *domain.Delivery) (*domain.Delivery, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	idemKey := idempotencyMapKey(d.TenantID, d.IdempotencyKey)
	if existingID, exists := s.idempotencyIdx[idemKey]; exists {
		existing := s.deliveries[existingID]
		return existing.Clone(), false, nil
	}

	cloned := d.Clone()
	s.deliveries[d.ID] = cloned
	s.idempotencyIdx[idemKey] = d.ID
	return cloned.Clone(), true, nil
}

func (s *Store) ClaimDue(ctx context.Context, now time.Time, limit int) ([]*domain.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 50
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	ts := now.UTC()
	var candidates []*domain.Delivery
	for _, d := range s.deliveries {
		if (d.Status == domain.StatusPending || d.Status == domain.StatusRetrying) && !d.NextAttemptAt.After(ts) {
			candidates = append(candidates, d)
		}
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].NextAttemptAt.Equal(candidates[j].NextAttemptAt) {
			return candidates[i].ID < candidates[j].ID
		}
		return candidates[i].NextAttemptAt.Before(candidates[j].NextAttemptAt)
	})

	if len(candidates) > limit {
		candidates = candidates[:limit]
	}

	claimed := make([]*domain.Delivery, 0, len(candidates))
	for _, d := range candidates {
		if err := d.MarkInFlight(ts); err != nil {
			continue
		}
		claimed = append(claimed, d.Clone())
	}
	return claimed, nil
}

func (s *Store) Update(ctx context.Context, d *domain.Delivery) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.deliveries[d.ID]; !ok {
		return domain.ErrDeliveryNotFound
	}
	s.deliveries[d.ID] = d.Clone()
	return nil
}

func (s *Store) GetByID(ctx context.Context, tenantID domain.TenantID, deliveryID domain.DeliveryID) (*domain.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	d, ok := s.deliveries[deliveryID]
	if !ok || d.TenantID != tenantID {
		return nil, domain.ErrDeliveryNotFound
	}
	return d.Clone(), nil
}

func (s *Store) ListDeadLetters(ctx context.Context, tenantID domain.TenantID, limit int) ([]*domain.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 50
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []*domain.Delivery
	for _, d := range s.deliveries {
		if d.TenantID == tenantID && d.Status == domain.StatusDeadLettered {
			out = append(out, d.Clone())
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].UpdatedAt.After(out[j].UpdatedAt)
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
