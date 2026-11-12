package observability

import (
	"sync"
	"time"
)

type bucket struct {
	tokens     float64
	lastRefill time.Time
}

// TenantRateLimiter enforces a per-tenant token bucket rate limit without external dependencies.
type TenantRateLimiter struct {
	mu      sync.Mutex
	rateRPS float64
	burst   float64
	buckets map[string]*bucket
}

func NewTenantRateLimiter(rateRPS int, burst int) *TenantRateLimiter {
	if rateRPS <= 0 {
		rateRPS = 100
	}
	if burst <= 0 {
		burst = rateRPS
	}
	return &TenantRateLimiter{
		rateRPS: float64(rateRPS),
		burst:   float64(burst),
		buckets: make(map[string]*bucket),
	}
}

func (rl *TenantRateLimiter) Allow(tenantKey string, now time.Time) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	b, exists := rl.buckets[tenantKey]
	if !exists {
		rl.buckets[tenantKey] = &bucket{
			tokens:     rl.burst - 1,
			lastRefill: now,
		}
		return true
	}

	elapsed := now.Sub(b.lastRefill).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * rl.rateRPS
		if b.tokens > rl.burst {
			b.tokens = rl.burst
		}
		b.lastRefill = now
	}

	if b.tokens >= 1.0 {
		b.tokens -= 1.0
		return true
	}
	return false
}
