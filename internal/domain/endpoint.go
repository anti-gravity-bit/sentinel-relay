package domain

import "time"

// Endpoint is the Aggregate Root representing a subscriber webhook destination.
type Endpoint struct {
	ID             EndpointID
	TenantID       TenantID
	TargetURL      string
	Secret         SecretKey
	Active         bool
	RateLimitRPS   int
	MaxConcurrency int
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func NewEndpoint(
	id EndpointID,
	tenantID TenantID,
	rawURL string,
	secret SecretKey,
	rateLimitRPS int,
	maxConcurrency int,
	now time.Time,
) (*Endpoint, error) {
	validURL, err := ValidateTargetURL(rawURL)
	if err != nil {
		return nil, err
	}
	if rateLimitRPS <= 0 {
		rateLimitRPS = 50
	}
	if maxConcurrency <= 0 {
		maxConcurrency = 10
	}
	return &Endpoint{
		ID:             id,
		TenantID:       tenantID,
		TargetURL:      validURL,
		Secret:         secret,
		Active:         true,
		RateLimitRPS:   rateLimitRPS,
		MaxConcurrency: maxConcurrency,
		CreatedAt:      now.UTC(),
		UpdatedAt:      now.UTC(),
	}, nil
}

func (e *Endpoint) Disable(now time.Time) {
	e.Active = false
	e.UpdatedAt = now.UTC()
}

func (e *Endpoint) Enable(now time.Time) {
	e.Active = true
	e.UpdatedAt = now.UTC()
}
