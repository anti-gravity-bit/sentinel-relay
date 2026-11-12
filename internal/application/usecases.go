package application

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"time"

	"github.com/anti-gravity-bit/sentinel-relay/internal/domain"
)

type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }

// RegisterEndpointCommand holds validated inputs for creating or updating an Endpoint.
type RegisterEndpointCommand struct {
	TenantID       string
	EndpointID     string
	TargetURL      string
	SecretKey      string
	RateLimitRPS   int
	MaxConcurrency int
}

// EnqueueWebhookCommand holds inputs for enqueuing an outbound webhook delivery.
type EnqueueWebhookCommand struct {
	TenantID       string
	EndpointID     string
	IdempotencyKey string
	EventType      string
	Payload        []byte
	MaxAttempts    int
}

type EnqueueWebhookResult struct {
	Delivery          *domain.Delivery
	IdempotentReplay  bool
}

// Consumer-segregated use-case interfaces for HTTP/gRPC transport handlers (ISP).
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

// RelayService implements the application command and query use cases.
type RelayService struct {
	endpoints  domain.EndpointRepository
	deliveries domain.DeliveryRepository
	clock      domain.Clock
	logger     *slog.Logger
}

func NewRelayService(
	endpoints domain.EndpointRepository,
	deliveries domain.DeliveryRepository,
	clock domain.Clock,
	logger *slog.Logger,
) *RelayService {
	if clock == nil {
		clock = SystemClock{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &RelayService{
		endpoints:  endpoints,
		deliveries: deliveries,
		clock:      clock,
		logger:     logger,
	}
}

func (s *RelayService) RegisterEndpoint(ctx context.Context, cmd RegisterEndpointCommand) (*domain.Endpoint, error) {
	tenantID, err := domain.NewTenantID(cmd.TenantID)
	if err != nil {
		return nil, MapDomainError(err)
	}
	endpointID, err := domain.NewEndpointID(cmd.EndpointID)
	if err != nil {
		return nil, MapDomainError(err)
	}
	secret, err := domain.NewSecretKey(cmd.SecretKey)
	if err != nil {
		return nil, MapDomainError(err)
	}
	ep, err := domain.NewEndpoint(
		endpointID,
		tenantID,
		cmd.TargetURL,
		secret,
		cmd.RateLimitRPS,
		cmd.MaxConcurrency,
		s.clock.Now(),
	)
	if err != nil {
		return nil, MapDomainError(err)
	}
	if err := s.endpoints.UpsertEndpoint(ctx, ep); err != nil {
		return nil, MapDomainError(err)
	}
	s.logger.InfoContext(ctx, "endpoint registered",
		slog.String("tenant_id", string(tenantID)),
		slog.String("endpoint_id", string(endpointID)),
		slog.String("target_url", ep.TargetURL),
	)
	return ep, nil
}

func (s *RelayService) EnqueueWebhook(ctx context.Context, cmd EnqueueWebhookCommand) (*EnqueueWebhookResult, error) {
	tenantID, err := domain.NewTenantID(cmd.TenantID)
	if err != nil {
		return nil, MapDomainError(err)
	}
	endpointID, err := domain.NewEndpointID(cmd.EndpointID)
	if err != nil {
		return nil, MapDomainError(err)
	}
	idemKey, err := domain.NewIdempotencyKey(cmd.IdempotencyKey)
	if err != nil {
		return nil, MapDomainError(err)
	}

	ep, err := s.endpoints.GetEndpoint(ctx, tenantID, endpointID)
	if err != nil {
		return nil, MapDomainError(err)
	}
	if !ep.Active {
		return nil, MapDomainError(domain.ErrEndpointDisabled)
	}

	deliveryID := generateDeliveryID()
	d, err := domain.NewDelivery(
		deliveryID,
		tenantID,
		endpointID,
		idemKey,
		cmd.EventType,
		cmd.Payload,
		cmd.MaxAttempts,
		s.clock.Now(),
	)
	if err != nil {
		return nil, MapDomainError(err)
	}

	saved, created, err := s.deliveries.Save(ctx, d)
	if err != nil {
		return nil, MapDomainError(err)
	}

	s.logger.InfoContext(ctx, "webhook delivery enqueued",
		slog.String("tenant_id", string(tenantID)),
		slog.String("endpoint_id", string(endpointID)),
		slog.String("delivery_id", string(saved.ID)),
		slog.Bool("idempotent_replay", !created),
	)

	return &EnqueueWebhookResult{
		Delivery:         saved,
		IdempotentReplay: !created,
	}, nil
}

func (s *RelayService) GetDelivery(ctx context.Context, rawTenantID, rawDeliveryID string) (*domain.Delivery, error) {
	tenantID, err := domain.NewTenantID(rawTenantID)
	if err != nil {
		return nil, MapDomainError(err)
	}
	deliveryID, err := domain.NewDeliveryID(rawDeliveryID)
	if err != nil {
		return nil, MapDomainError(err)
	}
	d, err := s.deliveries.GetByID(ctx, tenantID, deliveryID)
	if err != nil {
		return nil, MapDomainError(err)
	}
	return d, nil
}

func (s *RelayService) ListDeadLetters(ctx context.Context, rawTenantID string, limit int) ([]*domain.Delivery, error) {
	tenantID, err := domain.NewTenantID(rawTenantID)
	if err != nil {
		return nil, MapDomainError(err)
	}
	list, err := s.deliveries.ListDeadLetters(ctx, tenantID, limit)
	if err != nil {
		return nil, MapDomainError(err)
	}
	return list, nil
}

func (s *RelayService) ReplayDeadLetter(ctx context.Context, rawTenantID, rawDeliveryID string) (*domain.Delivery, error) {
	tenantID, err := domain.NewTenantID(rawTenantID)
	if err != nil {
		return nil, MapDomainError(err)
	}
	deliveryID, err := domain.NewDeliveryID(rawDeliveryID)
	if err != nil {
		return nil, MapDomainError(err)
	}
	d, err := s.deliveries.GetByID(ctx, tenantID, deliveryID)
	if err != nil {
		return nil, MapDomainError(err)
	}
	if err := d.ReplayDeadLetter(s.clock.Now()); err != nil {
		return nil, MapDomainError(err)
	}
	if err := s.deliveries.Update(ctx, d); err != nil {
		return nil, MapDomainError(err)
	}
	s.logger.InfoContext(ctx, "dead-letter delivery replayed",
		slog.String("tenant_id", string(tenantID)),
		slog.String("delivery_id", string(deliveryID)),
	)
	return d, nil
}

func generateDeliveryID() domain.DeliveryID {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return domain.DeliveryID("dlv_" + hex.EncodeToString(b[:]))
}
