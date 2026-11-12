package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/anti-gravity-bit/sentinel-relay/internal/application"
	"github.com/anti-gravity-bit/sentinel-relay/internal/domain"
	"github.com/anti-gravity-bit/sentinel-relay/internal/infrastructure/observability"
)

// Server wires narrow application use-case interfaces to Go 1.22 net/http routes.
type Server struct {
	registrar   application.EndpointRegistrar
	enqueuer    application.WebhookEnqueuer
	querier     application.DeliveryQuerier
	replayer    application.DeadLetterReplayer
	metrics     *observability.MetricsCollector
	rateLimiter *observability.TenantRateLimiter
	ready       atomic.Bool
	mux         *http.ServeMux
}

func NewServer(
	registrar application.EndpointRegistrar,
	enqueuer application.WebhookEnqueuer,
	querier application.DeliveryQuerier,
	replayer application.DeadLetterReplayer,
	metrics *observability.MetricsCollector,
	rateLimiter *observability.TenantRateLimiter,
) *Server {
	if metrics == nil {
		metrics = observability.NewMetricsCollector()
	}
	if rateLimiter == nil {
		rateLimiter = observability.NewTenantRateLimiter(200, 200)
	}
	s := &Server{
		registrar:   registrar,
		enqueuer:    enqueuer,
		querier:     querier,
		replayer:    replayer,
		metrics:     metrics,
		rateLimiter: rateLimiter,
		mux:         http.NewServeMux(),
	}
	s.ready.Store(true)
	s.registerRoutes()
	return s
}

func (s *Server) SetReady(ready bool) {
	s.ready.Store(ready)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /readyz", s.handleReadyz)
	s.mux.Handle("GET /metrics", s.metrics)

	s.mux.HandleFunc("POST /v1/endpoints", s.handleRegisterEndpoint)
	s.mux.HandleFunc("POST /v1/webhooks/enqueue", s.handleEnqueueWebhook)
	s.mux.HandleFunc("GET /v1/deliveries/{delivery_id}", s.handleGetDelivery)
	s.mux.HandleFunc("GET /v1/dlq", s.handleListDeadLetters)
	s.mux.HandleFunc("POST /v1/dlq/{delivery_id}/replay", s.handleReplayDeadLetter)
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if !s.ready.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "draining"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

type registerEndpointRequest struct {
	TenantID       string `json:"tenant_id"`
	EndpointID     string `json:"endpoint_id"`
	TargetURL      string `json:"target_url"`
	SecretKey      string `json:"secret_key"`
	RateLimitRPS   int    `json:"rate_limit_rps"`
	MaxConcurrency int    `json:"max_concurrency"`
}

func (s *Server) handleRegisterEndpoint(w http.ResponseWriter, r *http.Request) {
	var req registerEndpointRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON request body"})
		return
	}
	if !s.rateLimiter.Allow(req.TenantID, time.Now().UTC()) {
		s.metrics.IncRateLimited()
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "tenant rate limit exceeded"})
		return
	}

	ep, err := s.registrar.RegisterEndpoint(r.Context(), application.RegisterEndpointCommand{
		TenantID:       req.TenantID,
		EndpointID:     req.EndpointID,
		TargetURL:      req.TargetURL,
		SecretKey:      req.SecretKey,
		RateLimitRPS:   req.RateLimitRPS,
		MaxConcurrency: req.MaxConcurrency,
	})
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"tenant_id":       ep.TenantID,
		"endpoint_id":     ep.ID,
		"target_url":      ep.TargetURL,
		"active":          ep.Active,
		"rate_limit_rps":  ep.RateLimitRPS,
		"max_concurrency": ep.MaxConcurrency,
		"created_at":      ep.CreatedAt,
	})
}

type enqueueWebhookRequest struct {
	TenantID       string          `json:"tenant_id"`
	EndpointID     string          `json:"endpoint_id"`
	IdempotencyKey string          `json:"idempotency_key"`
	EventType      string          `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`
	MaxAttempts    int             `json:"max_attempts"`
}

func (s *Server) handleEnqueueWebhook(w http.ResponseWriter, r *http.Request) {
	var req enqueueWebhookRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON request body"})
		return
	}
	if !s.rateLimiter.Allow(req.TenantID, time.Now().UTC()) {
		s.metrics.IncRateLimited()
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "tenant rate limit exceeded"})
		return
	}

	idemKey := req.IdempotencyKey
	if headerKey := r.Header.Get("Idempotency-Key"); headerKey != "" && idemKey == "" {
		idemKey = headerKey
	}

	res, err := s.enqueuer.EnqueueWebhook(r.Context(), application.EnqueueWebhookCommand{
		TenantID:       req.TenantID,
		EndpointID:     req.EndpointID,
		IdempotencyKey: idemKey,
		EventType:      req.EventType,
		Payload:        []byte(req.Payload),
		MaxAttempts:    req.MaxAttempts,
	})
	if err != nil {
		writeAppError(w, err)
		return
	}

	status := http.StatusAccepted
	if res.IdempotentReplay {
		status = http.StatusOK
	} else {
		s.metrics.IncEnqueued()
	}

	writeJSON(w, status, map[string]any{
		"delivery_id":       res.Delivery.ID,
		"tenant_id":         res.Delivery.TenantID,
		"endpoint_id":       res.Delivery.EndpointID,
		"idempotency_key":   res.Delivery.IdempotencyKey,
		"status":            res.Delivery.Status,
		"idempotent_replay": res.IdempotentReplay,
		"next_attempt_at":   res.Delivery.NextAttemptAt,
	})
}

func (s *Server) handleGetDelivery(w http.ResponseWriter, r *http.Request) {
	tenantID := r.Header.Get("X-Tenant-ID")
	if tenantID == "" {
		tenantID = r.URL.Query().Get("tenant_id")
	}
	deliveryID := r.PathValue("delivery_id")

	d, err := s.querier.GetDelivery(r.Context(), tenantID, deliveryID)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, serializeDelivery(d))
}

func (s *Server) handleListDeadLetters(w http.ResponseWriter, r *http.Request) {
	tenantID := r.Header.Get("X-Tenant-ID")
	if tenantID == "" {
		tenantID = r.URL.Query().Get("tenant_id")
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	list, err := s.querier.ListDeadLetters(r.Context(), tenantID, limit)
	if err != nil {
		writeAppError(w, err)
		return
	}
	items := make([]map[string]any, 0, len(list))
	for _, d := range list {
		items = append(items, serializeDelivery(d))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"count": items,
	})
}

func (s *Server) handleReplayDeadLetter(w http.ResponseWriter, r *http.Request) {
	tenantID := r.Header.Get("X-Tenant-ID")
	if tenantID == "" {
		tenantID = r.URL.Query().Get("tenant_id")
	}
	deliveryID := r.PathValue("delivery_id")

	d, err := s.replayer.ReplayDeadLetter(r.Context(), tenantID, deliveryID)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, serializeDelivery(d))
}

func serializeDelivery(d *domain.Delivery) map[string]any {
	return map[string]any{
		"delivery_id":      d.ID,
		"tenant_id":        d.TenantID,
		"endpoint_id":      d.EndpointID,
		"idempotency_key":  d.IdempotencyKey,
		"event_type":       d.EventType,
		"status":           d.Status,
		"attempt_count":    d.AttemptCount,
		"max_attempts":     d.MaxAttempts,
		"last_status_code": d.LastStatusCode,
		"last_error":       d.LastError,
		"next_attempt_at":  d.NextAttemptAt,
		"updated_at":       d.UpdatedAt,
	}
}

func writeAppError(w http.ResponseWriter, err error) {
	var appErr *application.AppError
	if errors.As(err, &appErr) {
		switch appErr.Code {
		case application.CodeValidationError:
			writeJSON(w, http.StatusBadRequest, map[string]string{"code": string(appErr.Code), "error": appErr.Message})
		case application.CodeNotFound:
			writeJSON(w, http.StatusNotFound, map[string]string{"code": string(appErr.Code), "error": appErr.Message})
		case application.CodeConflict:
			writeJSON(w, http.StatusConflict, map[string]string{"code": string(appErr.Code), "error": appErr.Message})
		case application.CodePrecondition:
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"code": string(appErr.Code), "error": appErr.Message})
		default:
			writeJSON(w, http.StatusInternalServerError, map[string]string{"code": string(appErr.Code), "error": appErr.Message})
		}
		return
	}
	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
