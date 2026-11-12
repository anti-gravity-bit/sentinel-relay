package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anti-gravity-bit/sentinel-relay/internal/application"
	"github.com/anti-gravity-bit/sentinel-relay/internal/domain"
	"github.com/anti-gravity-bit/sentinel-relay/internal/infrastructure/httpapi"
	"github.com/anti-gravity-bit/sentinel-relay/internal/infrastructure/httpclient"
	"github.com/anti-gravity-bit/sentinel-relay/internal/infrastructure/memory"
	"github.com/anti-gravity-bit/sentinel-relay/internal/infrastructure/observability"
)

func TestEndToEndHTTPIntegrationAndSignatureVerification(t *testing.T) {
	t.Parallel()

	const webhookSecret = "whsec_integration_secret_0123456789"
	var receivedSignatures atomic.Int32

	// Spin up simulated customer receiver server that verifies X-Sentinel-Signature-256
	customerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		sig := r.Header.Get("X-Sentinel-Signature-256")
		tsRaw := r.Header.Get("X-Sentinel-Timestamp")
		var unixSec int64
		_, _ = fmt.Sscanf(tsRaw, "%d", &unixSec)

		secret, _ := domain.NewSecretKey(webhookSecret)
		if !secret.VerifySignature(time.Unix(unixSec, 0).UTC(), body, sig) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		receivedSignatures.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer customerSrv.Close()

	store := memory.NewStore()
	silentLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := observability.NewMetricsCollector()
	limiter := observability.NewTenantRateLimiter(500, 500)

	svc := application.NewRelayService(store, store, nil, silentLogger)
	apiServer := httpapi.NewServer(svc, svc, svc, svc, metrics, limiter)
	relaySrv := httptest.NewServer(apiServer)
	defer relaySrv.Close()

	// 1. Register customer endpoint via POST /v1/endpoints
	regBody, _ := json.Marshal(map[string]any{
		"tenant_id":       "tenant_cloud",
		"endpoint_id":     "ep_billing",
		"target_url":      customerSrv.URL,
		"secret_key":      webhookSecret,
		"rate_limit_rps":  100,
		"max_concurrency": 8,
	})
	resp, err := http.Post(relaySrv.URL+"/v1/endpoints", "application/json", bytes.NewReader(regBody))
	if err != nil || resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /v1/endpoints status=%v err=%v", resp, err)
	}
	_ = resp.Body.Close()

	// 2. Enqueue webhook via POST /v1/webhooks/enqueue
	enqBody, _ := json.Marshal(map[string]any{
		"tenant_id":       "tenant_cloud",
		"endpoint_id":     "ep_billing",
		"idempotency_key": "idem_inv_555",
		"event_type":      "invoice.paid",
		"payload":         map[string]any{"invoice_id": "inv_555", "amount": 9900},
		"max_attempts":    3,
	})
	enqResp, err := http.Post(relaySrv.URL+"/v1/webhooks/enqueue", "application/json", bytes.NewReader(enqBody))
	if err != nil || enqResp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /v1/webhooks/enqueue status=%v err=%v", enqResp, err)
	}
	var enqData struct {
		DeliveryID string `json:"delivery_id"`
	}
	_ = json.NewDecoder(enqResp.Body).Decode(&enqData)
	_ = enqResp.Body.Close()

	// 3. Dispatch using real httpclient.Dispatcher and WorkerPool
	dispatcher := httpclient.NewDispatcher(3 * time.Second)
	pool := application.NewWorkerPool(application.DefaultWorkerPoolConfig(), store, store, dispatcher, nil, metrics, silentLogger)
	processed, err := pool.ProcessBatchSynchronously(context.Background())
	if err != nil || processed != 1 {
		t.Fatalf("ProcessBatchSynchronously processed=%d err=%v", processed, err)
	}

	if receivedSignatures.Load() != 1 {
		t.Fatalf("expected customer server to verify 1 signed webhook, got %d", receivedSignatures.Load())
	}

	// 4. Query GET /v1/deliveries/{delivery_id} and verify SUCCEEDED status
	getReq, _ := http.NewRequest(http.MethodGet, relaySrv.URL+"/v1/deliveries/"+enqData.DeliveryID, nil)
	getReq.Header.Set("X-Tenant-ID", "tenant_cloud")
	getResp, err := http.DefaultClient.Do(getReq)
	if err != nil || getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/deliveries status=%v err=%v", getResp, err)
	}
	var statusData struct {
		Status string `json:"status"`
	}
	_ = json.NewDecoder(getResp.Body).Decode(&statusData)
	_ = getResp.Body.Close()

	if statusData.Status != string(domain.StatusSucceeded) {
		t.Fatalf("expected SUCCEEDED delivery status, got %s", statusData.Status)
	}
}

func BenchmarkEnqueueAndDispatchInMemory(b *testing.B) {
	ctx := context.Background()
	store := memory.NewStore()
	silentLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := application.NewRelayService(store, store, nil, silentLogger)

	_, err := svc.RegisterEndpoint(ctx, application.RegisterEndpointCommand{
		TenantID:       "tenant_bench",
		EndpointID:     "ep_bench",
		TargetURL:      "https://bench.example.internal/webhook",
		SecretKey:      "whsec_benchmark_secret_key_0123456789",
		RateLimitRPS:   10000,
		MaxConcurrency: 64,
	})
	if err != nil {
		b.Fatalf("RegisterEndpoint failed: %v", err)
	}

	payload := []byte(`{"event":"order.settled","order_id":"ord_998877","amount_cents":45000}`)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := svc.EnqueueWebhook(ctx, application.EnqueueWebhookCommand{
			TenantID:       "tenant_bench",
			EndpointID:     "ep_bench",
			IdempotencyKey: fmt.Sprintf("idem_bench_%d", i),
			EventType:      "order.settled",
			Payload:        payload,
			MaxAttempts:    3,
		})
		if err != nil {
			b.Fatalf("EnqueueWebhook failed: %v", err)
		}
	}
}
