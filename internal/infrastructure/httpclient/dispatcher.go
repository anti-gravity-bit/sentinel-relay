package httpclient

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/sentinel-engine/relay/internal/domain"
)

// Dispatcher executes signed outbound HTTP POST requests to subscriber endpoints.
type Dispatcher struct {
	client *http.Client
}

func NewDispatcher(timeout time.Duration) *Dispatcher {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	transport := &http.Transport{
		MaxIdleConns:        256,
		MaxIdleConnsPerHost: 32,
		IdleConnTimeout:     90 * time.Second,
	}
	return &Dispatcher{
		client: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
	}
}

func (d *Dispatcher) Dispatch(
	ctx context.Context,
	endpoint *domain.Endpoint,
	delivery *domain.Delivery,
	signature string,
	timestamp time.Time,
) domain.DispatchResult {
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.TargetURL, bytes.NewReader(delivery.Payload))
	if err != nil {
		return domain.DispatchResult{
			StatusCode: 0,
			Duration:   time.Since(start),
			Err:        fmt.Errorf("build request: %w", err),
		}
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Sentinel-Relay/1.0")
	req.Header.Set("X-Sentinel-Delivery-ID", string(delivery.ID))
	req.Header.Set("X-Sentinel-Event-Type", delivery.EventType)
	req.Header.Set("X-Sentinel-Timestamp", strconv.FormatInt(timestamp.Unix(), 10))
	req.Header.Set("X-Sentinel-Signature-256", signature)

	resp, err := d.client.Do(req)
	if err != nil {
		return domain.DispatchResult{
			StatusCode: 0,
			Duration:   time.Since(start),
			Err:        err,
		}
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
	}()

	var statusErr error
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		statusErr = fmt.Errorf("downstream returned HTTP %d", resp.StatusCode)
	}

	return domain.DispatchResult{
		StatusCode: resp.StatusCode,
		Duration:   time.Since(start),
		Err:        statusErr,
	}
}
