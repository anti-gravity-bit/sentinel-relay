package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/anti-gravity-bit/sentinel-relay/internal/application"
	"github.com/anti-gravity-bit/sentinel-relay/internal/infrastructure/httpapi"
	"github.com/anti-gravity-bit/sentinel-relay/internal/infrastructure/httpclient"
	"github.com/anti-gravity-bit/sentinel-relay/internal/infrastructure/memory"
	"github.com/anti-gravity-bit/sentinel-relay/internal/infrastructure/observability"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	addr := envOrDefault("RELAY_HTTP_ADDR", ":8080")
	workers := envIntOrDefault("RELAY_WORKERS", 8)
	rateLimitRPS := envIntOrDefault("RELAY_TENANT_RPS", 200)

	store := memory.NewStore()
	metrics := observability.NewMetricsCollector()
	limiter := observability.NewTenantRateLimiter(rateLimitRPS, rateLimitRPS)
	dispatcher := httpclient.NewDispatcher(5 * time.Second)

	relayService := application.NewRelayService(store, store, application.SystemClock{}, logger)
	poolCfg := application.DefaultWorkerPoolConfig()
	poolCfg.Workers = workers

	workerPool := application.NewWorkerPool(
		poolCfg,
		store,
		store,
		dispatcher,
		application.SystemClock{},
		metrics,
		logger,
	)

	apiHandler := httpapi.NewServer(
		relayService,
		relayService,
		relayService,
		relayService,
		metrics,
		limiter,
	)

	httpServer := &http.Server{
		Addr:              addr,
		Handler:           apiHandler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		_ = workerPool.Run(ctx)
	}()

	go func() {
		logger.Info("sentinel-relay listening",
			slog.String("addr", addr),
			slog.Int("workers", workers),
		)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server failed", slog.String("error", err.Error()))
			stop()
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received, draining readiness and in-flight workers")
	apiHandler.SetReady(false)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful http shutdown error", slog.String("error", err.Error()))
	}
	<-workerDone
	logger.Info("sentinel-relay stopped cleanly")
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envIntOrDefault(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			return parsed
		}
	}
	return fallback
}
