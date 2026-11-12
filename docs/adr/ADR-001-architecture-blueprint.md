# ADR-001: Sentinel Relay — Multi-Tenant Transactional Outbox & Webhook Delivery Engine

- **Status:** Accepted
- **Date:** 2026-11-12
- **Context:** Phase 1 Architecture Blueprint

## 1. Context & Problem Statement

Modern multi-tenant SaaS platforms emit high volumes of asynchronous webhook events (e.g., `invoice.paid`, `ledger.reconciled`, `subscription.updated`) to customer-controlled HTTPS endpoints. Customer endpoints exhibit unpredictable latency, transient 5xx failures, rate-limiting (`429 Too Many Requests`), and extended outages.

A production webhook delivery engine must guarantee:
1. **At-Least-Once Delivery with Deterministic Idempotency:** Duplicate ingestion of the same upstream event (`TenantID` + `IdempotencyKey`) must never create duplicate deliveries.
2. **Tenant Isolation & Quota Enforcement:** One noisy tenant or slow downstream endpoint must not exhaust worker capacity or starve other tenants.
3. **Resilient Failure Handling:** Per-endpoint Circuit Breakers and exponential backoff with full jitter prevent cascading retry storms, transitioning permanently failing deliveries to a Dead-Letter Queue (DLQ) once `MaxAttempts` is reached.
4. **Strict Architectural Boundaries:** The core domain model must remain 100% decoupled from HTTP transport, SQL drivers, and serialization frameworks.

## 2. Decision

We will build **Sentinel Relay**, a multi-tenant webhook delivery engine in Go (`go 1.22`), structured around Domain-Driven Design (DDD), SOLID principles, and Object-Oriented Design (OOD) via composition and narrow consumer-defined interfaces.

### Bounded Context: `relay` (Webhook Dispatch & Lifecycle)

- **Value Objects (Immutable, validated on construction):**
  - `TenantID`, `EndpointID`, `DeliveryID`, `IdempotencyKey` (non-empty, length-bounded identifiers)
  - `SecretKey` (HMAC-SHA256 signing secret, minimum 16 bytes)
  - `BackoffPolicy` (computes deterministic/jittered retry delays: `min(MaxInterval, InitialInterval * 2^attempt)`)
  - `Signature` (computed `sha256=<hex>` over `<timestamp>.<payload>`)
- **Entities & Aggregates:**
  - `Endpoint` (Aggregate Root for subscription configuration: `ID`, `TenantID`, `TargetURL`, `Secret`, `Active`, `RateLimitRPS`, `MaxConcurrency`)
  - `Delivery` (Aggregate Root for dispatch state machine: `ID`, `TenantID`, `EndpointID`, `IdempotencyKey`, `EventType`, `Payload`, `Status`, `AttemptCount`, `MaxAttempts`, `NextAttemptAt`, `LastStatusCode`, `LastError`, `UpdatedAt`)
  - `CircuitBreaker` (Per-endpoint state machine: `Closed` -> `Open` after `FailureThreshold` consecutive failures -> `HalfOpen` after `CooldownPeriod`)
- **Domain Events:**
  - `DeliveryEnqueued`, `DeliverySucceeded`, `DeliveryRetryScheduled`, `DeliveryDeadLettered`, `CircuitStateChanged`
- **Consumer-Owned Repository & Gateway Interfaces (ISP + DIP):**
  - `DeliveryCommands` (`Save`, `ClaimDue`, `UpdateState`)
  - `DeliveryQueries` (`GetByID`, `GetByIdempotencyKey`, `ListDeadLetters`)
  - `EndpointReader` / `EndpointWriter` (`GetEndpoint`, `UpsertEndpoint`)
  - `WebhookDispatcher` (`Dispatch(ctx, targetURL, headers, payload)`)

### SOLID & OOD Mapping

| Principle | Enforcement in Sentinel Relay |
| :--- | :--- |
| **Single Responsibility (SRP)** | `Delivery` owns state transitions and retry math; `DispatcherService` owns worker pool orchestration; `HTTPTransport` owns JSON serialization and status codes. |
| **Open/Closed (OCP)** | Retry strategies (`BackoffCalculator`), payload signers (`Signer`), and circuit policies plug into `DeliveryEngine` via composition without modifying aggregate transition rules. |
| **Liskov Substitution (LSP)** | Both `memory.Store` and `sqlstore.Store` satisfy `DeliveryRepository` and `EndpointRepository` with identical concurrency, idempotency, and claim semantics verified by a shared contract test suite. |
| **Interface Segregation (ISP)** | Transport handlers depend only on narrow single-method use-case interfaces (`EnqueueUseCase`, `QueryDeliveryUseCase`, `ReplayDLQUseCase`), never a monolithic repository or god service. |
| **Dependency Inversion (DIP)** | `internal/domain` and `internal/application` define all interfaces; `internal/infrastructure` (`httpapi`, `sqlstore`, `webhookclient`, `telemetry`) depends inward on domain/application. |

### Directory Layout

```text
sentinel-relay/
├── go.mod
├── Makefile
├── Dockerfile
├── docker-compose.yml
├── README.md
├── GLOSSARY.md
├── cmd/
│   └── relayd/
│       └── main.go
├── internal/
│   ├── domain/
│   │   ├── errors.go
│   │   ├── value_objects.go
│   │   ├── endpoint.go
│   │   ├── delivery.go
│   │   ├── circuit.go
│   │   ├── events.go
│   │   ├── ports.go
│   │   └── domain_test.go
│   ├── application/
│   │   ├── errors.go
│   │   ├── usecases.go
│   │   ├── worker_pool.go
│   │   └── application_test.go
│   └── infrastructure/
│       ├── memory/
│       │   ├── store.go
│       │   └── store_test.go
│       ├── sqlstore/
│       │   ├── store.go
│       │   └── store_test.go
│       ├── httpclient/
│       │   └── dispatcher.go
│       ├── httpapi/
│       │   ├── server.go
│       │   └── server_test.go
│       └── observability/
│           ├── metrics.go
│           ├── ratelimiter.go
│           └── observability_test.go
└── docs/
    ├── adr/
    │   └── ADR-001-architecture-blueprint.md
    ├── 01-domain-and-solid.md
    ├── 02-core-engine-and-concurrency.md
    ├── 03-persistence-and-api.md
    └── 04-production-ops-and-benchmarks.md
```

## 3. Scalability & Feasibility Plan

1. **Stateless Compute & Horizontal Scaling:** `relayd` nodes hold no sticky session state. Multiple `relayd` instances scale horizontally behind any L4/L7 load balancer.
2. **Optimistic Lease Claiming (`ClaimDue`):** Workers poll for due deliveries (`status IN ('PENDING', 'RETRYING') AND next_attempt_at <= NOW()`) and atomically transition claimed rows to `IN_FLIGHT` with a lease timestamp (`FOR UPDATE SKIP LOCKED` in PostgreSQL / atomic transaction in SQLite WAL), preventing duplicate concurrent execution across replicas.
3. **Idempotency Guarantee:** A composite unique constraint on `(tenant_id, idempotency_key)` guarantees that upstream retries return the existing `Delivery` record without side effects.
4. **Database Indexing & Partitioning:**
   - Primary lookup index: `idx_deliveries_tenant_idempotency (tenant_id, idempotency_key) UNIQUE`
   - Scheduler hot-path partial index: `idx_deliveries_due (next_attempt_at, status) WHERE status IN ('PENDING', 'RETRYING')`
   - High-volume production deployments partition `deliveries` by time range (`created_at` monthly/weekly) and archive `SUCCEEDED` records after retention window expiry.

## 4. Karpathy Anti-Overengineering Audit

- **Zero external framework bloat:** Built strictly on Go's standard library (`net/http` Go 1.22 routing, `context`, `log/slog`, `crypto/hmac`, `crypto/sha256`, `sync/atomic`, `database/sql`).
- **No speculative abstractions:** Every interface (`DeliveryRepository`, `WebhookDispatcher`, `Clock`) has at least two concrete implementations (production adapter + deterministic test/memory adapter).
- **Explicit assumptions:** Payloads are valid JSON bytes (`<= 1 MiB`); HMAC signatures use `X-Sentinel-Signature-256` and `X-Sentinel-Timestamp`.
