# Chapter 03: Persistence Adapters, HTTP Transport & Horizontal Scaling

[← Previous: 02. Core Engine & Concurrency](./02-core-engine-and-concurrency.md) | [Hub](../README.md) | [Next: 04. Production Ops & Benchmarks →](./04-production-ops-and-benchmarks.md)

## Prerequisites

- Completed [Chapter 02](./02-core-engine-and-concurrency.md)
- Familiarity with `database/sql` transactions and Go 1.22 `net/http` routing patterns

## 1. Atomic Lease Claiming & Partial Indexing (`sqlstore`)

When multiple `relayd` instances poll a shared database, two workers must never dispatch the same webhook simultaneously.

In `internal/infrastructure/sqlstore/store.go`, `ClaimDue` runs inside a database transaction:
1. It selects up to `limit` rows where `status IN ('PENDING', 'RETRYING') AND next_attempt_at <= ?`.
2. It executes an atomic conditional state transition:
   ```sql
   UPDATE deliveries
   SET status = 'IN_FLIGHT', updated_at = ?
   WHERE delivery_id = ? AND status IN ('PENDING', 'RETRYING');
   ```
3. Only rows where `RowsAffected() == 1` are returned to the worker pool.

To keep `ClaimDue` sub-millisecond even when the `deliveries` table holds millions of historical `SUCCEEDED` rows, `SchemaDDL` defines two partial indexes:

```sql
CREATE INDEX IF NOT EXISTS idx_deliveries_due
	ON deliveries (next_attempt_at, status)
	WHERE status IN ('PENDING', 'RETRYING');

CREATE INDEX IF NOT EXISTS idx_deliveries_dlq
	ON deliveries (tenant_id, updated_at DESC)
	WHERE status = 'DEAD_LETTERED';
```

## 2. Composite Idempotency Guarantee

Upstream billing or order services often retry `POST /v1/webhooks/enqueue` when network packets drop. `deliveries` enforces `UNIQUE (tenant_id, idempotency_key)`:
- On `Save(ctx, delivery)`, the store checks for an existing row matching `(tenant_id, idempotency_key)` inside the transaction.
- If found, `Save` returns `(existing, false, nil)`, and the HTTP layer responds with `200 OK` (`"idempotent_replay": true`) instead of `202 Accepted`.

## 3. Signed Outbound Webhook Dispatch (`httpclient`)

Customer webhook receivers must verify that incoming payloads originated from Sentinel Relay and were not replayed by an attacker. `httpclient.Dispatcher` (`internal/infrastructure/httpclient/dispatcher.go`) attaches four standard headers to every outbound `POST`:

| Header | Example Value | Purpose |
| :--- | :--- | :--- |
| `X-Sentinel-Delivery-ID` | `dlv_8f9a2b...` | Unique delivery identifier for downstream deduplication |
| `X-Sentinel-Event-Type` | `invoice.paid` | Event routing topic |
| `X-Sentinel-Timestamp` | `1700000000` | Unix epoch seconds included in the HMAC signed payload |
| `X-Sentinel-Signature-256` | `sha256=4a8b...` | HMAC-SHA256 over `<timestamp>.<raw_json_payload>` |

The response body is bounded with `io.LimitReader(resp.Body, 4096)` and drained to `io.Discard` so the underlying TCP/TLS connection returns cleanly to `http.Transport`'s idle connection pool (`MaxIdleConnsPerHost: 32`).

## 4. Go 1.22 Standard Library Routing (`httpapi`)

Go 1.22 enhanced `http.ServeMux` with method-prefixed patterns and path wildcards (`{delivery_id}`), eliminating the need for third-party routers like `chi` or `gin`:

```go
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
```

All request bodies are wrapped in `http.MaxBytesReader` (`64 KiB` for endpoint registration, `2 MiB` for webhook enqueue) to prevent memory exhaustion from oversized payloads.

## 5. Verification Checkpoint

Run the SQL schema and end-to-end HTTP integration tests (which spin up both a `relayd` test server and a simulated customer receiver verifying HMAC signatures):

```bash
go test -v -race ./internal/infrastructure/sqlstore/... ./internal/infrastructure/httpapi/...
```

Expected output:

```text
=== RUN   TestSchemaDDLIncludesIndexesAndConstraints
--- PASS: TestSchemaDDLIncludesIndexesAndConstraints (0.00s)
PASS
ok  	github.com/anti-gravity-bit/sentinel-relay/internal/infrastructure/sqlstore
=== RUN   TestEndToEndHTTPIntegrationAndSignatureVerification
--- PASS: TestEndToEndHTTPIntegrationAndSignatureVerification (0.00s)
PASS
ok  	github.com/anti-gravity-bit/sentinel-relay/internal/infrastructure/httpapi
```

[← Previous: 02. Core Engine & Concurrency](./02-core-engine-and-concurrency.md) | [Hub](../README.md) | [Next: 04. Production Ops & Benchmarks →](./04-production-ops-and-benchmarks.md)
