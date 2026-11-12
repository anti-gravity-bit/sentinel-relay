# Sentinel Relay — Ubiquitous Language Glossary (`GLOSSARY.md`)

| Canonical Term | Layer | Definition & Invariants |
| :--- | :--- | :--- |
| **Tenant** | Domain | An isolated organization (`TenantID`) that owns webhook `Endpoint` registrations and emits events. |
| **Endpoint** | Domain | A subscriber HTTPS target (`EndpointID`, `TargetURL`, `SecretKey`, `RateLimitRPS`) owned by a single `Tenant`. |
| **Delivery** | Domain | Aggregate root representing a single webhook dispatch lifecycle from `PENDING` to `SUCCEEDED` or `DEAD_LETTERED`. |
| **IdempotencyKey** | Domain | Client-supplied token unique per `TenantID` that prevents duplicate `Delivery` creation on upstream retries. |
| **Attempt** | Domain | A single HTTP `POST` execution of a `Delivery` against its `Endpoint.TargetURL`. |
| **BackoffPolicy** | Domain | Value object defining `InitialInterval`, `MaxInterval`, and `Multiplier` for exponential retry scheduling. |
| **Dead-Letter Queue (DLQ)** | Domain | Terminal state (`DEAD_LETTERED`) for deliveries that exhaust `MaxAttempts` or encounter non-retryable `4xx` errors (excluding `408`/`429`). |
| **CircuitBreaker** | Domain | Per-endpoint state machine (`Closed`, `Open`, `HalfOpen`) that halts dispatch to unhealthy endpoints during outages. |
| **ClaimLease** | Application / Infra | Atomic reservation that transitions due deliveries to `IN_FLIGHT` so concurrent worker pools never double-dispatch. |
| **Signature** | Domain | HMAC-SHA256 digest (`sha256=<hex>`) computed over `<unix_timestamp>.<raw_payload>` using the endpoint's `SecretKey`. |
