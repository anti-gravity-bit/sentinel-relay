package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/sentinel-engine/relay/internal/domain"
)

// SchemaDDL defines the relational tables and partial indexes for PostgreSQL / SQLite WAL deployments.
const SchemaDDL = `
CREATE TABLE IF NOT EXISTS endpoints (
	tenant_id       TEXT NOT NULL,
	endpoint_id     TEXT NOT NULL,
	target_url      TEXT NOT NULL,
	secret_key      TEXT NOT NULL,
	active           BOOLEAN NOT NULL DEFAULT TRUE,
	rate_limit_rps  INTEGER NOT NULL DEFAULT 50,
	max_concurrency INTEGER NOT NULL DEFAULT 10,
	created_at       TIMESTAMP NOT NULL,
	updated_at       TIMESTAMP NOT NULL,
	PRIMARY KEY (tenant_id, endpoint_id)
);

CREATE TABLE IF NOT EXISTS deliveries (
	delivery_id      TEXT PRIMARY KEY,
	tenant_id        TEXT NOT NULL,
	endpoint_id      TEXT NOT NULL,
	idempotency_key  TEXT NOT NULL,
	event_type       TEXT NOT NULL,
	payload          BLOB NOT NULL,
	status           TEXT NOT NULL,
	attempt_count    INTEGER NOT NULL DEFAULT 0,
	max_attempts     INTEGER NOT NULL DEFAULT 5,
	next_attempt_at  TIMESTAMP NOT NULL,
	last_status_code INTEGER NOT NULL DEFAULT 0,
	last_error       TEXT NOT NULL DEFAULT '',
	created_at       TIMESTAMP NOT NULL,
	updated_at       TIMESTAMP NOT NULL,
	UNIQUE (tenant_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_deliveries_due
	ON deliveries (next_attempt_at, status)
	WHERE status IN ('PENDING', 'RETRYING');

CREATE INDEX IF NOT EXISTS idx_deliveries_dlq
	ON deliveries (tenant_id, updated_at DESC)
	WHERE status = 'DEAD_LETTERED';
`

// Store implements domain.EndpointRepository and domain.DeliveryRepository backed by database/sql.
type Store struct {
	db *sql.DB
}

func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, SchemaDDL)
	return err
}

func (s *Store) UpsertEndpoint(ctx context.Context, ep *domain.Endpoint) error {
	const q = `
	INSERT INTO endpoints (
		tenant_id, endpoint_id, target_url, secret_key, active,
		rate_limit_rps, max_concurrency, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT (tenant_id, endpoint_id) DO UPDATE SET
		target_url = excluded.target_url,
		secret_key = excluded.secret_key,
		active = excluded.active,
		rate_limit_rps = excluded.rate_limit_rps,
		max_concurrency = excluded.max_concurrency,
		updated_at = excluded.updated_at;
	`
	_, err := s.db.ExecContext(ctx, q,
		string(ep.TenantID),
		string(ep.ID),
		ep.TargetURL,
		string(ep.Secret),
		ep.Active,
		ep.RateLimitRPS,
		ep.MaxConcurrency,
		ep.CreatedAt.UTC(),
		ep.UpdatedAt.UTC(),
	)
	return err
}

func (s *Store) GetEndpoint(ctx context.Context, tenantID domain.TenantID, endpointID domain.EndpointID) (*domain.Endpoint, error) {
	const q = `
	SELECT tenant_id, endpoint_id, target_url, secret_key, active,
	       rate_limit_rps, max_concurrency, created_at, updated_at
	FROM endpoints
	WHERE tenant_id = ? AND endpoint_id = ?;
	`
	var ep domain.Endpoint
	var tID, eID, sec string
	err := s.db.QueryRowContext(ctx, q, string(tenantID), string(endpointID)).Scan(
		&tID, &eID, &ep.TargetURL, &sec, &ep.Active,
		&ep.RateLimitRPS, &ep.MaxConcurrency, &ep.CreatedAt, &ep.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrEndpointNotFound
	}
	if err != nil {
		return nil, err
	}
	ep.TenantID = domain.TenantID(tID)
	ep.ID = domain.EndpointID(eID)
	ep.Secret = domain.SecretKey(sec)
	return &ep, nil
}

func (s *Store) Save(ctx context.Context, d *domain.Delivery) (*domain.Delivery, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback() }()

	const checkQ = `
	SELECT delivery_id, tenant_id, endpoint_id, idempotency_key, event_type,
	       payload, status, attempt_count, max_attempts, next_attempt_at,
	       last_status_code, last_error, created_at, updated_at
	FROM deliveries
	WHERE tenant_id = ? AND idempotency_key = ?;
	`
	existing, err := scanDeliveryRow(tx.QueryRowContext(ctx, checkQ, string(d.TenantID), string(d.IdempotencyKey)))
	if err == nil {
		_ = tx.Commit()
		return existing, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, false, err
	}

	const insertQ = `
	INSERT INTO deliveries (
		delivery_id, tenant_id, endpoint_id, idempotency_key, event_type,
		payload, status, attempt_count, max_attempts, next_attempt_at,
		last_status_code, last_error, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
	`
	_, err = tx.ExecContext(ctx, insertQ,
		string(d.ID),
		string(d.TenantID),
		string(d.EndpointID),
		string(d.IdempotencyKey),
		d.EventType,
		d.Payload,
		string(d.Status),
		d.AttemptCount,
		d.MaxAttempts,
		d.NextAttemptAt.UTC(),
		d.LastStatusCode,
		d.LastError,
		d.CreatedAt.UTC(),
		d.UpdatedAt.UTC(),
	)
	if err != nil {
		return nil, false, err
	}
	if err := tx.Commit(); err != nil {
		return nil, false, err
	}
	return d.Clone(), true, nil
}

func (s *Store) ClaimDue(ctx context.Context, now time.Time, limit int) ([]*domain.Delivery, error) {
	if limit <= 0 {
		limit = 50
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	const selectQ = `
	SELECT delivery_id, tenant_id, endpoint_id, idempotency_key, event_type,
	       payload, status, attempt_count, max_attempts, next_attempt_at,
	       last_status_code, last_error, created_at, updated_at
	FROM deliveries
	WHERE status IN ('PENDING', 'RETRYING') AND next_attempt_at <= ?
	ORDER BY next_attempt_at ASC, delivery_id ASC
	LIMIT ?;
	`
	rows, err := tx.QueryContext(ctx, selectQ, now.UTC(), limit)
	if err != nil {
		return nil, err
	}
	var candidates []*domain.Delivery
	for rows.Next() {
		d, err := scanDeliveryRows(rows)
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		candidates = append(candidates, d)
	}
	_ = rows.Close()

	const updateClaimQ = `
	UPDATE deliveries
	SET status = 'IN_FLIGHT', updated_at = ?
	WHERE delivery_id = ? AND status IN ('PENDING', 'RETRYING');
	`
	claimed := make([]*domain.Delivery, 0, len(candidates))
	for _, d := range candidates {
		res, err := tx.ExecContext(ctx, updateClaimQ, now.UTC(), string(d.ID))
		if err != nil {
			return nil, err
		}
		affected, _ := res.RowsAffected()
		if affected == 1 {
			_ = d.MarkInFlight(now.UTC())
			claimed = append(claimed, d)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return claimed, nil
}

func (s *Store) Update(ctx context.Context, d *domain.Delivery) error {
	const q = `
	UPDATE deliveries
	SET status = ?, attempt_count = ?, next_attempt_at = ?,
	    last_status_code = ?, last_error = ?, updated_at = ?
	WHERE delivery_id = ?;
	`
	res, err := s.db.ExecContext(ctx, q,
		string(d.Status),
		d.AttemptCount,
		d.NextAttemptAt.UTC(),
		d.LastStatusCode,
		d.LastError,
		d.UpdatedAt.UTC(),
		string(d.ID),
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return domain.ErrDeliveryNotFound
	}
	return nil
}

func (s *Store) GetByID(ctx context.Context, tenantID domain.TenantID, deliveryID domain.DeliveryID) (*domain.Delivery, error) {
	const q = `
	SELECT delivery_id, tenant_id, endpoint_id, idempotency_key, event_type,
	       payload, status, attempt_count, max_attempts, next_attempt_at,
	       last_status_code, last_error, created_at, updated_at
	FROM deliveries
	WHERE tenant_id = ? AND delivery_id = ?;
	`
	d, err := scanDeliveryRow(s.db.QueryRowContext(ctx, q, string(tenantID), string(deliveryID)))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrDeliveryNotFound
	}
	return d, err
}

func (s *Store) ListDeadLetters(ctx context.Context, tenantID domain.TenantID, limit int) ([]*domain.Delivery, error) {
	if limit <= 0 {
		limit = 50
	}
	const q = `
	SELECT delivery_id, tenant_id, endpoint_id, idempotency_key, event_type,
	       payload, status, attempt_count, max_attempts, next_attempt_at,
	       last_status_code, last_error, created_at, updated_at
	FROM deliveries
	WHERE tenant_id = ? AND status = 'DEAD_LETTERED'
	ORDER BY updated_at DESC
	LIMIT ?;
	`
	rows, err := s.db.QueryContext(ctx, q, string(tenantID), limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []*domain.Delivery
	for rows.Next() {
		d, err := scanDeliveryRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanDeliveryRow(r rowScanner) (*domain.Delivery, error) {
	var d domain.Delivery
	var id, tID, eID, idem, status string
	err := r.Scan(
		&id, &tID, &eID, &idem, &d.EventType,
		&d.Payload, &status, &d.AttemptCount, &d.MaxAttempts, &d.NextAttemptAt,
		&d.LastStatusCode, &d.LastError, &d.CreatedAt, &d.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	d.ID = domain.DeliveryID(id)
	d.TenantID = domain.TenantID(tID)
	d.EndpointID = domain.EndpointID(eID)
	d.IdempotencyKey = domain.IdempotencyKey(idem)
	d.Status = domain.DeliveryStatus(status)
	return &d, nil
}

func scanDeliveryRows(rows *sql.Rows) (*domain.Delivery, error) {
	return scanDeliveryRow(rows)
}
