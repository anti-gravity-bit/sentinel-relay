package sqlstore_test

import (
	"strings"
	"testing"

	"github.com/anti-gravity-bit/sentinel-relay/internal/infrastructure/sqlstore"
)

func TestSchemaDDLIncludesIndexesAndConstraints(t *testing.T) {
	t.Parallel()

	requiredClauses := []string{
		"CREATE TABLE IF NOT EXISTS endpoints",
		"CREATE TABLE IF NOT EXISTS deliveries",
		"UNIQUE (tenant_id, idempotency_key)",
		"CREATE INDEX IF NOT EXISTS idx_deliveries_due",
		"CREATE INDEX IF NOT EXISTS idx_deliveries_dlq",
	}
	for _, clause := range requiredClauses {
		if !strings.Contains(sqlstore.SchemaDDL, clause) {
			t.Fatalf("SchemaDDL missing required clause: %q", clause)
		}
	}
}
