package lspostgres

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/muir/libschema"
)

// TestScriptAutoClassification validates that non-transactional determination for
// CREATE INDEX CONCURRENTLY is deferred until execution (after server version
// heuristics can be applied) and that ForceTransactional overrides still work.
func TestScriptAutoClassification(t *testing.T) {
	dsn := os.Getenv("LIBSCHEMA_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_POSTGRES_TEST_DSN to run Postgres tests")
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	schema := libschema.New(context.Background(), libschema.Options{TrackingTable: "public.libschema_migrations"})
	log := libschema.LogFromLog(t)
	ldb, err := New(log, "test", schema, db)
	require.NoError(t, err)

	// Two migrations: one table we own, and a concurrent index on that table.
	migTable := Script("NORMAL", "CREATE TABLE IF NOT EXISTS tmp_ci_deferred (id int)")
	migConcurrent := Script("CIC", "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_deferred ON tmp_ci_deferred (id)")
	ldb.Migrations("L", migTable, migConcurrent)

	// Pre-execution expectations: deferred logic means neither is yet marked nonTransactional.
	assert.False(t, migConcurrent.Base().NonTransactional(), "deferred classification means not set yet")
	assert.False(t, migTable.Base().NonTransactional(), "regular table create remains transactional")

	// Run migrations through the schema since Database does not expose Migrate directly.
	require.NoError(t, schema.Migrate(context.Background()), "migrations should succeed")

	// After migration run, verify both succeeded and the index exists. We no longer rely on the
	// NonTransactional flag being mutated on the original registration instance (execution may occur on a copy).
	assert.False(t, migTable.Base().NonTransactional(), "regular table create remains transactional (flag perspective)")
	row := db.QueryRow(`SELECT 1 FROM pg_indexes WHERE schemaname = $1 AND indexname = 'idx_deferred'`, "public")
	var one int
	require.NoError(t, row.Scan(&one), "expected idx_deferred to exist")
}

// TestForceTransactionalOverride verifies ForceTransactional() can override auto non-tx classification.
func TestForceTransactionalOverride(t *testing.T) {
	dsn := os.Getenv("LIBSCHEMA_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_POSTGRES_TEST_DSN to run Postgres tests")
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	schema := libschema.New(context.Background(), libschema.Options{TrackingTable: "public.libschema_migrations"})
	log := libschema.LogFromLog(t)
	ldb, err := New(log, "test", schema, db)
	require.NoError(t, err)

	// Create a user-owned table first, then attempt a concurrent index inside a forced transaction.
	setup := Script("SETUP", "CREATE TABLE IF NOT EXISTS tmp_force (id int)")
	m := Script("CIC", "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_force ON tmp_force (id)", libschema.ForceTransactional())
	// Use a distinct library name to avoid prior test state (so migration actually runs and errors)
	ldb.Migrations("L_FORCE_TX", setup, m)
	assert.False(t, m.Base().NonTransactional(), "pre-run should still defer classification")
	err = schema.Migrate(context.Background())
	if assert.Error(t, err, "expected error forcing CONCURRENTLY inside transaction") {
		assert.Contains(t, err.Error(), "cannot run inside a transaction", "expected transactional error about concurrent index")
	}
	assert.False(t, m.Base().NonTransactional(), "force transactional keeps it transactional even though it failed")
}

// TestTrackingSchemaTableInvalid ensures invalid tracking table produces an error.
func TestTrackingSchemaTableInvalid(t *testing.T) {
	// Directly exercise helper with invalid format (three-part name)
	d := &libschema.Database{Options: libschema.Options{TrackingTable: "a.b.c"}}
	_, _, err := trackingSchemaTable(d)
	assert.Error(t, err, "expected error for three-part tracking table name")
}

// TestServerVersionCaching ensures the SHOW server_version query only executes once.
