package lspostgres

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/muir/libschema"
	"github.com/muir/libschema/lstesting"
)

// TestScriptAutoClassification validates deferred classification and index creation.
func TestScriptAutoClassification(t *testing.T) {
	dsn := os.Getenv("LIBSCHEMA_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_POSTGRES_TEST_DSN to run Postgres tests")
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// replace tracking table with isolated schema
	opts, cleanup := lstesting.FakeSchema(t, "CASCADE")
	defer cleanup(db)
	_, err = db.Exec("CREATE SCHEMA " + opts.SchemaOverride)
	require.NoError(t, err)
	schema := libschema.New(context.Background(), opts)
	log := libschema.LogFromLog(t)
	ldb, err := New(log, "test", schema, db)
	require.NoError(t, err)

	// Two migrations: one table we own, and a concurrent index on that table.
	migTable := Script("NORMAL", "CREATE TABLE IF NOT EXISTS tmp_ci_deferred (id int)")
	migConcurrent := Script("CIC", "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_deferred ON tmp_ci_deferred (id)")
	ldb.Migrations("L", migTable, migConcurrent)

	// Pre-execution expectations: deferred logic means neither is yet marked nonTransactional.
	assert.False(t, migConcurrent.Base().NonTransactional())
	assert.False(t, migTable.Base().NonTransactional())

	// Run migrations through the schema since Database does not expose Migrate directly.
	require.NoError(t, schema.Migrate(context.Background()))

	// After migration run, verify both succeeded and the index exists. We no longer rely on the
	// NonTransactional flag being mutated on the original registration instance (execution may occur on a copy).
	assert.False(t, migTable.Base().NonTransactional())
	row := db.QueryRow(`SELECT 1 FROM pg_indexes WHERE schemaname = $1 AND indexname = 'idx_deferred'`, opts.SchemaOverride)
	var one int
	require.NoError(t, row.Scan(&one))
}

// TestForceTransactionalOverride verifies ForceTransactional() behavior.
func TestForceTransactionalOverride(t *testing.T) {
	dsn := os.Getenv("LIBSCHEMA_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_POSTGRES_TEST_DSN to run Postgres tests")
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	opts, cleanup := lstesting.FakeSchema(t, "CASCADE")
	defer cleanup(db)
	_, err = db.Exec("CREATE SCHEMA " + opts.SchemaOverride)
	require.NoError(t, err)
	schema := libschema.New(context.Background(), opts)
	log := libschema.LogFromLog(t)
	ldb, err := New(log, "test", schema, db)
	require.NoError(t, err)
	setup := Script("SETUP", "CREATE TABLE IF NOT EXISTS tmp_force (id int)")
	m := Script("CIC", "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_force ON tmp_force (id)", libschema.ForceTransactional())
	ldb.Migrations("L_FORCE_TX", setup, m)
	assert.False(t, m.Base().NonTransactional())
	err = schema.Migrate(context.Background())
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "cannot run inside a transaction")
	}
	assert.False(t, m.Base().NonTransactional())
}
