package lspostgres_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/muir/libschema"
	"github.com/muir/libschema/lspostgres"
	"github.com/muir/libschema/lstesting"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests exercise the Postgres driver's automatic classification of certain
// statements as non-transactional, the enforcement of single-statement and
// idempotency rules for those statements, and the ability to register custom
// non-transactional patterns.

// helper to open db & schema options
func openTestDB(t *testing.T) (*sql.DB, libschema.Options, func(*sql.DB)) {
	dsn := sharedGetDSN(t)
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	opts, cleanup := lstesting.FakeSchema(t, "CASCADE")
	return db, opts, cleanup
}

func TestPostgresNonTx_CreateIndexConcurrently_IdempotencyMissing(t *testing.T) {
	db, opts, cleanup := openTestDB(t)
	defer func() { cleanup(db); assert.NoError(t, db.Close()) }()

	s := libschema.New(context.Background(), opts)
	dbase, err := lspostgres.New(libschema.LogFromLog(t), "test", s, db)
	require.NoError(t, err)

	dbase.Migrations("L1",
		lspostgres.Script("T1", `CREATE TABLE t1 (id int)`),
		// Missing IF NOT EXISTS should trigger idempotency enforcement error prior to execution
		lspostgres.Script("IDX", `CREATE INDEX CONCURRENTLY idx_t1_id ON t1 (id)`),
	)

	err = s.Migrate(context.Background())
	if assert.Error(t, err) {
		assert.ErrorIs(t, err, libschema.ErrNonIdempotentNonTx)
	}
}

func TestPostgresNonTx_CreateIndexConcurrently_Success(t *testing.T) {
	db, opts, cleanup := openTestDB(t)
	defer func() { cleanup(db); assert.NoError(t, db.Close()) }()

	s := libschema.New(context.Background(), opts)
	dbase, err := lspostgres.New(libschema.LogFromLog(t), "test", s, db)
	require.NoError(t, err)

	dbase.Migrations("L1",
		lspostgres.Script("T1", `CREATE TABLE t1 (id int)`),
		lspostgres.Script("IDX", `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_t1_id ON t1 (id)`),
	)

	err = s.Migrate(context.Background())
	require.NoError(t, err)

	// Verify index exists
	rows, err := db.Query(`SELECT indexname FROM pg_indexes WHERE schemaname = $1 AND tablename = 't1'`, opts.SchemaOverride)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var found bool
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		if name == "idx_t1_id" {
			found = true
		}
	}
	assert.True(t, found, "expected idx_t1_id to exist")
}

func TestPostgresNonTx_DropIndexConcurrently_IdempotencyMissing(t *testing.T) {
	db, opts, cleanup := openTestDB(t)
	defer func() { cleanup(db); assert.NoError(t, db.Close()) }()

	s := libschema.New(context.Background(), opts)
	dbase, err := lspostgres.New(libschema.LogFromLog(t), "test", s, db)
	require.NoError(t, err)

	dbase.Migrations("L1",
		lspostgres.Script("T1", `CREATE TABLE t1 (id int)`),
		lspostgres.Script("IDX", `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_t1_id ON t1 (id)`),
		// Missing IF EXISTS on drop should error
		lspostgres.Script("DROPIDX", `DROP INDEX CONCURRENTLY idx_t1_id`),
	)

	err = s.Migrate(context.Background())
	if assert.Error(t, err) {
		assert.ErrorIs(t, err, libschema.ErrNonIdempotentNonTx)
	}
}

func TestPostgresNonTx_DropIndexConcurrently_Success(t *testing.T) {
	db, opts, cleanup := openTestDB(t)
	defer func() { cleanup(db); assert.NoError(t, db.Close()) }()

	s := libschema.New(context.Background(), opts)
	dbase, err := lspostgres.New(libschema.LogFromLog(t), "test", s, db)
	require.NoError(t, err)

	dbase.Migrations("L1",
		lspostgres.Script("T1", `CREATE TABLE t1 (id int)`),
		lspostgres.Script("IDX", `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_t1_id ON t1 (id)`),
		lspostgres.Script("DROPIDX", `DROP INDEX CONCURRENTLY IF EXISTS idx_t1_id`),
	)

	err = s.Migrate(context.Background())
	require.NoError(t, err)
}

func TestPostgresNonTx_SingleStatementEnforced(t *testing.T) {
	db, opts, cleanup := openTestDB(t)
	defer func() { cleanup(db); assert.NoError(t, db.Close()) }()

	s := libschema.New(context.Background(), opts)
	dbase, err := lspostgres.New(libschema.LogFromLog(t), "test", s, db)
	require.NoError(t, err)

	dbase.Migrations("L1",
		lspostgres.Script("T1", `CREATE TABLE t1 (id int)`),
		// multiple statements in auto non-tx migration -> should error
		lspostgres.Script("IDX", `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_t1_id ON t1 (id); SELECT 1`),
	)

	err = s.Migrate(context.Background())
	if assert.Error(t, err) {
		assert.ErrorIs(t, err, libschema.ErrNonTxMultipleStatements)
	}
}

func TestPostgresNonTx_ForceOptionVacuum(t *testing.T) {
	db, opts, cleanup := openTestDB(t)
	defer func() { cleanup(db); assert.NoError(t, db.Close()) }()

	s := libschema.New(context.Background(), opts)
	dbase, err := lspostgres.New(libschema.LogFromLog(t), "test", s, db)
	require.NoError(t, err)

	dbase.Migrations("L1",
		lspostgres.Script("T1", `CREATE TABLE t1 (id int)`),
		// Force classification since VACUUM cannot run in a transaction, but we removed pattern registration.
		lspostgres.Script("VAC", `VACUUM ANALYZE`, libschema.ForceNonTransactional()),
	)

	err = s.Migrate(context.Background())
	require.NoError(t, err)
}
