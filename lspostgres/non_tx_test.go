package lspostgres

import (
	"context"
	"database/sql"
	"os"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/muir/libschema"
	"github.com/muir/libschema/lstesting"
)

// These tests exercise the Postgres driver's automatic classification of certain
// statements as non-transactional, the enforcement of single-statement and
// idempotency rules for those statements, and the ability to register custom
// non-transactional patterns.

// helper to open db & schema options
func openTestDB(t *testing.T) (*sql.DB, libschema.Options, func(*sql.DB)) {
	dsn := os.Getenv("LIBSCHEMA_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_POSTGRES_TEST_DSN to run Postgres tests")
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	opts, cleanup := lstesting.FakeSchema(t, "CASCADE")
	return db, opts, cleanup
}

func TestPostgresNonTx_CreateIndexConcurrently_IdempotencyMissing(t *testing.T) {
	db, opts, cleanup := openTestDB(t)
	defer func() { cleanup(db); assert.NoError(t, db.Close()) }()

	s := libschema.New(context.Background(), opts)
	dbase, err := New(libschema.LogFromLog(t), "test", s, db)
	require.NoError(t, err)

	dbase.Migrations("L1",
		Script("T1", `CREATE TABLE t1 (id int)`),
		// Missing IF NOT EXISTS should trigger idempotency enforcement error prior to execution
		Script("IDX", `CREATE INDEX CONCURRENTLY idx_t1_id ON t1 (id)`),
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
	dbase, err := New(libschema.LogFromLog(t), "test", s, db)
	require.NoError(t, err)

	dbase.Migrations("L1",
		Script("T1", `CREATE TABLE t1 (id int)`),
		Script("IDX", `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_t1_id ON t1 (id)`),
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
	dbase, err := New(libschema.LogFromLog(t), "test", s, db)
	require.NoError(t, err)

	dbase.Migrations("L1",
		Script("T1", `CREATE TABLE t1 (id int)`),
		Script("IDX", `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_t1_id ON t1 (id)`),
		// Missing IF EXISTS on drop should error
		Script("DROPIDX", `DROP INDEX CONCURRENTLY idx_t1_id`),
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
	dbase, err := New(libschema.LogFromLog(t), "test", s, db)
	require.NoError(t, err)

	dbase.Migrations("L1",
		Script("T1", `CREATE TABLE t1 (id int)`),
		Script("IDX", `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_t1_id ON t1 (id)`),
		Script("DROPIDX", `DROP INDEX CONCURRENTLY IF EXISTS idx_t1_id`),
	)

	err = s.Migrate(context.Background())
	require.NoError(t, err)
}

func TestPostgresNonTx_SingleStatementEnforced(t *testing.T) {
	db, opts, cleanup := openTestDB(t)
	defer func() { cleanup(db); assert.NoError(t, db.Close()) }()

	s := libschema.New(context.Background(), opts)
	dbase, err := New(libschema.LogFromLog(t), "test", s, db)
	require.NoError(t, err)

	dbase.Migrations("L1",
		Script("T1", `CREATE TABLE t1 (id int)`),
		// multiple statements in auto non-tx migration -> should error
		Script("IDX", `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_t1_id ON t1 (id); SELECT 1`),
	)

	err = s.Migrate(context.Background())
	if assert.Error(t, err) {
		assert.ErrorIs(t, err, libschema.ErrNonTxMultipleStatements)
	}
}

func TestPostgresNonTx_ForceOptionSimpleSelect(t *testing.T) {
	db, opts, cleanup := openTestDB(t)
	defer func() { cleanup(db); assert.NoError(t, db.Close()) }()

	s := libschema.New(context.Background(), opts)
	dbase, err := New(libschema.LogFromLog(t), "test", s, db)
	require.NoError(t, err)

	dbase.Migrations("L1",
		Script("T1", `CREATE TABLE t1 (id int)`),
		// Force a normally transactional statement (SELECT 1) to run non-transactionally.
		Script("FORCE", `SELECT 1`, libschema.ForceNonTransactional()),
	)
	err = s.Migrate(context.Background())
	require.NoError(t, err)
}

// TestEmptyNonTxScript ensures empty non-tx script is a no-op and marks migration done.
func TestEmptyNonTxScript(t *testing.T) {
	db, opts, cleanup := openTestDB(t)
	defer func() { cleanup(db); assert.NoError(t, db.Close()) }()

	s := libschema.New(context.Background(), opts)
	dbase, err := New(libschema.LogFromLog(t), "test", s, db)
	require.NoError(t, err)
	dbase.Migrations("L1", Script("EMPTY", ""))
	require.NoError(t, s.Migrate(context.Background()))
	m, ok := dbase.Lookup(libschema.MigrationName{Library: "L1", Name: "EMPTY"})
	if !ok || !m.Base().Status().Done {
		t.Fatalf("empty migration not marked done")
	}
}

// TestAdjustNonTxVersion ensures version-specific pruning logic behaves as expected.
func TestAdjustNonTxVersion(t *testing.T) {
	p := &Postgres{nonTxStmtRegex: append([]*regexp.Regexp{}, baseNonTxStmtRegex...)}
	original := len(p.nonTxStmtRegex)
	if original == 0 {
		t.Fatal("expected baseNonTxStmtRegex to have patterns")
	}
	p.adjustNonTxForVersion(12)
	if len(p.nonTxStmtRegex) >= original {
		t.Fatalf("expected fewer patterns after version adjust (orig=%d now=%d)", original, len(p.nonTxStmtRegex))
	}
}
