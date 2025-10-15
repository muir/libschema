package lspostgres_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/muir/libschema"
	"github.com/muir/libschema/lspostgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openPGNonTx(t *testing.T) *sql.DB {
	dsn := os.Getenv("LIBSCHEMA_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_POSTGRES_TEST_DSN to run Postgres tests")
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestNonTxSchemaOverride ensures search_path applied for non-transactional execution.
func TestNonTxSchemaOverride(t *testing.T) {
	db := openPGNonTx(t)
	s := libschema.New(context.Background(), libschema.Options{SchemaOverride: "public"})
	log := libschema.LogFromLog(t)
	dbase, err := lspostgres.New(log, "ntx_schema", s, db)
	require.NoError(t, err)
	// MustNonTx: CREATE INDEX CONCURRENTLY
	table := lspostgres.Script("TAB", "CREATE TABLE IF NOT EXISTS ntx_tab (id int)")
	idx := lspostgres.Script("IDX", "CREATE INDEX CONCURRENTLY IF NOT EXISTS ntx_idx ON ntx_tab (id)")
	dbase.Migrations("NTX_LIB", table, idx)
	require.NoError(t, s.Migrate(context.Background()))
	// Validate index exists
	row := db.QueryRow(`SELECT 1 FROM pg_indexes WHERE indexname = 'ntx_idx' AND schemaname = $1`, "public")
	var one int
	require.NoError(t, row.Scan(&one))
}

// TestNonTxMultiStatementError verifies multi-statement script downgraded to non-tx errors.
func TestNonTxMultiStatementError(t *testing.T) {
	db := openPGNonTx(t)
	s := libschema.New(context.Background(), libschema.Options{})
	log := libschema.LogFromLog(t)
	dbase, err := lspostgres.New(log, "ntx_multi", s, db)
	require.NoError(t, err)
	tab := lspostgres.Script("T", "CREATE TABLE IF NOT EXISTS mtab (id int)")
	// Two concurrent index statements -> must non-tx but multi causes error
	bad := lspostgres.Script("BAD", "CREATE INDEX CONCURRENTLY IF NOT EXISTS m1 ON mtab(id); CREATE INDEX CONCURRENTLY IF NOT EXISTS m2 ON mtab(id)")
	dbase.Migrations("NTX_MULTI", tab, bad)
	err = s.Migrate(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must contain exactly one SQL statement")
}

// TestNonTxNonIdempotentEasyFixError missing IF EXISTS on easy-fix ddl
func TestNonTxNonIdempotentEasyFixError(t *testing.T) {
	db := openPGNonTx(t)
	s := libschema.New(context.Background(), libschema.Options{})
	log := libschema.LogFromLog(t)
	dbase, err := lspostgres.New(log, "ntx_easy", s, db)
	require.NoError(t, err)
	tab := lspostgres.Script("T", "CREATE TABLE IF NOT EXISTS etab (id int)")
	// Non-idempotent easy-fix (CREATE INDEX CONCURRENTLY without IF NOT EXISTS)
	bad := lspostgres.Script("BAD", "CREATE INDEX CONCURRENTLY eidx ON etab(id)")
	dbase.Migrations("NTX_EASY", tab, bad)
	err = s.Migrate(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing IF [NOT] EXISTS")
}

// TestNonTxNonIdempotentGenericError generic non-idempotent without easy fix.
// Removed fragile generic non-idempotent execution test (CREATE DATABASE). Classification coverage exists elsewhere.

// TestNonTxSuccessIdempotent verifies a successful non-transactional idempotent migration.
func TestNonTxSuccessIdempotent(t *testing.T) {
	db := openPGNonTx(t)
	s := libschema.New(context.Background(), libschema.Options{})
	log := libschema.LogFromLog(t)
	dbase, err := lspostgres.New(log, "ntx_success", s, db)
	require.NoError(t, err)
	tab := lspostgres.Script("T", "CREATE TABLE IF NOT EXISTS st_tab (id int)")
	// Idempotent DDL (guarded table create) should execute non-transactionally only if classification downgrades (it doesn't here), so force non-tx via CONCURRENTLY index guarded
	idx := lspostgres.Script("IDX", "CREATE INDEX CONCURRENTLY IF NOT EXISTS st_idx ON st_tab (id)")
	dbase.Migrations("NTX_SUCCESS", tab, idx)
	require.NoError(t, s.Migrate(context.Background()))
	// Validate index exists
	row := db.QueryRow(`SELECT 1 FROM pg_indexes WHERE indexname = 'st_idx' AND schemaname = $1`, "public")
	var one int
	require.NoError(t, row.Scan(&one))
}

// TestEmptyNonTxScript ensures empty non-tx script is a no-op and marks migration done.
// (TestEmptyNonTxScript removed: empty scripts no longer required to be supported.)

// TestAdjustNonTxVersion ensures version-specific pruning logic behaves as expected.
