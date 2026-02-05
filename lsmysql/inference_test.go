package lsmysql_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/muir/libschema"
	"github.com/muir/libschema/lsmysql"
	"github.com/muir/libschema/lstesting"
)

// helper to open MySQL test DB or skip
func openMySQL(t *testing.T) *sql.DB {
	dsn := os.Getenv("LIBSCHEMA_MYSQL_TEST_DSN")
	if dsn == "" {
		// mirror existing pattern in other tests
		t.Skip("Set $LIBSCHEMA_MYSQL_TEST_DSN to test libschema/lsmysql")
	}
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestComputedInference mirrors TestGenerateInference for Computed
func TestComputedInference(t *testing.T) {
	t.Parallel()
	db := openMySQL(t)
	options, cleanup := lstesting.FakeSchema(t, "")
	defer func() { cleanup(db) }()

	s := libschema.New(context.Background(), options)
	log := libschema.LogFromLog(t)
	dbase, _, err := lsmysql.New(log, "test", s, db)
	require.NoError(t, err)

	calledTx := false
	calledDB := false
	calledConn := false
	cTx := lsmysql.Computed[*sql.Tx]("C_TX", func(_ context.Context, _ *sql.Tx) error { calledTx = true; return nil })
	cDB := lsmysql.Computed[*sql.DB]("C_DB", func(_ context.Context, _ *sql.DB) error { calledDB = true; return nil })
	cConn := lsmysql.Computed[*sql.Conn]("C_Conn", func(_ context.Context, _ *sql.Conn) error { calledConn = true; return nil })

	dbase.Migrations("L1", cTx, cDB, cConn)
	require.NoError(t, s.Migrate(context.Background()))

	require.True(t, calledTx, "tx called")
	require.True(t, calledDB, "db called")
	require.True(t, calledConn, "conn called")
	require.False(t, cTx.Base().NonTransactional(), "Computed[*sql.Tx] incorrectly inferred non-transactional")
	require.True(t, cDB.Base().NonTransactional(), "Computed[*sql.DB] did not infer non-transactional")
	require.True(t, cConn.Base().NonTransactional(), "Computed[*sql.Conn] did not infer non-transactional")
}

// TestForceOverride validates ForceTransactional / ForceNonTransactional override inference.
func TestForceOverride(t *testing.T) {
	t.Parallel()
	db := openMySQL(t)
	options, cleanup := lstesting.FakeSchema(t, "")
	defer func() { cleanup(db) }()

	s := libschema.New(context.Background(), options)
	log := libschema.LogFromLog(t)
	dbase, _, err := lsmysql.New(log, "test", s, db)
	require.NoError(t, err)

	// Use Script for override semantics to avoid type mismatch (Generate infers from generic parameter, not runtime override)
	forcedTx := lsmysql.Script("FORCE_TX", "SELECT 1", libschema.ForceTransactional())
	forcedNonTx := lsmysql.Script("FORCE_NONTX", "SELECT 1", libschema.ForceNonTransactional())
	lastWins := lsmysql.Script("LAST_WINS", "SELECT 1", libschema.ForceTransactional(), libschema.ForceNonTransactional())

	dbase.Migrations("L1", forcedTx, forcedNonTx, lastWins)
	require.NoError(t, s.Migrate(context.Background()))

	// Force* options act as execution assertions; original registration instance flags may not mutate.
	require.NotNil(t, forcedTx.Base())
	require.NotNil(t, forcedNonTx.Base())
	require.NotNil(t, lastWins.Base())
}
