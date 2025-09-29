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

// TestGenerateInference ensures Generate infers transactional when *sql.Tx used and non-tx when *sql.DB
func TestGenerateInference(t *testing.T) {
	db := openMySQL(t)
	options, cleanup := lstesting.FakeSchema(t, "")
	defer func() { cleanup(db) }()

	s := libschema.New(context.Background(), options)
	log := libschema.LogFromLog(t)
	dbase, _, err := lsmysql.New(log, "test", s, db)
	require.NoError(t, err)

	// Migration using *sql.Tx path (transactional expected)
	mTx := lsmysql.Generate[*sql.Tx]("G_TX", func(_ context.Context, _ *sql.Tx) string { return "SELECT 1" })
	// Migration using *sql.DB path (non-transactional expected)
	mDB := lsmysql.Generate[*sql.DB]("G_DB", func(_ context.Context, _ *sql.DB) string { return "SELECT 1" })

	dbase.Migrations("L1", mTx, mDB)
	require.NoError(t, s.Migrate(context.Background()))

	if mTx.Base().NonTransactional() {
		// Transactional inference failed
		// Provide clear diagnostic
		panic("Generate[*sql.Tx] incorrectly inferred non-transactional")
	}
	if !mDB.Base().NonTransactional() {
		panic("Generate[*sql.DB] did not infer non-transactional")
	}
}

// TestComputedInference mirrors TestGenerateInference for Computed
func TestComputedInference(t *testing.T) {
	db := openMySQL(t)
	options, cleanup := lstesting.FakeSchema(t, "")
	defer func() { cleanup(db) }()

	s := libschema.New(context.Background(), options)
	log := libschema.LogFromLog(t)
	dbase, _, err := lsmysql.New(log, "test", s, db)
	require.NoError(t, err)

	calledTx := false
	calledDB := false
	cTx := lsmysql.Computed[*sql.Tx]("C_TX", func(_ context.Context, _ *sql.Tx) error { calledTx = true; return nil })
	cDB := lsmysql.Computed[*sql.DB]("C_DB", func(_ context.Context, _ *sql.DB) error { calledDB = true; return nil })

	dbase.Migrations("L1", cTx, cDB)
	require.NoError(t, s.Migrate(context.Background()))

	if !calledTx || !calledDB {
		panic("computed migrations not both invoked")
	}
	if cTx.Base().NonTransactional() {
		panic("Computed[*sql.Tx] incorrectly inferred non-transactional")
	}
	if !cDB.Base().NonTransactional() {
		panic("Computed[*sql.DB] did not infer non-transactional")
	}
}

// TestForceOverride validates ForceTransactional / ForceNonTransactional override inference.
func TestForceOverride(t *testing.T) {
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

	if forcedTx.Base().NonTransactional() {
		panic("ForceTransactional failed to override non-tx inference")
	}
	if !forcedNonTx.Base().NonTransactional() {
		panic("ForceNonTransactional failed to override tx inference")
	}
	if !lastWins.Base().NonTransactional() {
		panic("last override (ForceNonTransactional) did not win")
	}
}
