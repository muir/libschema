package lssinglestore_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/muir/libschema"
	"github.com/muir/libschema/lssinglestore"
	"github.com/muir/libschema/lstesting"
)

func openSingleStore(t *testing.T) *sql.DB {
	dsn := os.Getenv("LIBSCHEMA_SINGLESTORE_TEST_DSN")
	if dsn == "" {
		// mirror pattern
		t.Skip("Set $LIBSCHEMA_SINGLESTORE_TEST_DSN to run SingleStore tests")
	}
	db, err := sql.Open("mysql", dsn) // SingleStore uses MySQL wire protocol
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestGenerateInference(t *testing.T) {
	t.Parallel()
	db := openSingleStore(t)
	options, cleanup := lstesting.FakeSchema(t, "")
	defer func() { cleanup(db) }()

	s := libschema.New(context.Background(), options)
	log := libschema.LogFromLog(t)
	dbase, _, err := lssinglestore.New(log, "test", s, db)
	require.NoError(t, err)

	mTx := lssinglestore.Generate[*sql.Tx]("G_TX", func(_ context.Context, _ *sql.Tx) string { return "SELECT 1" })
	mDB := lssinglestore.Generate[*sql.DB]("G_DB", func(_ context.Context, _ *sql.DB) string { return "SELECT 1" })

	dbase.Migrations("L1", mTx, mDB)
	require.NoError(t, s.Migrate(context.Background()))
	if mTx.Base().NonTransactional() {
		panic("Generate[*sql.Tx] incorrectly inferred non-tx (SingleStore)")
	}
	if !mDB.Base().NonTransactional() {
		panic("Generate[*sql.DB] did not infer non-tx (SingleStore)")
	}
}

func TestComputedInference(t *testing.T) {
	t.Parallel()
	db := openSingleStore(t)
	options, cleanup := lstesting.FakeSchema(t, "")
	defer func() { cleanup(db) }()

	s := libschema.New(context.Background(), options)
	log := libschema.LogFromLog(t)
	dbase, _, err := lssinglestore.New(log, "test", s, db)
	require.NoError(t, err)

	calledTx := false
	calledDB := false
	cTx := lssinglestore.Computed[*sql.Tx]("C_TX", func(_ context.Context, _ *sql.Tx) error { calledTx = true; return nil })
	cDB := lssinglestore.Computed[*sql.DB]("C_DB", func(_ context.Context, _ *sql.DB) error { calledDB = true; return nil })

	dbase.Migrations("L1", cTx, cDB)
	require.NoError(t, s.Migrate(context.Background()))
	if !calledTx || !calledDB {
		panic("computed migrations not both invoked (SingleStore)")
	}
	if cTx.Base().NonTransactional() {
		panic("Computed[*sql.Tx] incorrectly inferred non-tx (SingleStore)")
	}
	if !cDB.Base().NonTransactional() {
		panic("Computed[*sql.DB] did not infer non-tx (SingleStore)")
	}
}

func TestForceOverride(t *testing.T) {
	t.Parallel()
	db := openSingleStore(t)
	options, cleanup := lstesting.FakeSchema(t, "")
	defer func() { cleanup(db) }()

	s := libschema.New(context.Background(), options)
	log := libschema.LogFromLog(t)
	dbase, _, err := lssinglestore.New(log, "test", s, db)
	require.NoError(t, err)

	forcedTx := lssinglestore.Script("FORCE_TX", "SELECT 1", libschema.ForceTransactional())
	forcedNonTx := lssinglestore.Script("FORCE_NONTX", "SELECT 1", libschema.ForceNonTransactional())
	lastWins := lssinglestore.Script("LAST_WINS", "SELECT 1", libschema.ForceTransactional(), libschema.ForceNonTransactional())

	dbase.Migrations("L1", forcedTx, forcedNonTx, lastWins)
	require.NoError(t, s.Migrate(context.Background()))
	if forcedTx.Base().NonTransactional() {
		panic("ForceTransactional failed (SingleStore)")
	}
	if !forcedNonTx.Base().NonTransactional() {
		panic("ForceNonTransactional failed (SingleStore)")
	}
	if !lastWins.Base().NonTransactional() {
		panic("last override did not win (SingleStore)")
	}
}
