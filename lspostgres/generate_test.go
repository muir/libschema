package lspostgres_test

import (
    "context"
    "database/sql"
    "os"
    "testing"
    "github.com/stretchr/testify/require"
    "github.com/stretchr/testify/assert"
    "github.com/muir/libschema"
    "github.com/muir/libschema/lspostgres"
)

// helper to open or skip
func openPG(t *testing.T) *sql.DB {
    dsn := os.Getenv("LIBSCHEMA_POSTGRES_TEST_DSN")
    if dsn == "" { t.Skip("Set $LIBSCHEMA_POSTGRES_TEST_DSN to run Postgres tests") }
    db, err := sql.Open("postgres", dsn)
    require.NoError(t, err)
    t.Cleanup(func() { _ = db.Close() })
    return db
}

// TestGenerateSuccess ensures Generate executes produced SQL transactionally.
func TestGenerateSuccess(t *testing.T) {
    db := openPG(t)
    s := libschema.New(context.Background(), libschema.Options{})
    log := libschema.LogFromLog(t)
    dbase, err := lspostgres.New(log, "gen_success", s, db)
    require.NoError(t, err)

    mig := lspostgres.Generate("GEN_TABLE", func(_ context.Context, _ *sql.Tx) string { return "CREATE TABLE IF NOT EXISTS gen_table (id int)" })
    dbase.Migrations("GEN_LIB", mig)
    require.NoError(t, s.Migrate(context.Background()))
    assert.False(t, mig.Base().NonTransactional(), "Generate should remain transactional")
}

// TestGenerateEError ensures an error returned by the generator bubbles up and records status.
func TestGenerateEError(t *testing.T) {
    db := openPG(t)
    s := libschema.New(context.Background(), libschema.Options{})
    log := libschema.LogFromLog(t)
    dbase, err := lspostgres.New(log, "gen_error", s, db)
    require.NoError(t, err)

    genErr := assert.AnError
    mig := lspostgres.GenerateE("GEN_ERR", func(_ context.Context, _ *sql.Tx) (string, error) { return "", genErr })
    dbase.Migrations("GEN_ERR_LIB", mig)
    err = s.Migrate(context.Background())
    require.Error(t, err, "expected migration to fail due to generator error")
    stored, ok := dbase.Lookup(libschema.MigrationName{Library: "GEN_ERR_LIB", Name: "GEN_ERR"})
    require.True(t, ok)
    st := stored.Base().Status()
    assert.False(t, st.Done)
    assert.NotEmpty(t, st.Error)
}
