package lspostgres_test

import (
    "context"
    "database/sql"
    "os"
    "testing"

    "github.com/stretchr/testify/require"

    "github.com/muir/libschema"
    "github.com/muir/libschema/lspostgres"
)

// TestForceMismatchGenerate ensures creationErr is surfaced when generic tx expectation
// conflicts with a ForceNonTransactional override (and vice versa).
func TestForceMismatchGenerate(t *testing.T) {
    dsn := os.Getenv("LIBSCHEMA_POSTGRES_TEST_DSN")
    if dsn == "" { t.Skip("Set $LIBSCHEMA_POSTGRES_TEST_DSN to run Postgres tests") }
    db, err := sql.Open("postgres", dsn)
    require.NoError(t, err)
    t.Cleanup(func(){ _ = db.Close() })

    s := libschema.New(context.Background(), libschema.Options{})
    log := libschema.LogFromLog(t)
    dbase, err := lspostgres.New(log, "force_mismatch", s, db)
    require.NoError(t, err)

    g1 := lspostgres.Generate[*sql.Tx]("G_TX_FORCED_NONTX", func(_ context.Context, _ *sql.Tx) string { return "SELECT 1" }, libschema.ForceNonTransactional())
    g2 := lspostgres.Generate[*sql.DB]("G_DB_FORCED_TX", func(_ context.Context, _ *sql.DB) string { return "SELECT 1" }, libschema.ForceTransactional())

    dbase.Migrations("LIB", g1, g2)
    err = s.Migrate(context.Background())
    if err == nil { t.Fatalf("expected migration error due to force mismatch, got nil") }
}
