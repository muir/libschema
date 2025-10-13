package lsmysql_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/muir/libschema"
	"github.com/muir/libschema/lsmysql"
)

// TestGenerateMismatch ensures a Generate[*sql.Tx] + ForceNonTransactional records a creationErr surfaced at execution.
func TestGenerateMismatch(t *testing.T) {
	t.Parallel()
	dsn := os.Getenv("LIBSCHEMA_TEST_MYSQL_DSN")
	if dsn == "" {
		// Requires a real *sql.DB; skip if not configured.
		t.Skip("set LIBSCHEMA_TEST_MYSQL_DSN to run mismatch tests")
	}
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "open mysql")
	t.Cleanup(func() { _ = db.Close() })
	s := libschema.New(context.Background(), libschema.Options{})
	log := libschema.LogFromLog(t)
	_, driver, err := lsmysql.New(log, "db", s, db, lsmysql.WithoutDatabase)
	require.NoError(t, err, "new mysql")
	mig := lsmysql.Generate[*sql.Tx]("G_MISMATCH", func(context.Context, *sql.Tx) string { return "SELECT 1" }, libschema.ForceNonTransactional())
	database, _, err := lsmysql.New(log, "db", s, db)
	require.NoError(t, err, "second new")
	database.Migrations("L", mig)
	_, err = driver.DoOneMigration(context.Background(), log, database, mig)
	require.Error(t, err, "expected mismatch error")
}

func TestComputedMismatch(t *testing.T) {
	t.Parallel()
	dsn := os.Getenv("LIBSCHEMA_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set LIBSCHEMA_TEST_MYSQL_DSN to run mismatch tests")
	}
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "open mysql")
	t.Cleanup(func() { _ = db.Close() })
	s := libschema.New(context.Background(), libschema.Options{})
	log := libschema.LogFromLog(t)
	_, driver, err := lsmysql.New(log, "db", s, db, lsmysql.WithoutDatabase)
	require.NoError(t, err, "new mysql")
	mig := lsmysql.Computed[*sql.Tx]("C_MISMATCH", func(context.Context, *sql.Tx) error { return nil }, libschema.ForceNonTransactional())
	database, _, _ := lsmysql.New(log, "db", s, db)
	database.Migrations("L", mig)
	_, err = driver.DoOneMigration(context.Background(), log, database, mig)
	require.Error(t, err, "expected mismatch error")
}
