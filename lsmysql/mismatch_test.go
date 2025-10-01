package lsmysql_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/muir/libschema"
	"github.com/muir/libschema/lsmysql"
)

// TestGenerateMismatch ensures a Generate[*sql.Tx] + ForceNonTransactional records a creationErr surfaced at execution.
func TestGenerateMismatch(t *testing.T) {
	t.Parallel()
	// Minimal in-memory-style construct: use an empty *sql.DB (won't actually exec because creationErr fires first)
	fakeDB := &sql.DB{}
	s := libschema.New(context.Background(), libschema.Options{})
	log := libschema.LogFromLog(t)
	_, driver, err := lsmysql.New(log, "db", s, fakeDB, lsmysql.WithoutDatabase)
	require.NoError(t, err, "new mysql")
	mig := lsmysql.Generate[*sql.Tx]("G_MISMATCH", func(context.Context, *sql.Tx) string { return "SELECT 1" }, libschema.ForceNonTransactional())
	db2, _, err := lsmysql.New(log, "db", s, fakeDB)
	require.NoError(t, err, "second new")
	db2.Migrations("L", mig)
	_, err = driver.DoOneMigration(context.Background(), log, db2, mig)
	require.Error(t, err, "expected mismatch error")
}

func TestComputedMismatch(t *testing.T) {
	t.Parallel()
	fakeDB := &sql.DB{}
	s := libschema.New(context.Background(), libschema.Options{})
	log := libschema.LogFromLog(t)
	_, driver, err := lsmysql.New(log, "db", s, fakeDB, lsmysql.WithoutDatabase)
	require.NoError(t, err, "new mysql")
	mig := lsmysql.Computed[*sql.Tx]("C_MISMATCH", func(context.Context, *sql.Tx) error { return nil }, libschema.ForceNonTransactional())
	db2, _, _ := lsmysql.New(log, "db", s, fakeDB)
	db2.Migrations("L", mig)
	_, err = driver.DoOneMigration(context.Background(), log, db2, mig)
	require.Error(t, err, "expected mismatch error")
}
