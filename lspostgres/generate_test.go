package lspostgres_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/muir/libschema"
	"github.com/muir/libschema/lspostgres"
	"github.com/muir/libschema/lstesting"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// helper to open or skip
func openPG(t *testing.T) *sql.DB {
	dsn := os.Getenv("LIBSCHEMA_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_POSTGRES_TEST_DSN to run Postgres tests")
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestGenerateSuccess ensures Generate executes produced SQL transactionally.
func TestGenerateSuccess(t *testing.T) {
	db := openPG(t)
	opts, _ := lstesting.FakeSchema(t, "")
	s := libschema.New(context.Background(), opts)
	capLog := &captureLog{t: t}
	log := libschema.LogFromLogur(capLog)
	dbase, err := lspostgres.New(log, "gen_success", s, db)
	require.NoError(t, err)
	create := lspostgres.Generate("GEN_CREATE", func(_ context.Context, _ *sql.Tx) string {
		return "CREATE TYPE FOOBAR_TYPE AS ENUM ('A', 'B', 'C')"
	})
	forced := lspostgres.Generate("GEN_FORCE_NONTX", func(_ context.Context, _ *sql.Tx) string {
		return "ALTER TYPE FOOBAR_TYPE RENAME VALUE 'B' TO 'E'"
	}, libschema.ForceNonTransactional())
	dbase.Migrations("GEN_LIB", create, forced)
	require.NoError(t, s.Migrate(context.Background()))
	assert.False(t, create.Base().NonTransactional(), "create Generate should be transactional")
	assert.True(t, forced.Base().ForcedNonTransactional(), "forced non-tx Generate should be marked forced non-transactional")
	entries := capLog.Entries()
	found := false
	for _, entry := range entries {
		t.Logf("msg = '%s'", entry.msg)
		if entry.msg != "Warning - non-transactional migration contains non-idempotent command" {
			continue
		}
		found = true
		break
	}
	assert.True(t, found, "expected rows affected error, not found")
}
