package lspostgres

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/muir/libschema"
	"github.com/muir/libschema/lstesting"
	"github.com/muir/sqltoken"
)

func TestIsMigrationSupportedPreTokenized(t *testing.T) {
	p := &Postgres{}
	split := sqltoken.TokenizePostgreSQL("CREATE TABLE t (id int)").CmdSplitUnstripped()

	m := PreTokenized("test", split)
	err := p.IsMigrationSupported(nil, nil, m)
	assert.NoError(t, err, "PreTokenized migration must be accepted by IsMigrationSupported")
}

func TestIsMigrationSupportedEmpty(t *testing.T) {
	p := &Postgres{}
	// A pmigration with nothing set is not supported.
	pm := &pmigration{MigrationBase: libschema.MigrationBase{Name: libschema.MigrationName{Name: "empty"}}}
	err := p.IsMigrationSupported(nil, nil, pm)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")
}

func TestPreTokenizedPostgres(t *testing.T) {
	dsn := os.Getenv("LIBSCHEMA_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_POSTGRES_TEST_DSN to run Postgres tests")
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	opts, cleanup := lstesting.FakeSchema(t, "CASCADE")
	defer cleanup(db)
	_, err = db.Exec("CREATE SCHEMA " + opts.SchemaOverride)
	require.NoError(t, err)

	s := libschema.New(context.Background(), opts)
	log := libschema.LogFromLog(t)
	ldb, err := New(log, "test", s, db)
	require.NoError(t, err)

	split := sqltoken.TokenizePostgreSQL(
		"CREATE TABLE IF NOT EXISTS pretok_test (id int)",
	).CmdSplitUnstripped()

	ldb.Migrations("L1", PreTokenized("CREATE_TABLE", split))

	require.NoError(t, s.Migrate(context.Background()))
	m, ok := ldb.Lookup(libschema.MigrationName{Library: "L1", Name: "CREATE_TABLE"})
	require.True(t, ok)
	assert.True(t, m.Base().Status().Done)
}
