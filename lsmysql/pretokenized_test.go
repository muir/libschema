package lsmysql_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/muir/libschema"
	"github.com/muir/libschema/lsmysql"
	"github.com/muir/libschema/lstesting"
	"github.com/muir/sqltoken"
)

func TestPreTokenizedMySQL(t *testing.T) {
	t.Parallel()
	dsn := os.Getenv("LIBSCHEMA_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_MYSQL_TEST_DSN to test libschema/lsmysql")
	}
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	options, cleanup := lstesting.FakeSchema(t, "")
	defer func() { cleanup(db) }()

	s := libschema.New(context.Background(), options)
	log := libschema.LogFromLog(t)
	dbase, _, err := lsmysql.New(log, "test", s, db)
	require.NoError(t, err)

	split := sqltoken.TokenizeMySQLAPI(
		"CREATE TABLE IF NOT EXISTS pretok_test (id int)",
	).CmdSplitUnstripped()

	dbase.Migrations("L1", lsmysql.PreTokenized("CREATE_TABLE", split))

	require.NoError(t, s.Migrate(context.Background()))
	m, ok := dbase.Lookup(libschema.MigrationName{Library: "L1", Name: "CREATE_TABLE"})
	require.True(t, ok)
	assert.True(t, m.Base().Status().Done)
}
