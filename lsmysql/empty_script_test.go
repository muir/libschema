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

func TestEmptyScriptNoOp(t *testing.T) {
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

	dbase.Migrations("L1",
		lsmysql.Script("EMPTY", ""),
	)

	require.NoError(t, s.Migrate(context.Background()))
	m, _ := dbase.Lookup(libschema.MigrationName{Library: "L1", Name: "EMPTY"})
	if m == nil {
		t.Fatalf("migration not registered")
	}
	// Empty script returns early before exec; driver treats as success -> Done should be true.
	if !m.Base().Status().Done {
		t.Fatalf("empty script should mark migration done")
	}
}
