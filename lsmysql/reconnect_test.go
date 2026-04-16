package lsmysql_test

import (
	"context"
	"database/sql"
	"os"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/muir/libschema"
	"github.com/muir/libschema/lsmysql"
	"github.com/muir/libschema/lssinglestore"
	"github.com/muir/libschema/lstesting"
)

type reconnectNewWithOpener func(
	t *testing.T,
	name string,
	schema *libschema.Schema,
	getDB func() (*sql.DB, error),
) (*libschema.Database, error)

type reconnectScript func(name, sqlText string, opts ...libschema.MigrationOption) libschema.Migration

func testReconnectAfter(
	t *testing.T,
	dsn string,
	newWithOpener reconnectNewWithOpener,
	script reconnectScript,
) {
	options, cleanup := lstesting.FakeSchema(t, "")
	adminDB, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer func() { assert.NoError(t, adminDB.Close()) }()
	defer cleanup(adminDB)

	var openerCalls int32
	openDB := func() (*sql.DB, error) {
		atomic.AddInt32(&openerCalls, 1)
		return sql.Open("mysql", dsn)
	}

	s := libschema.New(context.Background(), options)
	dbase, err := newWithOpener(t, "test", s, openDB)
	require.NoError(t, err)
	defer func() { assert.NoError(t, dbase.DB().Close()) }()

	dbase.Migrations("L1", script("R1", `SELECT 1`, libschema.ReconnectAfter()))
	require.NoError(t, s.Migrate(context.Background()))
	assert.Equal(t, int32(2), atomic.LoadInt32(&openerCalls))
}

func TestMysqlReconnectAfter(t *testing.T) {
	t.Parallel()
	dsn := os.Getenv("LIBSCHEMA_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_MYSQL_TEST_DSN to test libschema/lsmysql")
	}
	testReconnectAfter(
		t,
		dsn,
		func(t *testing.T, name string, schema *libschema.Schema, getDB func() (*sql.DB, error)) (*libschema.Database, error) {
			dbase, _, err := lsmysql.NewWithOpener(libschema.LogFromLog(t), name, schema, getDB)
			return dbase, err
		},
		lsmysql.Script,
	)
}

func TestSingleStoreReconnectAfter(t *testing.T) {
	t.Parallel()
	dsn := os.Getenv("LIBSCHEMA_SINGLESTORE_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_SINGLESTORE_TEST_DSN to test SingleStore support in libschema/lsmysql")
	}
	testReconnectAfter(
		t,
		dsn,
		func(t *testing.T, name string, schema *libschema.Schema, getDB func() (*sql.DB, error)) (*libschema.Database, error) {
			dbase, _, err := lssinglestore.NewWithOpener(libschema.LogFromLog(t), name, schema, getDB)
			return dbase, err
		},
		lssinglestore.Script,
	)
}
