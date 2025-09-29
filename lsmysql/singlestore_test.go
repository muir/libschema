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
	"github.com/muir/libschema/lssinglestore"
	"github.com/muir/libschema/lstesting"
)

/*

Since MySQL does not support schemas (it treats them like databases),
LIBSCHEMA_SINGLESTORE_TEST_DSN has to give access to a user that can create
and destroy databases.

For example:

docker run -i --init \
	--name memsql-ciab \
	-e LICENSE_KEY="GET YOUR OWN LICENSE KEY" \
	-e ROOT_PASSWORD="YOUR S2 ROOT PASSWORD" \
	-p 3306:3306 -p 8080:8080 \
		memsql/cluster-in-a-box

export LIBSCHEMA_SINGLESTORE_TEST_DSN="root:${PASSWORD}@tcp(127.0.0.1:3306)/publi?tls=false"

*/

func singleStoreNew(t *testing.T, name string, schema *libschema.Schema, db *sql.DB) (*libschema.Database, MySQLInterface, error) {
	database, s2, err := lssinglestore.New(libschema.LogFromLog(t), name, schema, db)
	return database, s2, err
}

func TestSingleStoreHappyPath(t *testing.T) {
	dsn := os.Getenv("LIBSCHEMA_SINGLESTORE_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_SINGLESTORE_TEST_DSN to test SingleStore support in libschema/lsmysql")
	}
	testMysqlHappyPath(t, dsn, "", singleStoreNew, []string{"tracking_table_lock"})
}

func TestSingleStoreNotAllowed(t *testing.T) {
	dsn := os.Getenv("LIBSCHEMA_SINGLESTORE_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_SINGLESTORE_TEST_DSN to test SingleStore support in libschema/lsmysql")
	}
	testMysqlNotAllowed(t, dsn, "", singleStoreNew)
}

func TestSingleStoreFailedMigration(t *testing.T) {
	dsn := os.Getenv("LIBSCHEMA_SINGLESTORE_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_SINGLESTORE_TEST_DSN to test SingleStore support in libschema/lsmysql")
	}
	options, cleanup := lstesting.FakeSchema(t, "")
	options.DebugLogging = true

	for i := 0; i < 3; i++ {
		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err, "open database")
		defer func() {
			assert.NoError(t, db.Close())
		}()
		defer cleanup(db)

		s := libschema.New(context.Background(), options)
		dbase, _, err := lssinglestore.New(libschema.LogFromLog(t), "test", s, db)
		require.NoError(t, err, "libschema NewDatabase")

		t.Log("now we define the migrations")
		dbase.Migrations("L2", lsmysql.Script("T4", `CREATE TABLE IF NOT EXISTS T1 (id text foo)`))

		err = s.Migrate(context.Background())
		if assert.Error(t, err, "should error") {
			assert.Contains(t, err.Error(), "You have an error in your SQL syntax")
		}
	}
}
