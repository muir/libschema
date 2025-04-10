package lsmysql_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/muir/libschema"
	"github.com/muir/libschema/lsmysql"
	"github.com/muir/libschema/lstesting"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

/*

Since MySQL does not support schemas (it treats them like databases),
LIBSCHEMA_MYSQL_TEST_DSN has to give access to a user that can create
and destroy databases.

For example:

export PASSWORD=mysql
docker run -d -p 3306:3306 -e MYSQL_DATABASE=libschematest -e MYSQL_ROOT_PASSWORD=$PASSWORD --restart=always --name=libschema_mysql mysql:8
export LIBSCHEMA_MYSQL_TEST_DSN="root:${PASSWORD}@tcp(127.0.0.1:3306)/libschematest?tls=false"
mysql --protocol=tcp -P 3306 -u root --password=$PASSWORD --database=libschematest

*/

func TestMysqlHappyPath(t *testing.T) {
	dsn := os.Getenv("LIBSCHEMA_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_MYSQL_TEST_DSN to test libschema/lsmysql")
	}
	testMysqlHappyPath(t, dsn, "ENGINE = InnoDB", mysqlNew, nil)
}

type MySQLInterface interface {
	GetTableConstraint(table, column string) (string, bool, error)
	TableHasIndex(table, indexName string) (bool, error)
	DatabaseName() (string, error)
	HasPrimaryKey(table string) (bool, error)
	ColumnDefault(table, column string) (*string, error)
	DoesColumnExist(table, column string) (bool, error)
	UseDatabase(string)
}

type driverNew func(t *testing.T, name string, schema *libschema.Schema, db *sql.DB) (*libschema.Database, MySQLInterface, error)

func mysqlNew(t *testing.T, name string, schema *libschema.Schema, db *sql.DB) (*libschema.Database, MySQLInterface, error) {
	database, m, err := lsmysql.New(libschema.LogFromLog(t), name, schema, db)
	return database, m, err
}

func testMysqlHappyPath(t *testing.T, dsn string, createPostfix string, driverNew driverNew, extraTableNames []string) {
	t.Log("As we go, we will build a log of what has happened in array 'actions'")
	var actions []string

	options, cleanup := lstesting.FakeSchema(t, "")

	t.Log("Doing migrations in database/schema", options.SchemaOverride)

	t.Log("We will error on unknown migrations")
	options.ErrorOnUnknownMigrations = true

	t.Log("if we fail a migration we will simply add that to the 'actions' array")
	options.OnMigrationFailure = func(_ *libschema.Database, name libschema.MigrationName, err error) {
		actions = append(actions, fmt.Sprintf("FAIL %s: %s", name, err))
	}
	options.OnMigrationsStarted = func(_ *libschema.Database) {
		actions = append(actions, "START")
	}
	options.OnMigrationsComplete = func(_ *libschema.Database, err error) {
		if err != nil {
			actions = append(actions, "COMPLETE: "+err.Error())
		} else {
			actions = append(actions, "COMPLETE")
		}
	}
	options.DebugLogging = true

	t.Log("No opening the database...")
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "open database")
	defer func() {
		assert.NoError(t, db.Close())
	}()
	defer cleanup(db)

	s := libschema.New(context.Background(), options)
	dbase, _, err := driverNew(t, "test", s, db)
	require.NoError(t, err, "libschema NewDatabase")

	defineMigrations := func(dbase *libschema.Database, extra bool) {
		l1migrations := []libschema.Migration{
			lsmysql.Generate("T1", func(_ context.Context, _ *sql.Tx) string {
				actions = append(actions, "MIGRATE: L1.T1")
				return `CREATE TABLE IF NOT EXISTS T1 (id text) ` + createPostfix
			}),
			lsmysql.Script("PT1skip", `INSERT INTO T3 T3 T3 (id) VALUES ('PT1');`,
				libschema.SkipIf(func() (bool, error) {
					return true, nil
				})),
			lsmysql.Script("T2pre", `
					INSERT INTO T1 (id) VALUES ('T2');`),
			lsmysql.Script("T2pre2", `
					INSERT INTO T3 (id) VALUES ('T2');`,
				libschema.After("L2", "T3")),
			lsmysql.Computed("T2", func(_ context.Context, tx *sql.Tx) error {
				actions = append(actions, "MIGRATE: L1.T2")
				_, err := tx.Exec(`
					CREATE TABLE IF NOT EXISTS T2 (id text) ` + createPostfix)
				return err
			}),
			lsmysql.Generate("PT1", func(_ context.Context, _ *sql.Tx) string {
				actions = append(actions, "MIGRATE: L1.PT1")
				return `
					INSERT INTO T1 (id) VALUES ('PT1');
				`
			}),
			lsmysql.Script("PT1p1", `
					INSERT INTO T2 (id) VALUES ('PT1');`),
			lsmysql.Script("PT1p2", `
					INSERT INTO T3 (id) VALUES ('PT1');`),
		}
		if extra {
			l1migrations = append(l1migrations,
				lsmysql.Generate("G1", func(_ context.Context, _ *sql.Tx) string {
					actions = append(actions, "MIGRATE: G1")
					return `
						CREATE TABLE IF NOT EXISTS G1 (id text) 
					` + createPostfix
				}),
			)
		}

		dbase.Migrations("L1", l1migrations...)

		dbase.Migrations("L2",
			lsmysql.Script("T3pre", `
					INSERT INTO T1 (id) VALUES ('T3');`),
			lsmysql.Generate("T3", func(_ context.Context, _ *sql.Tx) string {
				actions = append(actions, "MIGRATE: L2.T3")
				return `
					CREATE TABLE IF NOT EXISTS T3 (id text) ` + createPostfix
			}),
			lsmysql.Script("T4pre1", `
					INSERT INTO T1 (id) VALUES ('T4');`),
			lsmysql.Script("T4pre2", `
					INSERT INTO T2 (id) VALUES ('T4');`),
			lsmysql.Script("T4pre3", `
					INSERT INTO T3 (id) VALUES ('T4');`),
			lsmysql.Generate("T4", func(_ context.Context, _ *sql.Tx) string {
				actions = append(actions, "MIGRATE: L2.T4")
				return `
					CREATE TABLE IF NOT EXISTS T4 (id text) ` + createPostfix
			}),
		)
	}

	t.Log("now we define the migrations")
	defineMigrations(dbase, false)

	t.Log("now we do the migrations")
	err = s.Migrate(context.Background())
	assert.NoError(t, err)

	t.Log("now we check that the sequence of actions matches our expectations")
	assert.Equal(t, []string{
		"START",
		"MIGRATE: L1.T1",
		"MIGRATE: L2.T3",
		"MIGRATE: L1.T2",
		"MIGRATE: L1.PT1",
		"MIGRATE: L2.T4",
		"COMPLETE",
	}, actions)

	t.Log("Now we check that the set of tables in information_schema matches our expectations")
	rows, err := db.Query(`
		SELECT	table_name
		FROM	information_schema.tables
		WHERE	table_schema = ?
		ORDER	BY table_name`, options.SchemaOverride)
	require.NoError(t, err, "query for list of tables")
	defer func() {
		assert.NoError(t, rows.Close())
	}()
	var names []string
	for rows.Next() {
		var name string
		assert.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	expectedTableNames := append([]string{"T1", "T2", "T3", "T4", "tracking_table"}, extraTableNames...)
	assert.Equal(t, expectedTableNames, names, "table names")

	t.Log("now we will start a new set of migrations, starting by reading the migration state")
	s = libschema.New(context.Background(), options)
	dbase, _, err = driverNew(t, "test", s, db)
	require.NoError(t, err, "libschema NewDatabase, 2nd time")

	t.Log("Now we define slightly more migrations")
	defineMigrations(dbase, true)

	actions = nil
	t.Log("now we do the migrations")
	err = s.Migrate(context.Background())
	assert.NoError(t, err)

	t.Log("now we check that the sequence of actions matches our expectations")
	assert.Equal(t, []string{
		"START",
		"MIGRATE: G1",
		"COMPLETE",
	}, actions)
}

func TestMysqlNotAllowed(t *testing.T) {
	dsn := os.Getenv("LIBSCHEMA_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_MYSQL_TEST_DSN to test libschema/lsmysql")
	}
	testMysqlNotAllowed(t, dsn, "ENGINE = InnoDB", mysqlNew)
}

func testMysqlNotAllowed(t *testing.T, dsn string, createPostfix string, driverNew driverNew) {
	cases := []struct {
		name      string
		migration string
		errorText string
	}{
		{
			name: "combines",
			migration: `
				CREATE TABLE T1 (id text) ` + createPostfix + `;
				INSERT INTO T1 (id) VALUES ('x');
				`,
			errorText: "migration combines DDL",
		},
		{
			name:      "unconditional",
			migration: `CREATE TABLE T1 (id text) ` + createPostfix,
			errorText: "unconditional migration has non-idempotent",
		},
	}

	for _, tc := range cases {
		options, cleanup := lstesting.FakeSchema(t, "")
		options.DebugLogging = true
		s := libschema.New(context.Background(), options)

		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err, "open database")
		defer func() {
			assert.NoError(t, db.Close())
		}()
		defer cleanup(db)

		dbase, _, err := driverNew(t, "test", s, db)
		require.NoError(t, err, "libschema NewDatabase")

		dbase.Migrations("L1", lsmysql.Script("x", tc.migration))

		err = s.Migrate(context.Background())
		if assert.Error(t, err, tc.name) {
			assert.Contains(t, err.Error(), tc.errorText, tc.name)
		}
	}
}
