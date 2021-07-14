package lsmysql_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/muir/libschema"
	"github.com/muir/libschema/lsmysql"
	"github.com/muir/libschema/lstesting"
	"github.com/muir/testinglogur"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMysqlHappyPath(t *testing.T) {
	dsn := os.Getenv("LIBSCHEMA_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_MYSQL_TEST_DSN to test libschema/lsmysql")
	}

	var actions []string

	options, cleanup := lstesting.FakeSchema(t)

	options.ErrorOnUnknownMigrations = true
	options.OnMigrationFailure = func(name libschema.MigrationName, err error) {
		actions = append(actions, fmt.Sprintf("FAIL %s: %s", name, err))
	}
	options.OnMigrationsStarted = func() {
		actions = append(actions, "START")
	}
	options.OnMigrationsComplete = func(err error) {
		if err != nil {
			actions = append(actions, "COMPLETE: "+err.Error())
		} else {
			actions = append(actions, "COMPLETE")
		}
	}
	options.DebugLogging = true
	s := libschema.New(context.Background(), options)

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "open database")
	defer db.Close()
	defer cleanup(db)

	dbase, err := lsmysql.New(testinglogur.Get(t), "test", s, db)
	require.NoError(t, err, "libschema NewDatabase")

	dbase.Migrations("L1",
		lsmysql.Generate("T1", func(_ context.Context, _ libschema.MyLogger, _ *sql.Tx) string {
			actions = append(actions, "MIGRATE: L1.T1")
			return `CREATE TABLE T1 (id text)`
		}),
		lsmysql.Computed("T2", func(_ context.Context, _ libschema.MyLogger, tx *sql.Tx) error {
			actions = append(actions, "MIGRATE: L1.T2")
			_, err := tx.Exec(`
				INSERT INTO T1 (id) VALUES ('T2');
				INSERT INTO T3 (id) VALUES ('T2');
				CREATE TABLE T2 (id text)`)
			return err
		}, libschema.After("L2", "T3")),
		lsmysql.Generate("PT1", func(_ context.Context, _ libschema.MyLogger, _ *sql.Tx) string {
			actions = append(actions, "MIGRATE: L1.PT1")
			return `
				INSERT INTO T1 (id) VALUES ('PT1');
				INSERT INTO T2 (id) VALUES ('PT1');
				INSERT INTO T3 (id) VALUES ('PT1');
			`
		}),
	)

	dbase.Migrations("L2",
		lsmysql.Generate("T3", func(_ context.Context, _ libschema.MyLogger, _ *sql.Tx) string {
			actions = append(actions, "MIGRATE: L2.T3")
			return `
				INSERT INTO T1 (id) VALUES ('T3');
				CREATE TABLE T3 (id text)`
		}),
		lsmysql.Generate("T4", func(_ context.Context, _ libschema.MyLogger, _ *sql.Tx) string {
			actions = append(actions, "MIGRATE: L2.T4")
			return `
				INSERT INTO T1 (id) VALUES ('T4');
				INSERT INTO T2 (id) VALUES ('T4');
				INSERT INTO T3 (id) VALUES ('T4');
				CREATE TABLE T4 (id text)`
		}),
	)

	err = s.Migrate(context.Background())
	assert.NoError(t, err)

	assert.Equal(t, []string{
		"START",
		"MIGRATE: L1.T1",
		"MIGRATE: L2.T3",
		"MIGRATE: L1.T2",
		"MIGRATE: L1.PT1",
		"MIGRATE: L2.T4",
		"COMPLETE",
	}, actions)

	rows, err := db.Query(`
		SELECT	table_name
		FROM	information_schema.tables
		WHERE	table_schema = $1
		ORDER	BY table_name`, options.SchemaOverride)
	require.NoError(t, err, "query for list of tables")
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		assert.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	assert.Equal(t, []string{"t1", "t2", "t3", "t4", "tracking_table"}, names, "table names")
}
