package lspostgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/muir/libschema"
	"github.com/muir/libschema/lspostgres"
	"github.com/muir/libschema/lstesting"
	"github.com/muir/testinglogur"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

/*

PostgreSQL supports schemas so testing can be done with a user that is limited
to a single database.

export PGPASSWORD=postgres
docker run --name postgres -p 5432:5432 -e POSTGRES_PASSWORD="$PGPASSWORD" --restart=always -d postgres:latest

psql -h localhost -p 5432 --user postgres <<END
CREATE DATABASE lstesting;
CREATE USER lstestuser WITH NOCREATEDB PASSWORD 'lstestpass';
GRANT ALL PRIVILEGES ON DATABASE lstesting TO lstestuser;
END

LIBSCHEMA_POSTGRES_TEST_DSN="postgresql://lstestuser:lstestpass@localhost:5432/lstesting?sslmode=disable"

*/

func TestPostgresMigrations(t *testing.T) {
	dsn := os.Getenv("LIBSCHEMA_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_POSTGRES_TEST_DSN to test libschema/lspostgres")
	}

	var actions []string

	options, cleanup := lstesting.FakeSchema(t, "CASCADE")

	options.ErrorOnUnknownMigrations = true
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

	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err, "open database")
	defer db.Close()
	defer cleanup(db)

	s := libschema.New(context.Background(), options)
	dbase, err := lspostgres.New(testinglogur.Get(t), "test", s, db)
	require.NoError(t, err, "libschema NewDatabase")

	defineMigrations := func(dbase *libschema.Database, extra bool) {
		l1migrations := []libschema.Migration{
			lspostgres.Generate("T1", func(_ context.Context, _ libschema.MyLogger, _ *sql.Tx) string {
				actions = append(actions, "MIGRATE: L1.T1")
				return `CREATE TABLE T1 (id text)`
			}),
			lspostgres.Computed("T2", func(_ context.Context, _ libschema.MyLogger, _ libschema.Migration, tx *sql.Tx) error {
				actions = append(actions, "MIGRATE: L1.T2")
				_, err := tx.Exec(`
				INSERT INTO T1 (id) VALUES ('T2');
				INSERT INTO T3 (id) VALUES ('T2');
				CREATE TABLE T2 (id text)`)
				return err
			}, libschema.After("L2", "T3")),
			lspostgres.Generate("PT1", func(_ context.Context, _ libschema.MyLogger, _ *sql.Tx) string {
				actions = append(actions, "MIGRATE: L1.PT1")
				return `
				INSERT INTO T1 (id) VALUES ('PT1');
				INSERT INTO T2 (id) VALUES ('PT1');
				INSERT INTO T3 (id) VALUES ('PT1');
			`
			}),
		}

		if extra {
			l1migrations = append(l1migrations,
				lspostgres.Generate("G1", func(_ context.Context, _ libschema.MyLogger, _ *sql.Tx) string {
					actions = append(actions, "MIGRATE: G1")
					return `CREATE TABLE G1 (id text);`
				}),
			)
		}

		dbase.Migrations("L1", l1migrations...)

		dbase.Migrations("L2",
			lspostgres.Generate("T3", func(_ context.Context, _ libschema.MyLogger, _ *sql.Tx) string {
				actions = append(actions, "MIGRATE: L2.T3")
				return `
				INSERT INTO T1 (id) VALUES ('T3');
				CREATE TABLE T3 (id text)`
			}),
			lspostgres.Generate("T4", func(_ context.Context, _ libschema.MyLogger, _ *sql.Tx) string {
				actions = append(actions, "MIGRATE: L2.T4")
				return `
				INSERT INTO T1 (id) VALUES ('T4');
				INSERT INTO T2 (id) VALUES ('T4');
				INSERT INTO T3 (id) VALUES ('T4');
				CREATE TABLE T4 (id text)`
			}),
			lspostgres.Generate("G2", func(_ context.Context, _ libschema.MyLogger, _ *sql.Tx) string {
				actions = append(actions, "MIGRATE: G2")
				return `CREATE TABLE G2 (id text);`
			}, libschema.SkipRemainingIf(func() (bool, error) {
				return !extra, nil
			})),
			lspostgres.Generate("G3", func(_ context.Context, _ libschema.MyLogger, _ *sql.Tx) string {
				actions = append(actions, "MIGRATE: G3")
				return `CREATE TABLE G3 (id text);`
			}),
		)
	}

	t.Log("now we define the migrations")
	defineMigrations(dbase, false)

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

	s = libschema.New(context.Background(), options)
	dbase, err = lspostgres.New(testinglogur.Get(t), "test", s, db)
	require.NoError(t, err, "libschema NewDatabase")

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
		"MIGRATE: G2",
		"MIGRATE: G3",
		"COMPLETE",
	}, actions)
}
