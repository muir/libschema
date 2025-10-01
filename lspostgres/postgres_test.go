package lspostgres_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/muir/libschema"
	"github.com/muir/libschema/lspostgres"
	"github.com/muir/libschema/lstesting"
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
	t.Parallel()
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
	defer func() {
		assert.NoError(t, db.Close())
	}()
	defer cleanup(db)

	s := libschema.New(context.Background(), options)
	dbase, err := lspostgres.New(libschema.LogFromLog(t), "test", s, db)
	require.NoError(t, err, "libschema NewDatabase")

	defineMigrations := func(dbase *libschema.Database, extra bool) {
		l1migrations := []libschema.Migration{
			lspostgres.Generate("T1", func(_ context.Context, _ *sql.Tx) string {
				actions = append(actions, "MIGRATE: L1.T1")
				return `CREATE TABLE T1 (id text)`
			}),
			lspostgres.Computed("T2", func(_ context.Context, tx *sql.Tx) error {
				actions = append(actions, "MIGRATE: L1.T2")
				_, err := tx.Exec(`
				INSERT INTO T1 (id) VALUES ('T2');
				INSERT INTO T3 (id) VALUES ('T2');
				CREATE TABLE T2 (id text)`)
				return err
			}, libschema.After("L2", "T3")),
			lspostgres.Generate("PT1", func(_ context.Context, _ *sql.Tx) string {
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
				lspostgres.Generate("G1", func(_ context.Context, _ *sql.Tx) string {
					actions = append(actions, "MIGRATE: G1")
					return `CREATE TABLE G1 (id text);`
				}),
			)
		}

		dbase.Migrations("L1", l1migrations...)

		dbase.Migrations("L2",
			lspostgres.Generate("T3", func(_ context.Context, _ *sql.Tx) string {
				actions = append(actions, "MIGRATE: L2.T3")
				return `
				INSERT INTO T1 (id) VALUES ('T3');
				CREATE TABLE T3 (id text)`
			}),
			lspostgres.Generate("T4", func(_ context.Context, _ *sql.Tx) string {
				actions = append(actions, "MIGRATE: L2.T4")
				return `
				INSERT INTO T1 (id) VALUES ('T4');
				INSERT INTO T2 (id) VALUES ('T4');
				INSERT INTO T3 (id) VALUES ('T4');
				CREATE TABLE T4 (id text)`
			}),
			lspostgres.Generate("G2", func(_ context.Context, _ *sql.Tx) string {
				actions = append(actions, "MIGRATE: G2")
				return `CREATE TABLE G2 (id text);`
			}, libschema.SkipRemainingIf(func() (bool, error) {
				return !extra, nil
			})),
			lspostgres.Generate("G3", func(_ context.Context, _ *sql.Tx) string {
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
	defer func() {
		assert.NoError(t, rows.Close())
	}()
	var names []string
	for rows.Next() {
		var name string
		assert.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	assert.Equal(t, []string{"t1", "t2", "t3", "t4", "tracking_table"}, names, "table names")

	s = libschema.New(context.Background(), options)
	dbase, err = lspostgres.New(libschema.LogFromLog(t), "test", s, db)
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

// TestGenerateDBInference ensures Generate[*sql.DB] is inferred non-transactional and executes.
func TestGenerateDBInference(t *testing.T) {
	dsn := os.Getenv("LIBSCHEMA_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_POSTGRES_TEST_DSN to test libschema/lspostgres")
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	s := libschema.New(ctx, libschema.Options{})
	log := libschema.LogFromLog(t)
	dbase, err := lspostgres.New(log, "test_gen_db", s, db)
	require.NoError(t, err)

	// Table setup transactional migration for context
	lib := fmt.Sprintf("%s_%d", t.Name(), time.Now().UnixNano())
	setup := lspostgres.Script("SETUP_"+lib, "CREATE TABLE IF NOT EXISTS gen_db_inf (id int)")
	// Non-transactional generation (uses *sql.DB) inserts a row
	gen := lspostgres.Generate[*sql.DB]("GEN_DB_"+lib, func(_ context.Context, _ *sql.DB) string { return "INSERT INTO gen_db_inf (id) VALUES (1)" })
	dbase.Migrations(lib, setup, gen)
	require.NoError(t, s.Migrate(ctx))
	assert.True(t, gen.Base().NonTransactional(), "Generate[*sql.DB] should be non-transactional")
}

// TestComputedDBInference ensures Computed[*sql.DB] is inferred non-transactional and runs logic.
func TestComputedDBInference(t *testing.T) {
	dsn := os.Getenv("LIBSCHEMA_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_POSTGRES_TEST_DSN to test libschema/lspostgres")
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	s := libschema.New(ctx, libschema.Options{})
	log := libschema.LogFromLog(t)
	dbase, err := lspostgres.New(log, "test_comp_db", s, db)
	require.NoError(t, err)

	ran := false
	lib := fmt.Sprintf("%s_%d", t.Name(), time.Now().UnixNano())
	comp := lspostgres.Computed[*sql.DB]("COMP_DB_"+lib, func(_ context.Context, db2 *sql.DB) error {
		if _, e := db2.Exec("CREATE TABLE IF NOT EXISTS comp_db_inf (id int)"); e != nil {
			return e
		}
		_, e := db2.Exec("INSERT INTO comp_db_inf (id) VALUES (2)")
		if e == nil {
			ran = true
		}
		return e
	})
	dbase.Migrations(lib, comp)
	require.NoError(t, s.Migrate(ctx))
	assert.True(t, comp.Base().NonTransactional(), "Computed[*sql.DB] should be non-transactional")
	assert.True(t, ran, "computed function body did not run")
}

// TestComputedFailure ensures a computed migration that returns an error surfaces it and records failure.
func TestComputedFailure(t *testing.T) {
	dsn := os.Getenv("LIBSCHEMA_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_POSTGRES_TEST_DSN to test libschema/lspostgres")
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	opts := libschema.Options{}
	s := libschema.New(ctx, opts)
	log := libschema.LogFromLog(t)
	dbase, err := lspostgres.New(log, "test_computed_fail", s, db)
	require.NoError(t, err)

	lib := fmt.Sprintf("FAIL_%d", time.Now().UnixNano())
	failErr := errors.New("boom-fail")
	bad := lspostgres.Computed("BAD", func(_ context.Context, _ *sql.Tx) error { return failErr })
	dbase.Migrations(lib, bad)
	err = s.Migrate(ctx)
	require.Error(t, err, "expected migration failure error")
	// Lookup the stored migration (Database.Migrations makes a copy)
	stored, ok := dbase.Lookup(libschema.MigrationName{Library: lib, Name: "BAD"})
	require.True(t, ok, "could not lookup stored migration copy")
	st := stored.Base().Status()
	assert.False(t, st.Done, "expected migration not marked done after failure")
	assert.NotEmpty(t, st.Error, "expected error message recorded; got empty status: %+v", st)
}
