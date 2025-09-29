package lspostgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/muir/libschema"
	"github.com/muir/libschema/internal"
	"github.com/muir/libschema/lspostgres"
	"github.com/muir/libschema/lstesting"
)

func TestOverrideNothing(t *testing.T) {
	var code string
	var code2 string
	_, err := doConfigMigrate(t, nil, getDSN(t), true, &code, &code2, nil)
	require.NoError(t, err)
	assert.Equal(t, "m2async", code)
	assert.Equal(t, "d2m1", code2)
}

func TestOverrideMigrateOnly(t *testing.T) {
	var code string
	assert.PanicsWithValue(t, "test exit: migrate only", func() {
		_, _ = doConfigMigrate(t, nil, getDSN(t), true, &code, nil, &libschema.OverrideOptions{
			MigrateOnly: true,
		})
	})
	assert.Equal(t, "m1", code)
}

func TestOverrideMigrateDatabaseNotMatching(t *testing.T) {
	var code string
	_, err := doConfigMigrate(t, nil, getDSN(t), false, &code, nil, &libschema.OverrideOptions{
		MigrateDatabase: "notmatching",
	})
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "migrate: database 'notmatching'")
	}
	assert.Equal(t, "", code)
}

func TestOverrideMigrateDatabaseMatching(t *testing.T) {
	var code string
	_, err := doConfigMigrate(t, nil, getDSN(t), true, &code, nil, &libschema.OverrideOptions{
		MigrateDatabase: "test",
	})
	require.NoError(t, err)
	assert.Equal(t, "m2async", code)
}

func TestOverrideMigrateDSNWithOneDatabase(t *testing.T) {
	var code string
	_, err := doConfigMigrate(t, nil, getDSN(t), true, &code, nil, &libschema.OverrideOptions{
		MigrateDSN: getDSN(t),
	})
	require.NoError(t, err)
	assert.Equal(t, "m2async", code)
}

func TestOverrideMigrateDSNWithoutDatabase(t *testing.T) {
	var code string
	var code2 string
	_, err := doConfigMigrate(t, nil, getDSN(t), true, &code, &code2, &libschema.OverrideOptions{
		MigrateDSN: getDSN(t),
	})
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "migrate: --migrate-dsn can only be used when there is only one database")
	}
	assert.Equal(t, "", code)
	assert.Equal(t, "", code2)
}

func TestOverrideMigrateDSNWithDatabaseSpecified(t *testing.T) {
	var code string
	var code2 string
	_, err := doConfigMigrate(t, nil, getDSN(t), false, &code, &code2, &libschema.OverrideOptions{
		MigrateDatabase: "test2",
		MigrateDSN:      getDSN(t),
	})
	require.NoError(t, err)
	assert.Equal(t, "", code)
	assert.Equal(t, "d2m1", code2)
}

func TestOverrideNoMigrate(t *testing.T) {
	var code string
	var code2 string
	_, err := doConfigMigrate(t, nil, getDSN(t), false, &code, &code2, &libschema.OverrideOptions{
		NoMigrate: true,
	})
	require.NoError(t, err)
	assert.Equal(t, "", code)
	assert.Equal(t, "", code2)
}

func TestOverrideErrorIfMigrateNeeded(t *testing.T) {
	options, cleanup := lstesting.FakeSchema(t, "CASCADE")
	t.Log("With ErrorIfMigrateNeeded: true we should get an error")
	var code string
	var code2 string
	db, err := doConfigMigrate(t, &options, getDSN(t), false, &code, &code2, &libschema.OverrideOptions{
		MigrateDatabase:      "test2",
		ErrorIfMigrateNeeded: true,
	})
	defer func() {
		t.Log("Closing db...")
		if db != nil {
			cleanup(db)
			assert.NoError(t, db.Close())
		}
		t.Log("done")
	}()
	require.Error(t, err)
	assert.Equal(t, "", code)
	assert.Equal(t, "", code2)
	assert.Contains(t, err.Error(), "migrations required for test2")

	t.Log("Now we actually do the migrations")
	_, err = doConfigMigrate(t, &options, getDSN(t), false, &code, &code2, &libschema.OverrideOptions{
		MigrateDatabase:      "test2",
		ErrorIfMigrateNeeded: false,
	})
	require.NoError(t, err)
	assert.Equal(t, "", code)
	assert.Equal(t, "d2m1", code2)

	t.Log("With ErrorIfMigrateNeeded: true we should not get an error")
	code2 = ""
	_, err = doConfigMigrate(t, &options, getDSN(t), false, &code, &code2, &libschema.OverrideOptions{
		MigrateDatabase:      "test2",
		ErrorIfMigrateNeeded: true,
	})
	require.NoError(t, err)
	assert.Equal(t, "", code)
	assert.Equal(t, "", code2)
}

func TestOverrideEverythingSynchronous(t *testing.T) {
	var code string
	var code2 string
	_, err := doConfigMigrate(t, nil, getDSN(t), false, &code, &code2, &libschema.OverrideOptions{
		EverythingSynchronous: true,
	})
	require.NoError(t, err)
	assert.Equal(t, "m2", code)
	assert.Equal(t, "d2m1", code2)
}

func getDSN(t *testing.T) string {
	dsn := os.Getenv("LIBSCHEMA_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_POSTGRES_TEST_DSN to run this test")
	}
	return dsn
}

func doConfigMigrate(t *testing.T, options *libschema.Options, dsn string, expectAsync bool, code *string, code2 *string, overrides *libschema.OverrideOptions) (*sql.DB, error) {
	internal.TestingMode = true // panic instead of os.Exit()
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	defer func() {
		if options == nil {
			t.Log("Closing db...")
			assert.NoError(t, db.Close())
			t.Log("done")
		}
	}()

	migrationComplete := make(chan struct{})
	migrateReturned := make(chan struct{})

	if options == nil {
		opts, cleanup := lstesting.FakeSchema(t, "CASCADE")
		defer func() {
			t.Log("Doing cleanup...")
			cleanup(db)
			t.Log("done")
		}()
		options = &opts
	}
	options.DebugLogging = true
	options.Overrides = overrides

	options.OnMigrationsComplete = func(_ *libschema.Database, err error) {
		close(migrationComplete)
		t.Log("migrations complete")
	}

	s := libschema.New(context.Background(), *options)

	if code2 != nil {
		_, cleanup2 := lstesting.FakeSchema(t, "CASCADE")
		defer func() {
			t.Log("Doing cleanup2...")
			cleanup2(db)
			t.Log("done")
		}()

		dbase2, err := lspostgres.New(libschema.LogFromLog(t), "test2", s, db)
		if err != nil {
			return db, fmt.Errorf("new2: %w", err)
		}
		dbase2.Options.OnMigrationsComplete = nil

		dbase2.Migrations("T2",
			lspostgres.Generate("D1M1", func(_ context.Context, _ *sql.Tx) string {
				*code2 = "d2m1"
				t.Log("migration D2M1")
				return `CREATE TABLE D2M1 (id text)`
			}),
		)
	}

	dbase, err := lspostgres.New(libschema.LogFromLog(t), "test", s, db)
	if err != nil {
		return db, fmt.Errorf("new: %w", err)
	}

	require.NotNil(t, dbase.Options.OnMigrationsComplete)

	dbase.Migrations("L1",
		lspostgres.Generate("M1", func(_ context.Context, _ *sql.Tx) string {
			*code = "m1"
			t.Log("migration M1")
			return `CREATE TABLE M1 (id text)`
		}),
		lspostgres.Generate("M2", func(_ context.Context, _ *sql.Tx) string {
			if expectAsync {
				t.Log("migration M2 waiting migration complete signal")
				nt := time.NewTimer(time.Second)
				select {
				case <-migrationComplete:
					assert.FailNow(t, "early complete", "early complete")
				case <-migrateReturned:
				case <-nt.C:
					assert.FailNow(t, "timeout", "timeout")
				}
				nt.Stop()
				*code = "m2async"
			} else {
				*code = "m2"
			}
			t.Log("migration M2 running")
			return `CREATE TABLE M2 (id text)`
		}, libschema.Asynchronous()),
	)

	t.Log("migrate!")
	err = s.Migrate(context.Background())
	if err != nil {
		return db, fmt.Errorf("migrate: %w", err)
	}

	close(migrateReturned)
	if expectAsync {
		t.Log("waiting for migrations to complete")
		nt := time.NewTimer(time.Second)
		select {
		case <-migrationComplete:
		case <-nt.C:
			assert.FailNow(t, "timeout2", "timeout2")
		}
		nt.Stop()
	}
	t.Log("all done")
	return db, nil
}
