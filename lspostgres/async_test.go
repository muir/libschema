package lspostgres_test

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/muir/libschema"
	"github.com/muir/libschema/lspostgres"
	"github.com/muir/libschema/lstesting"
)

func TestAsyncMigrations(t *testing.T) {
	dsn := os.Getenv("LIBSCHEMA_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_POSTGRES_TEST_DSN to test libschema/lspostgres")
	}

	migrationComplete := make(chan struct{})
	migrateReturned := make(chan struct{})
	m3 := make(chan struct{})

	options, cleanup := lstesting.FakeSchema(t, "CASCADE")
	options.ErrorOnUnknownMigrations = true
	options.OnMigrationsComplete = func(_ *libschema.Database, err error) {
		assert.NoError(t, err, "completion error")
		time.Sleep(10 * time.Millisecond)
		close(migrationComplete)
		t.Log("All migrations are complete (signaled)")
	}

	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err, "open database")
	defer func() {
		t.Log("Closing db...")
		_ = db.Close()
		t.Log("done")
	}()
	defer func() {
		t.Log("Doing cleanup...")
		cleanup(db)
		t.Log("done")
	}()

	s := libschema.New(context.Background(), options)
	dbase, err := lspostgres.New(libschema.LogFromLog(t), "test", s, db)
	require.NoError(t, err, "libschema NewDatabase")

	t.Log("define three migrations, two are async")
	dbase.Migrations("L2",
		lspostgres.Generate("M1", func(_ context.Context, _ *sql.Tx) string {
			t.Log("migration M1 running")
			return `CREATE TABLE M1 (id text)`
		}),
		lspostgres.Generate("M2", func(_ context.Context, _ *sql.Tx) string {
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
			t.Log("migration M2 running")
			return `CREATE TABLE M2 (id text)`
		}, libschema.Asynchronous()),
		lspostgres.Generate("M3", func(_ context.Context, _ *sql.Tx) string {
			t.Log("migration M3 running")
			close(m3)
			return `CREATE TABLE M3 (id text);`
		}, libschema.Asynchronous()),
	)

	t.Log("Starting migrations")
	err = s.Migrate(context.Background())
	assert.NoError(t, err)
	t.Log("Synchronous migrations complete")
	time.Sleep(10 * time.Millisecond)
	t.Log("Signalling that the migrate command returned")
	close(migrateReturned)

	t.Log("Waiting for migration M3 to be done")
	nt := time.NewTimer(time.Second)
	select {
	case <-m3:
	case <-migrationComplete:
		assert.FailNow(t, "early complete 2", "early complete 2")
	case <-nt.C:
		assert.FailNow(t, "timeout2", "timeout2")
	}
	nt.Reset(time.Second)

	t.Log("Waiting for the migration complete signal")
	select {
	case <-migrationComplete:
	case <-nt.C:
		assert.FailNow(t, "timeout2", "timeout2")
	}
	nt.Stop()
	t.Log("All done!")
}
