package lspostgres_test

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/memsql/errors"
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

func TestAsyncMigrationsKeepLockUntilDone(t *testing.T) {
	dsn := os.Getenv("LIBSCHEMA_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_POSTGRES_TEST_DSN to test libschema/lspostgres")
	}

	ctx := context.Background()
	options, cleanup := lstesting.FakeSchema(t, "CASCADE")

	db1, err := sql.Open("postgres", dsn)
	require.NoError(t, err, "open first database")
	defer func() {
		t.Log("Closing db1...")
		_ = db1.Close()
		t.Log("done")
	}()
	defer func() {
		t.Log("Doing cleanup...")
		cleanup(db1)
		t.Log("done")
	}()

	db2, err := sql.Open("postgres", dsn)
	require.NoError(t, err, "open second database")
	defer func() {
		t.Log("Closing db2...")
		_ = db2.Close()
		t.Log("done")
	}()

	asyncStarted := make(chan struct{})
	releaseAsync := make(chan struct{})
	firstComplete := make(chan struct{})

	options1 := options
	options1.OnMigrationsComplete = func(_ *libschema.Database, err error) {
		assert.NoError(t, err)
		close(firstComplete)
	}

	s1 := libschema.New(ctx, options1)
	dbase1, err := lspostgres.New(libschema.LogFromLog(t), "test", s1, db1)
	require.NoError(t, err)
	dbase1.Migrations("L1",
		lspostgres.Generate("M1", func(_ context.Context, _ *sql.Tx) string {
			return `CREATE TABLE m1_locktest (id text)`
		}),
		lspostgres.Computed("M2", func(_ context.Context, _ *sql.Tx) error {
			close(asyncStarted)
			nt := time.NewTimer(time.Second)
			defer nt.Stop()
			select {
			case <-releaseAsync:
				return nil
			case <-nt.C:
				return errors.Errorf("timeout waiting to release async migration")
			}
		}, libschema.Asynchronous()),
	)

	migrateCtx1, cancel1 := context.WithTimeout(ctx, 3*time.Second)
	defer cancel1()
	require.NoError(t, s1.Migrate(migrateCtx1), "first migrate should return after synchronous work")
	ntStart := time.NewTimer(time.Second)
	defer ntStart.Stop()
	select {
	case <-asyncStarted:
	case <-ntStart.C:
		assert.FailNow(t, "timeout waiting for async start", "async migration did not start")
	}

	startedSecond := make(chan struct{})
	options2 := options
	// Reaching OnMigrationsStarted means the second migrator acquired the table lock.
	options2.OnMigrationsStarted = func(_ *libschema.Database) {
		close(startedSecond)
	}

	s2 := libschema.New(ctx, options2)
	dbase2, err := lspostgres.New(libschema.LogFromLog(t), "test", s2, db2)
	require.NoError(t, err)
	dbase2.Migrations("L2",
		lspostgres.Computed("M3", func(_ context.Context, _ *sql.Tx) error {
			return nil
		}),
	)

	secondDone := make(chan error, 1)
	migrateCtx2, cancel2 := context.WithTimeout(ctx, 3*time.Second)
	defer cancel2()
	secondAttempted := make(chan struct{})
	go func() {
		close(secondAttempted)
		secondDone <- s2.Migrate(migrateCtx2)
	}()

	ntAttempt := time.NewTimer(time.Second)
	defer ntAttempt.Stop()
	select {
	case <-secondAttempted:
	case <-ntAttempt.C:
		assert.FailNow(t, "timeout waiting for second migrate attempt", "second migrate was not started")
	}

	select {
	case <-startedSecond:
		assert.FailNow(t, "second migrate started too early", "migration table lock released before async migration completed")
	default:
	}

	close(releaseAsync)

	nt := time.NewTimer(time.Second)
	defer nt.Stop()
	select {
	case <-firstComplete:
	case <-nt.C:
		assert.FailNow(t, "timeout waiting for first completion", "first migration sequence did not complete")
	}

	nt2 := time.NewTimer(time.Second)
	defer nt2.Stop()
	select {
	case err := <-secondDone:
		require.NoError(t, err, "second migrate should proceed once first async migration has completed")
	case <-nt2.C:
		assert.FailNow(t, "timeout waiting for second migrate", "second migrate did not complete after lock release")
	}
}
