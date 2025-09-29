package lspostgres_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/muir/libschema"
	"github.com/muir/libschema/lsmysql"
	"github.com/muir/libschema/lspostgres"
	"github.com/muir/libschema/lstesting"
)

func TestBadMigrationsPostgres(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		substring string
		sentinel  error
		define    func(*libschema.Database)
		reopt     func(o *libschema.Options)
		first     func(*libschema.Database)
	}{
		{
			name:      "table missing",
			substring: `relation "t1" does not exist`,
			define: func(dbase *libschema.Database) {
				dbase.Migrations("L2",
					lspostgres.Script("T4", `INSERT INTO T1 (id) VALUES ('T4')`),
				)
			},
		},
		{
			name:      "unknown migration",
			substring: `1 unknown migrations, including L1: M1`,
			first: func(dbase *libschema.Database) {
				dbase.Migrations("L1",
					lspostgres.Script("M1", `CREATE TABLE M1 (id text)`),
				)
			},
			define: func(dbase *libschema.Database) {
				dbase.Migrations("L2",
					lspostgres.Script("T4", `INSERT INTO T1 (id) VALUES ('T4')`),
				)
			},
			reopt: func(o *libschema.Options) {
				o.ErrorOnUnknownMigrations = true
			},
		},
		{
			name:      "bad skip",
			substring: `skipIf L2: T4: oops`,
			define: func(dbase *libschema.Database) {
				dbase.Migrations("L2",
					lspostgres.Script("T4", `INSERT INTO T1 (id) VALUES ('T4')`,
						libschema.SkipIf(func() (bool, error) {
							return false, fmt.Errorf("oops")
						})),
				)
			},
		},
		{
			name:      "wrong db",
			substring: `non-postgres`,
			define: func(dbase *libschema.Database) {
				dbase.Migrations("L2",
					lsmysql.Script("T4", `INSERT INTO T1 (id) VALUES ('T4')`),
				)
			},
		},
		{
			name:      "bad dependency",
			substring: `migration T4 for L2 is supposed to be after T9 for T1 but that cannot be found`,
			define: func(dbase *libschema.Database) {
				dbase.Migrations("L2",
					lsmysql.Script("T4", `INSERT INTO T1 (id) VALUES ('T4')`,
						libschema.After("T1", "T9"),
					),
				)
			},
		},
		{
			name:      "duplicate library",
			substring: `duplicate library 'L2'`,
			define: func(dbase *libschema.Database) {
				dbase.Migrations("L2", lspostgres.Script("T4", `CREATE TABLE T1 (id text)`))
				dbase.Migrations("L2", lspostgres.Script("T5", `CREATE TABLE T2 (id text)`))
			},
		},
		{
			name:      "bad table",
			substring: `tracking table 'foo.bar.baz' is not valid`,
			reopt: func(o *libschema.Options) {
				o.TrackingTable = "foo.bar.baz"
			},
		},
		{
			name:      "bad schema",
			substring: `no schema has been selected to create in`,
			reopt: func(o *libschema.Options) {
				o.SchemaOverride = "foo.bar.baz"
			},
		},
		{
			name:      "bad dsn",
			substring: `could not find appropriate database driver for DSN`,
			reopt: func(o *libschema.Options) {
				o.Overrides = &libschema.OverrideOptions{
					MigrateDSN: "xyz",
				}
			},
		},
		// Add sentinel-based validation cases (non-idempotent / mixes data & DDL) using stmtcheck
		{
			name:     "non idempotent",
			sentinel: libschema.ErrNonIdempotentNonTx,
			define: func(dbase *libschema.Database) {
				// Force non-transactional so idempotency validation executes for CREATE TABLE
				dbase.Migrations("L2", lspostgres.Script("T4", `CREATE TABLE t1 (id text)`, libschema.ForceNonTransactional()))
			},
		},
		{
			name:     "combines data & ddl (multi-stmt non-tx)",
			sentinel: libschema.ErrNonTxMultipleStatements,
			define: func(dbase *libschema.Database) {
				// Forcing non-transactional turns multi-statement into an error before stmtcheck classification
				dbase.Migrations("L2", lspostgres.Script("T4", `CREATE TABLE IF NOT EXISTS t1 (id text); INSERT INTO t1 (id) VALUES ('foo')`, libschema.ForceNonTransactional()))
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			testBadMigration(t, tc.substring, tc.sentinel, tc.define, tc.reopt, tc.first)
		})
	}
}

func testBadMigration(t *testing.T, expectedSubstring string, sentinel error,
	define func(*libschema.Database),
	reopt func(*libschema.Options),
	first func(*libschema.Database),
) {
	dsn := os.Getenv("LIBSCHEMA_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_POSTGRES_TEST_DSN to test libschema/lspostgres")
	}

	options, cleanup := lstesting.FakeSchema(t, "CASCADE")
	options.DebugLogging = true

	db, err := libschema.OpenAnyDB(dsn)
	require.NoError(t, err, "open database")
	defer func() {
		assert.NoError(t, db.Close())
	}()
	defer cleanup(db)

	if first != nil {
		s := libschema.New(context.Background(), options)
		dbase, err := lspostgres.New(libschema.LogFromLog(t), "test", s, db)
		require.NoError(t, err, "libschema NewDatabase")
		first(dbase)
		err = s.Migrate(context.Background())
		require.NoError(t, err, "first")
	}

	if reopt != nil {
		reopt(&options)
	}

	s := libschema.New(context.Background(), options)
	dbase, err := lspostgres.New(libschema.LogFromLog(t), "test", s, db)
	require.NoError(t, err, "libschema NewDatabase")

	t.Log("now we define the migrations")
	if define != nil {
		define(dbase)
	} else {
		dbase.Migrations("L2", lspostgres.Script("T9", `CREATE TABLE T1 (id text)`))
	}

	err = s.Migrate(context.Background())
	if assert.Error(t, err, "should error") {
		if sentinel != nil {
			assert.ErrorIs(t, err, sentinel)
		} else if expectedSubstring != "" {
			assert.Contains(t, err.Error(), expectedSubstring)
		} else {
			assert.Fail(t, "no expected substring or sentinel provided")
		}
	}
}
