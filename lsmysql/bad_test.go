package lsmysql_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/memsql/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/muir/libschema"
	"github.com/muir/libschema/internal/stmtcheck"
	"github.com/muir/libschema/lsmysql"
	"github.com/muir/libschema/lspostgres"
	"github.com/muir/libschema/lstesting"
)

func TestBadMigrationsMysql(t *testing.T) {
	t.Parallel()
	type testCase struct {
		name      string
		substring string
		sentinel  error
		define    func(*libschema.Database)
		reopt     func(o *libschema.Options)
	}
	cases := []testCase{
		{
			name:      "table missing",
			substring: `.T1' doesn't exist`,
			define: func(dbase *libschema.Database) {
				dbase.Migrations("L2",
					lsmysql.Script("T4", `INSERT INTO T1 (id) VALUES ('T4')`),
				)
			},
		},
		{
			name:      "wrong db",
			substring: `non-mysql`,
			define: func(dbase *libschema.Database) {
				dbase.Migrations("L2",
					lspostgres.Script("T4", `INSERT INTO T1 (id) VALUES ('T4')`),
				)
			},
		},
		{
			name:      "duplicate library",
			substring: `duplicate library 'L2'`,
			define: func(dbase *libschema.Database) {
				dbase.Migrations("L2", lsmysql.Script("T4", `CREATE TABLE T1 (id text)`))
				dbase.Migrations("L2", lsmysql.Script("T5", `CREATE TABLE T2 (id text)`))
			},
		},
		{
			name:      "bad table1",
			substring: `tracking table 'foo.bar.baz' is not valid`,
			reopt: func(o *libschema.Options) {
				o.TrackingTable = "foo.bar.baz"
			},
		},
		{
			name:      "bad table2",
			substring: `tracking table schema name must be a simple identifier, not 'foo'bar'`,
			reopt: func(o *libschema.Options) {
				o.TrackingTable = "foo'bar.baz"
			},
		},
		{
			name:     "non idempotent",
			sentinel: stmtcheck.ErrNonIdempotentDDL,
			define: func(dbase *libschema.Database) {
				dbase.Migrations("L2", lsmysql.Script("T4", `CREATE TABLE T1 (id text) TYPE = InnoDB`))
			},
		},
		{
			name:     "combines data & ddl",
			sentinel: stmtcheck.ErrDataAndDDL,
			define: func(dbase *libschema.Database) {
				dbase.Migrations("L2", lsmysql.Script("T4", `
					CREATE TABLE IF NOT EXISTST1 (id text) TYPE = InnoDB;
					INSERT INTO T1 (id) VALUES ('foo');
					`))
			},
		},
		{
			name:      "bad table3",
			substring: `tracking table table name must be a simple identifier, not 'bar'baz'`,
			reopt: func(o *libschema.Options) {
				o.TrackingTable = "foo.bar'baz"
			},
		},
		{
			name:      "bad table4",
			substring: `tracking table table name must be a simple identifier, not 'bar'baz'`,
			reopt: func(o *libschema.Options) {
				o.TrackingTable = "bar'baz"
			},
		},
		{
			name:      "bad schema",
			substring: `options.SchemaOverride must be a simple identifier`,
			reopt: func(o *libschema.Options) {
				o.SchemaOverride = `"foo."bar.baz"`
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			testBadMigration(t, tc.substring, tc.sentinel, tc.define, tc.reopt)
		})
	}
}

func testBadMigration(t *testing.T, expectedSubstring string, sentinel error, define func(*libschema.Database), reopt func(*libschema.Options)) {
	dsn := os.Getenv("LIBSCHEMA_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_MYSQL_TEST_DSN to test libschema/lsmysql")
	}

	options, cleanup := lstesting.FakeSchema(t, "")
	options.DebugLogging = true

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "open database")
	defer func() {
		assert.NoError(t, db.Close())
	}()
	defer cleanup(db)

	if reopt != nil {
		reopt(&options)
	}

	s := libschema.New(context.Background(), options)
	dbase, _, err := lsmysql.New(libschema.LogFromLog(t), "test", s, db)
	require.NoError(t, err, "libschema NewDatabase")

	t.Log("now we define the migrations")
	if define != nil {
		define(dbase)
	} else {
		dbase.Migrations("L2", lsmysql.Script("T4", `CREATE TABLE T1 (id text)`))
	}

	err = s.Migrate(context.Background())
	if assert.Error(t, err, "should error") {
		if sentinel != nil {
			assert.True(t, errors.Is(err, sentinel), "expected sentinel %v got %v", sentinel, err)
		} else if expectedSubstring != "" {
			assert.Contains(t, err.Error(), expectedSubstring)
		} else {
			assert.Fail(t, "no expected substring or sentinel provided")
		}
	}
}
