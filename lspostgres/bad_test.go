package lspostgres_test

import (
	"context"
	"os"
	"testing"

	"github.com/muir/libschema"
	"github.com/muir/libschema/lsmysql"
	"github.com/muir/libschema/lspostgres"
	"github.com/muir/libschema/lstesting"
	"github.com/muir/testinglogur"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBadMigrations(t *testing.T) {
	cases := []struct {
		name   string
		error  string
		define func(*libschema.Database)
	}{
		{
			name:  "table missing",
			error: `relation "t1" does not exist`,
			define: func(dbase *libschema.Database) {
				dbase.Migrations("L2",
					lspostgres.Script("T4", `INSERT INTO T1 (id) VALUES ('T4')`),
				)
			},
		},
		{
			name:  "table missing",
			error: `Non-postgres`,
			define: func(dbase *libschema.Database) {
				dbase.Migrations("L2",
					lsmysql.Script("T4", `INSERT INTO T1 (id) VALUES ('T4')`),
				)
			},
		},
		{
			name:  "duplicate library",
			error: `duplicate library 'L2'`,
			define: func(dbase *libschema.Database) {
				dbase.Migrations("L2", lspostgres.Script("T4", `CREATE TABLE T1 (id text)`))
				dbase.Migrations("L2", lspostgres.Script("T5", `CREATE TABLE T2 (id text)`))
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			testBadMigration(t, tc.error, tc.define)
		})
	}
}

func testBadMigration(t *testing.T, expected string, define func(*libschema.Database)) {
	dsn := os.Getenv("LIBSCHEMA_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_POSTGRES_TEST_DSN to test libschema/lspostgres")
	}

	options, cleanup := lstesting.FakeSchema(t, "CASCADE")
	options.DebugLogging = true

	db, err := libschema.OpenAnyDB(dsn)
	require.NoError(t, err, "open database")
	defer db.Close()
	defer cleanup(db)

	s := libschema.New(context.Background(), options)
	dbase, err := lspostgres.New(testinglogur.Get(t), "test", s, db)
	require.NoError(t, err, "libschema NewDatabase")

	t.Log("now we define the migrations")
	define(dbase)

	err = s.Migrate(context.Background())
	if assert.Error(t, err, "should error") {
		assert.Contains(t, err.Error(), expected)
	}
}
