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

// Since MySQL does not support schemas (it treats them like databases),
// LIBSCHEMA_MYSQL_TEST_DSN has to give access to a user that can create
// and destroy databases.

func TestMysqlHappyPath(t *testing.T) {
	dsn := os.Getenv("LIBSCHEMA_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_MYSQL_TEST_DSN to test libschema/lsmysql")
	}

	var actions []string

	options, cleanup := lstesting.FakeSchema(t, "")

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

	dbase, _, err := lsmysql.New(testinglogur.Get(t), "test", s, db)
	require.NoError(t, err, "libschema NewDatabase")

	dbase.Migrations("L1",
		lsmysql.Generate("T1", func(_ context.Context, _ libschema.MyLogger, _ *sql.Tx) string {
			actions = append(actions, "MIGRATE: L1.T1")
			return `CREATE TABLE IF NOT EXISTS T1 (id text) ENGINE = InnoDB`
		}),
		lsmysql.Script("T2pre", `
				INSERT INTO T1 (id) VALUES ('T2');`),
		lsmysql.Script("T2pre2", `
				INSERT INTO T3 (id) VALUES ('T2');`,
			libschema.After("L2", "T3")),
		lsmysql.Computed("T2", func(_ context.Context, _ libschema.MyLogger, tx *sql.Tx) error {
			actions = append(actions, "MIGRATE: L1.T2")
			_, err := tx.Exec(`
				CREATE TABLE IF NOT EXISTS T2 (id text) ENGINE = InnoDB`)
			return err
		}),
		lsmysql.Generate("PT1", func(_ context.Context, _ libschema.MyLogger, _ *sql.Tx) string {
			actions = append(actions, "MIGRATE: L1.PT1")
			return `
				INSERT INTO T1 (id) VALUES ('PT1');
			`
		}),
		lsmysql.Script("PT1p1", `
				INSERT INTO T2 (id) VALUES ('PT1');`),
		lsmysql.Script("PT1p2", `
				INSERT INTO T3 (id) VALUES ('PT1');`),
	)

	dbase.Migrations("L2",
		lsmysql.Script("T3pre", `
				INSERT INTO T1 (id) VALUES ('T3');`),
		lsmysql.Generate("T3", func(_ context.Context, _ libschema.MyLogger, _ *sql.Tx) string {
			actions = append(actions, "MIGRATE: L2.T3")
			return `
				CREATE TABLE IF NOT EXISTS T3 (id text) ENGINE = InnoDB`
		}),
		lsmysql.Script("T4pre1", `
				INSERT INTO T1 (id) VALUES ('T4');`),
		lsmysql.Script("T4pre2", `
				INSERT INTO T2 (id) VALUES ('T4');`),
		lsmysql.Script("T4pre3", `
				INSERT INTO T3 (id) VALUES ('T4');`),
		lsmysql.Generate("T4", func(_ context.Context, _ libschema.MyLogger, _ *sql.Tx) string {
			actions = append(actions, "MIGRATE: L2.T4")
			return `
				CREATE TABLE IF NOT EXISTS T4 (id text) ENGINE = InnoDB`
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
		WHERE	table_schema = ?
		ORDER	BY table_name`, options.SchemaOverride)
	require.NoError(t, err, "query for list of tables")
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		assert.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	assert.Equal(t, []string{"T1", "T2", "T3", "T4", "tracking_table"}, names, "table names")
}

func TestMysqlNotAllowed(t *testing.T) {
	dsn := os.Getenv("LIBSCHEMA_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_MYSQL_TEST_DSN to test libschema/lsmysql")
	}

	cases := []struct {
		name      string
		migration string
		errorText string
	}{
		{
			name:      "combines",
			migration: `CREATE TABLE T1 (id text) ENGINE=InnoDB; INSERT INTO T1 (id) VALUES ('x');`,
			errorText: "Migration combines DDL",
		},
		{
			name:      "unconditional",
			migration: `CREATE TABLE T1 (id text) ENGINE=InnoDB`,
			errorText: "Unconditional migration has non-idempotent",
		},
	}

	for _, tc := range cases {
		options, cleanup := lstesting.FakeSchema(t, "")
		options.DebugLogging = true
		s := libschema.New(context.Background(), options)

		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err, "open database")
		defer db.Close()
		defer cleanup(db)

		dbase, _, err := lsmysql.New(testinglogur.Get(t), "test", s, db)
		require.NoError(t, err, "libschema NewDatabase")

		dbase.Migrations("L1", lsmysql.Script("x", tc.migration))

		err = s.Migrate(context.Background())
		if assert.Error(t, err, tc.name) {
			assert.Contains(t, err.Error(), tc.errorText, tc.name)
		}
	}
}

func TestSkipFunctions(t *testing.T) {
	dsn := os.Getenv("LIBSCHEMA_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_MYSQL_TEST_DSN to test libschema/lsmysql")
	}

	options, cleanup := lstesting.FakeSchema(t, "")
	options.DebugLogging = true
	s := libschema.New(context.Background(), options)

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "open database")
	defer db.Close()
	defer cleanup(db)

	dbase, m, err := lsmysql.New(testinglogur.Get(t), "test", s, db)
	require.NoError(t, err, "libschema NewDatabase")

	dbase.Migrations("T",
		lsmysql.Script("setup1", `
			CREATE TABLE IF NOT EXISTS users (
				id	varchar(255),
				level	integer DEFAULT 37,
				PRIMARY KEY (id)
			) ENGINE=InnoDB`),
		lsmysql.Script("setup2", `
			CREATE TABLE IF NOT EXISTS accounts (
				id	varchar(255)
			) ENGINE=InnoDB`),
		lsmysql.Script("setup3", `
			ALTER TABLE users
				ADD CONSTRAINT hi_level 
					CHECK (level > 10) ENFORCED`,
			libschema.SkipIf(func() (bool, error) {
				t, _, err := m.GetTableConstraint("users", "hi_level")
				return t != "", err
			})),
	)

	err = s.Migrate(context.Background())
	assert.NoError(t, err)

	hasPK, err := m.HasPrimaryKey("users")
	if assert.NoError(t, err, "users has pk") {
		assert.True(t, hasPK, "users has pk")
	}
	hasPK, err = m.HasPrimaryKey("accounts")
	if assert.NoError(t, err, "accounts has pk") {
		assert.False(t, hasPK, "accounts has pk")
	}
	dflt, err := m.ColumnDefault("users", "id")
	if assert.NoError(t, err, "user id default") {
		assert.Nil(t, dflt, "user id default")
	}
	dflt, err = m.ColumnDefault("users", "level")
	if assert.NoError(t, err, "user level default") {
		if assert.NotNil(t, dflt, "user level default") {
			assert.Equal(t, "37", *dflt, "user id default")
		}
	}
	exists, err := m.DoesColumnExist("users", "foo")
	if assert.NoError(t, err, "users has foo") {
		assert.False(t, exists, "users has foo")
	}
	exists, err = m.DoesColumnExist("users", "level")
	if assert.NoError(t, err, "users has level") {
		assert.True(t, exists, "users has level")
	}
	typ, enf, err := m.GetTableConstraint("users", "hi_level")
	if assert.NoError(t, err, "users hi_level constraint") {
		assert.Equal(t, "CHECK", typ, "users hi_level constraint")
		assert.True(t, enf, "users hi_level constraint")
	}
}

func pointerToString(s string) *string {
	return &s
}
