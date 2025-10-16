package lsmysql_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/muir/libschema"
	"github.com/muir/libschema/lsmysql"
	"github.com/muir/libschema/lstesting"
)

func TestSkipFunctions(t *testing.T) {
	t.Parallel()
	dsn := os.Getenv("LIBSCHEMA_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_MYSQL_TEST_DSN to test libschema/lsmysql")
	}

	options, cleanup := lstesting.FakeSchema(t, "")
	options.DebugLogging = true
	s := libschema.New(context.Background(), options)

	t.Log("DSN=", dsn)
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "open database")
	defer func() {
		assert.NoError(t, db.Close())
	}()
	_ = cleanup
	// defer cleanup(db)

	dbase, m, err := lsmysql.New(libschema.LogFromLog(t), "test", s, db)
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
			libschema.ForceNonTransactional(),
			libschema.SkipIf(func() (bool, error) {
				t, _, err := m.GetTableConstraint("users", "hi_level")
				return t != "", err
			})),
		lsmysql.Script("setup4", `
				CREATE INDEX level_idx ON users(level);`,
			libschema.ForceNonTransactional(),
			libschema.SkipIf(func() (bool, error) {
				b, err := m.TableHasIndex("users", "level_idx")
				return b, err
			})),
	)

	err = s.Migrate(context.Background())
	assert.NoError(t, err)

	dbName, err := m.DatabaseName()
	if assert.NoError(t, err, "database name") {
		t.Log("database name is set because it is inherited from options")
		assert.Equal(t, options.SchemaOverride, dbName, "database name")
	}

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

	exists, err = m.TableHasIndex("users", "level_idx")
	if assert.NoError(t, err, "has index users.level_idx") {
		assert.True(t, exists, "has users.level_idx")
	}
	exists, err = m.TableHasIndex("foobar", "level_idx")
	if assert.NoError(t, err, "has foobar.level_idx") {
		assert.False(t, exists, "has foobar.level_idx")
	}
	exists, err = m.TableHasIndex("users", "foo_idx")
	if assert.NoError(t, err, "has users.foo_idx") {
		assert.False(t, exists, "has users.foo_idx")
	}

	m.UseDatabase("override")
	dbOverride, err := m.DatabaseName()
	if assert.NoError(t, err, "database name override") {
		assert.Equal(t, "override", dbOverride, "override")
	}
	m.UseDatabase("")
	dbNameReRestored, err := m.DatabaseName()
	if assert.NoError(t, err, "original database name #2") {
		assert.NotEqual(t, "override", dbNameReRestored, "un-override")
	}
}
