package lsmysql_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/muir/libschema"
	"github.com/muir/libschema/lsmysql"
	"github.com/muir/libschema/lstesting"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRepeat(t *testing.T) {
	dsn := os.Getenv("LIBSCHEMA_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_MYSQL_TEST_DSN to test libschema/lsmysql")
	}
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, db.Close())
	}()

	options, cleanup := lstesting.FakeSchema(t, "")
	defer func() {
		cleanup(db)
	}()
	options.DebugLogging = true

	s := libschema.New(context.Background(), options)

	dbase, _, err := lsmysql.New(libschema.LogFromLog(t), "test", s, db)
	require.NoError(t, err)

	dbase.Migrations("L1",
		lsmysql.Script("M1", `CREATE TABLE IF NOT EXISTS m1 (id text) ENGINE = InnoDB`),
		lsmysql.Script("M2", `
			INSERT INTO m1 (id) 
			VALUES ('x1'), ('x2'), ('x3')
		`),
		lsmysql.Script("M3", `
			UPDATE	m1
			SET	id = CONCAT('y', id)
			WHERE	id LIKE 'x%'
			LIMIT	1
		`, libschema.RepeatUntilNoOp()),
	)
	t.Log("migrate!")
	err = s.Migrate(context.Background())
	require.NoError(t, err)

	rows, err := db.Query(`
		SELECT	id
		FROM	` + options.SchemaOverride + `.m1
		ORDER	BY id ASC`)
	require.NoError(t, err)
	var found []string
	defer func() {
		_ = rows.Close()
	}()
	for rows.Next() {
		var id string
		err := rows.Scan(&id)
		require.NoError(t, err)
		found = append(found, id)
	}
	assert.Equal(t, []string{"yx1", "yx2", "yx3"}, found)
	t.Log("all done")
}
