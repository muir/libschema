package lssinglestore_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/muir/libschema"
	"github.com/muir/libschema/lssinglestore"
	"github.com/muir/libschema/lstesting"
)

func TestRepeat(t *testing.T) {
	dsn := os.Getenv("LIBSCHEMA_SINGLESTORE_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_SINGLESTORE_TEST_DSN to run SingleStore tests")
	}
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer func() { assert.NoError(t, db.Close()) }()

	options, cleanup := lstesting.FakeSchema(t, "")
	defer func() { cleanup(db) }()
	options.DebugLogging = true

	s := libschema.New(context.Background(), options)
	log := libschema.LogFromLog(t)
	dbase, _, err := lssinglestore.New(log, "test", s, db)
	require.NoError(t, err)

	dbase.Migrations("L1",
		lssinglestore.Script("M1", `CREATE TABLE IF NOT EXISTS m1 (id text)`),
		lssinglestore.Script("M2", `INSERT INTO m1 (id) VALUES ('x1'), ('x2'), ('x3')`),
		// SingleStore does not support UPDATE ... LIMIT across shards; select a single id via subquery.
		lssinglestore.Script("M3", `UPDATE m1 SET id = CONCAT('y', id) WHERE id = (
			SELECT id FROM m1 WHERE id LIKE 'x%' ORDER BY id LIMIT 1
		)`, libschema.RepeatUntilNoOp()),
	)

	require.NoError(t, s.Migrate(context.Background()))

	rows, err := db.Query(`SELECT id FROM ` + options.SchemaOverride + `.m1 ORDER BY id ASC`)
	require.NoError(t, err)
	var found []string
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id string
		require.NoError(t, rows.Scan(&id))
		found = append(found, id)
	}
	assert.Equal(t, []string{"yx1", "yx2", "yx3"}, found)
}
