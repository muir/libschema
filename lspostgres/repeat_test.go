package lspostgres_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/muir/libschema"
	"github.com/muir/libschema/lspostgres"
	"github.com/muir/libschema/lstesting"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRepeat(t *testing.T) {
	dsn := os.Getenv("LIBSCHEMA_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_POSTGRES_TEST_DSN to run this test")
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, db.Close())
	}()

	options, cleanup := lstesting.FakeSchema(t, "CASCADE")
	defer func() {
		cleanup(db)
	}()
	options.DebugLogging = true

	s := libschema.New(context.Background(), options)

	dbase, err := lspostgres.New(libschema.LogFromLog(t), "test", s, db)
	require.NoError(t, err)

	dbase.Migrations("L1",
		lspostgres.Script("M1", `CREATE TABLE m1 (id text)`),
		lspostgres.Script("M2", `
			INSERT INTO m1 (id) 
			VALUES ('x1'), ('x2'), ('x3')
		`),
		lspostgres.Script("M3", `
			WITH one AS (
				SELECT	id
				FROM	m1
				WHERE	id LIKE 'x%'
				LIMIT	1
			)
			UPDATE	m1
			SET	id = 'y' || m1.id
			FROM	one
			WHERE	m1.id = one.id
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
