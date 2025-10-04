package lspostgres_test

import (
    "context"
    "database/sql"
    "os"
    "testing"

    "github.com/stretchr/testify/require"
    "github.com/stretchr/testify/assert"

    "github.com/muir/libschema"
    "github.com/muir/libschema/lspostgres"
    "github.com/muir/libschema/lstesting"
)

// TestSchemaOverrideTransactional ensures that when SchemaOverride is set the driver
// issues a SET search_path inside the migration transaction so unqualified objects
// are created in the override schema.
func TestSchemaOverrideTransactional(t *testing.T) {
    dsn := os.Getenv("LIBSCHEMA_POSTGRES_TEST_DSN")
    if dsn == "" { t.Skip("Set $LIBSCHEMA_POSTGRES_TEST_DSN to run this test") }

    db, err := sql.Open("postgres", dsn)
    require.NoError(t, err)
    t.Cleanup(func(){ _ = db.Close() })

    opts, cleanup := lstesting.FakeSchema(t, "CASCADE")
    t.Cleanup(func(){ cleanup(db) })

    ctx := context.Background()
    s := libschema.New(ctx, opts)
    log := libschema.LogFromLog(t)
    dbase, err := lspostgres.New(log, "schema_override_tx", s, db)
    require.NoError(t, err)

    // Migration creates table with unqualified name; should land in SchemaOverride schema.
    dbase.Migrations("L1",
        lspostgres.Script("CREATE_T", "CREATE TABLE so_test (id int)"),
    )

    require.NoError(t, s.Migrate(ctx))

    // Verify table exists only within SchemaOverride schema.
    var count int
    err = db.QueryRow(`SELECT count(*) FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'so_test'`, opts.SchemaOverride).Scan(&count)
    require.NoError(t, err)
    assert.Equal(t, 1, count, "expected so_test in override schema")

    // Sanity: ensure no table with same name in public schema.
    err = db.QueryRow(`SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'so_test'`).Scan(&count)
    require.NoError(t, err)
    assert.Equal(t, 0, count, "should not create table in public schema when SchemaOverride set")
}
