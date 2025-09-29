package lspostgres

import (
    "context"
    "database/sql"
    "os"
    "sync/atomic"
    "testing"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"

    "github.com/muir/libschema"
)

// TestScriptAutoClassification ensures Script() marks known non-transactional statements.
func TestScriptAutoClassification(t *testing.T) {
    m1 := Script("CIC", "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx ON t (id)")
    assert.True(t, m1.Base().NonTransactional(), "expected create index concurrently to be auto non-tx")

    m2 := Script("NORMAL", "CREATE TABLE foo (id int)")
    assert.False(t, m2.Base().NonTransactional(), "regular CREATE TABLE should default to transactional")
}

// TestForceTransactionalOverride verifies ForceTransactional() can override auto non-tx classification.
func TestForceTransactionalOverride(t *testing.T) {
    m := Script("CIC", "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx ON t (id)", libschema.ForceTransactional())
    assert.False(t, m.Base().NonTransactional(), "force transactional should override auto classification")
}

// TestTrackingSchemaTableInvalid ensures invalid tracking table produces an error.
func TestTrackingSchemaTableInvalid(t *testing.T) {
    // Directly exercise helper with invalid format (three-part name)
    d := &libschema.Database{Options: libschema.Options{TrackingTable: "a.b.c"}}
    _, _, err := trackingSchemaTable(d)
    assert.Error(t, err, "expected error for three-part tracking table name")
}

// TestServerVersionCaching ensures the SHOW server_version query only executes once.
func TestServerVersionCaching(t *testing.T) {
    dsn := os.Getenv("LIBSCHEMA_POSTGRES_TEST_DSN")
    if dsn == "" {
        t.Skip("Set $LIBSCHEMA_POSTGRES_TEST_DSN to run Postgres tests")
    }
    db, err := sql.Open("postgres", dsn)
    require.NoError(t, err)
    defer func() { _ = db.Close() }()

    p := &Postgres{log: libschema.LogFromLog(t), nonTxStmtRegex: baseNonTxStmtRegex}
    major1, minor1 := p.ServerVersion(context.Background(), db)
    // Call again; if caching works, values identical and no second query side-effect we can directly observe.
    major2, minor2 := p.ServerVersion(context.Background(), db)
    assert.Equal(t, major1, major2)
    assert.Equal(t, minor1, minor2)
    // Indirectly assert serverOnce applied by toggling state & calling again should not change values.
    oldMajor := p.serverMajor
    p.serverMajor = 999 // mutate; next call should return mutated (since caching already done) and not re-query
    m3, _ := p.ServerVersion(context.Background(), db)
    // After mutation, value should reflect mutation proving no further query executed.
    assert.Equal(t, 999, m3)
    // restore for cleanliness
    p.serverMajor = oldMajor
    _ = atomic.Int32{} // avoid unused import if atomic added later; placeholder
}
