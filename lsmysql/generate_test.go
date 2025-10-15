package lsmysql_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/muir/libschema"
	"github.com/muir/libschema/lsmysql"
	"github.com/muir/libschema/lstesting"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// openMySQL helper already defined in inference_test.go; reuse it.

// TestMySQLGenerateClassification verifies Generate producing DDL is auto non-tx while pure DML stays tx.
func TestMySQLGenerateClassification(t *testing.T) {
	db := openMySQL(t)
	opts, cleanup := lstesting.FakeSchema(t, "")
	defer func() { cleanup(db) }()
	s := libschema.New(context.Background(), opts)
	log := libschema.LogFromLog(t)
	dbase, _, err := lsmysql.New(log, "gen_cls", s, db)
	require.NoError(t, err)
	ddl := lsmysql.Generate("DDL", func(_ context.Context, _ *sql.Tx) string { return "CREATE TABLE IF NOT EXISTS gen_cls (id int)" })
	dml := lsmysql.Generate("DML", func(_ context.Context, _ *sql.Tx) string { return "INSERT INTO gen_cls (id) VALUES (1)" })
	forced := lsmysql.Generate("FORCED_NONTX", func(_ context.Context, _ *sql.Tx) string { return "INSERT INTO gen_cls (id) VALUES (2)" }, libschema.ForceNonTransactional())
	dbase.Migrations("GEN_LIB", ddl, dml, forced)
	require.NoError(t, s.Migrate(context.Background()))

	// Lookup stored copies (Database stores migrated copies) and inspect nonTransactional flag state
	ddlStored, ok := dbase.Lookup(libschema.MigrationName{Library: "GEN_LIB", Name: "DDL"})
	require.True(t, ok)
	dmlStored, ok := dbase.Lookup(libschema.MigrationName{Library: "GEN_LIB", Name: "DML"})
	require.True(t, ok)
	assert.True(t, ddlStored.Base().NonTransactional(), "DDL migration should be downgraded to non-tx")
	assert.False(t, dmlStored.Base().NonTransactional(), "ordinary DML migration should remain transactional")
	forcedStored, ok := dbase.Lookup(libschema.MigrationName{Library: "GEN_LIB", Name: "FORCED_NONTX"})
	require.True(t, ok)
	assert.True(t, forcedStored.Base().ForcedNonTransactional(), "forced non-tx DML Generate should be marked forced non-transactional")

	// Verify row inserted
	var cnt int
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM gen_cls").Scan(&cnt))
	assert.Equal(t, 2, cnt)
}

// TestMySQLGenerateForceTransactionalError ensures forcing transactional on DDL causes error.
func TestMySQLGenerateForceTransactionalError(t *testing.T) {
	db := openMySQL(t)
	opts, cleanup := lstesting.FakeSchema(t, "")
	defer func() { cleanup(db) }()
	s := libschema.New(context.Background(), opts)
	log := libschema.LogFromLog(t)
	dbase, _, err := lsmysql.New(log, "gen_force", s, db)
	require.NoError(t, err)

	ddlForce := lsmysql.Generate("DDL_FORCE", func(_ context.Context, _ *sql.Tx) string { return "CREATE TABLE IF NOT EXISTS gen_force (id int)" }, libschema.ForceTransactional())
	dbase.Migrations("GEN_FORCE_LIB", ddlForce)
	err = s.Migrate(context.Background())
	require.Error(t, err, "expected error forcing transactional on DDL in MySQL")
	assert.Contains(t, err.Error(), "cannot force transactional")
	stored, ok := dbase.Lookup(libschema.MigrationName{Library: "GEN_FORCE_LIB", Name: "DDL_FORCE"})
	require.True(t, ok)
	st := stored.Base().Status()
	assert.False(t, st.Done, "migration should not be marked done after failure")
	assert.NotEmpty(t, st.Error)
}
