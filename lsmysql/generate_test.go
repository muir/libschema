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

// TestMySQLGenerateCases covers classification, forced non-tx, and force transactional error scenarios.
func TestMySQLGenerateCases(t *testing.T) {
	db := openMySQL(t)
	type result struct {
		expectErr   bool
		errContains string
		verify      func(t *testing.T, db *sql.DB, dbase *libschema.Database)
	}
	cases := []struct {
		name   string
		label  string
		build  func(libschema.Options, *sql.DB, *testing.T) ([]libschema.Migration, string)
		result result
	}{
		{
			name:  "classification_and_forced_non_tx",
			label: "gen_cls",
			build: func(opts libschema.Options, db *sql.DB, t *testing.T) ([]libschema.Migration, string) {
				ddl := lsmysql.Generate("DDL", func(_ context.Context, _ *sql.Tx) string { return "CREATE TABLE IF NOT EXISTS gen_cls (id int)" })
				dml := lsmysql.Generate("DML", func(_ context.Context, _ *sql.Tx) string { return "INSERT INTO gen_cls (id) VALUES (1)" })
				forced := lsmysql.Generate("FORCED_NONTX", func(_ context.Context, _ *sql.Tx) string { return "INSERT INTO gen_cls (id) VALUES (2)" }, libschema.ForceNonTransactional())
				return []libschema.Migration{ddl, dml, forced}, "GEN_LIB"
			},
			result: result{
				verify: func(t *testing.T, db *sql.DB, dbase *libschema.Database) {
					ddlStored, ok := dbase.Lookup(libschema.MigrationName{Library: "GEN_LIB", Name: "DDL"})
					require.True(t, ok)
					assert.True(t, ddlStored.Base().NonTransactional())
					dmlStored, ok := dbase.Lookup(libschema.MigrationName{Library: "GEN_LIB", Name: "DML"})
					require.True(t, ok)
					assert.False(t, dmlStored.Base().NonTransactional())
					forcedStored, ok := dbase.Lookup(libschema.MigrationName{Library: "GEN_LIB", Name: "FORCED_NONTX"})
					require.True(t, ok)
					assert.True(t, forcedStored.Base().ForcedNonTransactional())
					var cnt int
					require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM gen_cls").Scan(&cnt))
					assert.Equal(t, 2, cnt)
				},
			},
		},
		{
			name:  "force_transactional_error_on_ddl",
			label: "gen_force",
			build: func(opts libschema.Options, db *sql.DB, t *testing.T) ([]libschema.Migration, string) {
				ddlForce := lsmysql.Generate("DDL_FORCE", func(_ context.Context, _ *sql.Tx) string { return "CREATE TABLE IF NOT EXISTS gen_force (id int)" }, libschema.ForceTransactional())
				return []libschema.Migration{ddlForce}, "GEN_FORCE_LIB"
			},
			result: result{
				expectErr:   true,
				errContains: "cannot force transactional",
				verify: func(t *testing.T, db *sql.DB, dbase *libschema.Database) {
					stored, ok := dbase.Lookup(libschema.MigrationName{Library: "GEN_FORCE_LIB", Name: "DDL_FORCE"})
					require.True(t, ok)
					st := stored.Base().Status()
					assert.False(t, st.Done)
					assert.NotEmpty(t, st.Error)
				},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts, cleanup := lstesting.FakeSchema(t, "")
			defer cleanup(db)
			s := libschema.New(context.Background(), opts)
			log := libschema.LogFromLog(t)
			dbase, _, err := lsmysql.New(log, tc.label, s, db)
			require.NoError(t, err)
			migs, lib := tc.build(opts, db, t)
			dbase.Migrations(lib, migs...)
			err = s.Migrate(context.Background())
			if tc.result.expectErr {
				require.Error(t, err)
				if tc.result.errContains != "" {
					assert.Contains(t, err.Error(), tc.result.errContains)
				}
			} else {
				require.NoError(t, err)
			}
			if tc.result.verify != nil {
				tc.result.verify(t, db, dbase)
			}
		})
	}
}
