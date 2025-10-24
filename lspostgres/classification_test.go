package lspostgres

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/muir/libschema"
	"github.com/muir/libschema/lstesting"
)

// TestPostgresClassificationCases combines classification success and force transactional override error.
func TestPostgresClassificationCases(t *testing.T) {
	dsn := os.Getenv("LIBSCHEMA_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_POSTGRES_TEST_DSN to run Postgres tests")
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	type outcome struct {
		expectErr   bool
		errContains string
		verify      func(t *testing.T, db *sql.DB, opts libschema.Options)
	}
	cases := []struct {
		name  string
		label string
		build func(schema *libschema.Schema, db *sql.DB) ([]libschema.Migration, string, outcome)
	}{
		{
			name:  "auto_classification_success",
			label: "test",
			build: func(schema *libschema.Schema, db *sql.DB) ([]libschema.Migration, string, outcome) {
				migTable := Script("NORMAL", "CREATE TABLE IF NOT EXISTS tmp_ci_deferred (id int)")
				migConcurrent := Script("CIC", "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_deferred ON tmp_ci_deferred (id)")
				return []libschema.Migration{migTable, migConcurrent}, "L", outcome{
					verify: func(t *testing.T, db *sql.DB, opts libschema.Options) {
						row := db.QueryRow(`SELECT 1 FROM pg_indexes WHERE schemaname = $1 AND indexname = 'idx_deferred'`, opts.SchemaOverride)
						var one int
						require.NoError(t, row.Scan(&one))
					},
				}
			},
		},
		{
			name:  "force_transactional_error",
			label: "test",
			build: func(schema *libschema.Schema, db *sql.DB) ([]libschema.Migration, string, outcome) {
				setup := Script("SETUP", "CREATE TABLE IF NOT EXISTS tmp_force (id int)")
				m := Script("CIC", "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_force ON tmp_force (id)", libschema.ForceTransactional())
				return []libschema.Migration{setup, m}, "L_FORCE_TX", outcome{
					expectErr:   true,
					errContains: "cannot run inside a transaction",
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts, cleanup := lstesting.FakeSchema(t, "CASCADE")
			defer cleanup(db)
			_, err := db.Exec("CREATE SCHEMA " + opts.SchemaOverride)
			require.NoError(t, err)
			schema := libschema.New(context.Background(), opts)
			log := libschema.LogFromLog(t)
			ldb, err := New(log, tc.label, schema, db)
			require.NoError(t, err)
			migs, lib, out := tc.build(schema, db)
			ldb.Migrations(lib, migs...)
			err = schema.Migrate(context.Background())
			if out.expectErr {
				require.Error(t, err)
				if out.errContains != "" {
					assert.Contains(t, err.Error(), out.errContains)
				}
				return
			}
			require.NoError(t, err)
			if out.verify != nil {
				out.verify(t, db, opts)
			}
		})
	}
}
