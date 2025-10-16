package lsmysql_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/memsql/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/muir/libschema"
	"github.com/muir/libschema/lsmysql"
)

// TestMySQLErrorCases consolidates mismatch, computed failure, and generic bad migrations.
func TestMySQLErrorCases(t *testing.T) {
	dsn := os.Getenv("LIBSCHEMA_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_MYSQL_TEST_DSN to test libschema/lsmysql")
	}
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	type outcome struct {
		sentinel  error
		substring string
		reopt     func(*libschema.Options)
		define    func(*libschema.Database)
	}
	cases := []struct {
		name string
		outcome
	}{
		{ // mismatch: tx computed forced non-tx (creationErr)
			name: "computed_mismatch",
			outcome: outcome{substring: "cannot force non-transactional", define: func(d *libschema.Database) {
				mig := lsmysql.Computed[*sql.Tx]("C_MISMATCH", func(context.Context, *sql.Tx) error { return nil }, libschema.ForceNonTransactional())
				d.Migrations("L_MISMATCH", mig)
			}},
		},
		{ // computed failure: transactional function returns error
			name: "computed_failure",
			outcome: outcome{substring: "boom-fail-mysql", define: func(d *libschema.Database) {
				failErr := errors.New("boom-fail-mysql")
				mig := lsmysql.Computed[*sql.Tx]("BAD", func(context.Context, *sql.Tx) error { return failErr })
				d.Migrations("L_FAIL", mig)
			}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := libschema.Options{}
			if tc.reopt != nil {
				tc.reopt(&opts)
			}
			s := libschema.New(context.Background(), opts)
			log := libschema.LogFromLog(t)
			dbase, _, err := lsmysql.New(log, tc.name, s, db)
			require.NoError(t, err)
			if tc.define != nil {
				tc.define(dbase)
			}
			err = s.Migrate(context.Background())
			require.Error(t, err, "expected error")
			if tc.sentinel != nil {
				assert.True(t, errors.Is(err, tc.sentinel), "expected sentinel %v got %v", tc.sentinel, err)
			} else if tc.substring != "" {
				assert.Contains(t, err.Error(), tc.substring)
			} else {
				assert.Fail(t, "no sentinel or substring for error assertion")
			}
		})
	}
}
