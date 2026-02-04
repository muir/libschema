package lspostgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/muir/libschema"
	"github.com/muir/libschema/lspostgres"
	"github.com/muir/libschema/lstesting"
)

type logEntry struct {
	msg    string
	fields []map[string]interface{}
}

type captureLog struct {
	t       *testing.T
	mu      sync.Mutex
	entries []logEntry
}

func (l *captureLog) Trace(msg string, fields ...map[string]interface{}) { l.add(msg, fields) }
func (l *captureLog) Debug(msg string, fields ...map[string]interface{}) { l.add(msg, fields) }
func (l *captureLog) Info(msg string, fields ...map[string]interface{})  { l.add(msg, fields) }
func (l *captureLog) Warn(msg string, fields ...map[string]interface{})  { l.add(msg, fields) }
func (l *captureLog) Error(msg string, fields ...map[string]interface{}) { l.add(msg, fields) }

func (l *captureLog) add(msg string, fields []map[string]interface{}) {
	combined := msg
	for _, m := range fields {
		for k, v := range m {
			combined += " " + k + "=" + fmt.Sprint(v)
		}
	}
	l.t.Log("captured", combined)
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, logEntry{msg: msg, fields: fields})
}

func (l *captureLog) Entries() []logEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	entries := make([]logEntry, len(l.entries))
	copy(entries, l.entries)
	return entries
}

func openPGNonTx(t *testing.T) *sql.DB {
	dsn := os.Getenv("LIBSCHEMA_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_POSTGRES_TEST_DSN to run Postgres tests")
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestPostgresNonTxCases exercises non-transactional Postgres migrations including
// schema override application, validation errors, and successful idempotent execution.
func TestPostgresNonTxCases(t *testing.T) {
	db := openPGNonTx(t)
	type outcome struct {
		expectErr   bool
		errContains string
		verify      func(t *testing.T, db *sql.DB, ops libschema.Options)
		verifyLog   func(t *testing.T, entries []logEntry)
	}
	cases := []struct {
		name   string
		label  string
		script func() (setup []libschema.Migration, verify outcome)
	}{
		{
			name:  "schema_override_applied",
			label: "ntx_schema",
			script: func() ([]libschema.Migration, outcome) {
				table := lspostgres.Script("TAB", "CREATE TABLE IF NOT EXISTS ntx_tab (id int)")
				idx := lspostgres.Script("IDX", "CREATE INDEX CONCURRENTLY IF NOT EXISTS ntx_idx ON ntx_tab (id)")
				return []libschema.Migration{table, idx}, outcome{
					verify: func(t *testing.T, db *sql.DB, ops libschema.Options) {
						row := db.QueryRow(`SELECT 1 FROM pg_indexes WHERE indexname = 'ntx_idx' AND schemaname = $1`, ops.SchemaOverride)
						var one int
						require.NoError(t, row.Scan(&one))
					},
				}
			},
		},
		{
			name:  "multi_statement_error",
			label: "ntx_multi",
			script: func() ([]libschema.Migration, outcome) {
				tab := lspostgres.Script("T", "CREATE TABLE IF NOT EXISTS mtab (id int)")
				bad := lspostgres.Script("BAD", "CREATE INDEX CONCURRENTLY IF NOT EXISTS m1 ON mtab(id); CREATE INDEX CONCURRENTLY IF NOT EXISTS m2 ON mtab(id)")
				return []libschema.Migration{tab, bad}, outcome{
					expectErr:   true,
					errContains: "must contain exactly one SQL statement",
				}
			},
		},
		{
			name:  "easy_fix_missing_if_exists",
			label: "ntx_easy",
			script: func() ([]libschema.Migration, outcome) {
				tab := lspostgres.Script("T", "CREATE TABLE IF NOT EXISTS etab (id int)")
				bad := lspostgres.Script("BAD", "CREATE INDEX CONCURRENTLY eidx ON etab(id)")
				return []libschema.Migration{tab, bad}, outcome{
					expectErr:   true,
					errContains: "missing IF [NOT] EXISTS",
				}
			},
		},
		{
			name:  "success_idempotent",
			label: "ntx_success",
			script: func() ([]libschema.Migration, outcome) {
				tab := lspostgres.Script("T", "CREATE TABLE IF NOT EXISTS st_tab (id int)")
				idx := lspostgres.Script("IDX", "CREATE INDEX CONCURRENTLY IF NOT EXISTS st_idx ON st_tab (id)")
				return []libschema.Migration{tab, idx}, outcome{
					verify: func(t *testing.T, db *sql.DB, ops libschema.Options) {
						row := db.QueryRow(`SELECT 1 FROM pg_indexes WHERE indexname = 'st_idx' AND schemaname = $1`, ops.SchemaOverride)
						var one int
						require.NoError(t, row.Scan(&one))
					},
				}
			},
		},
		{
			name:  "runtime_failure",
			label: "rfail",
			script: func() ([]libschema.Migration, outcome) {
				tab1 := lspostgres.Script("T1", "CREATE TABLE IF NOT EXISTS rtf (id int)")
				tab2 := lspostgres.Script("T2", "CREATE TABLE rtf (id int)")
				return []libschema.Migration{tab1, tab2}, outcome{
					expectErr: true,
					verifyLog: func(t *testing.T, entries []logEntry) {
						found := false
						for _, entry := range entries {
							if entry.msg != "migration success" {
								continue
							}
							for _, fields := range entry.fields {
								if fields == nil {
									continue
								}
								if sqlText, ok := fields["sql"]; ok {
									assert.Equal(t, "CREATE TABLE IF NOT EXISTS rtf (id int)", sqlText)
									found = true
								}
							}
						}
						assert.True(t, found, "expected migration success log to include sql note")
					},
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ops, cleanup := lstesting.FakeSchema(t, "CASCADE")
			defer cleanup(db)
			_, err := db.Exec("CREATE SCHEMA " + ops.SchemaOverride)
			require.NoError(t, err)
			s := libschema.New(context.Background(), ops)
			capLog := &captureLog{t: t}
			log := libschema.LogFromLogur(capLog)
			dbase, err := lspostgres.New(log, tc.label, s, db)
			require.NoError(t, err)
			migs, out := tc.script()
			dbase.Migrations("NTX_LIB", migs...)
			err = s.Migrate(context.Background())
			if out.expectErr {
				require.Error(t, err)
				if out.errContains != "" {
					assert.Contains(t, err.Error(), out.errContains)
				}
				if out.verifyLog != nil {
					out.verifyLog(t, capLog.Entries())
				}
				return
			}
			require.NoError(t, err)
			if out.verify != nil {
				out.verify(t, db, ops)
			}
		})
	}
}
