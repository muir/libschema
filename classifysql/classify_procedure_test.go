package classifysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStoredProcedureClassification documents the expected behavior for
// classifying stored procedures. CREATE PROCEDURE statements should
// be classified as DDL only, even when the procedure body contains DML statements.
// The DML inside a procedure body is not executed during the CREATE - it's just
// defining what the procedure will do when called later.
func TestStoredProcedureClassification(t *testing.T) {
	cases := []struct {
		name      string
		dialect   Dialect
		sql       string
		wantDDL   bool
		wantDML   bool
		wantMulti bool
	}{
		{
			name:    "singlestore procedure with insert",
			dialect: DialectSingleStore,
			sql: `CREATE OR REPLACE PROCEDURE log_action(action_name TEXT) AS
BEGIN
    INSERT INTO audit_log (action, created_at) VALUES (action_name, NOW());
END;`,
			wantDDL:   true,
			wantDML:   false,
			wantMulti: false,
		},
		{
			name:    "singlestore procedure with update and delete",
			dialect: DialectSingleStore,
			sql: `CREATE PROCEDURE cleanup_old_records(days_old INT) AS
BEGIN
    DELETE FROM temp_data WHERE created_at < DATE_SUB(NOW(), INTERVAL days_old DAY);
    UPDATE stats SET last_cleanup = NOW();
END;`,
			wantDDL:   true,
			wantDML:   false,
			wantMulti: false,
		},
		{
			name:    "mysql procedure with insert",
			dialect: DialectMySQL,
			sql: `CREATE PROCEDURE add_user(IN username VARCHAR(50))
BEGIN
    INSERT INTO users (name) VALUES (username);
END;`,
			wantDDL:   true,
			wantDML:   false,
			wantMulti: false,
		},
		{
			name:    "postgres function with insert",
			dialect: DialectPostgres,
			sql: `CREATE OR REPLACE FUNCTION log_event(event_type TEXT)
RETURNS void AS $$
BEGIN
    INSERT INTO events (type, ts) VALUES (event_type, NOW());
END;
$$ LANGUAGE plpgsql`,
			wantDDL:   true,
			wantDML:   false,
			wantMulti: false, // $$ dollar-quoting preserves as single statement
		},
		{
			name:    "singlestore procedure exception handling",
			dialect: DialectSingleStore,
			sql: `CREATE OR REPLACE PROCEDURE safe_insert(val TEXT) AS
BEGIN
    INSERT INTO data_table (value) VALUES (val);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO error_log (message) VALUES ('insert failed');
END;`,
			wantDDL:   true,
			wantDML:   false,
			wantMulti: false,
		},
		{
			name:      "simple create procedure no body statements",
			dialect:   DialectSingleStore,
			sql:       `CREATE PROCEDURE noop() AS BEGIN END`,
			wantDDL:   true,
			wantDML:   false,
			wantMulti: false, // no internal semicolons
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmts, err := ClassifyTokens(tc.dialect, 0, tc.sql)
			require.NoError(t, err)
			require.NotEmpty(t, stmts, "expected at least one statement")

			agg := aggregateFlags(stmts)
			sum := stmts.Summarize()

			hasDDL := agg&IsDDL != 0
			hasDML := agg&IsDML != 0
			_, hasMulti := sum[IsMultipleStatements]

			// Assert expected behavior.
			assert.Equal(t, tc.wantDDL, hasDDL, "DDL flag")
			assert.Equal(t, tc.wantDML, hasDML, "DML flag")
			assert.Equal(t, tc.wantMulti, hasMulti, "Multiple statements flag")
		})
	}
}
