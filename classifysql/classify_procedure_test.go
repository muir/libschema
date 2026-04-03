package classifysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStoredProcedureClassification documents the CURRENT (suboptimal) behavior
// of classifying stored procedures. Ideally, CREATE PROCEDURE statements should
// be classified as DDL only, even when the procedure body contains DML statements.
// The DML inside a procedure body is not executed during the CREATE - it's just
// defining what the procedure will do when called later.
//
// CURRENT SUBOPTIMAL BEHAVIOR:
// - MySQL/SingleStore procedures with semicolons in the body are incorrectly
//   split into multiple statements (the tokenizer splits on ';' inside BEGIN...END)
// - This causes them to be incorrectly flagged as having both DDL and DML
// - PostgreSQL functions work correctly because $$ dollar-quoting preserves
//   the function body as a single token
// - Simple procedures without internal semicolons work correctly
//
// WORKAROUND: Use libschema.SkipClassificationCheck() for stored procedure
// migrations to bypass the DDL/DML validation.
//
// These test cases represent what gets passed to ClassifyTokens AFTER the
// DELIMITER handling has been processed by the tokenizer - i.e., the procedure
// body with the custom delimiter terminator (like //) but without the DELIMITER
// statements themselves.
func TestStoredProcedureClassification(t *testing.T) {
	cases := []struct {
		name            string
		dialect         Dialect
		sql             string
		wantDDL         bool
		wantDML         bool // true = suboptimal: incorrectly classified as DML
		wantMulti       bool // true = suboptimal: incorrectly split into multiple statements
		suboptimalNotes string
	}{
		{
			name:    "singlestore procedure with insert - post delimiter processing",
			dialect: DialectSingleStore,
			sql: `CREATE OR REPLACE PROCEDURE log_action(action_name TEXT) AS
BEGIN
    INSERT INTO audit_log (action, created_at) VALUES (action_name, NOW());
END //`,
			wantDDL:         true,
			wantDML:         false, // happens to not trigger DML because INSERT is absorbed into CREATE statement
			wantMulti:       true,  // SUBOPTIMAL: split on semicolon inside procedure body
			suboptimalNotes: "splits into 2 statements at semicolon inside BEGIN...END",
		},
		{
			name:    "singlestore procedure with update and delete",
			dialect: DialectSingleStore,
			sql: `CREATE PROCEDURE cleanup_old_records(days_old INT) AS
BEGIN
    DELETE FROM temp_data WHERE created_at < DATE_SUB(NOW(), INTERVAL days_old DAY);
    UPDATE stats SET last_cleanup = NOW();
END //`,
			wantDDL:         true,
			wantDML:         true, // SUBOPTIMAL: UPDATE seen as separate DML statement
			wantMulti:       true, // SUBOPTIMAL: split on semicolons inside procedure body
			suboptimalNotes: "splits into 3 statements; UPDATE incorrectly classified as DML",
		},
		{
			name:    "mysql procedure with insert",
			dialect: DialectMySQL,
			sql: `CREATE PROCEDURE add_user(IN username VARCHAR(50))
BEGIN
    INSERT INTO users (name) VALUES (username);
END //`,
			wantDDL:         true,
			wantDML:         false,
			wantMulti:       true, // SUBOPTIMAL: split on semicolon inside procedure body
			suboptimalNotes: "splits into 2 statements at semicolon inside BEGIN...END",
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
			wantDDL:         true,
			wantDML:         false,
			wantMulti:       false, // CORRECT: $$ dollar-quoting preserves as single statement
			suboptimalNotes: "",
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
END //`,
			wantDDL:         true,
			wantDML:         false,
			wantMulti:       true, // SUBOPTIMAL: split on semicolons inside procedure body
			suboptimalNotes: "splits into 3 statements at semicolons",
		},
		{
			name:            "simple create procedure no body statements",
			dialect:         DialectSingleStore,
			sql:             `CREATE PROCEDURE noop() AS BEGIN END`,
			wantDDL:         true,
			wantDML:         false,
			wantMulti:       false, // CORRECT: no internal semicolons
			suboptimalNotes: "",
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

			// Assert current behavior (including suboptimal cases)
			assert.Equal(t, tc.wantDDL, hasDDL, "DDL flag")
			assert.Equal(t, tc.wantDML, hasDML, "DML flag (suboptimal if true for procedures)")
			assert.Equal(t, tc.wantMulti, hasMulti, "Multiple statements flag (suboptimal if true for procedures)")

			// Log details for suboptimal cases
			if tc.suboptimalNotes != "" {
				t.Logf("SUBOPTIMAL BEHAVIOR: %s", tc.suboptimalNotes)
				t.Logf("  Found %d statements:", len(stmts))
				for i, st := range stmts {
					t.Logf("    [%d] flags=%v first=%q", i, st.Flags.Names(), st.FirstWord())
				}
			}
		})
	}
}
