package stmtclass

import (
	"testing"

	"github.com/muir/sqltoken"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAdditionalVerbCoverage exercises remaining first-token switch arms in ClassifyTokens
// that were not covered by earlier tests (rename, comment, update, delete, replace, call,
// do, load, handler, import) plus a default/unknown token path and empty statement entries.
func TestAdditionalVerbCoverage(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want uint32
	}{
		{"rename table", "RENAME TABLE t1 TO t2", IsDDL | IsNonIdempotent},
		{"comment on table (treated as ddl non-idempotent)", "COMMENT ON TABLE t1 IS 'x'", IsDDL | IsNonIdempotent},
		{"update dml", "UPDATE t1 SET c=1", IsDML},
		{"delete dml", "DELETE FROM t1 WHERE id=1", IsDML},
		{"replace dml", "REPLACE INTO t1 (id) VALUES (1)", IsDML},
		{"call proc dml", "CALL myproc()", IsDML},
		{"do expr dml", "DO 1", IsDML},
		{"load data dml", "LOAD DATA INFILE 'x' INTO TABLE t1", IsDML},
		{"handler dml", "HANDLER t1 OPEN", IsDML},
		{"import table dml", "IMPORT TABLE FROM 's3://bucket/obj'", IsDML},
		{"unknown leading token (select)", "SELECT 1", 0}, // default branch
		{"empty statements ignored", "CREATE TABLE IF NOT EXISTS t1(id int); ;  ;INSERT INTO t1 VALUES (1)", IsDDL | IsDML | IsMultipleStatements},
	}
	for _, c := range cases {
		toks := sqltoken.TokenizeMySQL(c.sql)
		_, agg := ClassifyTokens(DialectMySQL, toks)
		assert.Equalf(t, c.want, agg, "%s: flags", c.name)
	}
}

func TestFlagNamesAndSummarize(t *testing.T) {
	sql := "CREATE TABLE t1(id int); INSERT INTO t1 VALUES(1); DROP TABLE t1"
	toks := sqltoken.TokenizeMySQL(sql)
	stmts, agg := ClassifyTokens(DialectMySQL, toks)
	sum := SummarizeStatements(stmts, agg)
	require.True(t, sum.HasDDL && sum.HasDML, "expected both DDL and DML in summary: %+v", sum)
	require.NotEmpty(t, sum.FirstNonIdempotentDDL, "expected first non-idempotent DDL captured")
	names := FlagNames(agg)
	// Ensure expected flag name presence (Multi, DDL, DML, NonIdem due to DROP unguarded)
	required := map[string]bool{"DDL": false, "DML": false, "Multi": false, "NonIdem": false}
	for _, n := range names {
		if _, ok := required[n]; ok {
			required[n] = true
		}
	}
	for k, v := range required {
		assert.Truef(t, v, "missing flag name %s in %v", k, names)
	}
	// EasyFix presence depends on create/drop classification; assert at least one statement has EasyFix when raw CREATE TABLE without IF NOT EXISTS or DROP TABLE
	var sawEasy bool
	for _, st := range stmts {
		if st.Flags&IsEasilyIdempotentFix != 0 {
			sawEasy = true
			break
		}
	}
	assert.True(t, sawEasy, "expected to see at least one EasyFix flag")
}

func TestClassifySQLWrapper(t *testing.T) {
	sql := "CREATE TABLE t1(id int); INSERT INTO t1 VALUES(1)"
	stmts, agg := ClassifySQL(DialectMySQL, sql)
	require.Len(t, stmts, 2)
	require.Equal(t, IsDDL|IsDML|IsMultipleStatements, agg&(IsDDL|IsDML|IsMultipleStatements))
	// Postgres path trivial smoke
	pstmts, pagg := ClassifySQL(DialectPostgres, "CREATE TABLE t2(id int)")
	require.Len(t, pstmts, 1)
	require.NotZero(t, pagg&IsDDL)
}
