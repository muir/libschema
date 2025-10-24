package stmtclass

import (
	"testing"

	"github.com/muir/sqltoken"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func tokenize(d Dialect, sql string) sqltoken.Tokens {
	if d == DialectPostgres {
		return sqltoken.TokenizePostgreSQL(sql)
	}
	return sqltoken.TokenizeMySQL(sql)
}

func TestClassifyCreateGenericAndIfExistsBranches(t *testing.T) {
	// Generic create (view) missing IF NOT EXISTS -> NonIdempotent only
	stmts, agg := ClassifyTokens(DialectPostgres, 0, tokenize(DialectPostgres, "CREATE VIEW v AS SELECT 1"))
	require.Len(t, stmts, 1)
	assert.True(t, agg&IsDDL != 0)
	assert.True(t, agg&IsNonIdempotent != 0)
	assert.False(t, agg&IsEasilyIdempotentFix != 0)
	// CREATE TABLE guarded -> no NonIdempotent
	stmts, agg = ClassifyTokens(DialectMySQL, 0, tokenize(DialectMySQL, "CREATE TABLE IF NOT EXISTS x(id int)"))
	require.Len(t, stmts, 1)
	assert.True(t, agg&IsDDL != 0)
	assert.False(t, agg&IsNonIdempotent != 0)
}

func TestClassifyAlterIfExistsBranch(t *testing.T) {
	// ALTER with IF EXISTS should not be NonIdempotent
	stmts, agg := ClassifyTokens(DialectPostgres, 0, tokenize(DialectPostgres, "ALTER TABLE IF EXISTS x ADD COLUMN y int"))
	require.Len(t, stmts, 1)
	assert.True(t, agg&IsDDL != 0)
	assert.False(t, agg&IsNonIdempotent != 0)
}

func TestClassifyDropIfExistsBranch(t *testing.T) {
	stmts, agg := ClassifyTokens(DialectMySQL, 0, tokenize(DialectMySQL, "DROP TABLE IF EXISTS x"))
	require.Len(t, stmts, 1)
	assert.True(t, agg&IsDDL != 0)
	assert.False(t, agg&IsNonIdempotent != 0)
}

func TestFlagNamesOrdering(t *testing.T) {
	stmts, agg := ClassifyTokens(DialectPostgres, 11, tokenize(DialectPostgres, "ALTER TYPE mood ADD VALUE 'sad'; CREATE INDEX CONCURRENTLY idx ON t(id)"))
	require.Greater(t, len(stmts), 0)
	names := FlagNames(agg)
	// Names should appear in flagsInOrder sequence subset
	expectedOrder := []Flag{IsMultipleStatements, IsDDL, IsNonIdempotent, IsMustNonTx}
	// Build map for indices
	idx := map[string]int{}
	for i, n := range names {
		idx[n] = i
	}
	prev := -1
	for _, f := range expectedOrder {
		n := flagNameMap[f]
		if i, ok := idx[n]; ok {
			assert.Greater(t, i, prev)
			prev = i
		}
	}
}

// TestAdditionalVerbCoverage exercises remaining first-token switch arms in ClassifyTokens
// that were not covered by earlier tests (rename, comment, update, delete, replace, call,
// do, load, handler, import) plus a default/unknown token path and empty statement entries.
func TestAdditionalVerbCoverage(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want Flag
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
		_, agg := ClassifyTokens(DialectMySQL, 0, toks)
		assert.Equalf(t, c.want, agg, "%s: flags", c.name)
	}
}

func TestFlagNamesPresence(t *testing.T) {
	sql := "CREATE TABLE t1(id int); INSERT INTO t1 VALUES(1); DROP TABLE t1"
	toks := sqltoken.TokenizeMySQL(sql)
	stmts, agg := ClassifyTokens(DialectMySQL, 0, toks)
	require.Len(t, stmts, 3)
	names := FlagNames(agg)
	required := map[string]bool{"DDL": false, "DML": false, "Multi": false, "NonIdem": false}
	for _, n := range names {
		if _, ok := required[n]; ok {
			required[n] = true
		}
	}
	for k, v := range required {
		assert.Truef(t, v, "missing flag name %s in %v", k, names)
	}
	var sawEasy bool
	for _, st := range stmts {
		if st.Flags&IsEasilyIdempotentFix != 0 {
			sawEasy = true
			break
		}
	}
	assert.True(t, sawEasy, "expected to see at least one EasyFix flag")
}

func TestClassifyTokensWrapper(t *testing.T) {
	sql := "CREATE TABLE t1(id int); INSERT INTO t1 VALUES(1)"
	toks := sqltoken.TokenizeMySQL(sql)
	stmts, agg := ClassifyTokens(DialectMySQL, 0, toks)
	require.Len(t, stmts, 2)
	require.Equal(t, IsDDL|IsDML|IsMultipleStatements, agg&(IsDDL|IsDML|IsMultipleStatements))
	ptoks := sqltoken.TokenizePostgreSQL("CREATE TABLE t2(id int)")
	pstmts, pagg := ClassifyTokens(DialectPostgres, 0, ptoks)
	require.Len(t, pstmts, 1)
	require.NotZero(t, pagg&IsDDL)
}
