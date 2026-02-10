package classifysql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClassifyCreateGenericAndIfExistsBranches(t *testing.T) {
	stmts, err := ClassifyTokens(DialectPostgres, 0, "CREATE VIEW v AS SELECT 1")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	agg := aggregateFlags(stmts)
	assert.True(t, agg&IsDDL != 0)
	assert.True(t, agg&IsNonIdempotent != 0)
	assert.False(t, agg&IsEasilyIdempotentFix != 0)

	stmts, err = ClassifyTokens(DialectMySQL, 0, "CREATE TABLE IF NOT EXISTS x(id int)")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	agg = aggregateFlags(stmts)
	assert.True(t, agg&IsDDL != 0)
	assert.False(t, agg&IsNonIdempotent != 0)
}

func TestClassifyAlterIfExistsBranch(t *testing.T) {
	stmts, err := ClassifyTokens(DialectPostgres, 0, "ALTER TABLE IF EXISTS x ADD COLUMN y int")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	agg := aggregateFlags(stmts)
	assert.True(t, agg&IsDDL != 0)
	assert.False(t, agg&IsNonIdempotent != 0)
}

func TestClassifyDropIfExistsBranch(t *testing.T) {
	stmts, err := ClassifyTokens(DialectMySQL, 0, "DROP TABLE IF EXISTS x")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	agg := aggregateFlags(stmts)
	assert.True(t, agg&IsDDL != 0)
	assert.False(t, agg&IsNonIdempotent != 0)
}

func TestAdditionalVerbCoverage(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want Flag
	}{
		{"rename table", "RENAME TABLE t1 TO t2", IsDDL | IsNonIdempotent},
		{"comment on table", "COMMENT ON TABLE t1 IS 'x'", IsDDL | IsNonIdempotent},
		{"update dml", "UPDATE t1 SET c=1", IsDML},
		{"delete dml", "DELETE FROM t1 WHERE id=1", IsDML},
		{"replace dml", "REPLACE INTO t1 (id) VALUES (1)", IsDML},
		{"call proc dml", "CALL myproc()", IsDML},
		{"load data dml", "LOAD DATA INFILE 'x' INTO TABLE t1", IsDML},
		{"handler dml", "HANDLER t1 OPEN", IsDML},
		{"import table dml", "IMPORT TABLE FROM 's3://bucket/obj'", IsDML},
		{"unknown leading token (select)", "SELECT 1", 0},
		{"empty statements preserved", "CREATE TABLE IF NOT EXISTS t1(id int); ;  ;INSERT INTO t1 VALUES (1)", IsDDL | IsDML},
	}
	for _, c := range cases {
		stmts, err := ClassifyTokens(DialectMySQL, 0, c.sql)
		require.NoError(t, err, c.name)
		agg := aggregateFlags(stmts)
		// Multi presence check
		sum := stmts.Summarize()
		_, hasMulti := sum[IsMultipleStatements]
		if strings.Count(c.sql, ";") > 0 { // crude multi indicator
			assert.True(t, hasMulti, c.name+": expected multi flag in summary")
		}
		assert.Equalf(t, c.want, agg&c.want, "%s: flags", c.name)
	}
}

func TestFlagNamesPresence(t *testing.T) {
	stmts, err := ClassifyTokens(DialectMySQL, 0, "CREATE TABLE t1(id int); INSERT INTO t1 VALUES(1); DROP TABLE t1")
	require.NoError(t, err)
	require.Len(t, stmts, 3)
	agg := aggregateFlags(stmts) // excludes multi
	names := agg.Names()
	required := map[string]bool{"DDL": false, "DML": false, "NonIdem": false}
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
