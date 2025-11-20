package classifysql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDialectClassification(t *testing.T) {
	cases := []struct {
		name    string
		dialect Dialect
		sql     string
		want    Flag // excluding multi
		multi   bool
	}{
		{"pg create index unguarded", DialectPostgres, "CREATE INDEX idx ON t(col)", IsDDL | IsNonIdempotent | IsEasilyIdempotentFix, false},
		{"pg create index guarded", DialectPostgres, "CREATE INDEX IF NOT EXISTS idx ON t(col)", IsDDL, false},
		{"mysql create index unguarded", DialectMySQL, "CREATE INDEX idx ON t(col)", IsDDL | IsNonIdempotent, false},
		{"pg drop index unguarded", DialectPostgres, "DROP INDEX idx", IsDDL | IsNonIdempotent | IsEasilyIdempotentFix, false},
		{"pg drop index guarded", DialectPostgres, "DROP INDEX IF EXISTS idx", IsDDL, false},
		{"mysql drop database unguarded", DialectMySQL, "DROP DATABASE foo", IsDDL | IsNonIdempotent | IsEasilyIdempotentFix, false},
		{"mysql drop database guarded", DialectMySQL, "DROP DATABASE IF EXISTS foo", IsDDL, false},
		{"pg alter add column unguarded", DialectPostgres, "ALTER TABLE t ADD COLUMN c2 int", IsDDL | IsNonIdempotent | IsEasilyIdempotentFix, false},
		{"pg alter add column guarded", DialectPostgres, "ALTER TABLE t ADD COLUMN IF NOT EXISTS c2 int", IsDDL, false},
		{"mysql alter add column", DialectMySQL, "ALTER TABLE t ADD COLUMN c2 int", IsDDL | IsNonIdempotent, false},
		{"pg create sequence", DialectPostgres, "CREATE SEQUENCE s1", IsDDL | IsNonIdempotent | IsEasilyIdempotentFix, false},
		{"pg create sequence guarded", DialectPostgres, "CREATE SEQUENCE IF NOT EXISTS s1", IsDDL, false},
		{"comments & whitespace", DialectMySQL, "/*c*/  CREATE   TABLE t2 (id int)", IsDDL | IsNonIdempotent | IsEasilyIdempotentFix, false},
		{"with dml", DialectPostgres, "WITH x AS (SELECT 1) SELECT * FROM x", IsDML, false},
		{"multi union", DialectMySQL, "CREATE TABLE t (id int); DROP TABLE t", IsDDL | IsNonIdempotent | IsEasilyIdempotentFix, true},
	}
	for _, c := range cases {
		stmts, err := ClassifyTokens(c.dialect, 0, c.sql)
		require.NoError(t, err, c.name)
		require.NotEmpty(t, stmts, c.name)
		agg := aggregateFlags(stmts)
		assert.Equalf(t, c.want, agg&c.want, "%s: flags mismatch", c.name)
		sum := stmts.Summarize()
		_, hasMulti := sum[IsMultipleStatements]
		assert.Equal(t, c.multi, hasMulti, c.name+": multi presence")

		orig := strings.TrimSuffix(c.sql, ";")
		recon := strings.Join(stmts.TokensList().Strings(), ";")
		if strings.HasSuffix(c.sql, ";") {
			assert.True(t, recon == orig || recon+";" == c.sql, c.name+": round-trip mismatch")
		} else {
			assert.Equal(t, orig, recon, c.name+": round-trip mismatch")
		}
	}
}

func TestGuardInStringLiteral(t *testing.T) {
	sql := "CREATE TABLE t (c varchar(20) DEFAULT 'IF NOT EXISTS');"
	stmts, err := ClassifyTokens(DialectMySQL, 0, sql)
	require.NoError(t, err)
	agg := aggregateFlags(stmts)
	// Expect regex to match inside literal -> idempotent classification (no NonIdempotent)
	assert.Zero(t, agg&IsNonIdempotent, "expected idempotent classification due to naive regex, got non-idempotent flags=0x%x", agg)
}
