package classifysql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSummarizeAndFlagPaths(t *testing.T) {
	cases := []struct {
		name      string
		dialect   Dialect
		major     int
		sql       string
		expect    Flag // expected subset within aggregateFlags
		mustNonTx bool
		multi     bool
	}{
		{"mysql create table missing if exists", DialectMySQL, 0, "CREATE TABLE x (id int)", IsDDL | IsNonIdempotent | IsEasilyIdempotentFix, false, false},
		{"mysql create table guarded", DialectMySQL, 0, "CREATE TABLE IF NOT EXISTS x (id int)", IsDDL, false, false},
		{"mysql drop table missing if exists", DialectMySQL, 0, "DROP TABLE x", IsDDL | IsNonIdempotent | IsEasilyIdempotentFix, false, false},
		{"mysql alter add column non idempotent", DialectMySQL, 0, "ALTER TABLE x ADD COLUMN y int", IsDDL | IsNonIdempotent, false, false},
		{"postgres create index non tx", DialectPostgres, 0, "CREATE INDEX CONCURRENTLY idx ON t (id)", IsDDL | IsNonIdempotent | IsEasilyIdempotentFix | IsMustNonTx, true, false},
		{"postgres drop index non tx", DialectPostgres, 0, "DROP INDEX CONCURRENTLY idx", IsDDL | IsNonIdempotent | IsEasilyIdempotentFix | IsMustNonTx, true, false},
		{"postgres alter type add value pre12", DialectPostgres, 11, "ALTER TYPE mood ADD VALUE 'happy'", IsDDL | IsNonIdempotent | IsMustNonTx, true, false},
		{"postgres alter type add value post12", DialectPostgres, 13, "ALTER TYPE mood ADD VALUE 'happy'", IsDDL | IsNonIdempotent, false, false},
		{"multi statements", DialectPostgres, 0, "CREATE TABLE a(id int); CREATE TABLE b(id int)", IsDDL | IsNonIdempotent | IsEasilyIdempotentFix, false, true},
		{"pure dml insert", DialectMySQL, 0, "INSERT INTO t VALUES (1)", IsDML, false, false},
	}
	for _, c := range cases {
		stmts, err := ClassifyTokens(c.dialect, c.major, c.sql)
		require.NoError(t, err, c.name)
		require.NotEmpty(t, stmts)
		agg := aggregateFlags(stmts)
		assert.Equal(t, c.expect, agg&c.expect, c.name+": expected subset present")
		if c.mustNonTx {
			found := false
			for _, st := range stmts {
				if st.Flags&IsMustNonTx != 0 { found = true; break }
			}
			assert.True(t, found, c.name+": MustNonTx flag missing")
		}
		sum := stmts.Summarize()
		_, hasMulti := sum[IsMultipleStatements]
		assert.Equal(t, c.multi, hasMulti, c.name+": multi summary presence")
		// Summary tokens non-nil except synthetic multi
		for f, toks := range sum {
			if f == IsMultipleStatements { continue }
			assert.NotNil(t, toks, c.name+": summary tokens nil for flag")
		}
		orig := strings.TrimSuffix(c.sql, ";")
		recon := strings.Join(stmts.TokensList().Strings(), ";")
		if strings.HasSuffix(c.sql, ";") {
			assert.True(t, recon == orig || recon+";" == c.sql, c.name+": round-trip mismatch")
		} else {
			assert.Equal(t, orig, recon, c.name+": round-trip mismatch")
		}
	}
}
