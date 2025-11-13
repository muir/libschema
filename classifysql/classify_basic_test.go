package classifysql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// aggregateFlags recreates historical aggregate semantics excluding IsMultipleStatements (handled via Summary).
func aggregateFlags(stmts Statements) Flag {
	var agg Flag
	for _, st := range stmts {
		agg |= st.Flags
	}
	return agg
}

func TestClassifyBasicCases(t *testing.T) {
	cases := []struct {
		name string
		d    Dialect
		major int
		sql  string
		want Flag // aggregate (excluding multi) expected bits
		multi bool
	}{
		{"simple ddl non-idempotent", DialectMySQL, 0, "CREATE TABLE t (id int)", IsDDL | IsNonIdempotent | IsEasilyIdempotentFix, false},
		{"ddl idempotent", DialectMySQL, 0, "CREATE TABLE IF NOT EXISTS t (id int)", IsDDL, false},
		{"dml", DialectMySQL, 0, "INSERT INTO t VALUES (1)", IsDML, false},
		{"multiple stmts", DialectMySQL, 0, "CREATE TABLE IF NOT EXISTS t (id int); INSERT INTO t VALUES (1)", IsDDL | IsDML | IsMultipleStatements, true},
		{"truncate only", DialectMySQL, 0, "TRUNCATE TABLE t", IsDDL, false},
	}
	for _, tc := range cases {
		stmts, err := ClassifyTokens(tc.d, tc.major, tc.sql)
		require.NoError(t, err, tc.name)
		agg := aggregateFlags(stmts)
		assert.Equal(t, tc.want, agg, tc.name)
		sum := stmts.Summarize()
		_, hasMulti := sum[IsMultipleStatements]
		assert.Equal(t, tc.multi, hasMulti, tc.name+": multi flag presence mismatch")

		// Round trip check (ignoring trailing semicolon in original)
		orig := strings.TrimSuffix(tc.sql, ";")
		recon := strings.Join(stmts.TokensList().Strings(), ";")
		if strings.HasSuffix(tc.sql, ";") {
			assert.True(t, recon == orig || recon+";" == tc.sql, tc.name+": round-trip mismatch")
		} else {
			assert.Equal(t, orig, recon, tc.name+": round-trip mismatch")
		}
	}
}
