package stmtclass

import (
	"testing"

	"github.com/muir/sqltoken"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEasilyIdempotentFixFlag(t *testing.T) {
	cases := []struct {
		name        string
		sql         string
		wantEasy    bool
		wantNonIdem bool
	}{
		{"create table raw", "CREATE TABLE t1 (id int)", true, true},
		{"create table guarded", "CREATE TABLE IF NOT EXISTS t1 (id int)", false, false},
		{"create index raw", "CREATE INDEX idx ON t1(id)", false, true},
		{"alter table add col", "ALTER TABLE t1 ADD COLUMN c2 int", false, true},
	}
	for _, c := range cases {
		toks := sqltoken.TokenizeMySQL(c.sql)
		stmts, _ := ClassifyScript(toks)
		require.Len(t, stmts, 1, c.name)
		got := stmts[0].Flags
		assert.Equalf(t, c.wantEasy, got&IsEasilyIdempotentFix != 0, "%s: easy-fix flag mismatch", c.name)
		assert.Equalf(t, c.wantNonIdem, got&IsNonIdempotent != 0, "%s: non-idempotent flag mismatch", c.name)
	}
}
