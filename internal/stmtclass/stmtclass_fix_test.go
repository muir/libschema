package stmtclass

import (
	"testing"

	"github.com/muir/sqltoken"
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
		// naive dialect choice: MySQL tokenizer is fine for these generic statements
		// (merged declaration+assignment to satisfy staticcheck S1021)
		toks := sqltoken.TokenizeMySQL(c.sql)
		stmts, _ := ClassifyScript(toks)
		if len(stmts) != 1 {
			t.Fatalf("expected 1 stmt for %s", c.name)
		}
		got := stmts[0].Flags
		if (got&IsEasilyIdempotentFix != 0) != c.wantEasy {
			if c.wantEasy {
				t.Errorf("%s: expected IsEasilyIdempotentFix set", c.name)
			} else {
				t.Errorf("%s: expected IsEasilyIdempotentFix not set", c.name)
			}
		}
		if (got&IsNonIdempotent != 0) != c.wantNonIdem {
			if c.wantNonIdem {
				t.Errorf("%s: expected IsNonIdempotent set", c.name)
			} else {
				t.Errorf("%s: expected IsNonIdempotent not set", c.name)
			}
		}
	}
}
