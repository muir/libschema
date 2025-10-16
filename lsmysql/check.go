package lsmysql

import (
	"github.com/muir/libschema"
	"github.com/muir/libschema/internal/stmtclass"
	"github.com/muir/sqltoken"
)

var (
	ErrDataAndDDL       = libschema.ErrDataAndDDL       // Deprecated: use libschema.ErrDataAndDDL
	ErrNonIdempotentDDL = libschema.ErrNonIdempotentDDL // Deprecated: use libschema.ErrNonIdempotentDDL
)

// Deprecated: no longer a supported API
func CheckScript(s string) error {
	ts := sqltoken.TokenizeMySQL(s)
	stmts, agg := stmtclass.ClassifyTokens(stmtclass.DialectMySQL, 0, ts)
	sum := stmtclass.Summarize(stmts, agg)
	if _, hasDDL := sum[stmtclass.IsDDL]; hasDDL && (agg&stmtclass.IsDML) != 0 {
		var firstDDL, firstDML string
		for _, st := range stmts {
			if firstDDL == "" && st.Flags&stmtclass.IsDDL != 0 {
				firstDDL = st.Text
			}
			if firstDML == "" && st.Flags&stmtclass.IsDML != 0 {
				firstDML = st.Text
			}
			if firstDDL != "" && firstDML != "" {
				break
			}
		}
		return libschema.ErrDataAndDDL.Errorf("data command '%s' combined with DDL command '%s'", firstDML, firstDDL)
	}
	if first, hasNonIdem := sum[stmtclass.IsNonIdempotent]; hasNonIdem && (agg&stmtclass.IsDDL) != 0 {
		return libschema.ErrNonIdempotentDDL.Errorf("non-idempotent DDL '%s'", first)
	}
	return nil
}
