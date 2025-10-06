package lsmysql

import (
	"github.com/memsql/errors"
	"github.com/muir/libschema"
	"github.com/muir/libschema/internal/stmtclass"
	"github.com/muir/sqltoken"
)

// Deprecated: Use libschema.ErrDataAndDDL / libschema.ErrNonIdempotentDDL directly.
var (
	ErrDataAndDDL       = libschema.ErrDataAndDDL
	ErrNonIdempotentDDL = libschema.ErrNonIdempotentDDL
)

// CheckScript attempts to validate that an SQL command does not do
// both schema changes (DDL) and data changes.  The returned error
// will be (when checked with errors.Is()) nil, ErrDataAndDDL, or
// ErrNonIdempotentDDL.
func CheckScript(s string) error {
	ts := sqltoken.TokenizeMySQL(s)
	stmts, agg := stmtclass.ClassifyScript(ts)
	sum := stmtclass.SummarizeStatements(stmts, agg)
	if sum.HasDDL && sum.HasDML {
		// discover first DDL & DML for message reconstruction
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
		return errors.Errorf("data command '%s' combined with DDL command '%s': %w", firstDML, firstDDL, ErrDataAndDDL)
	}
	if sum.FirstNonIdempotentDDL != "" && agg&stmtclass.IsNonIdempotent != 0 {
		return errors.Errorf("non-idempotent DDL '%s': %w", sum.FirstNonIdempotentDDL, ErrNonIdempotentDDL)
	}
	return nil
}
