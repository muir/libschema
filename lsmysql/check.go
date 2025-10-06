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
	// Detect mixed DDL + DML
	var sawDDL, sawDML bool
	var firstDDL, firstDML, firstNonIdem string
	for _, st := range stmts {
		if st.Flags&stmtclass.IsDDL != 0 {
			if !sawDDL {
				firstDDL = st.Text
			}
			sawDDL = true
			if st.Flags&stmtclass.IsNonIdempotent != 0 && firstNonIdem == "" {
				firstNonIdem = st.Text
			}
		}
		if st.Flags&stmtclass.IsDML != 0 {
			if !sawDML {
				firstDML = st.Text
			}
			sawDML = true
		}
	}
	if sawDDL && sawDML {
		return errors.Errorf("data command '%s' combined with DDL command '%s': %w", firstDML, firstDDL, ErrDataAndDDL)
	}
	if agg&stmtclass.IsNonIdempotent != 0 && firstNonIdem != "" {
		return errors.Errorf("non-idempotent DDL '%s': %w", firstNonIdem, ErrNonIdempotentDDL)
	}
	return nil
}
