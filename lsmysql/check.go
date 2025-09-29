package lsmysql

import (
	"github.com/memsql/errors"
	"github.com/muir/sqltoken"

	"github.com/muir/libschema/internal/stmtcheck"
)

var (
	// ErrDataAndDDL indicates a script mixes data-changing and schema-changing statements
	ErrDataAndDDL = stmtcheck.ErrDataAndDDL
	// ErrNonIdempotentDDL indicates a DDL statement lacking an IF (NOT) EXISTS guard
	ErrNonIdempotentDDL = stmtcheck.ErrNonIdempotentDDL
)

// CheckScript attempts to validate that an SQL command does not do
// both schema changes (DDL) and data changes.  The returned error
// will be (when checked with errors.Is()) nil, ErrDataAndDDL, or
// ErrNonIdempotentDDL.
func CheckScript(s string) error {
	ts := sqltoken.TokenizeMySQL(s)
	err := stmtcheck.AnalyzeTokens(ts)
	if err == nil {
		return nil
	}
	// Preserve error wrapping semantics referencing historical error variables
	if errors.Is(err, stmtcheck.ErrDataAndDDL) || errors.Is(err, stmtcheck.ErrNonIdempotentDDL) {
		return err
	}
	// Fallback: return as-is
	return err
}
