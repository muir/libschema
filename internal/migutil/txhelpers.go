package migutil

import (
	"strings"

	"github.com/memsql/errors"

	"github.com/muir/libschema"
	"github.com/muir/libschema/classifysql"
)

// CheckNonIdempotentDDLFix validates DDL idempotency requirements. For any statement flagged as
// non-idempotent but easily fixable (missing IF EXISTS / IF NOT EXISTS) we require a SkipIf-style
// safeguard (indicated by hasSkipIf). If absent, we error. Non-idempotent hard cases also error.
func CheckNonIdempotentDDLFix(
	migName string,
	hasSkipIf bool,
	stmts []classifysql.Statement,
) error {
	for _, st := range stmts {
		flags := st.Flags
		if flags&classifysql.IsDDL == 0 {
			continue
		}
		if flags&classifysql.IsNonIdempotent == 0 {
			continue
		}
		if hasSkipIf { // tolerated under SkipIf contract
			continue
		}
		text := strings.TrimSpace(st.Tokens.Strip().String())
		if flags&classifysql.IsEasilyIdempotentFix != 0 {
			return errors.Wrapf(libschema.ErrNonIdempotentDDL, "non-idempotent DDL missing IF [NOT] EXISTS in migration %s: %s", migName, text)
		}
		return errors.Wrapf(libschema.ErrNonIdempotentDDL, "non-idempotent DDL in migration %s: %s", migName, text)
	}
	return nil
}
