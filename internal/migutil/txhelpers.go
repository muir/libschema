package migutil

import (
	"strings"

	"github.com/memsql/errors"

	"github.com/muir/libschema"
	"github.com/muir/libschema/internal/stmtclass"
)

// CheckNonIdempotentDDLFix validates DDL idempotency requirements. For any statement flagged as
// non-idempotent but easily fixable (missing IF EXISTS / IF NOT EXISTS) we require a SkipIf-style
// safeguard (indicated by hasSkipIf). If absent, we error. Non-idempotent hard cases also error.
func CheckNonIdempotentDDLFix(
	migName string,
	hasSkipIf bool,
	stmts []stmtclass.StatementFlags,
) error {
	for _, st := range stmts {
		flags := st.Flags
		if flags&stmtclass.IsDDL == 0 {
			continue
		}
		if flags&stmtclass.IsNonIdempotent == 0 {
			continue
		}
		if hasSkipIf { // tolerated under SkipIf contract
			continue
		}
		if flags&stmtclass.IsEasilyIdempotentFix != 0 {
			return errors.Wrapf(libschema.ErrNonIdempotentDDL, "non-idempotent DDL missing IF [NOT] EXISTS in migration %s: %s", migName, strings.TrimSpace(st.Text))
		}
		return errors.Wrapf(libschema.ErrNonIdempotentDDL, "non-idempotent DDL in migration %s: %s", migName, strings.TrimSpace(st.Text))
	}
	return nil
}

// (Former helper functions removed as they added indirection without meaningful reuse.)
