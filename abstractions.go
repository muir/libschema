package libschema

import (
	"context"
	"database/sql"

	"github.com/memsql/errors"
)

// ExecConn is the minimal common surface between *sql.DB and *sql.Tx used by drivers
// for executing migrations. Drivers may define additional constraints but should rely
// on this interface for generic migration implementations.
type ExecConn interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// TxLike is implemented by *sql.Tx (or wrappers) and is used to detect transactional scope.
type TxLike interface {
	Commit() error
	Rollback() error
}

// Sentinel errors (use errors.String for zero-allocation identifiable values)
var (
	// ErrNonTxMultipleStatements indicates a non-transactional (idempotent) script migration
	// attempted to execute multiple SQL statements when only one is allowed.
	ErrNonTxMultipleStatements errors.String = "non-transactional migration has multiple statements"
	// ErrNonIdempotentNonTx indicates a required-to-be-idempotent non-transactional migration
	// failed idempotency validation heuristics.
	ErrNonIdempotentNonTx errors.String = "non-idempotent non-transactional migration"
)

// ForceNonTransactional forces a migration (Script, Generate, or Computed) to run without
// a wrapping transaction. By using this you assert the migration is idempotent (safe to retry).
// This overrides any automatic inference the driver would normally perform.
func ForceNonTransactional() MigrationOption {
	return func(m Migration) {
		b := m.Base()
		b.SetNonTransactional(true)
		b.ApplyForceOverride(false) // ensure forcedTx false
	}
}

// ForceTransactional forces a migration to run inside a transaction even if automatic inference
// would choose non-transactional execution.
//
// WARNING (foot-gun): On MySQL / SingleStore this DOES NOT make DDL atomic. Those engines
// autocommit each DDL statement regardless of any BEGIN you issue. By forcing transactional mode:
//   - Earlier DML in the same migration may roll back while preceding DDL remains applied.
//   - The bookkeeping UPDATE that records migration completion can fail while schema changes
//     have already been partially or fully applied (leaving the migration marked incomplete and
//     retried later against an already-changed schema).
//   - Mixed DDL + DML ordering inside a forced transactional migration can produce inconsistent,
//     misleading results during retries or partial failures.
//
// Use this only if you fully understand these semantics and prefer to retain a transactional wrapper
// for non-DDL statements. If you simply need safe DDL, prefer writing idempotent statements and let
// the driver downgrade automatically.
func ForceTransactional() MigrationOption {
	return func(m Migration) {
		b := m.Base()
		b.SetNonTransactional(false)
		b.ApplyForceOverride(true)
	}
}
