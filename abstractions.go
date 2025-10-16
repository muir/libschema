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
func ForceNonTransactional() MigrationOption {
	return func(m Migration) { m.Base().SetNonTransactional(true) }
}

// ForceTransactional forces a migration to run inside a transaction even if auto-detection or
// generic inference would choose non-transactional execution. Use with caution.
func ForceTransactional() MigrationOption {
	return func(m Migration) { m.Base().SetNonTransactional(false) }
}
