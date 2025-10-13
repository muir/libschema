package migfinalize

import (
	"context"

	"github.com/memsql/errors"

	"github.com/muir/libschema/internal"
)

// Finalizer orchestrates a migration attempt and status persistence. It exists to
// allow unit testing of the logic and to make the logic shared between database drivers.
//
// Assumptions / rules:
//   - TX must be a pointer type (e.g. *sql.Tx); we rely on direct nil comparisons.
//   - No optional callbacks: every function field must be set (supply no-op when unused).
//   - Any opened transaction that isn't committed gets rolled back by a dedicated defer.
type Finalizer[DB any, TX any] struct {
	Ctx              context.Context
	DB               *DB
	RunTransactional bool
	Log              *internal.Log // required logger for secondary error visibility

	BeginTx        func(context.Context, *DB) (*TX, error)
	BodyTx         func(context.Context, *TX) error
	BodyNonTx      func(context.Context, *DB) error
	SaveStatusInTx func(context.Context, *TX) error
	CommitTx       func(*TX) error
	RollbackTx     func(*TX)

	BeginStatusTx func(context.Context, *DB) (*TX, error)
	// SaveStatusSeparate persists migration status outside the primary migration tx.
	// The error passed is the primary migration outcome (body or in-tx finalize error).
	SaveStatusSeparate func(context.Context, *TX, error) error
	CommitStatusTx     func(*TX) error
	RollbackStatusTx   func(*TX)

	SetDone  func()
	SetError func(error)
}

func (f *Finalizer[DB, TX]) Run() (finalErr error) {
	var tx *TX // migration tx (nil if not opened)
	var committed bool
	var stx *TX // separate status tx (nil if not opened)
	var statusCommitted bool

	// finalErr is the combo of all of these
	var bodyErr error
	var txErr error
	var tx2Err error

	defer func() {
		if tx != nil && !committed {
			f.RollbackTx(tx)
		}
	}()

	defer func() {
		if stx != nil && !statusCommitted {
			f.RollbackStatusTx(stx)
		}
	}()

	defer func() {
		if finalErr != nil {
			f.SetError(finalErr)
		} else {
			f.SetDone()
		}
	}()

	defer func() {
		finalErr = errors.Join(bodyErr, txErr, tx2Err)
	}()

	// commit / status persistence
	defer func() {
		if txErr != nil {
			return
		}

		// Attempt in-tx finalize
		if f.RunTransactional && tx != nil && bodyErr == nil {
			if txErr = f.SaveStatusInTx(f.Ctx, tx); txErr == nil {
				if txErr = f.CommitTx(tx); txErr == nil {
					committed = true
					return // Success path complete; no separate status tx
				}
			}
		}

		// Need separate status tx if non-tx path OR transactional attempt not committed
		if !f.RunTransactional || !committed {
			if stx, tx2Err = f.BeginStatusTx(f.Ctx, f.DB); tx2Err != nil {
				return
			}
			joinedErr := errors.Join(bodyErr, txErr) // failed if not nil
			if tx2Err = f.SaveStatusSeparate(f.Ctx, stx, joinedErr); tx2Err != nil {
				return
			}
			if tx2Err = f.CommitStatusTx(stx); tx2Err != nil {
				return
			}
			statusCommitted = true
		}
	}()

	// Body execution (outside finalization logic). Capture bodyErr only.
	if f.RunTransactional {
		tx, txErr = f.BeginTx(f.Ctx, f.DB)
		if txErr != nil {
			return finalErr
		}
		bodyErr = f.BodyTx(f.Ctx, tx)
	} else {
		bodyErr = f.BodyNonTx(f.Ctx, f.DB)
	}
	return finalErr
}
