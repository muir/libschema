package migfinalize

import (
	"context"

	"github.com/muir/libschema/internal"
)

// Finalizer orchestrates a migration attempt and status persistence.
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
	var bodyErr error

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

	// commit / status persistence
	defer func() {
		if finalErr == nil {
			finalErr = bodyErr
		}

		// Attempt in-tx finalize
		if f.RunTransactional && tx != nil {
			if bodyErr == nil {
				if err := f.SaveStatusInTx(f.Ctx, tx); err != nil {
					finalErr = err
				} else if err := f.CommitTx(tx); err != nil {
					finalErr = err
				} else {
					committed = true
					return // Success path complete; no separate status tx
				}
			}
		}

		// Need separate status tx if non-tx path OR transactional attempt not committed
		if !f.RunTransactional || !committed {
			st, err := f.BeginStatusTx(f.Ctx, f.DB)
			if err != nil {
				// only persist this error if finalErr is not set
				if finalErr == nil {
					finalErr = err
				} else {
					f.Log.Error("secondary status begin error", map[string]interface{}{"error": err.Error()})
				}
				return
			}
			stx = st
			// fianlErr must be the bodyErr or the error from committing the change -- in
			// either case, the status is failed if finalErr is not nil
			if err := f.SaveStatusSeparate(f.Ctx, stx, finalErr); err != nil {
				if finalErr == nil {
					finalErr = err
				} else {
					f.Log.Error("secondary status save error", map[string]interface{}{"error": err.Error()})
				}
				return
			}
			if err := f.CommitStatusTx(stx); err != nil {
				if finalErr == nil {
					finalErr = err
				} else {
					f.Log.Error("secondary status commit error", map[string]interface{}{"error": err.Error()})
				}
				return
			}
			statusCommitted = true
		}
	}()

	// Body execution (outside finalization logic). Capture bodyErr only.
	if f.RunTransactional {
		t, err := f.BeginTx(f.Ctx, f.DB)
		if err != nil {
			return err
		}
		tx = t
		bodyErr = f.BodyTx(f.Ctx, tx)
	} else {
		bodyErr = f.BodyNonTx(f.Ctx, f.DB)
	}
	return // finalErr set in defer
}
