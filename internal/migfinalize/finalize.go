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
	Ctx        context.Context
	DB         *DB
	Log        *internal.Log
	BeginTx    func(context.Context, *DB) (*TX, error)
	BodyTx     func(context.Context, *TX) error        // executes inside tx or runs outside work using pooled conn; returns error for body
	SaveStatus func(context.Context, *TX, error) error // persist final outcome (err==nil success)
	CommitTx   func(*TX) error
	RollbackTx func(*TX)
	SetDone    func()
	SetError   func(error)
}

func (f *Finalizer[DB, TX]) Run() (finalErr error) {
	var tx *TX
	var bodyErr error
	var saveErr error

	defer func() {
		finalErr = errors.Join(bodyErr, saveErr)
		if finalErr != nil {
			f.SetError(finalErr)
		} else {
			f.SetDone()
		}
	}()

	defer func() {
		if tx != nil {
			f.RollbackTx(tx)
		}
	}()

	tx, bodyErr = f.BeginTx(f.Ctx, f.DB)
	if bodyErr != nil {
		return finalErr
	}

	bodyErr = f.BodyTx(f.Ctx, tx)
	if bodyErr != nil {
		f.RollbackTx(tx)
		tx, saveErr = f.BeginTx(f.Ctx, f.DB)
		if saveErr != nil {
			return finalErr
		}
	}

	saveErr = f.SaveStatus(f.Ctx, tx, bodyErr)
	if saveErr != nil {
		return finalErr
	}

	saveErr = f.CommitTx(tx)
	return finalErr
}
