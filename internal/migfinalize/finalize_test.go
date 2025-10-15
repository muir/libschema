package migfinalize

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// minimal fake DB / TX types
type (
	fakeDB struct{ id int }
	fakeTx struct{ id int }
)

func TestFinalizer_BodyAndSavePaths(t *testing.T) {
	type scenario struct {
		name           string
		bodyErr        string
		saveErr        string
		commitErr      string
		secondBeginErr string // error when re-begin after body failure
		wantPrimary    string
		wantSecondary  string
	}
	cases := []scenario{
		{name: "body success", wantPrimary: ""},
		{name: "body error only", bodyErr: "body fail", wantPrimary: "body fail"},
		{name: "save error only", saveErr: "save fail", wantPrimary: "save fail"},
		{name: "commit error", commitErr: "commit fail", wantPrimary: "commit fail"},
		{name: "body + save error", bodyErr: "body fail", saveErr: "save fail", wantPrimary: "body fail", wantSecondary: "save fail"},
		{name: "body + commit error", bodyErr: "body fail", commitErr: "commit fail", wantPrimary: "body fail", wantSecondary: "commit fail"},
		{name: "body error second begin err", bodyErr: "body fail", secondBeginErr: "rebegin fail", wantPrimary: "body fail", wantSecondary: "rebegin fail"},
	}
	mkErr := func(msg string) error {
		if msg == "" {
			return nil
		}
		return errors.New(msg)
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var began int
			f := &Finalizer[fakeDB, fakeTx]{
				Ctx: context.Background(),
				DB:  &fakeDB{1},
				BeginTx: func(context.Context, *fakeDB) (*fakeTx, error) {
					began++
					if began == 2 && c.secondBeginErr != "" {
						return nil, mkErr(c.secondBeginErr)
					}
					return &fakeTx{began}, nil
				},
				BodyTx:     func(context.Context, *fakeTx) error { return mkErr(c.bodyErr) },
				SaveStatus: func(_ context.Context, _ *fakeTx, bodyErr error) error { return mkErr(c.saveErr) },
				CommitTx:   func(*fakeTx) error { return mkErr(c.commitErr) },
				RollbackTx: func(*fakeTx) {},
				SetDone:    func() {},
				SetError:   func(error) {},
			}
			err := f.Run()
			if c.wantPrimary == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), c.wantPrimary)
			if c.wantSecondary != "" {
				assert.Contains(t, err.Error(), c.wantSecondary)
			}
		})
	}
}
