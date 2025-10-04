package migfinalize

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/muir/libschema"
)

// minimal fake DB / TX types
type (
	fakeDB struct{ id int }
	fakeTx struct{ id int }
)

func TestFinalizer_Scenarios(t *testing.T) {
	type scenario struct {
		name    string
		tx      bool
		body    string
		saveIn  string
		commit  string
		beginSt string
		saveSep string
		commitS string
		want    string
		notWant string
	}
	cases := []scenario{
		{
			name: "transactional body error",
			tx:   true,
			body: "body fail",
			want: "body fail",
		},
		{
			name:   "in-tx save status error",
			tx:     true,
			saveIn: "save-in error",
			want:   "save-in error",
		},
		{
			name:   "in-tx commit error",
			tx:     true,
			commit: "commit error",
			want:   "commit error",
		},
		{
			name:    "non-tx separate save error",
			saveSep: "separate save error",
			want:    "separate save error",
		},
		{
			name:    "non-tx separate commit error",
			commitS: "separate commit error",
			want:    "separate commit error",
		},
		{
			name:    "begin status error no prior",
			beginSt: "begin status error",
			want:    "begin status error",
		},
		{
			name:    "begin status error with prior body",
			tx:      true,
			body:    "body fail",
			beginSt: "begin status error",
			want:    "body fail",
			notWant: "begin status error",
		},
		{
			name:    "separate save error with prior body",
			tx:      true,
			body:    "body fail",
			saveSep: "separate save error",
			want:    "body fail",
			notWant: "separate save error",
		},
		{
			name:    "separate commit error with prior body",
			tx:      true,
			body:    "body fail",
			commitS: "commit status error",
			want:    "body fail",
			notWant: "commit status error",
		},
	}

	mkErr := func(msg string) error {
		if msg == "" {
			return nil
		}
		return errors.New(msg)
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			log := libschema.LogFromLog(t)
			f := &Finalizer[fakeDB, fakeTx]{
				Ctx:              context.Background(),
				DB:               &fakeDB{1},
				RunTransactional: c.tx,
				Log:              log,
				BeginTx:          func(context.Context, *fakeDB) (*fakeTx, error) { return &fakeTx{1}, nil },
				BodyTx:           func(context.Context, *fakeTx) error { return mkErr(c.body) },
				BodyNonTx:        func(context.Context, *fakeDB) error { return mkErr(c.body) },
				SaveStatusInTx:   func(context.Context, *fakeTx) error { return mkErr(c.saveIn) },
				CommitTx:         func(*fakeTx) error { return mkErr(c.commit) },
				RollbackTx:       func(*fakeTx) {},
				BeginStatusTx: func(context.Context, *fakeDB) (*fakeTx, error) {
					if c.beginSt != "" {
						return nil, mkErr(c.beginSt)
					}
					return &fakeTx{2}, nil
				},
				SaveStatusSeparate: func(context.Context, *fakeTx, error) error { return mkErr(c.saveSep) },
				CommitStatusTx:     func(*fakeTx) error { return mkErr(c.commitS) },
				RollbackStatusTx:   func(*fakeTx) {},
				SetDone:            func() {},
				SetError:           func(error) {},
			}
			err := f.Run()
			require.Error(t, err)
			assert.ErrorContains(t, err, c.want)
			if c.notWant != "" {
				assert.NotContains(t, err.Error(), c.notWant)
			}
		})
	}
}
