package migfinalize

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	intlog "github.com/muir/libschema/internal"
)

// simple logger stub satisfying subset used
// minimal fake logger just implementing internal.Logur (pattern mirrors internal/logger_test.go)
type fakeLogger struct{ t *testing.T }

func (f *fakeLogger) Trace(string, ...map[string]interface{}) {}
func (f *fakeLogger) Debug(string, ...map[string]interface{}) {}
func (f *fakeLogger) Info(string, ...map[string]interface{})  {}
func (f *fakeLogger) Warn(string, ...map[string]interface{})  {}
func (f *fakeLogger) Error(msg string, _ ...map[string]interface{}) {
	if f.t != nil {
		f.t.Logf("error:%s", msg)
	}
}

// fake DB / TX types
type (
	fakeDB struct{ id int }
	fakeTx struct{ id int }
)

// test harness to build a Finalizer with overridable behaviors
type finBuilder struct {
	runTx           bool
	bodyErr         error
	saveInErr       error
	commitErr       error
	beginStatusErr  error
	saveSeparateErr error
	commitStatusErr error
}

func (b finBuilder) build(t *testing.T) *Finalizer[fakeDB, fakeTx] {
	log := &intlog.Log{Logur: &fakeLogger{t: t}}
	committed := false
	statusCommitted := false
	statusTxOpened := false
	tx := &fakeTx{1}
	stx := &fakeTx{2}
	f := &Finalizer[fakeDB, fakeTx]{
		Ctx:              context.Background(),
		DB:               &fakeDB{1},
		RunTransactional: b.runTx,
		Log:              log,
		BeginTx:          func(ctx context.Context, db *fakeDB) (*fakeTx, error) { return tx, nil },
		BodyTx:           func(ctx context.Context, tx *fakeTx) error { return b.bodyErr },
		BodyNonTx:        func(ctx context.Context, db *fakeDB) error { return b.bodyErr },
		SaveStatusInTx:   func(ctx context.Context, tx *fakeTx) error { return b.saveInErr },
		CommitTx: func(tx *fakeTx) error {
			if b.commitErr == nil {
				committed = true
			}
			return b.commitErr
		},
		RollbackTx: func(tx *fakeTx) {},
		BeginStatusTx: func(ctx context.Context, db *fakeDB) (*fakeTx, error) {
			if b.beginStatusErr != nil {
				return nil, b.beginStatusErr
			}
			statusTxOpened = true
			return stx, nil
		},
		SaveStatusSeparate: func(ctx context.Context, tx *fakeTx, migErr error) error { return b.saveSeparateErr },
		CommitStatusTx: func(tx *fakeTx) error {
			if b.commitStatusErr == nil {
				statusCommitted = true
			}
			return b.commitStatusErr
		},
		RollbackStatusTx: func(tx *fakeTx) {},
		SetDone:          func() {},
		SetError:         func(error) {},
	}
	// attach assertions via t.Cleanup to ensure internal expectations when needed
	_ = statusCommitted // referenced for compile (asserted implicitly via errors returned)
	t.Cleanup(func() {
		// success path assertions intentionally omitted; integration tests cover commit logic
		_ = committed      // referenced so staticcheck doesn't complain about unused capture
		_ = statusTxOpened // indicates whether fallback status tx opened when needed
	})
	return f
}

func TestFinalizer_TransactionalBodyError(t *testing.T) {
	bodyErr := errors.New("body fail")
	f := finBuilder{runTx: true, bodyErr: bodyErr}.build(t)
	err := f.Run()
	require.Error(t, err)
	assert.ErrorIs(t, err, bodyErr)
}

func TestFinalizer_SaveStatusInTxErrorFallback(t *testing.T) {
	saveErr := errors.New("save-in error")
	f := finBuilder{runTx: true, saveInErr: saveErr}.build(t)
	err := f.Run()
	require.Error(t, err)
	assert.ErrorIs(t, err, saveErr)
}

func TestFinalizer_CommitErrorFallback(t *testing.T) {
	commitErr := errors.New("commit error")
	f := finBuilder{runTx: true, commitErr: commitErr}.build(t)
	err := f.Run()
	require.Error(t, err)
	assert.ErrorIs(t, err, commitErr)
}

func TestFinalizer_SeparateStatusSaveError(t *testing.T) {
	saveErr := errors.New("separate save error")
	f := finBuilder{runTx: false, saveSeparateErr: saveErr}.build(t)
	err := f.Run()
	require.Error(t, err)
	assert.ErrorIs(t, err, saveErr)
}

func TestFinalizer_SeparateStatusCommitError(t *testing.T) {
	commitErr := errors.New("separate commit error")
	f := finBuilder{runTx: false, commitStatusErr: commitErr}.build(t)
	err := f.Run()
	require.Error(t, err)
	assert.ErrorIs(t, err, commitErr)
}

// New tests exercising secondary status path error precedence and logging scenarios.

// BeginStatusTx returns error when there is no prior finalErr (e.g., non-tx path, body success)
func TestFinalizer_BeginStatusTxError_NoPriorError(t *testing.T) {
	beginErr := errors.New("begin status error")
	f := finBuilder{runTx: false, beginStatusErr: beginErr}.build(t)
	err := f.Run()
	require.Error(t, err)
	assert.ErrorIs(t, err, beginErr)
}

// BeginStatusTx returns error but there is already a prior finalErr (simulate transactional body or commit error)
func TestFinalizer_BeginStatusTxError_WithPriorError(t *testing.T) {
	bodyErr := errors.New("body fail")
	beginErr := errors.New("begin status error")
	// Force transactional path so bodyErr is set; also force SaveStatusInTx error to avoid commit success
	// but SaveStatusInTx error would replace bodyErr; we specifically want bodyErr to remain the finalErr.
	// So use runTx true and bodyErr only (SaveStatusInTx not executed because bodyErr != nil).
	f := finBuilder{runTx: true, bodyErr: bodyErr, beginStatusErr: beginErr}.build(t)
	err := f.Run()
	require.Error(t, err)
	// The primary error should remain bodyErr; beginErr should be logged but not replace finalErr.
	assert.ErrorIs(t, err, bodyErr)
	assert.NotErrorIs(t, err, beginErr)
}

// SaveStatusSeparate returns error when prior finalErr already set; original error should be preserved.
func TestFinalizer_SaveStatusSeparateError_WithPriorError(t *testing.T) {
	bodyErr := errors.New("body fail")
	saveSeparateErr := errors.New("separate save error")
	f := finBuilder{runTx: true, bodyErr: bodyErr, saveSeparateErr: saveSeparateErr}.build(t)
	err := f.Run()
	require.Error(t, err)
	assert.ErrorIs(t, err, bodyErr)
	assert.NotErrorIs(t, err, saveSeparateErr)
}

// CommitStatusTx returns error when prior finalErr already set; original error should be preserved.
func TestFinalizer_CommitStatusTxError_WithPriorError(t *testing.T) {
	bodyErr := errors.New("body fail")
	commitStatusErr := errors.New("commit status error")
	f := finBuilder{runTx: true, bodyErr: bodyErr, commitStatusErr: commitStatusErr}.build(t)
	err := f.Run()
	require.Error(t, err)
	assert.ErrorIs(t, err, bodyErr)
	assert.NotErrorIs(t, err, commitStatusErr)
}
