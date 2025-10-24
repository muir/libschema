package lssinglestore

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/memsql/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/muir/libschema"
)

// TestComputedFailure ensures a transactional computed SingleStore migration records failure status.
func TestComputedFailure(t *testing.T) {
	dsn := os.Getenv("LIBSCHEMA_SINGLESTORE_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_SINGLESTORE_TEST_DSN to test libschema/lssinglestore")
	}
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	opts := libschema.Options{}
	s := libschema.New(ctx, opts)
	log := libschema.LogFromLog(t)
	dbase, _, err := New(log, "test_singlestore_fail", s, db)
	require.NoError(t, err)

	lib := time.Now().Format("FAIL_20060102150405.000000000")
	failErr := errors.New("boom-fail-singlestore")
	mig := Computed("BAD", func(_ context.Context, _ *sql.Tx) error { return failErr })
	dbase.Migrations(lib, mig)
	err = s.Migrate(ctx)
	require.Error(t, err, "expected migration failure error")
	stored, ok := dbase.Lookup(libschema.MigrationName{Library: lib, Name: "BAD"})
	require.True(t, ok, "could not lookup stored migration copy")
	st := stored.Base().Status()
	assert.False(t, st.Done, "expected migration not marked done after failure")
	assert.NotEmpty(t, st.Error, "expected error message recorded; got empty status: %+v", st)
}
