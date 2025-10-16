package lspostgres_test

import (
	"os"
	"testing"
)

// getDSN centralizes retrieval of the Postgres test DSN so individual tests
// don't duplicate the skip logic. Existing tests can progressively migrate to
// using this helper.
func sharedGetDSN(t *testing.T) string {
	dsn := os.Getenv("LIBSCHEMA_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $LIBSCHEMA_POSTGRES_TEST_DSN to run this test")
	}
	return dsn
}
