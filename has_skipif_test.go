package libschema_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/muir/libschema"
	"github.com/muir/libschema/lspostgres"
)

// TestHasSkipIf ensures the HasSkipIf branch is covered. fakeMigration is a lightweight
// migration to test options wiring.
func TestHasSkipIf(t *testing.T) {
	m := lspostgres.Script("S", "SELECT 1", libschema.SkipIf(func() (bool, error) { return false, nil }))
	require.True(t, m.Base().HasSkipIf(), "expected HasSkipIf to be true")
	m2 := lspostgres.Script("S2", "SELECT 1", libschema.SkipRemainingIf(func() (bool, error) { return false, nil }))
	require.False(t, m2.Base().HasSkipIf(), "SkipRemainingIf should not set skipIf predicate")
}
