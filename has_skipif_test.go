package libschema_test

import (
    "testing"

    "github.com/muir/libschema"
    "github.com/muir/libschema/lspostgres"
)

// TestHasSkipIf ensures the HasSkipIf branch is covered.
// fakeMigration is a lightweight migration to test options wiring.

func TestHasSkipIf(t *testing.T) {
    m := lspostgres.Script("S", "SELECT 1", libschema.SkipIf(func() (bool, error) { return false, nil }))
    if !m.Base().HasSkipIf() { t.Fatalf("expected HasSkipIf to be true") }
    m2 := lspostgres.Script("S2", "SELECT 1", libschema.SkipRemainingIf(func() (bool, error) { return false, nil }))
    if m2.Base().HasSkipIf() { t.Fatalf("SkipRemainingIf should not set skipIf predicate") }
}
