package migutil

import (
	"testing"

	"github.com/muir/libschema/internal/stmtclass"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckNonIdempotentDDLFix(t *testing.T) {
	mk := func(flags stmtclass.Flag, text string) []stmtclass.StatementFlags {
		return []stmtclass.StatementFlags{{Flags: flags, Text: text}}
	}
	// DDL + NonIdempotent + EasyFix without SkipIf should error
	err := CheckNonIdempotentDDLFix("M1", false, mk(stmtclass.IsDDL|stmtclass.IsNonIdempotent|stmtclass.IsEasilyIdempotentFix, "CREATE TABLE x (id int)"))
	require.Error(t, err)
	// DDL + NonIdempotent hard case should error
	err = CheckNonIdempotentDDLFix("M2", false, mk(stmtclass.IsDDL|stmtclass.IsNonIdempotent, "ALTER TABLE x ADD COLUMN y int"))
	require.Error(t, err)
	// DDL + NonIdempotent with SkipIf passes
	err = CheckNonIdempotentDDLFix("M3", true, mk(stmtclass.IsDDL|stmtclass.IsNonIdempotent, "ALTER TABLE x ADD COLUMN y int"))
	assert.NoError(t, err)
	// Pure DDL idempotent passes
	err = CheckNonIdempotentDDLFix("M4", false, mk(stmtclass.IsDDL, "CREATE TABLE IF NOT EXISTS x (id int)"))
	assert.NoError(t, err)
	// DML ignored
	err = CheckNonIdempotentDDLFix("M5", false, mk(stmtclass.IsDML, "INSERT INTO t VALUES (1)"))
	assert.NoError(t, err)
}
