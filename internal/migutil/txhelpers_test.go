package migutil

import (
	"testing"

	"github.com/muir/libschema/classifysql"
	"github.com/muir/sqltoken"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckNonIdempotentDDLFix(t *testing.T) {
	mk := func(flags classifysql.Flag, text string) []classifysql.Statement {
		return []classifysql.Statement{{Flags: flags, Tokens: sqltoken.TokenizeMySQL(text)}}
	}
	// DDL + NonIdempotent + EasyFix without SkipIf should error
	err := CheckNonIdempotentDDLFix("M1", false, mk(classifysql.IsDDL|classifysql.IsNonIdempotent|classifysql.IsEasilyIdempotentFix, "CREATE TABLE x (id int)"))
	require.Error(t, err)
	// DDL + NonIdempotent hard case should error
	err = CheckNonIdempotentDDLFix("M2", false, mk(classifysql.IsDDL|classifysql.IsNonIdempotent, "ALTER TABLE x ADD COLUMN y int"))
	require.Error(t, err)
	// DDL + NonIdempotent with SkipIf passes
	err = CheckNonIdempotentDDLFix("M3", true, mk(classifysql.IsDDL|classifysql.IsNonIdempotent, "ALTER TABLE x ADD COLUMN y int"))
	assert.NoError(t, err)
	// Pure DDL idempotent passes
	err = CheckNonIdempotentDDLFix("M4", false, mk(classifysql.IsDDL, "CREATE TABLE IF NOT EXISTS x (id int)"))
	assert.NoError(t, err)
	// DML ignored
	err = CheckNonIdempotentDDLFix("M5", false, mk(classifysql.IsDML, "INSERT INTO t VALUES (1)"))
	assert.NoError(t, err)
}
