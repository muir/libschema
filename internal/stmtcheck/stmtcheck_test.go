package stmtcheck

import (
	"testing"

	"github.com/muir/sqltoken"
	"github.com/stretchr/testify/require"
)

func mustTokenizeMySQL(t *testing.T, s string) sqltoken.Tokens {
	t.Helper()
	return sqltoken.TokenizeMySQL(s)
}

func TestAnalyzeTokens_DataAndDDL(t *testing.T) {
	ts := mustTokenizeMySQL(t, `CREATE TABLE t1 (id int); INSERT INTO t1 (id) VALUES (1)`)
	err := AnalyzeTokens(ts)
	require.Error(t, err)
	require.True(t, IsDataAndDDL(err), "expected ErrDataAndDDL sentinel")
}

func TestAnalyzeTokens_NonIdempotentDDL(t *testing.T) {
	ts := mustTokenizeMySQL(t, `CREATE TABLE t1 (id int)`)
	err := AnalyzeTokens(ts)
	require.Error(t, err)
	require.True(t, IsNonIdempotentDDL(err), "expected ErrNonIdempotentDDL sentinel")
}

func TestAnalyzeTokens_IdempotentDDLAllowed(t *testing.T) {
	ts := mustTokenizeMySQL(t, `CREATE TABLE IF NOT EXISTS t1 (id int)`)
	err := AnalyzeTokens(ts)
	require.NoError(t, err)
}

func TestAnalyzeTokens_DataOnlyOK(t *testing.T) {
	ts := mustTokenizeMySQL(t, `INSERT INTO t1 (id) VALUES (1)`)
	err := AnalyzeTokens(ts)
	require.NoError(t, err)
}

func TestAnalyzeTokens_WhitespaceAndCase(t *testing.T) {
	ts := mustTokenizeMySQL(t, `  cReAtE   TABLE   IF   NOT   EXISTS  t1 (id int)  `)
	err := AnalyzeTokens(ts)
	require.NoError(t, err)
}
