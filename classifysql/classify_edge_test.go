package classifysql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSummarizeMultiSynthetic(t *testing.T) {
	sql := "CREATE TABLE a(id int); INSERT INTO a VALUES(1); DROP TABLE a"
	stmts, err := ClassifyTokens(DialectMySQL, 0, sql)
	require.NoError(t, err)
	require.Len(t, stmts, 3)
	sum := stmts.Summarize()
	_, hasMulti := sum[IsMultipleStatements]
	assert.True(t, hasMulti)
	for f, toks := range sum {
		if f == IsMultipleStatements {
			continue
		}
		assert.NotNil(t, toks)
		assert.NotEmpty(t, toks.Strip().String())
	}
	// Round trip
	orig := strings.TrimSuffix(sql, ";")
	recon := strings.Join(stmts.TokensList().Strings(), ";")
	assert.True(t, recon == orig || recon+";" == sql)
}

func TestPostgresAlterTypeVersionGating(t *testing.T) {
	pre := "ALTER TYPE mood ADD VALUE 'sad'"
	stmtsPre, err := ClassifyTokens(DialectPostgres, 11, pre)
	require.NoError(t, err)
	require.Len(t, stmtsPre, 1)
	assert.NotZero(t, stmtsPre[0].Flags&IsMustNonTx)
	stmtsPost, err := ClassifyTokens(DialectPostgres, 12, pre)
	require.NoError(t, err)
	require.Len(t, stmtsPost, 1)
	assert.Zero(t, stmtsPost[0].Flags&IsMustNonTx)
}

func TestWithLeadingTokenClassification(t *testing.T) {
	sql := "WITH cte AS (SELECT 1) SELECT * FROM cte"
	stmts, err := ClassifyTokens(DialectPostgres, 0, sql)
	require.NoError(t, err)
	agg := aggregateFlags(stmts)
	assert.NotZero(t, agg&IsDML)
	assert.Zero(t, agg&IsDDL)
}
