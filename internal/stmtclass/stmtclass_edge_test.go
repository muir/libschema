package stmtclass

import (
    "testing"
    "github.com/muir/sqltoken"
    "github.com/stretchr/testify/require"
    "github.com/stretchr/testify/assert"
)

// TestSummarizeMultiSynthetic verifies Summarize records empty string for synthetic multi flag
// while capturing first concrete statements for other flags.
func TestSummarizeMultiSynthetic(t *testing.T) {
    sql := "CREATE TABLE a(id int); INSERT INTO a VALUES(1); DROP TABLE a"
    toks := sqltoken.TokenizeMySQL(sql)
    stmts, agg := ClassifyTokens(DialectMySQL, 0, toks)
    require.Len(t, stmts, 3)
    require.NotZero(t, agg&IsMultipleStatements)
    sum := Summarize(stmts, agg)
    // Multi should map to empty string
    if v, ok := sum[IsMultipleStatements]; ok {
        assert.Equal(t, "", v, "Multi flag should map to empty string")
    } else {
        t.Fatalf("Multi flag missing from summary")
    }
    // Other flags should map to non-empty first statement texts
    for f, txt := range sum {
        if f == IsMultipleStatements { continue }
        assert.NotEmpty(t, txt, "expected first statement text for flag")
    }
}

// TestPostgresAlterTypeVersionGating isolates version-dependent MustNonTx behavior.
func TestPostgresAlterTypeVersionGating(t *testing.T) {
    pre := "ALTER TYPE mood ADD VALUE 'sad'"
    // Version 11 should mark MustNonTx
    toksPre := sqltoken.TokenizePostgreSQL(pre)
    stmtsPre, aggPre := ClassifyTokens(DialectPostgres, 11, toksPre)
    require.Len(t, stmtsPre, 1)
    assert.NotZero(t, aggPre&IsMustNonTx, "expected MustNonTx pre-12")
    // Version 12 should not
    toksPost := sqltoken.TokenizePostgreSQL(pre)
    stmtsPost, aggPost := ClassifyTokens(DialectPostgres, 12, toksPost)
    require.Len(t, stmtsPost, 1)
    assert.Zero(t, aggPost&IsMustNonTx, "did not expect MustNonTx for 12+")
}

// TestWithLeadingTokenClassification covers the 'with' leading token branch (treated as DML).
func TestWithLeadingTokenClassification(t *testing.T) {
    sql := "WITH cte AS (SELECT 1) SELECT * FROM cte"
    toks := sqltoken.TokenizePostgreSQL(sql)
    _, agg := ClassifyTokens(DialectPostgres, 0, toks)
    assert.NotZero(t, agg&IsDML)
    assert.Zero(t, agg&IsDDL)
}
