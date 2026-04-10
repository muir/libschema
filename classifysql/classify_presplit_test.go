package classifysql

import (
	"testing"

	"github.com/muir/sqltoken"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClassifyPreSplitBoundariesPreserved(t *testing.T) {
	// Two statements that would merge into one if re-tokenized and split differently.
	// Using a SQL string that tokenizes into 2 separate statements.
	tokens := sqltoken.TokenizeMySQLAPI("CREATE TABLE a (id int); INSERT INTO a VALUES (1)")
	split := tokens.CmdSplitUnstripped()
	require.Len(t, split, 2, "pre-condition: must have two statements")

	// Feed pre-split directly: boundaries must be exactly as provided.
	stmts, err := ClassifyPreSplit(DialectMySQL, 0, split)
	require.NoError(t, err)
	require.Len(t, stmts, 2, "ClassifyPreSplit must preserve caller-provided statement count")
}

func TestClassifyPreSplitCommentOnly(t *testing.T) {
	tokens := sqltoken.TokenizePostgreSQL("-- just a comment\n")
	split := tokens.CmdSplitUnstripped()

	stmts, err := ClassifyPreSplit(DialectPostgres, 0, split)
	require.NoError(t, err)
	// comment-only entry must not acquire any classification flags
	for _, st := range stmts {
		assert.Equal(t, Flag(0), st.Flags, "comment-only statement must have no flags")
	}
	assert.Equal(t, 0, stmts.CountNonEmpty(), "comment-only yields zero real statements")
}

func TestClassifyPreSplitIsMultipleStatements(t *testing.T) {
	cases := []struct {
		name  string
		sql   string
		multi bool
	}{
		{"single statement", "CREATE TABLE t (id int)", false},
		{"two statements", "CREATE TABLE t (id int); CREATE TABLE u (id int)", true},
	}
	for _, tc := range cases {
		tokens := sqltoken.TokenizeMySQLAPI(tc.sql)
		split := tokens.CmdSplitUnstripped()

		stmts, err := ClassifyPreSplit(DialectMySQL, 0, split)
		require.NoError(t, err, tc.name)
		sum := stmts.Summarize()
		_, hasMulti := sum[IsMultipleStatements]
		assert.Equal(t, tc.multi, hasMulti, tc.name+": IsMultipleStatements mismatch")
	}
}

func TestClassifyPreSplitInvalidDialect(t *testing.T) {
	_, err := ClassifyPreSplit(DialectInvalid, 0, sqltoken.TokensList{})
	require.Error(t, err)
}

func TestClassifyPreSplitEmpty(t *testing.T) {
	stmts, err := ClassifyPreSplit(DialectMySQL, 0, nil)
	require.NoError(t, err)
	assert.Nil(t, stmts)
}
