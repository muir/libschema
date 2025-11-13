package classifysql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoundTripPreservesOriginalSegmentation(t *testing.T) {
	cases := []string{
		"CREATE TABLE t(id int)",
		"CREATE TABLE t(id int);", // trailing semicolon allowed to drop
		"CREATE TABLE t(id int); INSERT INTO t VALUES (1)",
		"CREATE TABLE t(id int); INSERT INTO t VALUES (1);", // trailing semicolon
		"CREATE TABLE t(id int); ; ; INSERT INTO t VALUES (1)", // empty statements preserved
		"/*c*/ CREATE TABLE t(id int); -- inline comment\nINSERT INTO t VALUES(1)",
	}
	for _, orig := range cases {
		stmts, err := ClassifyTokens(DialectMySQL, 0, orig)
		require.NoError(t, err, orig)
		recon := strings.Join(stmts.TokensList().Strings(), ";")
		trimmed := strings.TrimSuffix(orig, ";")
		if strings.HasSuffix(orig, ";") {
			assert.True(t, recon == trimmed || recon+";" == orig, "round-trip mismatch for %q: %q", orig, recon)
		} else {
			assert.Equal(t, trimmed, recon, "round-trip mismatch for %q", orig)
		}
	}
}
