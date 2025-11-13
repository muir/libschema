package classifysql

import (
	"testing"
	"github.com/stretchr/testify/require"
)

func TestRegroupPostgres(t *testing.T) {
	// Mix of regular DDL/DML plus a MustNonTx requiring isolation
	sql := "CREATE TABLE a(id int); CREATE INDEX CONCURRENTLY idx ON a(id); INSERT INTO a VALUES(1); VACUUM FULL"
	stmts, err := ClassifyTokens(DialectPostgres, 0, sql)
	require.NoError(t, err)
	groups := stmts.Regroup()
	// Expect: [ group(normal statements without MustNonTx) ][ each MustNonTx isolated ]
	// Statements with MustNonTx: CREATE INDEX CONCURRENTLY, VACUUM FULL
	// First group should have CREATE TABLE and INSERT
	require.GreaterOrEqual(t, len(groups), 3)
	// Verify isolation of MustNonTx flags
	var mustNonTxCount int
	for _, g := range groups {
		if len(g) == 1 && (g[0].Flags&IsMustNonTx) != 0 {
			mustNonTxCount++
			require.Zero(t, g[0].Flags&IsMultipleStatements, "isolated MustNonTx statement should not retain Multi flag")
		}
	}
	require.Equal(t, 2, mustNonTxCount)
}

func TestRegroupMySQL(t *testing.T) {
	// MySQL: Each DDL isolated; consecutive DML grouped
	sql := "CREATE TABLE a(id int); INSERT INTO a VALUES(1); INSERT INTO a VALUES(2); DROP TABLE a; UPDATE a SET id=3"
	stmts, err := ClassifyTokens(DialectMySQL, 0, sql)
	require.NoError(t, err)
	groups := stmts.Regroup()
	// Expect pattern: [DDL][DML grouped][DDL][DML]
	require.GreaterOrEqual(t, len(groups), 4)
	// Count groups with multiple entries (should be exactly one: combined DML inserts)
	var multiGroup int
	for _, g := range groups { if len(g) > 1 { multiGroup++ } }
	require.Equal(t, 1, multiGroup)
}
