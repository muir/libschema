package stmtclass

import (
    "testing"
    "github.com/stretchr/testify/require"
    "github.com/stretchr/testify/assert"
    "github.com/muir/sqltoken"
)

func flags(sql string) (uint32, []StatementFlags) {
    stmts, agg := ClassifyScript(sqltoken.TokenizeMySQL(sql))
    return agg, stmts
}

func TestClassifyScript(t *testing.T) {
    cases := []struct {
        name string
        sql  string
        want uint32
    }{
        {"simple ddl non-idempotent", "CREATE TABLE t (id int)", IsDDL | IsNonIdempotent | IsEasilyIdempotentFix},
        {"ddl idempotent", "CREATE TABLE IF NOT EXISTS t (id int)", IsDDL},
        {"dml", "INSERT INTO t VALUES (1)", IsDML},
        {"multiple stmts", "CREATE TABLE IF NOT EXISTS t (id int); INSERT INTO t VALUES (1)", IsDDL | IsDML | IsMultipleStatements},
        {"truncate only", "TRUNCATE TABLE t", IsDDL},
    }
    for _, tc := range cases {
        t.Run(tc.name, func(t *testing.T) {
            agg, _ := flags(tc.sql)
            assert.Equal(t, tc.want, agg)
        })
    }
}

func TestPerStatementFlags(t *testing.T) {
    sql := `CREATE TABLE IF NOT EXISTS t (id int); INSERT INTO t VALUES (1); DROP TABLE t`
    stmts, agg := ClassifyScript(sqltoken.TokenizeMySQL(sql))
    require.Len(t, stmts, 3)
    assert.Equal(t, IsDDL, stmts[0].Flags)
    assert.Equal(t, IsDML, stmts[1].Flags)
    // last drop is non-idempotent DDL; now marked easily fixable (IF EXISTS can be added)
    assert.Equal(t, IsDDL|IsNonIdempotent|IsEasilyIdempotentFix, stmts[2].Flags)
    assert.Equal(t, IsDDL|IsDML|IsNonIdempotent|IsEasilyIdempotentFix|IsMultipleStatements, agg)
}
