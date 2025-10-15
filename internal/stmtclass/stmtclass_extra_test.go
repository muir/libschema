package stmtclass

import (
    "testing"
    "github.com/stretchr/testify/require"
    "github.com/stretchr/testify/assert"
    "github.com/muir/sqltoken"
)

// helper to classify raw sql for convenience
func classify(d Dialect, major int, sql string) ([]StatementFlags, Flag) {
    var toks sqltoken.Tokens
    switch d {
    case DialectMySQL:
        toks = sqltoken.TokenizeMySQL(sql)
    case DialectPostgres:
        toks = sqltoken.TokenizePostgreSQL(sql)
    }
    return ClassifyTokens(d, major, toks)
}

func TestSummarizeAndFlagPaths(t *testing.T) {
    cases := []struct{
        name string
        dialect Dialect
        major int
        sql string
        expect Flag
        mustNonTx bool
    }{
        {name: "mysql create table missing if exists", dialect: DialectMySQL, sql: "CREATE TABLE x (id int)", expect: IsDDL|IsNonIdempotent|IsEasilyIdempotentFix},
        {name: "mysql create table guarded", dialect: DialectMySQL, sql: "CREATE TABLE IF NOT EXISTS x (id int)", expect: IsDDL},
        {name: "mysql drop table missing if exists", dialect: DialectMySQL, sql: "DROP TABLE x", expect: IsDDL|IsNonIdempotent|IsEasilyIdempotentFix},
        {name: "mysql alter add column non idempotent", dialect: DialectMySQL, sql: "ALTER TABLE x ADD COLUMN y int", expect: IsDDL|IsNonIdempotent},
        {name: "postgres create index non tx", dialect: DialectPostgres, sql: "CREATE INDEX CONCURRENTLY idx ON t (id)", expect: IsDDL|IsNonIdempotent|IsEasilyIdempotentFix, mustNonTx: true},
        {name: "postgres drop index non tx", dialect: DialectPostgres, sql: "DROP INDEX CONCURRENTLY idx", expect: IsDDL|IsNonIdempotent|IsEasilyIdempotentFix, mustNonTx: true},
        {name: "postgres alter type add value pre12", dialect: DialectPostgres, major: 11, sql: "ALTER TYPE mood ADD VALUE 'happy'", expect: IsDDL|IsNonIdempotent, mustNonTx: true},
        {name: "postgres alter type add value post12", dialect: DialectPostgres, major: 13, sql: "ALTER TYPE mood ADD VALUE 'happy'", expect: IsDDL|IsNonIdempotent},
        {name: "multi statements", dialect: DialectPostgres, sql: "CREATE TABLE a(id int); CREATE TABLE b(id int)", expect: IsMultipleStatements|IsDDL|IsNonIdempotent|IsEasilyIdempotentFix},
        {name: "pure dml insert", dialect: DialectMySQL, sql: "INSERT INTO t VALUES (1)", expect: IsDML},
    }
    for _, c := range cases {
        t.Run(c.name, func(t *testing.T) {
            stmts, agg := classify(c.dialect, c.major, c.sql)
            require.NotEmpty(t, stmts)
            assert.Equal(t, c.expect, agg & c.expect, "expected flags subset present")
            if c.mustNonTx {
                // at least one statement must have MustNonTx
                found := false
                for _, st := range stmts { if st.Flags & IsMustNonTx != 0 { found = true; break } }
                assert.True(t, found, "MustNonTx flag missing")
            }
            // Summarize should map each flag to first statement text
            sum := Summarize(stmts, agg)
            for f := range sum { assert.NotNil(t, sum[f]) }
        })
    }
}
