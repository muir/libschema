package stmtclass

import (
    "testing"
    "github.com/muir/sqltoken"
)

// TestAdditionalVerbCoverage exercises remaining first-token switch arms in ClassifyTokens
// that were not covered by earlier tests (rename, comment, update, delete, replace, call,
// do, load, handler, import) plus a default/unknown token path and empty statement entries.
func TestAdditionalVerbCoverage(t *testing.T) {
    cases := []struct {
        name string
        sql  string
        want uint32
    }{
        {"rename table", "RENAME TABLE t1 TO t2", IsDDL | IsNonIdempotent},
        {"comment on table (treated as ddl non-idempotent)", "COMMENT ON TABLE t1 IS 'x'", IsDDL | IsNonIdempotent},
        {"update dml", "UPDATE t1 SET c=1", IsDML},
        {"delete dml", "DELETE FROM t1 WHERE id=1", IsDML},
        {"replace dml", "REPLACE INTO t1 (id) VALUES (1)", IsDML},
        {"call proc dml", "CALL myproc()", IsDML},
        {"do expr dml", "DO 1", IsDML},
        {"load data dml", "LOAD DATA INFILE 'x' INTO TABLE t1", IsDML},
        {"handler dml", "HANDLER t1 OPEN", IsDML},
        {"import table dml", "IMPORT TABLE FROM 's3://bucket/obj'", IsDML},
        {"unknown leading token (select)", "SELECT 1", 0}, // default branch
        {"empty statements ignored", "CREATE TABLE IF NOT EXISTS t1(id int); ;  ;INSERT INTO t1 VALUES (1)", IsDDL | IsDML | IsMultipleStatements},
    }
    for _, c := range cases {
        toks := sqltoken.TokenizeMySQL(c.sql)
        _, agg := ClassifyTokens(DialectMySQL, toks)
        if agg != c.want {
            t.Errorf("%s: got 0x%x want 0x%x", c.name, agg, c.want)
        }
    }
}
