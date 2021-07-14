package lsmysql

import (
	"regexp"
	"strings"

	"github.com/muir/libschema/sqltoken"
)

type CheckResult string

const (
	Safe             CheckResult = "safe"
	DataAndDDL                   = "dataAndDDL"
	NonIdempotentDDL             = "nonIdempotentDDL"
)

var ifExistsRE = regexp.MustCompile(`(?i)\bIF (?:NOT )?EXISTS\b`)

// CheckScript attempts to validate that an SQL command does not do
// both schema changes (DDL) and data changes.
func CheckScript(s string) CheckResult {
	var seenDDL int
	var seenData int
	var idempotent int
	ts := sqltoken.TokenizeMySQL(s)
	for _, cmd := range ts.Strip().CmdSplit() {
		word := strings.ToLower(cmd[0].Text)
		switch word {
		case "alter", "rename", "create", "drop", "comment":
			seenDDL++
			if ifExistsRE.MatchString(cmd.String()) {
				idempotent++
			}
		case "truncate":
			seenDDL++
			idempotent++
		case "use", "set":
			// neither
		case "values", "table", "select":
			// doesn't modify anything
		case "call", "delete", "do", "handler", "import", "insert", "load", "replace", "update", "with":
			seenData++
		}
	}
	if seenDDL > 0 && seenData > 0 {
		return DataAndDDL
	}
	if seenDDL > idempotent {
		return NonIdempotentDDL
	}
	return Safe
}
