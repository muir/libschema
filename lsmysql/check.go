package lsmysql

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/muir/sqltoken"
	"github.com/pkg/errors"
)

var ErrDataAndDDL = errors.New("Migration combines DDL (Data Definition Language [schema changes]) and data manipulation")
var ErrNonIdempotentDDL = errors.New("Unconditional migration has non-idempotent DDL (Data Definition Language [schema changes])")
var ifExistsRE = regexp.MustCompile(`(?i)\bIF (?:NOT )?EXISTS\b`)

// CheckScript attempts to validate that an SQL command does not do
// both schema changes (DDL) and data changes.  The returned error
// will be (when checked with errors.Is()) nil, ErrDataAndDDL, or
// ErrNonIdempotentDDL.
func CheckScript(s string) error {
	var seenDDL string
	var seenData string
	var nonIdempotent string
	ts := sqltoken.TokenizeMySQL(s)
	for _, cmd := range ts.Strip().CmdSplit() {
		word := strings.ToLower(cmd[0].Text)
		switch word {
		case "alter", "rename", "create", "drop", "comment":
			seenDDL = cmd.String()
			if !ifExistsRE.MatchString(cmd.String()) {
				nonIdempotent = cmd.String()
			}
		case "truncate":
			seenDDL = cmd.String()
		case "use", "set":
			// neither
		case "values", "table", "select":
			// doesn't modify anything
		case "call", "delete", "do", "handler", "import", "insert", "load", "replace", "update", "with":
			seenData = cmd.String()
		}
	}
	if seenDDL != "" && seenData != "" {
		return fmt.Errorf("data command '%s' combined with DDL command '%s': %w", seenData, seenDDL, ErrDataAndDDL)
	}
	if nonIdempotent != "" {
		return fmt.Errorf("non-idempotent DDL '%s': %w", nonIdempotent, ErrNonIdempotentDDL)
	}
	return nil
}
