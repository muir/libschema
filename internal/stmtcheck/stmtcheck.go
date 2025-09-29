package stmtcheck

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/memsql/errors"
	"github.com/muir/sqltoken"
)

var (
	// ErrDataAndDDL indicates a script mixes data-changing and schema-changing statements
	ErrDataAndDDL errors.String = "migration combines DDL (schema changes) and data manipulation"
	// ErrNonIdempotentDDL indicates a DDL statement lacking an IF (NOT) EXISTS guard
	ErrNonIdempotentDDL errors.String = "unconditional migration has non-idempotent DDL"
	ifExistsRE                        = regexp.MustCompile(`(?i)\bIF\s+(?:NOT\s+)?EXISTS\b`)
)

// CommandClassification holds flags discovered for a single statement.
type CommandClassification struct {
	IsDDL         bool
	IsData        bool
	NonIdempotent bool
	Text          string
}

// classify determines characteristics of a statement token slice (already a single statement).
func classify(cmd sqltoken.Tokens) CommandClassification {
	c := CommandClassification{Text: cmd.String()}
	if len(cmd) == 0 {
		return c
	}
	first := strings.ToLower(cmd[0].Text)
	switch first {
	case "alter", "rename", "create", "drop", "comment":
		c.IsDDL = true
		if !ifExistsRE.MatchString(c.Text) {
			c.NonIdempotent = true
		}
	case "truncate":
		c.IsDDL = true
	case "use", "set":
		// ignore
	case "values", "table", "select":
		// read-only
	case "call", "delete", "do", "handler", "import", "insert", "load", "replace", "update", "with":
		c.IsData = true
	}
	return c
}

// AnalyzeTokens inspects tokenized SQL (already produced for specific dialect) and returns
// either nil or a wrapped error containing ErrDataAndDDL / ErrNonIdempotentDDL.
func AnalyzeTokens(ts sqltoken.Tokens) error {
	var seenDDL, seenData, nonIdempotent string
	for _, stmt := range ts.Strip().CmdSplit() {
		info := classify(stmt)
		if info.IsDDL {
			seenDDL = info.Text
			if info.NonIdempotent {
				nonIdempotent = info.Text
			}
		}
		if info.IsData {
			seenData = info.Text
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
