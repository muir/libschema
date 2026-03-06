package mhelp

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/muir/libschema"
	"github.com/muir/libschema/classifysql"
	"github.com/muir/libschema/internal"
	"github.com/muir/sqltoken"
	"github.com/pkg/errors"
)

type CanExecContext interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func RunSQL(ctx context.Context, log *internal.Log, tx CanExecContext, statements classifysql.Statements, rowsAffected *int64, m libschema.Migration, d *libschema.Database) error {
	for _, tokens := range statements.TokensList() {
		if !m.Base().PreserveComments() {
			tokens = tokens.Strip()
		}
		if len(tokens) == 0 {
			continue
		}
		if tokens[0].Type == sqltoken.DelimiterStatement {
			log.Debug("Stripping leading delimiter statement from migration", map[string]any{
				"name":    m.Base().Name.Name,
				"library": m.Base().Name.Library,
			})
			tokens = tokens[1:]
		}
		if tokens[len(tokens)-1].Type == sqltoken.DelimiterStatement {
			log.Debug("Stripping trailing delimiter statement from migration", map[string]any{
				"name":    m.Base().Name.Name,
				"library": m.Base().Name.Library,
			})
			tokens = tokens[:len(tokens)-1]
		}
		commandSQL := tokens.String()
		result, err := tx.ExecContext(ctx, commandSQL)
		if d.Options.DebugLogging {
			log.Debug("Executed SQL", map[string]any{
				"name":    m.Base().Name.Name,
				"library": m.Base().Name.Library,
				"sql":     commandSQL,
				"method":  fmt.Sprintf("%T", tx),
				"err":     err,
			})
		}
		if err != nil {
			return errors.Wrap(err, commandSQL)
		}
		ra, err := result.RowsAffected()
		if err != nil {
			log.Info("Could not get rows affected, ignoring error", map[string]any{
				"name":    m.Base().Name.Name,
				"library": m.Base().Name.Library,
				"sql":     commandSQL,
				"method":  fmt.Sprintf("%T", tx),
				"err":     err,
			})
			if m.Base().RepeatUntilNoOp() {
				// can't ignore the error
				return errors.WithStack(err)
			}
			ra = 0
		}
		*rowsAffected += ra
	}
	return nil
}
