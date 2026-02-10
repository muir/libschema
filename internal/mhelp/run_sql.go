package mhelp

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/muir/libschema"
	"github.com/muir/libschema/classifysql"
	"github.com/muir/libschema/internal"
	"github.com/pkg/errors"
)

type CanExecContext interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func RunSQL(ctx context.Context, log *internal.Log, tx CanExecContext, statements classifysql.Statements, rowsAffected *int64, m libschema.Migration, d *libschema.Database) error {
	for _, commandSQL := range statements.TokensList().Strings() {
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
