package lsmysql

import (
	"github.com/muir/libschema"
	"github.com/muir/libschema/classifysql"
)

var (
	ErrDataAndDDL       = libschema.ErrDataAndDDL       // Deprecated: use libschema.ErrDataAndDDL
	ErrNonIdempotentDDL = libschema.ErrNonIdempotentDDL // Deprecated: use libschema.ErrNonIdempotentDDL
)

// Deprecated: no longer a supported API
func CheckScript(s string) error {
	stmts, err := classifysql.ClassifyTokens(classifysql.DialectMySQL, 0, s)
	if err != nil {
		return err
	}
	// Build aggregate and summary using new API
	var agg classifysql.Flag
	for _, st := range stmts {
		agg |= st.Flags
	}
	sum := stmts.Summarize()
	if sum.Includes(classifysql.IsDDL, classifysql.IsDML) {
		return libschema.ErrDataAndDDL.Errorf("data command '%s' combined with DDL command '%s'", sum[classifysql.IsDML].String(), sum[classifysql.IsDDL].String())
	}
	if sum.Includes(classifysql.IsNonIdempotent, classifysql.IsDDL) {
		return libschema.ErrNonIdempotentDDL.Errorf("non-idempotent DDL '%s'", sum[classifysql.IsNonIdempotent].String())
	}
	return nil
}
