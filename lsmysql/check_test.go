package lsmysql

import (
	"testing"

	"github.com/muir/libschema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCheckScriptTableDriven exercises all classification / error branches for deprecated CheckScript.
func TestCheckScriptTableDriven(t *testing.T) {
	cases := []struct {
		name    string
		sql     string
		wantErr error
	}{
		{name: "pure dml ok", sql: "INSERT INTO t VALUES (1)"},
		{name: "pure idempotent ddl ok (guarded create table)", sql: "CREATE TABLE IF NOT EXISTS t(id int)"},
		{name: "pure idempotent ddl ok (guarded drop table)", sql: "DROP TABLE IF EXISTS t"},
		{name: "non-idempotent create table easy fix error", sql: "CREATE TABLE t(id int)", wantErr: libschema.ErrNonIdempotentDDL},
		{name: "non-idempotent drop table easy fix error", sql: "DROP TABLE t", wantErr: libschema.ErrNonIdempotentDDL},
		{name: "mixed ddl dml error", sql: "CREATE TABLE t(id int); INSERT INTO t VALUES (1)", wantErr: libschema.ErrDataAndDDL},
		{name: "generic non-idempotent ddl (create view)", sql: "CREATE VIEW v AS SELECT 1", wantErr: libschema.ErrNonIdempotentDDL},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := CheckScript(c.sql)
			if c.wantErr == nil {
				require.NoError(t, err, "expected success for %s", c.name)
				return
			}
			require.Error(t, err, "expected error for %s", c.name)
			assert.ErrorIs(t, err, c.wantErr, "expected wrapped sentinel error")
		})
	}
}
