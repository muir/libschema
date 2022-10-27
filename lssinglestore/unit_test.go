package lssinglestore

import (
	"testing"

	"github.com/muir/libschema"

	"github.com/stretchr/testify/assert"
)

func TestAsString(t *testing.T) {
	assert.Equal(t, "", asString(nil), "asString nil")
}

func TestTrackingSchemaTable(t *testing.T) {
	cases := []struct {
		tt     string
		err    bool
		schema string
		table  string
	}{
		{
			tt:     "`foo`.xk-z",
			schema: "`foo`",
			table:  "`foo`.`xk-z`",
		},
		{
			tt:  "`foo.xk-z",
			err: true,
		},
		{
			tt:     "foo",
			schema: "",
			table:  "foo",
		},
		{
			tt:  "x.y.z",
			err: true,
		},
		{
			tt:  "`x",
			err: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.tt, func(t *testing.T) {
			d := &libschema.Database{
				Options: libschema.Options{
					TrackingTable: tc.tt,
				},
			}
			schema, table, err := trackingSchemaTable(d)
			if tc.err {
				assert.Error(t, err)
			} else {
				if assert.NoError(t, err) {
					assert.Equal(t, tc.schema, schema, "schema")
					assert.Equal(t, tc.table, table, "table")
				}
			}
		})
	}
}
