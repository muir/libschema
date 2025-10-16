package lstesting

import (
	"database/sql"
	"math/rand"
	"time"

	"github.com/muir/libschema"
	"github.com/stretchr/testify/assert"
)

var seed = rand.New(rand.NewSource(time.Now().UnixNano()))

// RandomString returns a lowercase string of length n
func RandomString(n int) string {
	res := make([]byte, n)
	for i := range res {
		res[i] = 'a' + uint8(seed.Intn(26))
	}
	return string(res)
}

type T interface {
	assert.TestingT
	Logf(string, ...interface{})
}

// FakeSchema generates an Options config with a fake random schema name
// that begins with "lstest_".  It also returns a function to
// remove that schema -- the function should work with Postgres and Mysql.
func FakeSchema(t T, cascade string) (libschema.Options, func(db *sql.DB)) {
	schemaName := "lstest_" + RandomString(15)
	return libschema.Options{
			TrackingTable:  schemaName + ".tracking_table",
			SchemaOverride: schemaName,
		}, func(db *sql.DB) {
			if db == nil {
				return
			}
			_, err := db.Exec(`DROP SCHEMA IF EXISTS ` + schemaName + ` ` + cascade)
			assert.NoErrorf(t, err, "drop schema %s", schemaName)
			// log after successful drop to retain prior visibility
			t.Logf("DROPPED %s", schemaName)
		}
}
