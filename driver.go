package libschema

import (
	"database/sql"
	"fmt"
	"strings"
)

// maps from DSN to driver name
var driverAliases = map[string]string{
	"postgresql": "postgres",
}

func OpenAnyDB(dsn string) (*sql.DB, error) {
	if i := strings.Index(dsn, "://"); i != -1 {
		wanted := dsn[0:i]
		if alias, ok := driverAliases[wanted]; ok {
			wanted = alias
		}
		for _, driver := range sql.Drivers() {
			if driver == wanted {
				return sql.Open(driver, dsn)
			}
		}
		return nil, fmt.Errorf("could not find database driver matching %s", wanted)
	}
	return nil, fmt.Errorf("could not find appropriate database driver for DSN")
}
