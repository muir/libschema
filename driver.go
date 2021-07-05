package libschema

import (
	"database/sql"
	"fmt"
	"strings"
)

func OpenAnyDB(dsn string) (*sql.DB, error) {
	for _, driver := range sql.Drivers() {
		if strings.HasPrefix(dsn, driver+"://") {
			return sql.Open(driver, dsn)
		}
	}
	if i := strings.Index(dsn, "://"); i != -1 {
		return nil, fmt.Errorf("Could not find database driver matching %s", dsn[:i])
	}
	return nil, fmt.Errorf("Could not find appropriate database driver for DSN")
}
