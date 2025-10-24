package lsmysql

import (
	"database/sql"

	"github.com/memsql/errors"
)

// ColumnDefault returns the default value for a column.  If there
// is no default value, then nil is returned.
// The table is assumed to be in the current database unless m.UseDatabase() has been called.
func (p *MySQL) ColumnDefault(table, column string) (*string, error) {
	database, err := p.DatabaseName()
	if err != nil {
		return nil, err
	}
	var dflt *string
	err = p.db.QueryRow(`
		SELECT	column_default
		FROM	information_schema.columns
		WHERE	table_schema = ?
		AND	table_name = ?
		AND	column_name = ?`,
		database, table, column).Scan(&dflt)
	return dflt, errors.Wrapf(err, "get default for %s.%s", table, column)
}

// HasPrimaryKey returns true if the table has a primary key
// The table is assumed to be in the current database unless m.UseDatabase() has been called.
func (p *MySQL) HasPrimaryKey(table string) (bool, error) {
	database, err := p.DatabaseName()
	if err != nil {
		return false, err
	}
	var count int
	err = p.db.QueryRow(`
		SELECT	COUNT(*)
		FROM	information_schema.columns
		WHERE	table_schema = ?
		AND	table_name = ?
		AND	column_key = 'PRI'`,
		database, table).Scan(&count)
	return count != 0, errors.Wrapf(err, "has primary key %s.%s", database, table)
}

// ColumnIsInPrimaryKey returns true if the column part of the prmary key.
// The table is assumed to be in the current database unless m.UseDatabase() has been called.
func (p *MySQL) ColumnIsInPrimaryKey(table string, column string) (bool, error) {
	database, err := p.DatabaseName()
	if err != nil {
		return false, err
	}
	var count int
	err = p.db.QueryRow(`
		SELECT	COUNT(*)
		FROM	information_schema.columns
		WHERE	table_schema = ?
		AND	table_name = ?
		AND	column_name = ?
		AND	column_key = 'PRI'`,
		database, table, column).Scan(&count)
	return count != 0, errors.Wrapf(err, "column is in primary key %s.%s.%s", database, table, column)
}

// TableHasIndex returns true if there is an index matching the
// name given.
// The table is assumed to be in the current database unless m.UseDatabase() has been called.
func (p *MySQL) TableHasIndex(table, indexName string) (bool, error) {
	database, err := p.DatabaseName()
	if err != nil {
		return false, err
	}
	var count int
	err = p.db.QueryRow(`
		SELECT	COUNT(*)
		FROM	information_schema.statistics
		WHERE	table_schema = ?
		AND	table_name = ?
		AND	index_name = ?`,
		database, table, indexName).Scan(&count)
	return count != 0, errors.Wrapf(err, "has table index %s.%s", table, indexName)
}

// DoesColumnExist returns true if the column exists
// The table is assumed to be in the current database unless m.UseDatabase() has been called.
func (p *MySQL) DoesColumnExist(table, column string) (bool, error) {
	database, err := p.DatabaseName()
	if err != nil {
		return false, err
	}
	var count int
	err = p.db.QueryRow(`
		SELECT	COUNT(*)
		FROM	information_schema.columns
		WHERE	table_schema = ?
		AND	table_name = ?
		AND	column_name = ?`,
		database, table, column).Scan(&count)
	return count != 0, errors.Wrapf(err, "get column exist %s.%s", table, column)
}

// GetTableConstraints returns the type of constraint and if it is enforced.
// The table is assumed to be in the current database unless m.UseDatabase() has been called.
func (p *MySQL) GetTableConstraint(table, constraintName string) (string, bool, error) {
	database, err := p.DatabaseName()
	if err != nil {
		return "", false, err
	}
	var typ *string
	var enforced *string
	err = p.db.QueryRow(`
		SELECT	constraint_type, enforced
		FROM	information_schema.table_constraints
		WHERE	constraint_schema = ?
		AND	table_name = ?
		AND	constraint_name = ?`,
		database, table, constraintName).Scan(&typ, &enforced)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	return asString(typ), asString(enforced) == "YES", errors.Wrapf(err, "get table constraint %s.%s", table, constraintName)
}

// DatabaseName returns the name of the current database (aka schema for MySQL).
// A call to UseDatabase() overrides all future calls to DatabaseName().  If the
// MySQL object was created from a libschema.Schema that had SchemaOverride set
// then this will return whatever that value was.  It is reccomened that
// UseDatabase() be called to make sure that the right database is returned.
func (m *MySQL) DatabaseName() (string, error) {
	if m.databaseName != "" {
		return m.databaseName, nil
	}
	var database string
	err := m.db.QueryRow(`SELECT DATABASE()`).Scan(&database)
	return database, errors.Wrap(err, "select database()")
}

// UseDatabase() overrides the default database for DatabaseName(), ColumnDefault(), HasPrimaryKey(),
// HasTableIndex(), DoesColumnExist(), and GetTableConstraint().
// If name is empty then the override is removed and the database will be queried from
// the mysql server.  Due to connection pooling in Go, that's a bad idea.
func (m *MySQL) UseDatabase(name string) {
	m.databaseName = name
}

func asString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
