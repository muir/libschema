package lsmysql

import (
	"database/sql"

	"github.com/pkg/errors"
)

// ColumnDefault returns the default value for a column.  If there
// is no default value, then nil is returned.
func (m *MySQL) ColumnDefault(table, column string) (*string, error) {
	database, err := m.DatabaseName()
	if err != nil {
		return nil, err
	}
	var dflt *string
	err = m.db.QueryRow(`
		SELECT	column_default
		FROM	information_schema.columns
		WHERE	table_schema = ?
		AND	table_name = ?
		AND	column_name = ?`,
		database, table, column).Scan(&dflt)
	return dflt, errors.Wrapf(err, "get default for %s.%s", table, column)
}

// HasPrimaryKey returns true if the table has a primary key
func (m *MySQL) HasPrimaryKey(table string) (bool, error) {
	return m.TableHasIndex(table, "PRIMARY")
}

// TableHasIndex returns true if there is an index matching the
// name given.
func (m *MySQL) TableHasIndex(table, indexName string) (bool, error) {
	database, err := m.DatabaseName()
	if err != nil {
		return false, err
	}
	var count int
	err = m.db.QueryRow(`
		SELECT	COUNT(*)
		FROM	information_schema.statistics
		WHERE	table_schema = ?
		AND	table_name = ?
		AND	index_name = ?`,
		database, table, indexName).Scan(&count)
	return count != 0, errors.Wrapf(err, "has table index %s.%s", table, indexName)

}

// DoesColumnExist returns true if the column exists
func (m *MySQL) DoesColumnExist(table, column string) (bool, error) {
	database, err := m.DatabaseName()
	if err != nil {
		return false, err
	}
	var count int
	err = m.db.QueryRow(`
		SELECT	COUNT(*)
		FROM	information_schema.columns
		WHERE	table_schema = ?
		AND	table_name = ?
		AND	column_name = ?`,
		database, table, column).Scan(&count)
	return count != 0, errors.Wrapf(err, "get column exist %s.%s", table, column)
}

// GetTableConstraints returns the type of constraint and if it is enforced.
func (m *MySQL) GetTableConstraint(table, constraintName string) (string, bool, error) {
	database, err := m.DatabaseName()
	if err != nil {
		return "", false, err
	}
	var typ *string
	var enforced *string
	err = m.db.QueryRow(`
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

// DatabaseName returns the name of the current database (aka schema for MySQL)
func (m *MySQL) DatabaseName() (string, error) {
	var database string
	err := m.db.QueryRow(`SELECT DATABASE()`).Scan(&database)
	return database, errors.Wrap(err, "select database()")
}

func asString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
