// Package lssinglestore is a libschema.Driver for connecting to SingleStore databases.
package lssinglestore

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/memsql/errors"

	"github.com/muir/libschema"
	"github.com/muir/libschema/internal"
	"github.com/muir/libschema/lsmysql"
)

// SingleStore is a libschema.Driver for connecting to SingleStore databases.
//
// Because SingleStore DDL commands cause transactions to autocommit, tracking the schema changes in
// a secondary table (like libschema does) is inherently unsafe.  The SingleStore driver will
// record that it is about to attempt a migration and it will record if that attempts succeeds
// or fails, but if the program terminates mid-transaction, it is beyond the scope of libschema
// to determine if the transaction succeeded or failed.  Such transactions will be retried.
// For this reason, it is reccomend that DDL commands be written such that they are idempotent.
//
// There are methods the SingleStore type (inherited from lsmysql.MySQL) that can be used to query the
// state of the database and thus transform DDL commands that are not idempotent into
// idempotent commands by only running them if they need to be run.
//
// Because Go's database/sql uses connection pooling and the SingleStore "USE database" command leaks
// out of transactions, it is strongly recommended that the libschema.Option value of
// SchemaOverride be set when creating the libschema.Schema object.  That SchemaOverride will
// be propagated into the SingleStore object and be used as a default table for all of the
// functions to interrogate data defintion status.
type SingleStore struct {
	*lsmysql.MySQL
	lockTx *sql.Tx
	lock   sync.Mutex
	db     *sql.DB
}

// New creates a libschema.Database with a Singlestore driver built in.
func New(log *internal.Log, dbName string, schema *libschema.Schema, db *sql.DB) (*libschema.Database, *SingleStore, error) {
	_, mysql, err := lsmysql.New(log, dbName, schema, db,
		lsmysql.WithoutDatabase,
		lsmysql.WithTrackingTableQuoter(trackingSchemaTable),
	)
	if err != nil {
		return nil, nil, err
	}
	s2 := &SingleStore{
		MySQL: mysql,
		db:    db,
	}
	database, err := schema.NewDatabase(log, dbName, db, s2)
	if err != nil {
		return nil, nil, err
	}
	if database.Options.SchemaOverride != "" {
		//nolint:staticcheck // QF1008: could remove embedded field "MySQL" from selector
		s2.MySQL.UseDatabase(database.Options.SchemaOverride)
	}
	return database, s2, nil
}

// Script defines a literal SQL statement migration.
// To run the migration, libschema automatically chooses transactional (*sql.Tx)
// or non-transactional (*sql.DB)
// execution based on SingleStore rules for statements that cannot run inside a
// transaction (DML (insert/update) can be in a transaction but DDL (create table, etc) cannot be).
// The choice of transactional vs non-transactional Can be overridden with
// ForceNonTransactional() or ForceTransactional() options.
func Script(name string, sqlText string, opts ...libschema.MigrationOption) libschema.Migration {
	// Delegates to MySQL implementation which now applies ApplyForceOverride early.
	return lsmysql.Script(name, sqlText, opts...)
}

// Generate registers a callback that returns a migration in a string.
// To run the migration, libschema automatically chooses transactional (*sql.Tx)
// or non-transactional (*sql.DB)
// execution based on SingleStore rules for statements that cannot run inside a
// transaction (DML (insert/update) can be in a transaction but DDL (create table, etc) cannot be).
// If the migration will be run transactionally, it will run in the same transaction
// as the callback that returned the string. If it runs non-transactionally, the
// transaction that returned the string will be idle (hanging around) while the migration runs.
// The choice of transactional vs non-transactional Can be overridden with
// ForceNonTransactional() or ForceTransactional() options.
func Generate(name string, generator func(context.Context, *sql.Tx) string, opts ...libschema.MigrationOption) libschema.Migration {
	return lsmysql.Generate(name, generator, opts...)
}

// Computed defines a migration that runs arbitrary Go code.
// The signature of the action callback determines if the migration runs
// transactionally or if it runs outside a transaction:
//
//	func(context.Context, *sql.Tx) error // run transactionlly
//	func(context.Context, *sql.DB) error // run non-transactionally
func Computed[T lsmysql.ConnPtr](
	name string,
	action func(context.Context, T) error,
	opts ...libschema.MigrationOption,
) libschema.Migration {
	return lsmysql.Computed[T](name, action, opts...)
}

// LockMigrationsTable locks the migration tracking table for exclusive use by the
// migrations running now.
// It is expected to be called by libschema.
func (p *SingleStore) LockMigrationsTable(ctx context.Context, _ *internal.Log, d *libschema.Database) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.lockTx != nil {
		return errors.Errorf("migrations already locked")
	}
	_, tableName, _, err := trackingSchemaTable(d)
	if err != nil {
		return err
	}
	_, err = d.DB().ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s_lock (
			anything	int NOT NULL,
			SORT KEY	(anything),
			SHARD KEY	(anything),
			PRIMARY KEY	(anything)
		)`, tableName))
	if err != nil {
		return errors.Wrapf(err, "could not create libschema migrations table '%s'", tableName)
	}
	tx, err := d.DB().BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "could not begin tx")
	}
	_, err = tx.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s_lock (anything) VALUES (1)
	`, tableName))
	if err != nil {
		return errors.Wrap(err, "insert into lock table")
	}
	p.lockTx = tx
	return nil
}

func (p *SingleStore) UnlockMigrationsTable(_ *internal.Log) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.lockTx == nil {
		return errors.Errorf("migrations not locked")
	}
	tx := p.lockTx
	p.lockTx = nil
	err := tx.Rollback()
	return errors.Wrap(err, "rollback lock-holding transaction")
}

var (
	simpleIdentifierRE = regexp.MustCompile(`\A[A-Za-z][A-Za-z0-9_]*\z`)
	alreadyQuotedRE    = regexp.MustCompile("`[^`]+`")
	cannotQuoteRE      = regexp.MustCompile("`")
)

func makeID(raw string) (string, error) {
	switch {
	case simpleIdentifierRE.MatchString(raw):
		return raw, nil
	case alreadyQuotedRE.MatchString(raw):
		return raw, nil
	case cannotQuoteRE.MatchString(raw):
		return "", errors.Errorf("cannot quote identifier (%s)", raw)
	default:
		return "`" + raw + "`", nil
	}
}

func trackingSchemaTable(d *libschema.Database) (string, string, string, error) {
	tableName := d.Options.TrackingTable
	s := strings.Split(tableName, ".")
	switch len(s) {
	case 2:
		schema, err := makeID(s[0])
		if err != nil {
			return "", "", "", errors.Wrap(err, "cannot make tracking table schema name")
		}
		table, err := makeID(s[1])
		if err != nil {
			return "", "", "", errors.Wrap(err, "cannot make tracking table table name")
		}
		return schema, schema + "." + table, table, nil
	case 1:
		table, err := makeID(tableName)
		if err != nil {
			return "", "", "", errors.Wrap(err, "cannot make tracking table table name")
		}
		return "", table, table, nil
	default:
		return "", "", "", errors.Errorf("tracking table '%s' is not valid", tableName)
	}
}

// CreateSchemaTableIfNotExists creates the migration tracking table for libschema.
func (p *SingleStore) CreateSchemaTableIfNotExists(ctx context.Context, _ *internal.Log, d *libschema.Database) error {
	schema, tableName, _, err := trackingSchemaTable(d)
	if err != nil {
		return err
	}
	if schema != "" {
		_, err := d.DB().ExecContext(ctx, fmt.Sprintf(`
				CREATE DATABASE IF NOT EXISTS %s PARTITIONS 2
				`, schema))
		if err != nil {
			return errors.Wrapf(err, "could not create libschema schema '%s'", schema)
		}
	}
	_, err = d.DB().ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			db_name		varchar(255) NOT NULL,
			library		varchar(255) NOT NULL,
			migration	varchar(255) NOT NULL,
			done		boolean NOT NULL,
			error		text NOT NULL,
			updated_at	timestamp DEFAULT now(),
			SORT KEY	(library, migration),
			SHARD KEY	(library, migration),
			PRIMARY KEY	(db_name, library, migration)
		)`, tableName))
	if err != nil {
		return errors.Wrapf(err, "could not create libschema migrations table '%s'", tableName)
	}
	return nil
}

// GetTableConstraints returns the type of constraint and if it is enforced.
// The table is assumed to be in the current database unless m.UseDatabase() has been called.
func (p *SingleStore) GetTableConstraint(table, constraintName string) (string, bool, error) {
	database, err := p.DatabaseName()
	if err != nil {
		return "", false, err
	}
	var typ *string
	err = p.db.QueryRow(`
		SELECT	constraint_type
		FROM	information_schema.table_constraints
		WHERE	constraint_schema = ?
		AND	table_name = ?
		AND	constraint_name = ?`,
		database, table, constraintName).Scan(&typ)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	return asString(typ), true, errors.Wrapf(err, "get table constraint %s.%s", table, constraintName)
}

func asString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
