// Package lsmysql has a libschema.Driver support MySQL
package lsmysql

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/memsql/errors"
	"github.com/muir/sqltoken"

	"github.com/muir/libschema"
	"github.com/muir/libschema/internal"
)

// MySQL is a libschema.Driver for connecting to MySQL-like databases that
// have the following characteristics:
// * CANNOT do DDL commands inside transactions
// * Support UPSERT using INSERT ... ON DUPLICATE KEY UPDATE
// * uses /* -- and # for comments
// * supports advisory locks
// * has quoting modes (ANSI_QUOTES)
// * can use ALTER TABLE to modify the primary key of a table
//
// Because mysql DDL commands cause transactions to autocommit, tracking the schema changes in
// a secondary table (like libschema does) is inherently unsafe.  The MySQL driver will
// record that it is about to attempt a migration and it will record if that attempts succeeds
// or fails, but if the program terminates mid-transaction, it is beyond the scope of libschema
// to determine if the transaction succeeded or failed.  Such transactions will be retried.
// For this reason, it is reccomend that DDL commands be written such that they are idempotent.
//
// There are methods the MySQL type that can be used to query the state of the database and
// thus transform DDL commands that are not idempotent (like CREATE INDEX) into idempotent
// commands by only running them if they need to be run.
//
// Because Go's database/sql uses connection pooling and the mysql "USE database" command leaks
// out of transactions, it is strongly recommended that the libschema.Option value of
// SchemaOverride be set when creating the libschema.Schema object.  That SchemaOverride will
// be propagated into the MySQL object and be used as a default table for all of the
// functions to interrogate data defintion status.
type MySQL struct {
	lockTx              *sql.Tx
	lockStr             string
	db                  *sql.DB
	databaseName        string
	lock                sync.Mutex
	trackingSchemaTable func(*libschema.Database) (string, string, string, error)
	skipDatabase        bool
}

type MySQLOpt func(*MySQL)

// WithoutDatabase skips creating a *libschema.Database.  Without it,
// functions for getting and setting the dbNames are required.
func WithoutDatabase(p *MySQL) {
	p.skipDatabase = true
}

// New creates a libschema.Database with a mysql driver built in.  The dbName
// parameter specifies the name of the database, but that name is not actaully
// used anywhere except for logging.  To override the database used for the
// migrations set the SchemaOverride option.
func New(log *internal.Log, dbName string, schema *libschema.Schema, db *sql.DB, options ...MySQLOpt) (*libschema.Database, *MySQL, error) {
	m := &MySQL{
		db:                  db,
		trackingSchemaTable: trackingSchemaTable,
	}
	for _, opt := range options {
		opt(m)
	}
	var d *libschema.Database
	if !m.skipDatabase {
		var err error
		d, err = schema.NewDatabase(log, dbName, db, m)
		if err != nil {
			return nil, nil, err
		}
		m.databaseName = d.Options.SchemaOverride
	}
	return d, m, nil
}

// ExecConn provides the minimal surface common to *sql.DB and *sql.Tx used by MySQL migrations.
type ExecConn interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// TxLike is implemented by *sql.Tx; presence indicates transactional scope.
type TxLike interface {
	Commit() error
	Rollback() error
}

type mmigration struct {
	libschema.MigrationBase
	scriptAny   func(context.Context, ExecConn) (string, error)
	computedAny func(context.Context, ExecConn) (bool, error)
}

func (m *mmigration) Copy() libschema.Migration {
	return &mmigration{MigrationBase: m.MigrationBase.Copy(), scriptAny: m.scriptAny, computedAny: m.computedAny}
}
func (m *mmigration) Base() *libschema.MigrationBase { return &m.MigrationBase }

// Script registers a literal SQL migration. Always transactional by default (uses *sql.Tx)
// unless overridden with ForceNonTransactional().
func Script(name string, sqlText string, opts ...libschema.MigrationOption) libschema.Migration {
	pm := &mmigration{MigrationBase: libschema.MigrationBase{Name: libschema.MigrationName{Name: name}}}
	pm.scriptAny = func(_ context.Context, _ ExecConn) (string, error) { return sqlText, nil }
	m := libschema.Migration(pm)
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Generate defines a migration returning a SQL string. If T implements TxLike it's transactional else non-transactional.
func Generate[T ExecConn](name string, generator func(context.Context, T) string, opts ...libschema.MigrationOption) libschema.Migration {
	var z T
	_, isTx := any(z).(TxLike)
	pm := &mmigration{MigrationBase: libschema.MigrationBase{Name: libschema.MigrationName{Name: name}}}
	pm.scriptAny = func(ctx context.Context, ec ExecConn) (string, error) {
		v, ok := ec.(T)
		if !ok {
			return "", errors.Errorf("script migration %s connection type mismatch", name)
		}
		return generator(ctx, v), nil
	}
	if !isTx {
		//nolint:staticcheck // QF1008: keep explicit embedded field qualifier for clarity
		pm.MigrationBase.SetNonTransactional(true)
	}
	m := libschema.Migration(pm)
	for _, opt := range opts {
		opt(m)
	}
	if isTx == pm.MigrationBase.NonTransactional() {
		panic(errors.Errorf("Generate[%s] migration %s has inconsistent transactional override (generic implies %s)",
			fmt.Sprintf("%T", z), name, ternary(isTx, "transactional", "non-transactional")))
	}
	return m
}

// Computed defines a migration that runs arbitrary Go code.
func Computed[T ExecConn](name string, action func(context.Context, T) error, opts ...libschema.MigrationOption) libschema.Migration {
	var z T
	_, isTx := any(z).(TxLike)
	pm := &mmigration{MigrationBase: libschema.MigrationBase{Name: libschema.MigrationName{Name: name}}}
	pm.computedAny = func(ctx context.Context, ec ExecConn) (bool, error) {
		v, ok := ec.(T)
		if !ok {
			return false, errors.Errorf("computed migration %s connection type mismatch", name)
		}
		return true, action(ctx, v)
	}
	if !isTx {
		//nolint:staticcheck // QF1008: keep explicit embedded field qualifier for clarity
		pm.MigrationBase.SetNonTransactional(true)
	}
	m := libschema.Migration(pm)
	for _, opt := range opts {
		opt(m)
	}
	if isTx == pm.MigrationBase.NonTransactional() {
		panic(errors.Errorf("Computed[%s] migration %s has inconsistent transactional override (generic implies %s)",
			fmt.Sprintf("%T", z), name, ternary(isTx, "transactional", "non-transactional")))
	}
	return m
}

// ternary small helper for concise error formatting parity with Postgres driver.
func ternary[T any](cond bool, a, b T) T {
	if cond {
		return a
	}
	return b
}

// DoOneMigration applies a single migration.
// It is expected to be called by libschema and is not
// called internally which means that is safe to override
// in types that embed MySQL.
func (p *MySQL) DoOneMigration(ctx context.Context, log *internal.Log, d *libschema.Database, m libschema.Migration) (res sql.Result, err error) {
	pm := m.(*mmigration)
	runTransactional := !m.Base().NonTransactional()
	var tx *sql.Tx
	var execConn ExecConn
	if runTransactional {
		if tx, err = d.DB().BeginTx(ctx, d.Options.MigrationTxOptions); err != nil {
			return nil, errors.Wrapf(err, "begin Tx for migration %s", m.Base().Name)
		}
		execConn = tx
		if d.Options.SchemaOverride != "" {
			if !simpleIdentifierRE.MatchString(d.Options.SchemaOverride) {
				return nil, errors.Errorf("options.SchemaOverride must be a simple identifier, not '%s'", d.Options.SchemaOverride)
			}
			if _, err2 := tx.Exec(`USE ` + d.Options.SchemaOverride); err2 != nil {
				_ = tx.Rollback()
				return nil, errors.Wrapf(err2, "set search path to %s for %s", d.Options.SchemaOverride, m.Base().Name)
			}
		}
	} else {
		execConn = d.DB()
	}
	defer func() {
		done := err == nil
		if runTransactional {
			if err == nil {
				if serr := p.saveStatus(log, tx, d, m, true, nil); serr != nil {
					err = serr
					_ = tx.Rollback()
					return
				}
				if cerr := tx.Commit(); cerr != nil {
					err = errors.Wrapf(cerr, "commit migration %s", m.Base().Name)
					return
				}
				m.Base().SetStatus(libschema.MigrationStatus{Done: true})
				return
			}
			_ = tx.Rollback()
		}
		// Non-transactional or failed transactional path: record separately
		stx, txErr := d.DB().BeginTx(ctx, d.Options.MigrationTxOptions)
		if txErr != nil {
			if err == nil {
				err = errors.Wrapf(txErr, "begin status tx for %s", m.Base().Name)
			} else {
				err = errors.Wrapf(err, "(and could not begin status tx: %s)", txErr)
			}
			return
		}
		if serr := p.saveStatus(log, stx, d, m, done, err); serr != nil {
			if err == nil {
				err = serr
			} else {
				err = errors.Wrapf(err, "save status for %s also failed: %s", m.Base().Name, serr)
			}
			_ = stx.Rollback()
			return
		}
		if cerr := stx.Commit(); cerr != nil {
			if err == nil {
				err = errors.Wrapf(cerr, "commit status tx for %s", m.Base().Name)
			} else {
				err = errors.Wrapf(err, "commit status tx for %s also failed: %s", m.Base().Name, cerr)
			}
			return
		}
		if done && err == nil {
			m.Base().SetStatus(libschema.MigrationStatus{Done: true})
		}
	}()

	if pm.scriptAny != nil {
		sqlText, err := pm.scriptAny(ctx, execConn)
		if err != nil {
			return nil, err
		}
		trim := strings.TrimSpace(sqlText)
		if trim == "" {
			return nil, nil
		}
		// Basic validation (reuse existing CheckScript logic) for transactional scripts only; for non-tx path also okay.
		if runTransactional {
			if err = CheckScript(sqlText); err != nil {
				return nil, errors.Wrap(err, sqlText)
			}
		}
		// Non-transactional path: optionally we could split multiple statements; mimic old behavior by executing as-is.
		if !runTransactional {
			// Rough multi-statement guard for future parity (optional). For now allow multiples similar to previous driver behavior.
			_ = sqltoken.TokenizeMySQL(sqlText) // placeholder to show parity readiness
		}
		res, err = execConn.ExecContext(ctx, sqlText)
		if err != nil {
			return nil, errors.Wrap(err, sqlText)
		}
		return res, nil
	}
	if pm.computedAny != nil {
		_, err = pm.computedAny(ctx, execConn)
		return nil, err
	}
	return nil, errors.Errorf("migration %s has neither script nor computed body", m.Base().Name)
}

// CreateSchemaTableIfNotExists creates the migration tracking table for libschema.
// It is expected to be called by libschema and is not
// called internally which means that is safe to override
// in types that embed MySQL.
func (p *MySQL) CreateSchemaTableIfNotExists(ctx context.Context, _ *internal.Log, d *libschema.Database) error {
	schema, tableName, _, err := p.trackingSchemaTable(d)
	if err != nil {
		return err
	}
	if schema != "" {
		_, err := d.DB().ExecContext(ctx, fmt.Sprintf(`
				CREATE SCHEMA IF NOT EXISTS %s
				`, schema))
		if err != nil {
			return errors.Wrapf(err, "could not create libschema schema '%s'", schema)
		}
	}
	_, err = d.DB().ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			library		varchar(255) NOT NULL,
			migration	varchar(255) NOT NULL,
			done		boolean NOT NULL,
			error		text NOT NULL,
			updated_at	timestamp DEFAULT now(),
			PRIMARY KEY	(library, migration)
		) ENGINE = InnoDB`, tableName))
	if err != nil {
		return errors.Wrapf(err, "could not create libschema migrations table '%s'", tableName)
	}
	return nil
}

var simpleIdentifierRE = regexp.MustCompile(`\A[A-Za-z][A-Za-z0-9_]*\z`)

// WithTrackingTableQuoter is a somewhat internal function -- used by lssinglestore.
// It replaces the private function that takes apart the name of the tracking
// table and provides the components.
func WithTrackingTableQuoter(f func(*libschema.Database) (schemaName, tableName, simpleTableName string, err error)) MySQLOpt {
	return func(p *MySQL) {
		p.trackingSchemaTable = f
	}
}

// When MySQL is in ANSI_QUOTES mode, it allows "table_name" quotes but when
// it is not then it does not.  There is no prefect option: in ANSI_QUOTES
// mode, you could have a table called `table` (eg: `CREATE TABLE "table"`) but
// if you're not in ANSI_QUOTES mode then you cannot.  We're going to assume
// that we're not in ANSI_QUOTES mode because we cannot assume that we are.
func trackingSchemaTable(d *libschema.Database) (string, string, string, error) {
	tableName := d.Options.TrackingTable
	s := strings.Split(tableName, ".")
	switch len(s) {
	case 2:
		schema := s[0]
		if !simpleIdentifierRE.MatchString(schema) {
			return "", "", "", errors.Errorf("tracking table schema name must be a simple identifier, not '%s'", schema)
		}
		table := s[1]
		if !simpleIdentifierRE.MatchString(table) {
			return "", "", "", errors.Errorf("tracking table table name must be a simple identifier, not '%s'", table)
		}
		return schema, schema + "." + table, table, nil
	case 1:
		if !simpleIdentifierRE.MatchString(tableName) {
			return "", "", "", errors.Errorf("tracking table table name must be a simple identifier, not '%s'", tableName)
		}
		return "", tableName, tableName, nil
	default:
		return "", "", "", errors.Errorf("tracking table '%s' is not valid", tableName)
	}
}

// trackingTable returns the schema+table reference for the migration tracking table.
// The name is already quoted properly for use as a save mysql identifier.
func (p *MySQL) trackingTable(d *libschema.Database) string {
	_, table, _, _ := p.trackingSchemaTable(d)
	return table
}

func (p *MySQL) saveStatus(log *internal.Log, tx *sql.Tx, d *libschema.Database, m libschema.Migration, done bool, migrationError error) error {
	var estr string
	if migrationError != nil {
		estr = migrationError.Error()
	}
	log.Info("Saving migration status", map[string]interface{}{
		"migration": m.Base().Name,
		"done":      done,
		"error":     migrationError,
	})
	q := fmt.Sprintf(`
		REPLACE INTO %s (db_name, library, migration, done, error, updated_at)
		VALUES (?, ?, ?, ?, ?, now())`, p.trackingTable(d))
	_, err := tx.Exec(q, p.databaseName, m.Base().Name.Library, m.Base().Name.Name, done, estr)
	if err != nil {
		return errors.Wrapf(err, "save status for %s", m.Base().Name)
	}
	return nil
}

// LockMigrationsTable locks the migration tracking table for exclusive use by the
// migrations running now.
//
// It is expected to be called by libschema and is not
// called internally which means that is safe to override
// in types that embed MySQL.
//
// In MySQL, locks are _not_ tied to transactions so closing the transaction
// does not release the lock.  We'll use a transaction just to make sure that
// we're using the same connection.  If LockMigrationsTable succeeds, be sure to
// call UnlockMigrationsTable.
func (p *MySQL) LockMigrationsTable(ctx context.Context, _ *internal.Log, d *libschema.Database) (finalErr error) {
	schema, tableName, simpleTableName, err := p.trackingSchemaTable(d)
	if err != nil {
		return err
	}

	// LockMigrationsTable is overridden for SingleStore
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.lockTx != nil {
		return errors.Errorf("libschema migrations table, '%s' already locked", tableName)
	}
	tx, err := d.DB().BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return errors.Wrap(err, "Could not start transaction: %s")
	}
	p.lockStr = "libschema_" + tableName
	var gotLock int
	err = tx.QueryRow(`SELECT GET_LOCK(?, -1)`, p.lockStr).Scan(&gotLock)
	if err != nil {
		return errors.Wrapf(err, "could not get lock for libschema migrations")
	}
	p.lockTx = tx

	// This moment, after getting an exclusive lock on the migrations table, is
	// the right moment to do any schema upgrades of the migrations table.

	defer func() {
		if finalErr != nil {
			_, _ = tx.Exec(`SELECT RELEASE_LOCK(?)`, p.lockStr)
			_ = tx.Rollback()
			p.lockTx = nil
		}
	}()

	currentDatabaseValue := p.databaseName
	defer func() {
		p.databaseName = currentDatabaseValue
	}()
	p.databaseName = schema

	ok, err := p.DoesColumnExist(simpleTableName, "db_name")
	if err != nil {
		return errors.Wrapf(err, "could not check if %s has a db_name column", tableName)
	}
	if !ok {
		_, err = d.DB().ExecContext(ctx, fmt.Sprintf(`
		ALTER TABLE %s 
			ADD COLUMN db_name varchar(255)`, tableName))
		if err != nil {
			return errors.Wrapf(err, "could not add db_name column to %s", tableName)
		}
	}
	ok, err = p.ColumnIsInPrimaryKey(simpleTableName, "db_name")
	if err != nil {
		return errors.Wrapf(err, "could not check if %s.db_name column is in the primary key", tableName)
	}
	if ok {
		return nil
	}
	_, err = d.DB().ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s
		SET	db_name = ?
		WHERE	db_name IS NULL`, tableName), p.databaseName)
	if err != nil {
		return errors.Wrapf(err, "could not set %s.db_name column", tableName)
	}
	_, err = d.DB().ExecContext(ctx, fmt.Sprintf(`
		ALTER TABLE %s 
			DROP PRIMARY KEY, ADD PRIMARY KEY (db_name, library, migration)`, tableName))
	if err != nil {
		return errors.Wrapf(err, "could change primary key for %s", tableName)
	}
	return nil
}

// UnlockMigrationsTable unlocks the migration tracking table.
//
// It is expected to be called by libschema and is not
// called internally which means that is safe to override
// in types that embed MySQL.
func (p *MySQL) UnlockMigrationsTable(_ *internal.Log) error {
	// UnlockMigrationsTable is overridden for SingleStore
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.lockTx == nil {
		return errors.Errorf("libschema migrations table, not locked")
	}
	defer func() {
		_ = p.lockTx.Rollback()
		p.lockTx = nil
	}()
	_, err := p.lockTx.Exec(`SELECT RELEASE_LOCK(?)`, p.lockStr)
	if err != nil {
		return errors.Wrap(err, "Could not release explicit lock for schema migrations")
	}
	return nil
}

// LoadStatus loads the current status of all migrations from the migration tracking table.
//
// It is expected to be called by libschema and is not
// called internally which means that is safe to override
// in types that embed MySQL.
func (p *MySQL) LoadStatus(ctx context.Context, _ *internal.Log, d *libschema.Database) (_ []libschema.MigrationName, err error) {
	// TODO: DRY
	tableName := p.trackingTable(d)
	rows, err := d.DB().QueryContext(ctx, fmt.Sprintf(`
		SELECT	library, migration, done
		FROM	%s
		WHERE	db_name = ?`, tableName), p.databaseName)
	if err != nil {
		return nil, errors.Wrap(err, "cannot query migration status")
	}
	defer func() {
		e := rows.Close()
		if e != nil && err == nil {
			err = errors.Wrap(e, "close scan on migration table")
		}
	}()
	var unknowns []libschema.MigrationName
	for rows.Next() {
		var (
			name   libschema.MigrationName
			status libschema.MigrationStatus
		)
		err := rows.Scan(&name.Library, &name.Name, &status.Done)
		if err != nil {
			return nil, errors.Wrap(err, "cannot scan migration status")
		}
		if m, ok := d.Lookup(name); ok {
			m.Base().SetStatus(status)
		} else if status.Done {
			unknowns = append(unknowns, name)
		}
	}
	return unknowns, nil
}

// IsMigrationSupported checks to see if a migration is well-formed.  Absent a code change, this
// should always return nil.
//
// It is expected to be called by libschema and is not
// called internally which means that is safe to override
// in types that embed MySQL.
func (p *MySQL) IsMigrationSupported(d *libschema.Database, _ *internal.Log, migration libschema.Migration) error {
	if _, ok := migration.(*mmigration); !ok {
		return fmt.Errorf("non-mysql migration %s registered with mysql migrations", migration.Base().Name)
	}
	// All mmigration instances are supported; body presence checked at execution.
	return nil
}
