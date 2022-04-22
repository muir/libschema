package lsmysql

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/muir/libschema"
	"github.com/pkg/errors"
)

// MySQL is a libschema.Driver for connecting to MySQL-like databases that
// have the following characteristics:
// * CANNOT do DDL commands inside transactions
// * Support UPSERT using INSERT ... ON DUPLICATE KEY UPDATE
// * uses /* -- and # for comments
//
// Because mysql DDL commands cause transactions to autocommit, tracking the schema changes in
// a secondary table (like libschema does) is inherently unsafe.  The MySQL driver will
// record that it is about to attempt a migration and it will record if that attempts succeeds
// or fails, but if the program terminates mid-transaction, it is beyond the scope of libschema
// to determine if the transaction succeeded or failed.  Such transactions will be retried.
// For this reason, it is reccomend that DDL commands be written such that they are idempotent.
type MySQL struct {
	lockTx       *sql.Tx
	lockStr      string
	db           *sql.DB
	databaseName string // used in skip.go only
}

// New creates a libschema.Database with a mysql driver built in.
func New(log libschema.MyLogger, name string, schema *libschema.Schema, db *sql.DB) (*libschema.Database, *MySQL, error) {
	m := &MySQL{db: db}
	d, err := schema.NewDatabase(log, name, db, m)
	return d, m, err
}

type mmigration struct {
	libschema.MigrationBase
	script   func(context.Context, libschema.MyLogger, *sql.Tx) string
	computed func(context.Context, libschema.MyLogger, *sql.Tx) error
}

func (m *mmigration) Copy() libschema.Migration {
	return &mmigration{
		MigrationBase: m.MigrationBase.Copy(),
		script:        m.script,
		computed:      m.computed,
	}
}

func (m *mmigration) Base() *libschema.MigrationBase {
	return &m.MigrationBase
}

// Script creates a libschema.Migration from a SQL string
func Script(name string, sqlText string, opts ...libschema.MigrationOption) libschema.Migration {
	return Generate(name, func(_ context.Context, _ libschema.MyLogger, _ *sql.Tx) string {
		return sqlText
	}, opts...)
}

// Generate creates a libschema.Migration from a function that returns a SQL string
func Generate(
	name string,
	generator func(context.Context, libschema.MyLogger, *sql.Tx) string,
	opts ...libschema.MigrationOption) libschema.Migration {
	return mmigration{
		MigrationBase: libschema.MigrationBase{
			Name: libschema.MigrationName{
				Name: name,
			},
		},
		script: generator,
	}.applyOpts(opts)
}

// Computed creates a libschema.Migration from a Go function to run
// the migration directly.
func Computed(
	name string,
	action func(context.Context, libschema.MyLogger, *sql.Tx) error,
	opts ...libschema.MigrationOption) libschema.Migration {
	return mmigration{
		MigrationBase: libschema.MigrationBase{
			Name: libschema.MigrationName{
				Name: name,
			},
		},
		computed: action,
	}.applyOpts(opts)
}

func (m mmigration) applyOpts(opts []libschema.MigrationOption) libschema.Migration {
	lsm := libschema.Migration(&m)
	for _, opt := range opts {
		opt(lsm)
	}
	return lsm
}

// DoOneMigration applies a single migration.
// It is expected to be called by libschema.
func (p *MySQL) DoOneMigration(ctx context.Context, log libschema.MyLogger, d *libschema.Database, m libschema.Migration) (err error) {
	// TODO: DRY
	defer func() {
		if err == nil {
			m.Base().SetStatus(libschema.MigrationStatus{
				Done: true,
			})
		}
	}()
	tx, err := d.DB().BeginTx(ctx, d.Options.MigrationTxOptions)
	if err != nil {
		return errors.Wrapf(err, "Begin Tx for migration %s", m.Base().Name)
	}
	if d.Options.SchemaOverride != "" {
		if !simpleIdentifierRE.MatchString(d.Options.SchemaOverride) {
			return errors.Errorf("Options.SchemaOverride must be a simple identifier, not '%s'", d.Options.SchemaOverride)
		}
		_, err := tx.Exec(`USE ` + d.Options.SchemaOverride)
		if err != nil {
			return errors.Wrapf(err, "Set search path to %s for %s", d.Options.SchemaOverride, m.Base().Name)
		}
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		} else {
			err = errors.Wrapf(tx.Commit(), "Commit migration %s", m.Base().Name)
		}
	}()
	pm := m.(*mmigration)
	if pm.script != nil {
		script := pm.script(ctx, log, tx)
		switch CheckScript(script) {
		case Safe:
		case DataAndDDL:
			err = errors.New("Migration combines DDL (Data Definition Language [schema changes]) and data manipulation")
		case NonIdempotentDDL:
			if !m.Base().HasSkipIf() {
				err = errors.New("Unconditional migration has non-idempotent DDL (Data Definition Language [schema changes])")
			}
		}
		if err == nil {
			_, err = tx.Exec(script)
		}
		err = errors.Wrap(err, script)
	} else {
		err = pm.computed(ctx, log, tx)
	}
	if err != nil {
		err = errors.Wrapf(err, "Problem with migration %s", m.Base().Name)
		_ = tx.Rollback()
		ntx, txerr := d.DB().BeginTx(ctx, d.Options.MigrationTxOptions)
		if txerr != nil {
			return errors.Wrapf(err, "Tx for saving status for %s also failed with %s", m.Base().Name, txerr)
		}
		tx = ntx
	}
	txerr := p.saveStatus(log, tx, d, m, err == nil, err)
	if txerr != nil {
		if err == nil {
			err = txerr
		} else {
			err = errors.Wrapf(err, "Save status for %s also failed: %s", m.Base().Name, txerr)
		}
	}
	return
}

// CreateSchemaTableIfNotExists creates the migration tracking table for libschema.
// It is expected to be called by libschema.
func (p *MySQL) CreateSchemaTableIfNotExists(ctx context.Context, _ libschema.MyLogger, d *libschema.Database) error {
	schema, tableName, err := trackingSchemaTable(d)
	if err != nil {
		return err
	}
	if schema != "" {
		_, err := d.DB().ExecContext(ctx, fmt.Sprintf(`
				CREATE SCHEMA IF NOT EXISTS %s
				`, schema))
		if err != nil {
			return errors.Wrapf(err, "Could not create libschema schema '%s'", schema)
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
		return errors.Wrapf(err, "Could not create libschema migrations table '%s'", tableName)
	}
	return nil
}

var simpleIdentifierRE = regexp.MustCompile(`\A[A-Za-z][A-Za-z0-9_]*\z`)

// When MySQL is in ANSI_QUOTES mode, it allows "table_name" quotes but when
// it is not then it does not.  There is no prefect option: in ANSI_QUOTES
// mode, you could have a table called `table` (eg: `CREATE TABLE "table"`) but
// if you're not in ANSI_QUOTES mode then you cannot.  We're going to assume
// that we're not in ANSI_QUOTES mode because we cannot assume that we are.
func trackingSchemaTable(d *libschema.Database) (string, string, error) {
	tableName := d.Options.TrackingTable
	s := strings.Split(tableName, ".")
	switch len(s) {
	case 2:
		schema := s[0]
		if !simpleIdentifierRE.MatchString(schema) {
			return "", "", errors.Errorf("Tracking table schema name must be a simple identifier, not '%s'", schema)
		}
		table := s[1]
		if !simpleIdentifierRE.MatchString(table) {
			return "", "", errors.Errorf("Tracking table table name must be a simple identifier, not '%s'", table)
		}
		return schema, schema + "." + table, nil
	case 1:
		if !simpleIdentifierRE.MatchString(tableName) {
			return "", "", errors.Errorf("Tracking table table name must be a simple identifier, not '%s'", tableName)
		}
		return "", tableName, nil
	default:
		return "", "", errors.Errorf("Tracking table '%s' is not valid", tableName)
	}
}

// trackingTable returns the schema+table reference for the migration tracking table.
// The name is already quoted properly for use as a save postgres identifier.
// TODO: DRY
func trackingTable(d *libschema.Database) string {
	_, table, _ := trackingSchemaTable(d)
	return table
}

func (p *MySQL) saveStatus(log libschema.MyLogger, tx *sql.Tx, d *libschema.Database, m libschema.Migration, done bool, migrationError error) error {
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
		REPLACE INTO %s (library, migration, done, error, updated_at)
		VALUES (?, ?, ?, ?, now())`, trackingTable(d))
	_, err := tx.Exec(q, m.Base().Name.Library, m.Base().Name.Name, done, estr)
	if err != nil {
		return errors.Wrapf(err, "Save status for %s", m.Base().Name)
	}
	return nil
}

// LockMigrationsTable locks the migration tracking table for exclusive use by the
// migrations running now.
// It is expected to be called by libschema.
// In MySQL, locks are _not_ tied to transactions so closing the transaction
// does not release the lock.  We'll use a transaction just to make sure that
// we're using the same connection.  If LockMigrationsTable succeeds, be sure to
// call UnlockMigrationsTable.
func (p *MySQL) LockMigrationsTable(ctx context.Context, _ libschema.MyLogger, d *libschema.Database) error {
	_, tableName, err := trackingSchemaTable(d)
	if err != nil {
		return err
	}
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
		return errors.Wrapf(err, "Could not get lock for libschema migrations")
	}
	p.lockTx = tx
	return nil
}

// UnlockMigrationsTable unlocks the migration tracking table.
// It is expected to be called by libschema.
func (p *MySQL) UnlockMigrationsTable(_ libschema.MyLogger) error {
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
// It is expected to be called by libschema.
func (p *MySQL) LoadStatus(ctx context.Context, _ libschema.MyLogger, d *libschema.Database) ([]libschema.MigrationName, error) {
	// TODO: DRY
	tableName := trackingTable(d)
	rows, err := d.DB().QueryContext(ctx, fmt.Sprintf(`
		SELECT	library, migration, done
		FROM	%s`, tableName))
	if err != nil {
		return nil, errors.Wrap(err, "Cannot query migration status")
	}
	defer rows.Close()
	var unknowns []libschema.MigrationName
	for rows.Next() {
		var (
			name   libschema.MigrationName
			status libschema.MigrationStatus
		)
		err := rows.Scan(&name.Library, &name.Name, &status.Done)
		if err != nil {
			return nil, errors.Wrap(err, "Cannot scan migration status")
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
// It is expected to be called by libschema.
func (p *MySQL) IsMigrationSupported(d *libschema.Database, _ libschema.MyLogger, migration libschema.Migration) error {
	m, ok := migration.(*mmigration)
	if !ok {
		return fmt.Errorf("Non-postgres migration %s registered with postgres migrations", migration.Base().Name)
	}
	if m.script != nil {
		return nil
	}
	if m.computed != nil {
		return nil
	}
	return errors.Errorf("Migration %s is not supported", m.Name)
}
