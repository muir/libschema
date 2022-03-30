package lsmysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/lib/pq" // used for QuoteIdentifier
	"github.com/muir/libschema"
	"github.com/pkg/errors"
)

// Mysql is a libschema.Driver for connecting to Mysql-like databases that
// have the following characteristics:
// * CANNOT do DDL commands inside transactions
// * Support UPSERT using INSERT ... ON DUPLICATE KEY UPDATE
// * uses /* -- and # for comments
//
// Because mysql DDL commands cause transactions to autocommit, tracking the schema changes in
// a secondary table (like libschema does) is inherently unsafe.  The Mysql driver will
// record that it is about to attempt a migration and it will record if that attempts succeeds
// or fails, but if the program terminates mid-transaction, it is beyond the scope of libschema
// to determine if the transaction succeeded or failed.  Such transactions will be retried.
// For this reason, it is reccomend that DDL commands be written such that they are idempotent.
type Mysql struct {
	lockTx *sql.Tx
}

// New creates a libschema.Database with a mysql driver built in.
func New(log libschema.MyLogger, name string, schema *libschema.Schema, db *sql.DB) (*libschema.Database, error) {
	return schema.NewDatabase(log, name, db, &Mysql{})
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

// Generate creates a libschema.Migration from a function that returns
// SQL string
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

// Computed creates a libschema.Migration from a Go function to run to do
// the mmigration directly.
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
// TODO: DRY
func (p *Mysql) DoOneMigration(ctx context.Context, log libschema.MyLogger, d *libschema.Database, m libschema.Migration) (err error) {
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
		_, err := tx.Exec(`USE ` + pq.QuoteIdentifier(d.Options.SchemaOverride))
		if err != nil {
			return errors.Wrapf(err, "Set search path to %s for %s", d.Options.SchemaOverride, m.Base().Name)
		}
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			err = errors.Wrapf(tx.Commit(), "Commit migration %s", m.Base().Name)
		}
		return
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
		tx.Rollback()
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
func (p *Mysql) CreateSchemaTableIfNotExists(ctx context.Context, _ libschema.MyLogger, d *libschema.Database) error {
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
			metadata	varchar(255) NOT NULL DEFAULT '',
			library		varchar(255) NOT NULL,
			migration	varchar(255) NOT NULL,
			done		boolean NOT NULL,
			error		text NOT NULL,
			updated_at	timestamp with time zone DEFAULT now(),
			PRIMARY KEY	(metadata, library, migration)
		)`, tableName))
	if err != nil {
		return errors.Wrapf(err, "Could not create libschema migrations table '%s'", tableName)
	}
	return nil
}

// TODO: DRY
func trackingSchemaTable(d *libschema.Database) (string, string, error) {
	tableName := d.Options.TrackingTable
	s := strings.Split(tableName, ".")
	switch len(s) {
	case 2:
		schema := pq.QuoteIdentifier(s[0])
		table := pq.QuoteIdentifier(s[1])
		return schema, schema + "." + table, nil
	case 1:
		return "", pq.QuoteIdentifier(tableName), nil
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

func (p *Mysql) saveStatus(log libschema.MyLogger, tx *sql.Tx, d *libschema.Database, m libschema.Migration, done bool, migrationError error) error {
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
		INSERT INTO %s (library, migration, done, error, updated_at)
		VALUES ($1, $2, $3, $4, now())
		ON DUPLICATE KEY UPDATE 
			done = new.done,
			error = new.error,
			updated_at = new.updated_at
			`, trackingTable(d))
	_, err := tx.Exec(q, m.Base().Name.Library, m.Base().Name.Name, done, estr)
	if err != nil {
		return errors.Wrapf(err, "Save status for %s", m.Base().Name)
	}
	return nil
}

// LockMigrationsTable locks the migration tracking table for exclusive use by the
// migrations running now.
// It is expected to be called by libschema.
// TODO: DRY
func (p *Mysql) LockMigrationsTable(ctx context.Context, _ libschema.MyLogger, d *libschema.Database) error {
	tableName := trackingTable(d)
	if p.lockTx != nil {
		return errors.Errorf("libschema migrations table, '%s' already locked", tableName)
	}
	_, err := d.DB().ExecContext(ctx, fmt.Sprintf(`
		INSERT IGNORE INTO %s (metadata, library, migration, done, error)
		VALUES ('lock', '', '', true, '')`,
		tableName))
	if err != nil {
		return errors.Wrapf(err, "Could not add lock row to %s", tableName)
	}
	tx, err := d.DB().BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return errors.Wrap(err, "Could not start transaction: %s")
	}
	var junk string
	err = tx.QueryRow(fmt.Sprintf(`
		SELECT	metadata
		FROM	%s
		WHERE	metadata = 'lock'
		FOR UPDATE`, tableName)).Scan(&junk)
	if err != nil {
		return errors.Wrapf(err, "Could not lock libschema migrations table '%s'", tableName)
	}
	p.lockTx = tx
	return nil
}

// UnlockMigrationsTable unlocks the migration tracking table.
// It is expected to be called by libschema.
// TODO: DRY
func (p *Mysql) UnlockMigrationsTable(_ libschema.MyLogger) error {
	if p.lockTx == nil {
		return errors.Errorf("libschema migrations table, not locked")
	}
	_ = p.lockTx.Rollback()
	p.lockTx = nil
	return nil
}

// LoadStatus loads the current status of all migrations from the migration tracking table.
// It is expected to be called by libschema.
// TODO: DRY
func (p *Mysql) LoadStatus(ctx context.Context, _ libschema.MyLogger, d *libschema.Database) ([]libschema.MigrationName, error) {
	tableName := trackingTable(d)
	rows, err := d.DB().QueryContext(ctx, fmt.Sprintf(`
		SELECT	library, migration, done
		FROM	%s
		WHERE	metadata = ''`, tableName))
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
func (p *Mysql) IsMigrationSupported(d *libschema.Database, _ libschema.MyLogger, migration libschema.Migration) error {
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
