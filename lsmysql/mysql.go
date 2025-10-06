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
	"github.com/muir/libschema/internal/migfinalize"
	"github.com/muir/libschema/internal/stmtclass"
)

// retained regexp use for identifier validation

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
// ExecConn & TxLike kept for external parity (exported via libschema); internal
// generic constraint uses ConnPtr only for *sql.Tx | *sql.DB to avoid broad assertions.
type ExecConn interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}
type TxLike interface {
	Commit() error
	Rollback() error
}

// ConnPtr restricts generic migrations to concrete *sql.Tx or *sql.DB.
type ConnPtr interface{ *sql.Tx | *sql.DB }

type mmigration struct {
	libschema.MigrationBase
	scriptTx    func(context.Context, *sql.Tx) (string, error)
	scriptDB    func(context.Context, *sql.DB) (string, error)
	scriptSQL   string // original literal SQL when defined via Script(); used for pre-analysis
	computedTx  func(context.Context, *sql.Tx) error
	computedDB  func(context.Context, *sql.DB) error
	creationErr error
	// classification-derived flags (not persisted outside execution)
	hasNonIdempotentDDL       bool
	unguardedNonIdempotentDDL bool // true only for Script()-origin, missing SkipIf
}

func (m *mmigration) Copy() libschema.Migration {
	n := *m
	n.MigrationBase = m.MigrationBase.Copy()
	return &n
}

func (m *mmigration) Base() *libschema.MigrationBase { return &m.MigrationBase }

// Script registers a literal SQL migration. Always transactional by default (uses *sql.Tx)
// unless overridden with ForceNonTransactional().
func Script(name string, sqlText string, opts ...libschema.MigrationOption) libschema.Migration {
	pm := &mmigration{MigrationBase: libschema.MigrationBase{Name: libschema.MigrationName{Name: name}}, scriptSQL: sqlText}
	// Always transactional unless overridden explicitly. Keep original SQL for pre-downgrade detection.
	pm.scriptTx = func(_ context.Context, _ *sql.Tx) (string, error) { return sqlText, nil }
	m := libschema.Migration(pm)
	for _, opt := range opts {
		opt(m)
	}
	// After options, if NonTransactional true switch to DB variant
	if m.Base().NonTransactional() {
		pm.scriptDB = func(_ context.Context, _ *sql.DB) (string, error) { return sqlText, nil }
		pm.scriptTx = nil
	}
	return m
}

// Generate defines a migration returning a SQL string. If T implements TxLike it's transactional else non-transactional.
func Generate[T ConnPtr](name string, generator func(context.Context, T) string, opts ...libschema.MigrationOption) libschema.Migration {
	pm := &mmigration{MigrationBase: libschema.MigrationBase{Name: libschema.MigrationName{Name: name}}}
	var zero T
	var mustBeNonTransactional bool
	switch any(zero).(type) {
	case *sql.Tx:
		pm.scriptTx = func(ctx context.Context, tx *sql.Tx) (string, error) { return generator(ctx, any(tx).(T)), nil }
	case *sql.DB:
		mustBeNonTransactional = true
		pm.SetNonTransactional(true)
		pm.scriptDB = func(ctx context.Context, db *sql.DB) (string, error) { return generator(ctx, any(db).(T)), nil }
	}
	m := libschema.Migration(pm)
	for _, opt := range opts {
		opt(m)
	}
	expectedTx := !mustBeNonTransactional
	finalTx := !pm.NonTransactional()
	if expectedTx != finalTx && pm.creationErr == nil {
		pm.creationErr = errors.Errorf("Generate[%T] %s transactional override mismatch (expected %s)", zero, name, ternary(expectedTx, "transactional", "non-transactional"))
	}
	return m
}

// Computed defines a migration that runs arbitrary Go code.
func Computed[T ConnPtr](name string, action func(context.Context, T) error, opts ...libschema.MigrationOption) libschema.Migration {
	var zero T
	pm := &mmigration{MigrationBase: libschema.MigrationBase{Name: libschema.MigrationName{Name: name}}}
	expectedTx := true
	switch any(zero).(type) {
	case *sql.Tx:
		pm.computedTx = func(ctx context.Context, tx *sql.Tx) error { return action(ctx, any(tx).(T)) }
	case *sql.DB:
		expectedTx = false
		pm.computedDB = func(ctx context.Context, db *sql.DB) error { return action(ctx, any(db).(T)) }
		pm.SetNonTransactional(true)
	}
	m := libschema.Migration(pm)
	for _, opt := range opts {
		opt(m)
	}
	finalTx := !m.Base().NonTransactional()
	if expectedTx != finalTx && pm.creationErr == nil {
		pm.creationErr = errors.Errorf("Computed[%T] %s transactional override mismatch (expected %s)", zero, name, ternary(expectedTx, "transactional", "non-transactional"))
	}
	if m.Base().NonTransactional() && pm.computedDB == nil && pm.creationErr == nil {
		pm.computedDB = func(ctx context.Context, db *sql.DB) error { return action(ctx, any(db).(T)) }
		pm.computedTx = nil
	} else if !m.Base().NonTransactional() && pm.computedTx == nil && pm.creationErr == nil {
		pm.computedTx = func(ctx context.Context, tx *sql.Tx) error { return action(ctx, any(tx).(T)) }
		pm.computedDB = nil
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
func (p *MySQL) DoOneMigration(ctx context.Context, log *internal.Log, d *libschema.Database, m libschema.Migration) (sql.Result, error) {
	pm := m.(*mmigration)
	if pm.creationErr != nil {
		return nil, pm.creationErr
	}
	if log != nil && d.Options.DebugLogging {
		log.Debug("migration start", map[string]interface{}{"migration": m.Base().Name, "nonTx": m.Base().NonTransactional(), "hasScriptSQL": pm.scriptSQL != "", "hasScriptTx": pm.scriptTx != nil})
	}
	var res sql.Result

	// Pre-execution downgrade: If defined via Script() (literal SQL captured) and marked transactional
	// but contains DDL, automatically force non-transactional execution so that DDL executes safely
	// outside a transaction (MySQL autocommit semantics). Only perform when the user did not already
	// mark migration as NonTransactional().
	if pm.scriptSQL != "" && !pm.NonTransactional() && pm.scriptTx != nil {
		if toks := sqltoken.TokenizeMySQL(pm.scriptSQL); true {
			stmts, agg := stmtclass.ClassifyTokens(stmtclass.DialectMySQL, toks)
			if log != nil && d.Options.DebugLogging {
				flagNames := func(f uint32) []string {
					var n []string
					if f&stmtclass.IsDDL != 0 {
						n = append(n, "DDL")
					}
					if f&stmtclass.IsDML != 0 {
						n = append(n, "DML")
					}
					if f&stmtclass.IsNonIdempotent != 0 {
						n = append(n, "NonIdem")
					}
					if f&stmtclass.IsMultipleStatements != 0 {
						n = append(n, "Multi")
					}
					return n
				}
				for i, s := range stmts {
					log.Debug("classification stmt", map[string]interface{}{"phase": "pre-downgrade", "index": i, "text": s.Text, "flags": flagNames(s.Flags)})
				}
				log.Debug("classification aggregate", map[string]interface{}{"phase": "pre-downgrade", "flags": flagNames(agg), "sql": pm.scriptSQL})
			}
			if agg&stmtclass.IsDDL != 0 { // downgrade for any DDL presence
				pm.SetNonTransactional(true)
				pm.scriptDB = func(_ context.Context, _ *sql.DB) (string, error) { return pm.scriptSQL, nil }
				pm.scriptTx = nil
				// Determine mixture & non-idempotency with correct precedence
				var seenDDL, seenDML bool
				var firstNonIdem string
				for _, s := range stmts {
					if s.Flags&stmtclass.IsDDL != 0 {
						seenDDL = true
					}
					if s.Flags&stmtclass.IsDML != 0 {
						seenDML = true
					}
					if firstNonIdem == "" && (s.Flags&(stmtclass.IsDDL|stmtclass.IsNonIdempotent) == (stmtclass.IsDDL | stmtclass.IsNonIdempotent)) {
						firstNonIdem = s.Text
					}
				}
				if seenDDL && seenDML {
					pm.creationErr = errors.Errorf("mixed DDL and DML: %w", libschema.ErrDataAndDDL)
				} else {
					if firstNonIdem != "" {
						pm.hasNonIdempotentDDL = true
						if pm.scriptSQL != "" && !pm.Base().HasSkipIf() { // Script without external guard
							pm.unguardedNonIdempotentDDL = true
						}
					}
					// Additional rule: non-idempotent DDL that is easily fixable (CREATE TABLE lacking IF NOT EXISTS) remains an error
					if pm.creationErr == nil { // only if no prior mixed error
						for _, s := range stmts {
							if s.Flags&(stmtclass.IsEasilyIdempotentFix|stmtclass.IsNonIdempotent) == (stmtclass.IsEasilyIdempotentFix | stmtclass.IsNonIdempotent) {
								pm.creationErr = errors.Errorf("non-idempotent DDL '%s': %w", s.Text, libschema.ErrNonIdempotentDDL)
								break
							}
						}
					}
				}
			}
		}
	}
	// Honor any creationErr discovered during pre-classification
	if pm.creationErr != nil {
		if log != nil && d.Options.DebugLogging {
			log.Debug("creationErr after pre-classification", map[string]interface{}{"error": pm.creationErr.Error(), "migration": m.Base().Name})
		}
		return nil, pm.creationErr
	}

	f := &migfinalize.Finalizer[sql.DB, sql.Tx]{
		Ctx:              ctx,
		DB:               d.DB(),
		RunTransactional: !m.Base().NonTransactional(),
		Log:              log,
		BeginTx: func(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
			tx, err := db.BeginTx(ctx, d.Options.MigrationTxOptions)
			if d.Options.SchemaOverride != "" {
				if !simpleIdentifierRE.MatchString(d.Options.SchemaOverride) {
					_ = tx.Rollback()
					return nil, errors.Errorf("options.SchemaOverride must be a simple identifier, not '%s'", d.Options.SchemaOverride)
				}
				if _, err2 := tx.Exec(`USE ` + d.Options.SchemaOverride); err2 != nil {
					_ = tx.Rollback()
					return nil, errors.Wrapf(err2, "set search path to %s for %s", d.Options.SchemaOverride, m.Base().Name)
				}
			}
			if err != nil {
				return nil, errors.Wrapf(err, "begin Tx for migration %s", m.Base().Name)
			}
			return tx, nil
		},
		BodyTx: func(ctx context.Context, tx *sql.Tx) error {
			if pm.scriptTx != nil || pm.scriptDB != nil {
				if pm.scriptTx == nil {
					return errors.Errorf("migration %s transactional mode mismatch", m.Base().Name)
				}
				sqlText, err := pm.scriptTx(ctx, tx)
				if err != nil {
					return errors.WithStack(err)
				}
				if strings.TrimSpace(sqlText) == "" {
					return nil
				}
				execRes, execErr := tx.ExecContext(ctx, sqlText)
				if execErr != nil {
					return errors.Wrap(execErr, sqlText)
				}
				res = execRes
				return nil
			}
			if pm.computedTx != nil || pm.computedDB != nil {
				if pm.computedTx == nil {
					return errors.Errorf("computed migration %s transactional mode mismatch", m.Base().Name)
				}
				return errors.WithStack(pm.computedTx(ctx, tx))
			}
			return errors.Errorf("migration %s has neither script nor computed body", m.Base().Name)
		},
		BodyNonTx: func(ctx context.Context, db *sql.DB) error {
			if pm.scriptTx != nil || pm.scriptDB != nil {
				if pm.scriptDB == nil {
					return errors.Errorf("migration %s transactional mode mismatch", m.Base().Name)
				}
				// Ensure schema override is applied for non-transactional execution paths (including downgraded ones).
				if d.Options.SchemaOverride != "" {
					if !simpleIdentifierRE.MatchString(d.Options.SchemaOverride) {
						return errors.Errorf("options.SchemaOverride must be a simple identifier, not '%s'", d.Options.SchemaOverride)
					}
					if _, err := db.ExecContext(ctx, `USE `+d.Options.SchemaOverride); err != nil {
						return errors.Wrapf(err, "set schema/database to %s for %s", d.Options.SchemaOverride, m.Base().Name)
					}
				}
				sqlText, err := pm.scriptDB(ctx, db)
				if err != nil {
					return errors.WithStack(err)
				}
				if strings.TrimSpace(sqlText) == "" {
					return nil
				}
				stmts, agg := stmtclass.ClassifyTokens(stmtclass.DialectMySQL, sqltoken.TokenizeMySQL(sqlText))
				if log != nil && d.Options.DebugLogging {
					flagNames := func(f uint32) []string {
						var n []string
						if f&stmtclass.IsDDL != 0 {
							n = append(n, "DDL")
						}
						if f&stmtclass.IsDML != 0 {
							n = append(n, "DML")
						}
						if f&stmtclass.IsNonIdempotent != 0 {
							n = append(n, "NonIdem")
						}
						if f&stmtclass.IsMultipleStatements != 0 {
							n = append(n, "Multi")
						}
						return n
					}
					for i, s := range stmts {
						log.Debug("classification stmt", map[string]interface{}{"phase": "body-non-tx", "index": i, "text": s.Text, "flags": flagNames(s.Flags)})
					}
					log.Debug("classification aggregate", map[string]interface{}{"phase": "body-non-tx", "flags": flagNames(agg), "sql": sqlText})
				}
				// Mixed DDL + DML disallowed for MySQL (both transactional & downgraded non-tx paths)
				if (agg&stmtclass.IsDDL != 0) && (agg&stmtclass.IsDML != 0) {
					return errors.Errorf("mixed DDL and DML: %w", libschema.ErrDataAndDDL)
				}
				// Non-idempotent DDL: allow if not a CREATE TABLE missing IF NOT EXISTS. If unguarded Script, warn once.
				if pm.hasNonIdempotentDDL && pm.unguardedNonIdempotentDDL && log != nil && d.Options.DebugLogging {
					log.Warn("unguarded non-idempotent DDL; consider adding SkipIf or making idempotent", map[string]interface{}{"migration": m.Base().Name})
				}
				// Enforce easy-fix non-idempotent statements at execution time if somehow not caught earlier
				for _, s := range stmts {
					if s.Flags&(stmtclass.IsEasilyIdempotentFix|stmtclass.IsNonIdempotent) == (stmtclass.IsEasilyIdempotentFix | stmtclass.IsNonIdempotent) {
						return errors.Errorf("non-idempotent DDL '%s': %w", s.Text, libschema.ErrNonIdempotentDDL)
					}
				}
				execRes, execErr := db.ExecContext(ctx, sqlText)
				if execErr != nil {
					return errors.Wrap(execErr, sqlText)
				}
				res = execRes
				return nil
			}
			if pm.computedTx != nil || pm.computedDB != nil {
				if pm.computedDB == nil {
					return errors.Errorf("computed migration %s transactional mode mismatch", m.Base().Name)
				}
				return errors.WithStack(pm.computedDB(ctx, db))
			}
			return errors.Errorf("migration %s has neither script nor computed body", m.Base().Name)
		},
		SaveStatusInTx: func(ctx context.Context, tx *sql.Tx) error {
			if log != nil && d.Options.DebugLogging {
				log.Debug("save status (in-tx)", map[string]interface{}{"migration": m.Base().Name})
			}
			return errors.WithStack(p.saveStatus(log, tx, d, m, true, nil))
		},
		CommitTx:   func(tx *sql.Tx) error { return errors.WithStack(tx.Commit()) },
		RollbackTx: func(tx *sql.Tx) { _ = tx.Rollback() },
		BeginStatusTx: func(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
			stx, err := db.BeginTx(ctx, d.Options.MigrationTxOptions)
			if err != nil {
				return nil, errors.Wrapf(err, "begin status tx for %s", m.Base().Name)
			}
			return stx, nil
		},
		SaveStatusSeparate: func(ctx context.Context, stx *sql.Tx, migErr error) error {
			if log != nil && d.Options.DebugLogging {
				log.Debug("save status (separate)", map[string]interface{}{"migration": m.Base().Name, "migErr": func() string {
					if migErr != nil {
						return migErr.Error()
					}
					return ""
				}()})
			}
			return errors.WithStack(p.saveStatus(log, stx, d, m, migErr == nil, migErr))
		},
		CommitStatusTx:   func(stx *sql.Tx) error { return errors.WithStack(stx.Commit()) },
		RollbackStatusTx: func(stx *sql.Tx) { _ = stx.Rollback() },
		SetDone:          func() { m.Base().SetStatus(libschema.MigrationStatus{Done: true}) },
		SetError:         func(err error) { m.Base().SetStatus(libschema.MigrationStatus{Error: err.Error()}) },
	}

	return res, errors.WithStack(f.Run())
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
		return errors.Errorf("non-mysql migration %s registered with mysql migrations", migration.Base().Name)
	}
	// All mmigration instances are supported; body presence checked at execution.
	return nil
}
