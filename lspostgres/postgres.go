// Package lspostgres has a libschema.Driver support PostgreSQL
package lspostgres

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq"
	"github.com/memsql/errors"
	"github.com/muir/sqltoken"

	"github.com/muir/libschema"
	"github.com/muir/libschema/internal"
	"github.com/muir/libschema/internal/migfinalize"
	"github.com/muir/libschema/internal/stmtclass"
)

// Postgres is a libschema.Driver for connecting to Postgres-like databases that
// have the following characteristics:
// * Can do DDL commands inside transactions
// * Support UPSERT using INSERT ... ON CONFLICT
type Postgres struct {
	lockTx      *sql.Tx
	log         *internal.Log
	serverMajor int
	serverMinor int
	serverOnce  sync.Once
}

// New creates a libschema.Database with a postgres driver built in.  The dbName
// parameter is used internally by libschema, but does not affect where migrations
// are actually applied.
func New(log *internal.Log, dbName string, schema *libschema.Schema, db *sql.DB) (*libschema.Database, error) {
	return schema.NewDatabase(log, dbName, db, &Postgres{log: log})
}

type ConnPtr interface{ *sql.Tx | *sql.DB }

type pmigration struct {
	libschema.MigrationBase
	scriptSQL string
	// genFn always uses the (string, error) internal form.
	genFn      func(context.Context, *sql.Tx) (string, error)
	computedTx func(context.Context, *sql.Tx) error
	computedDB func(context.Context, *sql.DB) error
}

// applySchemaOverridePostgres sets search_path in a transaction if override provided.
func applySchemaOverridePostgres(ctx context.Context, tx *sql.Tx, override, migName string) error {
	if override == "" {
		return nil
	}
	if _, err := tx.Exec(`SET search_path TO ` + pq.QuoteIdentifier(override)); err != nil {
		return errors.Wrapf(err, "set search path to %s for %s", override, migName)
	}
	return nil
}

func (m *pmigration) Copy() libschema.Migration {
	n := *m
	n.MigrationBase = m.MigrationBase.Copy()
	return &n
}

func (m *pmigration) Base() *libschema.MigrationBase {
	return &m.MigrationBase
}

// Script defines a literal SQL statement migration.
// To run the migration, libschema automatically chooses transactional (*sql.Tx)
// or non-transactional (*sql.DB)
// execution based on Postgres rules for statements that cannot run inside a
// transaction (e.g. CREATE INDEX CONCURRENTLY, VACUUM FULL, CREATE DATABASE).
// The choice of transactional vs non-transactional Can be overridden with
// ForceNonTransactional() or ForceTransactional() options.
func Script(name string, sqlText string, opts ...libschema.MigrationOption) libschema.Migration {
	pm := &pmigration{MigrationBase: libschema.MigrationBase{Name: libschema.MigrationName{Name: name}}, scriptSQL: sqlText}
	m := libschema.Migration(pm)
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Generate registers a callback that returns a migration in a string.
// To run the migration, libschema automatically chooses transactional (*sql.Tx)
// or non-transactional (*sql.DB)
// execution based on Postgres rules for statements that cannot run inside a
// transaction (e.g. CREATE INDEX CONCURRENTLY, VACUUM FULL, CREATE DATABASE).
// If the migration will be run transactionally, it will run in the same transaction
// as the callback that returned the string. If it runs non-transactionally, the
// transaction that returned the string will be idle (hanging around) while the migration runs.
// The choice of transactional vs non-transactional Can be overridden with
// ForceNonTransactional() or ForceTransactional() options.
func Generate(name string, generator func(context.Context, *sql.Tx) string, opts ...libschema.MigrationOption) libschema.Migration {
	pm := &pmigration{MigrationBase: libschema.MigrationBase{Name: libschema.MigrationName{Name: name}}, genFn: func(ctx context.Context, tx *sql.Tx) (string, error) { return generator(ctx, tx), nil }}
	m := libschema.Migration(pm)
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Computed defines a migration that runs arbitrary Go code.
// The signature of the action callback determines if the migration runs
// transactionally or if it runs outside a transaction:
//
//	func(context.Context, *sql.Tx) error // run transactionlly
//	func(context.Context, *sql.DB) error // run non-transactionally
func Computed[T ConnPtr](name string, action func(context.Context, T) error, opts ...libschema.MigrationOption) libschema.Migration {
	pm := &pmigration{MigrationBase: libschema.MigrationBase{Name: libschema.MigrationName{Name: name}}}
	// Detect which concrete generic type was requested by instantiating a zero value and
	// performing type switch on its dynamic type via any(zero).
	var zero T
	switch any(zero).(type) {
	case *sql.Tx:
		pm.computedTx = func(ctx context.Context, tx *sql.Tx) error { return action(ctx, any(tx).(T)) }
	case *sql.DB:
		pm.MigrationBase.SetNonTransactional(true) //nolint:staticcheck
		pm.computedDB = func(ctx context.Context, db *sql.DB) error { return action(ctx, any(db).(T)) }
	default:
		panic("unsupported generic type in Computed")
	}
	lsm := libschema.Migration(pm)
	for _, opt := range opts {
		opt(lsm)
	}
	return lsm
}

// DoOneMigration applies a single migration.
// It is expected to be called by libschema.
func (p *Postgres) DoOneMigration(ctx context.Context, log *internal.Log, d *libschema.Database, m libschema.Migration) (result sql.Result, _ error) {
	pm := m.(*pmigration)

	// Initialize stmtclass version pruning once.
	if p.serverMajor == 0 {
		if maj, min := p.ServerVersion(ctx, d.DB()); maj != 0 {
			p.serverMajor, p.serverMinor = maj, min
		}
	}

	pm.ApplyForceOverride()

	f := &migfinalize.Finalizer[sql.DB, sql.Tx]{
		Ctx: ctx,
		DB:  d.DB(),
		Log: log,
		BeginTx: func(ctx context.Context, db *sql.DB) (tx *sql.Tx, err error) {
			defer func() {
				if err != nil && tx != nil {
					_ = tx.Rollback()
				}
			}()
			opts := d.Options.MigrationTxOptions
			if pm.genFn != nil && m.Base().ForcedNonTransactional() { // forced downgrade generate: use READ COMMITTED
				if opts == nil {
					opts = &sql.TxOptions{Isolation: sql.LevelReadCommitted}
				} else {
					cpy := *opts
					cpy.Isolation = sql.LevelReadCommitted
					opts = &cpy
				}
			}
			tx, err = db.BeginTx(ctx, opts)
			if err != nil {
				return nil, errors.Wrapf(err, "begin Tx for migration %s", m.Base().Name)
			}
			if err := applySchemaOverridePostgres(ctx, tx, d.Options.SchemaOverride, m.Base().Name.Name); err != nil {
				return nil, err
			}
			return tx, nil
		},
		BodyTx: func(ctx context.Context, tx *sql.Tx) error {
			// Handle computed first since it's simple
			if pm.computedTx != nil {
				if m.Base().NonTransactional() {
					return errors.Errorf("cannot execute *sql.Tx computed callback %s when forced non-transactional", m.Base().Name)
				}
				return errors.Wrapf(pm.computedTx(ctx, tx), "callback %s", m.Base().Name)
			}
			if pm.computedDB != nil {
				if !m.Base().NonTransactional() {
					return errors.Errorf("cannot execute *sql.DB computed callback %s when forced transactional", m.Base().Name)
				}
				return errors.Wrapf(pm.computedDB(ctx, d.DB()), "callback %s", m.Base().Name)
			}
			// Script / Generate path
			scriptSQL := pm.scriptSQL
			if pm.genFn != nil {
				sqlText, err := pm.genFn(ctx, tx)
				if err != nil {
					return errors.WithStack(err)
				}
				scriptSQL = sqlText
			}
			scriptSQL = strings.TrimSpace(scriptSQL)
			if scriptSQL == "" {
				return nil
			}
			// Unified classification & downgrade via stmtclass
			ts := sqltoken.TokenizePostgreSQL(scriptSQL)
			stmts, agg := stmtclass.ClassifyTokens(stmtclass.DialectPostgres, p.serverMajor, ts)
			if !pm.Base().ForcedTransactional() && !pm.Base().NonTransactional() {
				mustNonTx := false
				for _, st := range stmts {
					if (st.Flags & stmtclass.IsMustNonTx) != 0 {
						mustNonTx = true
						break
					}
				}
				if mustNonTx {
					pm.SetNonTransactional(true)
				}
			}
			if !pm.Base().NonTransactional() {
				var err error
				result, err = tx.ExecContext(ctx, scriptSQL)
				return errors.Wrapf(err, "ran %s in a transaction", scriptSQL)
			}
			// Non-transactional validation (final mode is non-tx)
			// Apply schema override (search_path) for non-transactional execution if needed.
			if d.Options.SchemaOverride != "" {
				if _, err := d.DB().ExecContext(ctx, "SET search_path TO "+pq.QuoteIdentifier(d.Options.SchemaOverride)); err != nil {
					return errors.Wrapf(err, "set search_path to %s for %s", d.Options.SchemaOverride, m.Base().Name)
				}
			}
			if agg&stmtclass.IsMultipleStatements != 0 || len(stmts) != 1 {
				return errors.Wrapf(libschema.ErrNonTxMultipleStatements, "non-transactional migration %s must contain exactly one SQL statement", m.Base().Name)
			}
			st := stmts[0]
			if st.Flags&stmtclass.IsNonIdempotent != 0 && !m.Base().HasSkipIf() {
				if st.Flags&stmtclass.IsEasilyIdempotentFix != 0 {
					return errors.Wrapf(libschema.ErrNonIdempotentNonTx, "non-transactional migration %s contains non-idempotent statement missing IF [NOT] EXISTS: %s", m.Base().Name, strings.TrimSpace(st.Text))
				}
				return errors.Wrapf(libschema.ErrNonIdempotentNonTx, "non-transactional migration %s contains non-idempotent: %s", m.Base().Name, strings.TrimSpace(st.Text))
			}
			var err error
			result, err = d.DB().ExecContext(ctx, scriptSQL)
			return errors.Wrap(err, scriptSQL)
		},
		SaveStatus: func(ctx context.Context, tx *sql.Tx, migErr error) error {
			return errors.WithStack(p.saveStatus(log, tx, d, m, migErr == nil, migErr))
		},
		CommitTx:   func(tx *sql.Tx) error { return errors.WithStack(tx.Commit()) },
		RollbackTx: func(tx *sql.Tx) { _ = tx.Rollback() },
		SetDone:    func() { m.Base().SetStatus(libschema.MigrationStatus{Done: true}) },
		SetError:   func(err error) { m.Base().SetStatus(libschema.MigrationStatus{Error: err.Error()}) },
	}

	return result, errors.WithStack(f.Run())
}

// CreateSchemaTableIfNotExists creates the migration tracking table for libschema.
// It is expected to be called by libschema.
func (p *Postgres) CreateSchemaTableIfNotExists(ctx context.Context, _ *internal.Log, d *libschema.Database) error {
	schema, tableName, err := trackingSchemaTable(d)
	if err != nil {
		return err
	}
	for {
		if schema != "" {
			_, err := d.DB().ExecContext(ctx, fmt.Sprintf(`
					CREATE SCHEMA IF NOT EXISTS %s
					`, schema))
			if err != nil {
				if strings.Contains(err.Error(), `pq: duplicate key value violates unique constraint "pg_namespace_nspname_index"`) {
					p.log.Warn("Ignoring create schema collision with another transaction and trying again")
					time.Sleep(time.Second)
					continue
				}
				return errors.Wrapf(err, "could not create libschema schema '%s'", schema)
			}
		}
		break
	}
	for {
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
			if strings.Contains(err.Error(), `duplicate key value violates unique constraint "pg_type_typname_nsp_index"`) {
				p.log.Warn("Ignoring create table collision with another transaction and trying again")
				time.Sleep(time.Second)
				continue
			}
			return errors.Wrapf(err, "could not create libschema migrations table '%s'", tableName)
		}
		break
	}
	return nil
}

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
		return "", "", errors.Errorf("tracking table '%s' is not valid", tableName)
	}
}

// trackingTable returns the schema+table reference for the migration tracking table.
// The name is already quoted properly for use as a save postgres identifier.
func trackingTable(d *libschema.Database) string {
	_, table, _ := trackingSchemaTable(d)
	return table
}

func (p *Postgres) saveStatus(log *internal.Log, tx *sql.Tx, d *libschema.Database, m libschema.Migration, done bool, migrationError error) error {
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
		ON CONFLICT (metadata, library, migration) DO UPDATE
		SET	done = EXCLUDED.done,
			error = EXCLUDED.error,
			updated_at = EXCLUDED.updated_at
			`, trackingTable(d))
	_, err := tx.Exec(q, m.Base().Name.Library, m.Base().Name.Name, done, estr)
	if err != nil {
		return errors.Wrapf(err, "save status for %s", m.Base().Name)
	}
	return nil
}

// LockMigrationsTable locks the migration tracking table for exclusive use by the
// migrations running now.
// It is expected to be called by libschema.
func (p *Postgres) LockMigrationsTable(ctx context.Context, _ *internal.Log, d *libschema.Database) error {
	tableName := trackingTable(d)
	if p.lockTx != nil {
		return errors.Errorf("libschema migrations table, '%s' already locked", tableName)
	}
	_, err := d.DB().ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (metadata, library, migration, done, error)
		VALUES ('lock', '', '', true, '')
		ON CONFLICT DO NOTHING`, tableName))
	if err != nil {
		return errors.Wrapf(err, "could not add lock row to %s", tableName)
	}
	tx, err := d.DB().BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return errors.Wrap(err, "could not start transaction: %s")
	}
	var junk string
	err = tx.QueryRow(fmt.Sprintf(`
		SELECT	metadata
		FROM	%s
		WHERE	metadata = 'lock'
		FOR UPDATE`, tableName)).Scan(&junk)
	if err != nil {
		return errors.Wrapf(err, "could not lock libschema migrations table '%s'", tableName)
	}
	p.lockTx = tx
	return nil
}

// UnlockMigrationsTable unlocks the migration tracking table.
// It is expected to be called by libschema.
func (p *Postgres) UnlockMigrationsTable(_ *internal.Log) error {
	if p.lockTx == nil {
		return errors.Errorf("libschema migrations table, not locked")
	}
	_ = p.lockTx.Rollback()
	p.lockTx = nil
	return nil
}

// LoadStatus loads the current status of all migrations from the migration tracking table.
// It is expected to be called by libschema.
func (p *Postgres) LoadStatus(ctx context.Context, _ *internal.Log, d *libschema.Database) (_ []libschema.MigrationName, err error) {
	tableName := trackingTable(d)
	rows, err := d.DB().QueryContext(ctx, fmt.Sprintf(`
		SELECT	library, migration, done
		FROM	%s
		WHERE	metadata = ''`, tableName))
	if err != nil {
		return nil, errors.Wrap(err, "cannot query migration status")
	}
	defer func() {
		e := rows.Close()
		if e != nil && err == nil {
			err = errors.Wrap(e, "close scan migration status")
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
// It is expected to be called by libschema.
func (p *Postgres) IsMigrationSupported(d *libschema.Database, _ *internal.Log, migration libschema.Migration) error {
	m, ok := migration.(*pmigration)
	if !ok {
		return errors.Errorf("non-postgres migration %s registered with postgres migrations", migration.Base().Name)
	}
	if m.genFn != nil || m.computedTx != nil || m.computedDB != nil || m.scriptSQL != "" {
		return nil
	}
	return errors.Errorf("migration %s is not supported", migration.Base().Name)
}

// ServerVersion queries and caches the PostgreSQL server version (major, minor).
// It returns zeroes if the version cannot be determined.
func (p *Postgres) ServerVersion(ctx context.Context, db *sql.DB) (major, minor int) {
	p.serverOnce.Do(func() {
		// SHOW server_version returns strings like 16.3, 15.11, 12.15 (also possible: 14beta1)
		var v string
		if err := db.QueryRowContext(ctx, "SHOW server_version").Scan(&v); err != nil {
			return
		}
		maj, min := parsePostgresServerVersion(v)
		if maj != 0 {
			p.serverMajor, p.serverMinor = maj, min
		}
	})
	return p.serverMajor, p.serverMinor
}

// parsePostgresServerVersion parses a PostgreSQL server_version string (e.g. "16.3", "14beta1", "15.11 (Ubuntu 15.11-....)")
// extracting major and minor numbers. Returns (0,0) if a major version cannot be determined.
var pgVersionRE = regexp.MustCompile(`^([0-9]+)(?:\.([0-9]+))?`)

func parsePostgresServerVersion(v string) (major, minor int) {
	if i := strings.IndexByte(v, ' '); i >= 0 { // strip trailing build details
		v = v[:i]
	}
	m := pgVersionRE.FindStringSubmatch(v)
	if len(m) < 2 {
		return 0, 0
	}
	_, _ = fmt.Sscanf(m[1], "%d", &major)
	if len(m) >= 3 && m[2] != "" {
		_, _ = fmt.Sscanf(m[2], "%d", &minor)
	}
	if major == 0 {
		return 0, 0
	}
	return major, minor
}
