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
	"github.com/muir/libschema/internal/stmtcheck"
)

// Postgres is a libschema.Driver for connecting to Postgres-like databases that
// have the following characteristics:
// * Can do DDL commands inside transactions
// * Support UPSERT using INSERT ... ON CONFLICT
type Postgres struct {
	lockTx *sql.Tx
	log    *internal.Log
	// cached server version (major, minor) once queried; zero values mean unknown
	serverMajor int
	serverMinor int
	serverOnce  sync.Once // ensures we attempt version detection only once
	// compiled non-transactional statement patterns (pruned after version detection)
	nonTxStmtRegex []*regexp.Regexp
}

// NOTE: Sentinel errors for non-transactional validation now live in root package (libschema).

// nonTxIdempotencyRequirements lists patterns of mandatory non-transactional statements
// that must include a particular substring to be considered idempotent. This mirrors
// the regex-driven classification approach used by Script() for determining non-tx
// execution. If a pattern matches but the required substring is missing, execution
// is blocked with ErrNonIdempotentNonTx before any SQL is run.
var nonTxIdempotencyRequirements = []struct {
	re             *regexp.Regexp
	requiredSubstr string
}{
	{regexp.MustCompile(`(?i)^create\s+(unique\s+)?index\s+concurrently\b`), "if not exists"},
	{regexp.MustCompile(`(?i)^drop\s+index\s+concurrently\b`), "if exists"},
}

// New creates a libschema.Database with a postgres driver built in.  The dbName
// parameter is used internally by libschema, but does not affect where migrations
// are actually applied.
func New(log *internal.Log, dbName string, schema *libschema.Schema, db *sql.DB) (*libschema.Database, error) {
	return schema.NewDatabase(log, dbName, db, &Postgres{log: log, nonTxStmtRegex: append([]*regexp.Regexp{}, baseNonTxStmtRegex...)})
}

// Type aliases to central abstractions for brevity within this driver.
type (
	ExecConn = libschema.ExecConn
	TxLike   = libschema.TxLike
)

// ConnPtr constrains generic migrations to only *sql.Tx or *sql.DB eliminating
// the need for runtime type assertions in execution paths.
type ConnPtr interface{ *sql.Tx | *sql.DB }

type pmigration struct {
	libschema.MigrationBase
	scriptTx    func(context.Context, *sql.Tx) (string, error)
	scriptDB    func(context.Context, *sql.DB) (string, error)
	computedTx  func(context.Context, *sql.Tx) error
	computedDB  func(context.Context, *sql.DB) error
	creationErr error
}

func (m *pmigration) Copy() libschema.Migration {
	return &pmigration{MigrationBase: m.MigrationBase.Copy(), scriptTx: m.scriptTx, scriptDB: m.scriptDB, computedTx: m.computedTx, computedDB: m.computedDB, creationErr: m.creationErr}
}

func (m *pmigration) Base() *libschema.MigrationBase {
	return &m.MigrationBase
}

// regexByVersion groups regexes that should be dropped at or after the specified
// major version (dropAtMajor == 0 means keep always). Grouping reduces visual noise.
type regexByVersion struct {
	re          []*regexp.Regexp
	dropAtMajor int
}

var nonTxGroups = []regexByVersion{
	{ // always required non-transactional statements
		re: []*regexp.Regexp{
			regexp.MustCompile(`(?is)^create\s+(unique\s+)?index\s+concurrently\b`),
			regexp.MustCompile(`(?is)^drop\s+index\s+concurrently\b`),
			regexp.MustCompile(`(?is)^refresh\s+materialized\s+view\s+concurrently\b`),
			regexp.MustCompile(`(?is)^reindex\s+concurrently\b`),
			regexp.MustCompile(`(?is)^vacuum\s+full\b`),
			regexp.MustCompile(`(?is)^cluster\b`),
			regexp.MustCompile(`(?is)^create\s+database\b`),
			regexp.MustCompile(`(?is)^drop\s+database\b`),
			regexp.MustCompile(`(?is)^create\s+tablespace\b`),
			regexp.MustCompile(`(?is)^drop\s+tablespace\b`),
			regexp.MustCompile(`(?is)^create\s+subscription\b`),
			regexp.MustCompile(`(?is)^alter\s+subscription\b`),
			regexp.MustCompile(`(?is)^drop\s+subscription\b`),
		},
		dropAtMajor: 0,
	},
	{ // becomes safe inside transactions in PG 12+
		re: []*regexp.Regexp{
			regexp.MustCompile(`(?is)^alter\s+type\s+[^;]+\s+add\s+value\b`),
		},
		dropAtMajor: 12,
	},
}

var baseNonTxStmtRegex = func() []*regexp.Regexp { // initial full set
	var res []*regexp.Regexp
	for _, g := range nonTxGroups {
		res = append(res, g.re...)
	}
	return res
}()

// adjustNonTxForVersion is called lazily once we know server version to remove
// patterns that are not needed for newer versions (e.g., ALTER TYPE ADD VALUE >= 12).
func (p *Postgres) adjustNonTxForVersion(major int) {
	filtered := p.nonTxStmtRegex[:0]
	filtered = filtered[:0]
	for _, g := range nonTxGroups {
		if g.dropAtMajor != 0 && major >= g.dropAtMajor {
			continue
		}
		filtered = append(filtered, g.re...)
	}
	p.nonTxStmtRegex = filtered
}

// Script is a convenience helper for a literal SQL statement migration. It
// automatically chooses transactional (*sql.Tx) or non-transactional (*sql.DB)
// execution based on Postgres rules for statements that cannot run inside a
// transaction (e.g. CREATE INDEX CONCURRENTLY, VACUUM FULL, CREATE DATABASE).
// The choice of transactional vs non-transactional Can be overridden with
// ForceNonTransactional() or ForceTransactional() options.
func Script(name string, sqlText string, opts ...libschema.MigrationOption) libschema.Migration {
	pm := &pmigration{MigrationBase: libschema.MigrationBase{Name: libschema.MigrationName{Name: name}}}
	// Initial auto-classification (only sets the NonTransactional flag, deferring function assignment)
	if parts := sqltoken.TokenizePostgreSQL(sqlText).Strip().CmdSplit().Strings(); len(parts) > 0 {
		first := strings.ToLower(strings.TrimSpace(parts[0]))
		for _, re := range baseNonTxStmtRegex {
			if re.MatchString(first) {
				pm.SetNonTransactional(true)
				break
			}
		}
	}
	m := libschema.Migration(pm)
	for _, opt := range opts {
		opt(m)
	}
	if pm.NonTransactional() {
		pm.scriptDB = func(context.Context, *sql.DB) (string, error) { return sqlText, nil }
	} else {
		pm.scriptTx = func(context.Context, *sql.Tx) (string, error) { return sqlText, nil }
	}
	return m
}

// Generate defines a migration that returns a SQL string. If T implements TxLike
// the migration is transactional; otherwise it is marked non-transactional.
func Generate[T ConnPtr](name string, generator func(context.Context, T) string, opts ...libschema.MigrationOption) libschema.Migration {
	pm := &pmigration{MigrationBase: libschema.MigrationBase{Name: libschema.MigrationName{Name: name}}}
	var zero T
	var mustBeNonTransactional bool
	switch any(zero).(type) {
	case *sql.Tx:
		pm.scriptTx = func(ctx context.Context, tx *sql.Tx) (string, error) { return generator(ctx, any(tx).(T)), nil }
	case *sql.DB:
		mustBeNonTransactional = true
		pm.MigrationBase.SetNonTransactional(true) //nolint:staticcheck // QF1008: keep explicit for clarity and grepability
		pm.scriptDB = func(ctx context.Context, db *sql.DB) (string, error) { return generator(ctx, any(db).(T)), nil }
	}
	lsm := libschema.Migration(pm)
	for _, opt := range opts {
		opt(lsm)
	}
	// Verify no conflicting override: expected transactional mode derives from the generic type.
	expectedTx := !mustBeNonTransactional
	finalTx := !pm.NonTransactional()
	if expectedTx != finalTx && pm.creationErr == nil {
		pm.creationErr = errors.Errorf("Generate[%T] %s transactional override mismatch (expected %s)", zero, name, ternary(expectedTx, "transactional", "non-transactional"))
	}
	return lsm
}

// Computed defines a migration that runs arbitrary Go code.
func Computed[T ConnPtr](name string, action func(context.Context, T) error, opts ...libschema.MigrationOption) libschema.Migration {
	pm := &pmigration{MigrationBase: libschema.MigrationBase{Name: libschema.MigrationName{Name: name}}}
	var zero T
	switch any(zero).(type) {
	case *sql.Tx:
		pm.computedTx = func(ctx context.Context, tx *sql.Tx) error { return action(ctx, any(tx).(T)) }
	case *sql.DB:
		pm.MigrationBase.SetNonTransactional(true) //nolint:staticcheck // QF1008: intentional explicit embedded field reference
		pm.computedDB = func(ctx context.Context, db *sql.DB) error { return action(ctx, any(db).(T)) }
	}
	lsm := libschema.Migration(pm)
	for _, opt := range opts {
		opt(lsm)
	}
	expectedTx := (func() bool { _, is := any(zero).(*sql.Tx); return is })()
	finalTx := !pm.NonTransactional()
	if expectedTx != finalTx && pm.creationErr == nil {
		pm.creationErr = errors.Errorf("Computed[%T] %s transactional override mismatch (expected %s)", zero, name, ternary(expectedTx, "transactional", "non-transactional"))
	}
	return lsm
}

// ternary is a tiny helper to avoid repeating inline branching in error formatting.
func ternary[T any](cond bool, a, b T) T {
	if cond {
		return a
	}
	return b
}

// DoOneMigration applies a single migration.
// It is expected to be called by libschema.
func (p *Postgres) DoOneMigration(ctx context.Context, log *internal.Log, d *libschema.Database, m libschema.Migration) (res sql.Result, err error) {
	pm := m.(*pmigration)
	if pm.creationErr != nil {
		return nil, pm.creationErr
	}

	// Ensure server version adjustments applied once we have a DB.
	if p.serverMajor == 0 {
		if maj, _ := p.ServerVersion(ctx, d.DB()); maj != 0 {
			p.adjustNonTxForVersion(maj)
		}
	}

	runTransactional := !m.Base().NonTransactional()
	var tx *sql.Tx
	var execConn ExecConn
	if runTransactional {
		tx, err = d.DB().BeginTx(ctx, d.Options.MigrationTxOptions)
		if err != nil {
			return nil, errors.Wrapf(err, "begin Tx for migration %s", m.Base().Name)
		}
		execConn = tx
		if d.Options.SchemaOverride != "" {
			if _, err2 := tx.Exec(`SET search_path TO ` + pq.QuoteIdentifier(d.Options.SchemaOverride)); err2 != nil {
				_ = tx.Rollback()
				return nil, errors.Wrapf(err2, "set search path to %s for %s", d.Options.SchemaOverride, m.Base().Name)
			}
		}
	} else {
		execConn = d.DB()
	}

	// Defer handles status persistence & transaction finalization.
	defer func() {
		// Decide done flag before potential tx rollback.
		done := (err == nil)

		if runTransactional {
			if err == nil {
				// Attempt to write status inside same tx for atomicity.
				if serr := p.saveStatus(log, tx, d, m, true, nil); serr != nil {
					err = serr
					_ = tx.Rollback() // rollback migration + status on failure
				} else if cerr := tx.Commit(); cerr != nil {
					err = errors.Wrapf(cerr, "commit migration %s", m.Base().Name)
				} else {
					m.Base().SetStatus(libschema.MigrationStatus{Done: true})
				}
				return
			}
			// Migration failed: rollback original tx (if exists) then write status in fresh tx
			if tx != nil {
				_ = tx.Rollback()
			}
		}

		// Non-transactional or failed transactional path: record status separately
		stx, txErr := d.DB().BeginTx(ctx, d.Options.MigrationTxOptions)
		if txErr != nil {
			// Can't save status; append context
			if err == nil {
				err = errors.Wrapf(txErr, "begin tx to save status for %s", m.Base().Name)
			} else {
				err = errors.Wrapf(err, "(and could not open status tx: %s)", txErr)
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
			// Successful migration (non-transactional or after failure path recovery)
			m.Base().SetStatus(libschema.MigrationStatus{Done: true})
		} else if !done && err != nil {
			// Record failure in in-memory status so callers/tests can inspect Error without reloading from DB.
			m.Base().SetStatus(libschema.MigrationStatus{Error: err.Error()})
		}
	}()

	if pm.scriptTx != nil || pm.scriptDB != nil {
		var sqlText string
		if runTransactional && pm.scriptTx != nil {
			sqlText, err = pm.scriptTx(ctx, tx)
		} else if !runTransactional && pm.scriptDB != nil {
			sqlText, err = pm.scriptDB(ctx, d.DB())
		} else {
			return nil, errors.Errorf("migration %s transactional mode mismatch with script function", m.Base().Name)
		}
		if err != nil {
			return nil, err
		}
		trim := strings.TrimSpace(sqlText)
		if trim == "" { // treat as no-op, allow generated empty migrations
			return nil, nil
		}
		if !runTransactional {
			ts := sqltoken.TokenizePostgreSQL(sqlText)
			cmds := ts.Strip().CmdSplit()
			if len(cmds) != 1 {
				return nil, errors.Wrapf(libschema.ErrNonTxMultipleStatements, "non-transactional migration %s must contain exactly one SQL statement (convert to Computed[*sql.DB] for complex logic)", m.Base().Name)
			}
			if err := stmtcheck.AnalyzeTokens(ts); err != nil { // shadow
				if errors.Is(err, stmtcheck.ErrDataAndDDL) {
					return nil, errors.Wrapf(err, "validation failure for %s", m.Base().Name)
				}
				if errors.Is(err, stmtcheck.ErrNonIdempotentDDL) {
					return nil, errors.Wrapf(libschema.ErrNonIdempotentNonTx, "validation failure for %s: %v", m.Base().Name, err)
				}
			}
			lower := strings.ToLower(sqlText)
			for _, req := range nonTxIdempotencyRequirements {
				if req.re.MatchString(lower) && !strings.Contains(lower, req.requiredSubstr) {
					return nil, errors.Wrapf(libschema.ErrNonIdempotentNonTx, "non-transactional migration %s uses statement matching %q without %s (required for idempotency)", m.Base().Name, req.re.String(), req.requiredSubstr)
				}
			}
		}
		res, err = execConn.ExecContext(ctx, sqlText)
		if err != nil {
			return nil, errors.Wrap(err, sqlText)
		}
		return res, nil
	}
	if pm.computedTx != nil || pm.computedDB != nil {
		if runTransactional && pm.computedTx != nil {
			err = pm.computedTx(ctx, tx)
		} else if !runTransactional && pm.computedDB != nil {
			err = pm.computedDB(ctx, d.DB())
		} else {
			return nil, errors.Errorf("migration %s transactional mode mismatch with computed function", m.Base().Name)
		}
		return nil, err
	}
	return nil, errors.Errorf("migration %s has neither script nor computed body", m.Base().Name)
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
	if m.scriptTx != nil || m.scriptDB != nil || m.computedTx != nil || m.computedDB != nil {
		return nil
	}
	if m.creationErr != nil {
		return m.creationErr
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
func parsePostgresServerVersion(v string) (major, minor int) {
	if i := strings.IndexByte(v, ' '); i >= 0 { // strip trailing build details
		v = v[:i]
	}
	cleaned := make([]rune, 0, len(v))
	for _, r := range v {
		if (r >= '0' && r <= '9') || r == '.' { // keep digits/dots until first non-digit/dot
			cleaned = append(cleaned, r)
		} else {
			break
		}
	}
	parts := strings.Split(string(cleaned), ".")
	if len(parts) >= 1 {
		_, _ = fmt.Sscanf(parts[0], "%d", &major)
	}
	if len(parts) >= 2 {
		_, _ = fmt.Sscanf(parts[1], "%d", &minor)
	}
	if major == 0 {
		return 0, 0
	}
	return major, minor
}
