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

type ConnPtr interface{ *sql.Tx | *sql.DB }

type pmigration struct {
	libschema.MigrationBase
	scriptTx    func(context.Context, *sql.Tx) (string, error)
	scriptDB    func(context.Context, *sql.DB) (string, error)
	scriptSQL   string // raw SQL for deferred classification
	computedTx  func(context.Context, *sql.Tx) error
	computedDB  func(context.Context, *sql.DB) error
	creationErr error
}

func (m *pmigration) Copy() libschema.Migration {
	n := *m
	n.MigrationBase = m.MigrationBase.Copy()
	return &n
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
	pm := &pmigration{
		MigrationBase: libschema.MigrationBase{
			Name: libschema.MigrationName{
				Name: name,
			},
		},
		scriptSQL: sqlText,
	}
	m := libschema.Migration(pm)
	for _, opt := range opts {
		opt(m)
	}

	return m
}

// Generate defines a migration that returns a SQL string.
// Transactionality is fixed by the generic type parameter T:
//   - Generate[*sql.Tx]  => transactional
//   - Generate[*sql.DB]  => non-transactional
//
// The driver does not attempt to coerce or flip this at runtime; choose the type that matches the desired mode.
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
	// Apply any force override prior to evaluating mismatch so forced direction is reflected in finalTx.
	pm.ApplyForceOverride()
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
func (p *Postgres) DoOneMigration(ctx context.Context, log *internal.Log, d *libschema.Database, m libschema.Migration) (sql.Result, error) {
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

	pm.ApplyForceOverride()

	// Deferred non-tx decision: replicate original Script() classification using tokenizer but
	// perform it here (after server version pruning) so we have accurate regex set.
	// We only inspect the first command (comments stripped) – multi-statement scripts are
	// disallowed for non-tx later; if user forces transactional we intentionally skip downgrade.
	if pm.scriptSQL != "" {
		if !pm.Base().ForcedTransactional() { // only attempt downgrade if user did not force transactional
			parts := sqltoken.TokenizePostgreSQL(pm.scriptSQL).Strip().CmdSplit().Strings()
			if len(parts) > 0 {
				first := strings.ToLower(strings.TrimSpace(parts[0]))
				for _, re := range p.nonTxStmtRegex { // version-adjusted subset
					if re.MatchString(first) {
						pm.SetNonTransactional(true)
						pm.scriptDB = func(context.Context, *sql.DB) (string, error) { return pm.scriptSQL, nil }
						pm.scriptTx = nil
						break
					}
				}
			}
		}

		// If downgrade logic above changed transactional mode we already set the appropriate script* closure.
		// If neither closure set yet (no downgrade, not forced non-tx), assign transactional version now.
		if pm.scriptTx == nil && pm.scriptDB == nil {
			if pm.NonTransactional() { // forced non-tx (user override) but not downgraded path
				pm.scriptDB = func(context.Context, *sql.DB) (string, error) { return pm.scriptSQL, nil }
			} else {
				pm.scriptTx = func(context.Context, *sql.Tx) (string, error) { return pm.scriptSQL, nil }
			}
		}
	}

	var res sql.Result

	f := &migfinalize.Finalizer[sql.DB, sql.Tx]{
		Ctx:              ctx,
		DB:               d.DB(),
		RunTransactional: !m.Base().NonTransactional(),
		Log:              log,
		BeginTx: func(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
			tx, err := db.BeginTx(ctx, d.Options.MigrationTxOptions)
			if err != nil {
				return nil, errors.Wrapf(err, "begin Tx for migration %s", m.Base().Name)
			}
			if d.Options.SchemaOverride != "" { // set search_path early
				if _, err2 := tx.Exec(`SET search_path TO ` + pq.QuoteIdentifier(d.Options.SchemaOverride)); err2 != nil {
					_ = tx.Rollback()
					return nil, errors.Wrapf(err2, "set search path to %s for %s", d.Options.SchemaOverride, m.Base().Name)
				}
			}
			return tx, nil
		},
		BodyTx: func(ctx context.Context, tx *sql.Tx) error {
			if pm.scriptTx != nil || pm.scriptDB != nil {
				if pm.scriptTx == nil { // mismatch
					return errors.Errorf("migration %s transactional mode mismatch with script function", m.Base().Name)
				}
				sqlText, err := pm.scriptTx(ctx, tx)
				if err != nil {
					return errors.WithStack(err)
				}
				trim := strings.TrimSpace(sqlText)
				if trim == "" {
					return nil
				}
				res, err = tx.ExecContext(ctx, sqlText)
				if err != nil {
					return errors.Wrap(err, sqlText)
				}
				return nil
			}
			// Computed path
			if pm.computedTx != nil || pm.computedDB != nil {
				if pm.computedTx == nil {
					return errors.Errorf("migration %s transactional mode mismatch with computed function", m.Base().Name)
				}
				return errors.WithStack(pm.computedTx(ctx, tx))
			}
			return errors.Errorf("migration %s has neither script nor computed body", m.Base().Name)
		},
		BodyNonTx: func(ctx context.Context, db *sql.DB) error {
			if pm.scriptTx != nil || pm.scriptDB != nil {
				if pm.scriptDB == nil {
					return errors.Errorf("migration %s transactional mode mismatch with script function", m.Base().Name)
				}
				sqlText, err := pm.scriptDB(ctx, db)
				if err != nil {
					return errors.WithStack(err)
				}
				trim := strings.TrimSpace(sqlText)
				if trim == "" {
					return nil
				}
				// If user explicitly forced non-transactional, bypass validation (single-stmt & idempotency heuristics).
				if !pm.Base().ForcedNonTransactional() { // normal auto-downgraded non-tx path enforces safety checks
					ts := sqltoken.TokenizePostgreSQL(sqlText)
					cmds := ts.Strip().CmdSplit()
					if len(cmds) != 1 {
						return errors.Wrapf(libschema.ErrNonTxMultipleStatements, "non-transactional migration %s must contain exactly one SQL statement (convert to Computed[*sql.DB] for complex logic)", m.Base().Name)
					}
					stmts, agg := stmtclass.ClassifyTokens(stmtclass.DialectPostgres, ts)
					if agg&stmtclass.IsMultipleStatements != 0 || len(stmts) != 1 { // defensive; parser split above should already enforce 1
						return errors.Wrapf(libschema.ErrNonTxMultipleStatements, "non-transactional migration %s must contain exactly one SQL statement", m.Base().Name)
					}
					f := stmts[0].Flags
					if f&stmtclass.IsNonIdempotent != 0 {
						if f&stmtclass.IsEasilyIdempotentFix != 0 {
							// Easy fix (e.g. CREATE TABLE/INDEX/SEQUENCE without IF NOT EXISTS, DROP without IF EXISTS, etc.) -> hard error
							return errors.Wrapf(libschema.ErrNonIdempotentNonTx, "non-transactional migration %s contains non-idempotent statement that is easily idempotent (add IF [NOT] EXISTS): %s", m.Base().Name, strings.TrimSpace(stmts[0].Text))
						}
						// Harder to make idempotent automatically: allow but warn.
						p.log.Warn("allowing non-transactional non-idempotent (hard-fix) statement in " + m.Base().Name.String() + ": " + strings.TrimSpace(stmts[0].Text))
					}
					// Retain specialized regex requirements (eg CREATE INDEX CONCURRENTLY) until folded fully into classifier.
					lower := strings.ToLower(sqlText)
					for _, req := range nonTxIdempotencyRequirements {
						if req.re.MatchString(lower) && !strings.Contains(lower, req.requiredSubstr) {
							return errors.Wrapf(libschema.ErrNonIdempotentNonTx, "non-transactional migration %s uses statement matching %q without %s (required for idempotency)", m.Base().Name, req.re.String(), req.requiredSubstr)
						}
					}
				} else if p.log != nil {
					p.log.Warn("bypassing non-transactional validation due to ForceNonTransactional() on " + m.Base().Name.String())
				}
				res, err = db.ExecContext(ctx, sqlText)
				if err != nil {
					return errors.Wrap(err, sqlText)
				}
				return nil
			}
			// Computed path (non-transactional)
			if pm.computedTx != nil || pm.computedDB != nil {
				if pm.computedDB == nil {
					return errors.Errorf("migration %s transactional mode mismatch with computed function", m.Base().Name)
				}
				return pm.computedDB(ctx, db)
			}
			return errors.Errorf("migration %s has neither script nor computed body", m.Base().Name)
		},
		SaveStatusInTx: func(ctx context.Context, tx *sql.Tx) error {
			return errors.WithStack(p.saveStatus(log, tx, d, m, true, nil))
		},
		CommitTx:   func(tx *sql.Tx) error { return errors.WithStack(tx.Commit()) },
		RollbackTx: func(tx *sql.Tx) { _ = tx.Rollback() },
		BeginStatusTx: func(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
			stx, err := db.BeginTx(ctx, d.Options.MigrationTxOptions)
			if err != nil {
				return nil, errors.Wrapf(err, "begin tx to save status for %s", m.Base().Name)
			}
			return stx, nil
		},
		SaveStatusSeparate: func(ctx context.Context, stx *sql.Tx, migErr error) error {
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
	fmt.Printf("XXX IsMigrationSupported name=%s scriptSQL.len=%d scriptTx=%v scriptDB=%v computedTx=%v computedDB=%v nonTx=%v forcedTx?=%v forcedNonTx?=%v\n",
		m.Base().Name.Name, len(m.scriptSQL), m.scriptTx != nil, m.scriptDB != nil, m.computedTx != nil, m.computedDB != nil, m.NonTransactional(), m.ForcedTransactional(), m.ForcedNonTransactional())
	if m.scriptTx != nil || m.scriptDB != nil || m.computedTx != nil || m.computedDB != nil || m.scriptSQL != "" {
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
