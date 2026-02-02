package libschema

import (
	"context"
	"database/sql"

	"github.com/memsql/errors"

	"github.com/muir/libschema/internal"
)

const (
	// ErrDataAndDDL indicates a single migration mixes schema changes (DDL) and data
	// manipulation (DML) where that combination is disallowed.
	ErrDataAndDDL errors.String = "migration combines DDL (schema changes) and data manipulation"

	// ErrNonIdempotentDDL indicates a non-transactional migration contains easily
	// guardable DDL lacking an IF (NOT) EXISTS clause.
	ErrNonIdempotentDDL errors.String = "unconditional migration has non-idempotent DDL"

	// ErrNonTxMultipleStatements indicates a non-transactional (idempotent) script migration
	// attempted to execute multiple SQL statements when only one is allowed.
	ErrNonTxMultipleStatements errors.String = "non-transactional migration has multiple statements"

	// ErrNonIdempotentNonTx indicates a required-to-be-idempotent non-transactional migration
	// failed idempotency validation heuristics.
	ErrNonIdempotentNonTx errors.String = "non-idempotent non-transactional migration"
)

const DefaultTrackingTable = "libschema.migration_status"

// Driver interface is what's required to use libschema with a new
// database.
type Driver interface {
	CreateSchemaTableIfNotExists(context.Context, *internal.Log, *Database) error
	LockMigrationsTable(context.Context, *internal.Log, *Database) error
	UnlockMigrationsTable(*internal.Log) error

	// DoOneMigration must update the both the migration status in
	// the Database object and it must persist the migration status
	// in the tracking table.  It also does the migration.
	// The returned sql.Result is optional: Computed() migrations do not
	// need to provide results.  The result is used for RepeatUntilNoOp.
	DoOneMigration(context.Context, *internal.Log, *Database, Migration) (sql.Result, error)

	// IsMigrationSupported exists to guard against additional migration
	// options and features.  It should return nil except if there are new
	// migration features added that haven't been included in all support
	// libraries.
	IsMigrationSupported(*Database, *internal.Log, Migration) error

	LoadStatus(context.Context, *internal.Log, *Database) ([]MigrationName, error)
}

// MigrationName holds both the name of the specific migration and the library to
// which it belongs.
type MigrationName struct {
	Name    string
	Library string
}

// MigrationOption modifies a migration to set additional parameters
type MigrationOption func(Migration)

// Migration defines a single database defintion update.
type MigrationBase struct {
	Name             MigrationName
	async            bool
	rawAfter         []MigrationName
	order            int // overall desired ordring across all libraries, ignores runAfter
	status           MigrationStatus
	skipIf           func() (bool, error)
	skipRemainingIf  func() (bool, error)
	repeatUntilNoOp  bool
	nonTransactional bool  // set automatically or by ForceNonTransactional / inference
	forcedTx         *bool // if not nil, explicitly chosen transactional mode (true=transactional, false=non-transactional)
}

func (m MigrationBase) Copy() MigrationBase {
	if m.rawAfter != nil {
		ra := make([]MigrationName, len(m.rawAfter))
		copy(ra, m.rawAfter)
		m.rawAfter = ra
	}
	return m
}

// MigrationBase is a workaround for lacking object inheritance.
type Migration interface {
	Base() *MigrationBase
	Copy() Migration
}

// MigrationStatus tracks if a migration is complete or not.
type MigrationStatus struct {
	Done  bool
	Error string // If an attempt was made but failed, this will be set
}

// Database tracks all of the migrations for a specific database.
type Database struct {
	libraries         []string
	migrations        []Migration // in order of definition
	byLibrary         map[string][]Migration
	migrationIndex    map[MigrationName]Migration
	errors            []error
	db                *sql.DB
	DBName            string
	driver            Driver
	sequence          []Migration // in order of execution
	status            map[MigrationName]*MigrationStatus
	parent            *Schema
	Options           Options
	log               *internal.Log
	asyncInProgress   bool
	unknownMigrations []MigrationName
}

// Options operate at the Database level but are specified at the Schema level
// at least initially.  If you want separate options on a per-Database basis,
// you must override the values after attaching the database to the Schema.
type Options struct {
	// Overrides change the behavior of libschema in big ways: causing it to
	// call os.Exit() when finished or not migrating.  If overrides is not
	// specified then DefaultOverrides is used.
	Overrides *OverrideOptions

	// TrackingTable is the name of the table used to track which migrations
	// have been applied
	TrackingTable string

	// SchemaOverride is used to override the default schema.  This is most useful
	// for testing schema migrations
	SchemaOverride string

	// These TxOptions will be used for all migration transactions.
	MigrationTxOptions *sql.TxOptions

	ErrorOnUnknownMigrations bool

	// OnMigrationFailure is only called when there is a failure
	// of a specific migration.  OnMigrationsComplete will also
	// be called.  OnMigrationFailure is called for each Database
	// (if there is a failure).
	OnMigrationFailure func(dbase *Database, n MigrationName, err error)

	// OnMigrationsStarted is only called if migrations are needed
	// OnMigrationsStarted is called for each Database (if needed).
	OnMigrationsStarted func(dbase *Database)

	// OnMigrationsComplete called even if no migrations are needed.  It
	// will be called when async migrations finish even if they finish
	// with an error.  OnMigrationsComplete is called for each Database.
	OnMigrationsComplete func(dbase *Database, err error)

	// DebugLogging turns on extra debug logging
	DebugLogging bool
}

// Schema tracks all the migrations
type Schema struct {
	databases     map[string]*Database
	databaseOrder []*Database
	options       Options
	context       context.Context
}

// New creates a schema object.
func New(ctx context.Context, options Options) *Schema {
	if options.TrackingTable == "" {
		options.TrackingTable = DefaultTrackingTable
	}
	if options.Overrides == nil {
		options.Overrides = &DefaultOverrides
	}
	return &Schema{
		options:   options,
		context:   ctx,
		databases: make(map[string]*Database),
	}
}

// NewDatabase creates a Database object.  For Postgres and Mysql this is bundled into
// lspostgres.New() and lsmysql.New().
func (s *Schema) NewDatabase(log *internal.Log, dbName string, db *sql.DB, driver Driver) (*Database, error) {
	if _, ok := s.databases[dbName]; ok {
		return nil, errors.Errorf("Duplicate database '%s'", dbName)
	}
	database := &Database{
		DBName:         dbName,
		db:             db,
		byLibrary:      make(map[string][]Migration),
		migrationIndex: make(map[MigrationName]Migration),
		parent:         s,
		Options:        s.options,
		driver:         driver,
		log:            log,
	}
	s.databases[dbName] = database
	s.databaseOrder = append(s.databaseOrder, database)
	return database, nil
}

// Asynchronous marks a migration is okay to run asynchronously.  If all of the
// remaining migrations can be asynchronous, then schema.Migrate() will return
// while the remaining migrations run.
func Asynchronous() MigrationOption {
	return func(m Migration) {
		m.Base().async = true
	}
}

// RepeatUntilNoOp marks a migration (Script or Generate) to be re-executed until
// the underlying driver reports zero rows affected. Use it for single, pure DML
// statements that progressively transform data (e.g. UPDATE batches that move a
// limited subset each run). It is NOT a generic looping primitive.
//
// Safety / correctness guidelines:
//   - Single statement only - multi-statement scripts can give a meaningless final RowsAffected().
//   - DML only - avoid DDL (CREATE/ALTER/DROP/REFRESH) or utility commands; most return 0 and will
//     terminate immediately or loop uselessly.
//   - Idempotent per batch - repeating the same statement after partial success must not corrupt data.
//   - Do NOT mix with non-transactional Postgres DDL (concurrent index builds, REFRESH MATERIALIZED VIEW CONCURRENTLY).
//   - If logic needs conditionals or multiple statements, write a Computed migration and loop explicitly.
//
// Prefer a Computed migration when:
//   - You need to run multiple statements per batch
//   - You must inspect progress with custom queries
//   - RowsAffected() is unreliable or driver-dependent
//
// Future note: libschema may emit debug warnings for obviously unreliable usages
// (e.g. DDL + RepeatUntilNoOp). Treat the above bullets as normative behavior now.
func RepeatUntilNoOp() MigrationOption {
	return func(m Migration) {
		m.Base().repeatUntilNoOp = true
	}
}

// After sets up a dependency between one migration and another.  This can be
// across library boundaries.  By default, migrations are dependent on the
// prior migration defined.  After specifies that the current migration must
// run after the named migration.
func After(lib, migration string) MigrationOption {
	return func(m Migration) {
		base := m.Base()
		rawAfter := make([]MigrationName, len(base.rawAfter)+1)
		copy(rawAfter, base.rawAfter) // copy in case there is another reference
		rawAfter[len(base.rawAfter)] = MigrationName{
			Library: lib,
			Name:    migration,
		}
		base.rawAfter = rawAfter
	}
}

// SkipIf is checked before the migration is run.  If the function returns true
// then this migration is skipped.  For MySQL, this allows migrations
// that are not idempotent to be checked before they're run and skipped
// if they have already been applied.
func SkipIf(pred func() (bool, error)) MigrationOption {
	return func(m Migration) {
		m.Base().skipIf = pred
	}
}

// SkipRemainingIf is checked before the migration is run.  If the function
// returns true then this migration and all following it are not run at this
// time.  One use for this to hold back migrations that have not been released
// yet.  For example, in a blue-green deploy organization, you could first
// do a migration that creates another column, then later do a migration that
// removes the old column.  The migration to remove the old column can be defined
// and tested in advance but held back by SkipRemainingIf until it's time to
// deploy it.
func SkipRemainingIf(pred func() (bool, error)) MigrationOption {
	return func(m Migration) {
		m.Base().skipRemainingIf = pred
	}
}

// ForceNonTransactional forces a migration (Script, Generate, or Computed) to run without
// a wrapping transaction. By using this you assert the migration is idempotent (safe to retry).
// Overrides any automatic inference the driver would normally perform.
//
// Generate note: For drivers where generation always occurs inside a transaction context (e.g. Postgres
// Generate with *sql.Tx generator) forcing non-transactional only affects execution of the
// produced SQL, not the generator callback itself. Drivers will reject impossible combinations (e.g.
// attempting to force non-transactional on a generator that fundamentally requires *sql.Tx if they cannot
// safely downgrade).
func ForceNonTransactional() MigrationOption {
	return func(m Migration) {
		b := m.Base()
		v := false // false means non-transactional
		b.forcedTx = &v
	}
}

// ForceTransactional forces a migration to run inside a transaction even if automatic inference
// would choose non-transactional execution.
//
// Generate note: For generator-based migrations whose callback already receives *sql.Tx (Generate)
// this is effectively a no-op (they are already transactional). For generators that inherently execute
// outside a transaction (none exist in current public API) forcing transactional would be rejected.
//
// WARNING (foot-gun): On MySQL / SingleStore this DOES NOT make DDL atomic. Those engines
// autocommit each DDL statement regardless of any BEGIN you issue. By forcing transactional mode:
//   - Earlier DML in the same migration may roll back while preceding DDL remains applied.
//   - The bookkeeping UPDATE that records migration completion can fail while schema changes
//     have already been partially or fully applied (leaving the migration marked incomplete and
//     retried later against an already-changed schema).
//   - Mixed DDL + DML ordering inside a forced transactional migration can produce inconsistent,
//     misleading results during retries or partial failures.
//
// Use this only if you fully understand these semantics and prefer to retain a transactional wrapper
// for non-DDL statements. If you simply need safe DDL, prefer writing idempotent statements and let
// the driver downgrade automatically.
func ForceTransactional() MigrationOption {
	return func(m Migration) {
		b := m.Base()
		v := true // true means transactional
		b.forcedTx = &v
	}
}

// ApplyForceOverride overrides transactionality for any prior force call (ForceTransactional
// or ForceNonTransactional)
func (m *MigrationBase) ApplyForceOverride() {
	if m.forcedTx != nil { // forced override present
		m.SetNonTransactional(!*m.forcedTx)
	}
}

func (d *Database) DB() *sql.DB {
	return d.db
}

func (d *Database) Lookup(name MigrationName) (Migration, bool) {
	m, ok := d.migrationIndex[name]
	return m, ok
}

// Migrations specifies the migrations needed for a library.  By default, each
// migration is dependent upon the prior migration and they'll run in the order
// given.  By default, all the migrations for a library will run in the order in
// which the library migrations are defined.
//
// Migrations for a specific library are not additive. All must be given at once.
func (d *Database) Migrations(libraryName string, migrations ...Migration) {
	if _, ok := d.byLibrary[libraryName]; ok {
		d.errors = append(d.errors, errors.Errorf("duplicate library '%s' registered with a call to Database.Migrations()", libraryName))
		return
	}
	d.libraries = append(d.libraries, libraryName)
	mList := make([]Migration, len(migrations))
	for i, migration := range migrations {
		migration := migration.Copy()
		migration.Base().Name.Library = libraryName
		d.migrationIndex[migration.Base().Name] = migration
		mList[i] = migration
		migration.Base().order = len(d.migrations)
		d.migrations = append(d.migrations, migration)
	}
	d.byLibrary[libraryName] = mList
}

func (m *MigrationBase) Status() MigrationStatus {
	return m.status
}

func (m *MigrationBase) SetStatus(status MigrationStatus) {
	m.status = status
}

func (m *MigrationBase) HasSkipIf() bool {
	return m.skipIf != nil
}

// NonTransactional reports if the migration must not be wrapped in a transaction.
// This is automatically set for drivers (e.g. Postgres) that provide generic
// migration helpers which infer non-transactional status from the connection
// type used (e.g. *sql.DB vs *sql.Tx). A migration marked nonTransactional will
// be executed without an encompassing BEGIN/COMMIT; status recording still
// occurs within its own small transaction when supported.
func (m *MigrationBase) NonTransactional() bool {
	return m.nonTransactional
}

func (m *MigrationBase) SetNonTransactional(v bool) {
	m.nonTransactional = v
}

// ForcedTransactional reports if ForceTransactional() was explicitly called.
func (m *MigrationBase) ForcedTransactional() bool { return m.forcedTx != nil && *m.forcedTx }

// ForcedNonTransactional reports if ForceNonTransactional() was explicitly called.
func (m *MigrationBase) ForcedNonTransactional() bool { return m.forcedTx != nil && !*m.forcedTx }

func (n MigrationName) String() string {
	return n.Library + ": " + n.Name
}
