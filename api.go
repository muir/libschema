package libschema

import (
	"context"
	"database/sql"

	"github.com/muir/libschema/internal"

	"github.com/pkg/errors"
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
	Name            MigrationName
	async           bool
	rawAfter        []MigrationName
	order           int // overall desired ordring across all libraries, ignores runAfter
	status          MigrationStatus
	skipIf          func() (bool, error)
	skipRemainingIf func() (bool, error)
	repeatUntilNoOp bool
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

// RepeatUntilNoOp marks a migration as potentially being needed to run multiple times.
// It will run over and over until the database reports that the migration
// modified no rows.  This can useuflly be combined with Asychnronous.
//
// This marking only applies to Script() and Generated() migrations.  The migration
// must be a single statement.
//
// For Computed() migrations, do not use RepeatUntilNoOp.  Instead simply write the
// migration use Driver.DB() to get a database handle and use it to do many little
// transactions, each one modifying a few rows until there is no more work to do.
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

func (n MigrationName) String() string {
	return n.Library + ": " + n.Name
}
