package libschema

import (
	"context"
	"database/sql"

	"github.com/pkg/errors"
)

const DefaultTrackingTable = "libschema.migration_status"

// Driver interface is what's required to use libschema with a new
// database.
type Driver interface {
	CreateSchemaTableIfNotExists(context.Context, MyLogger, *Database) error
	LockMigrationsTable(context.Context, MyLogger, *Database) error
	UnlockMigrationsTable(MyLogger) error

	// DoOneMigration must update the both the migration status in
	// the Database object and it must persist the migration status
	// in the tracking table.  It also does the migration.
	DoOneMigration(context.Context, MyLogger, *Database, Migration) error

	// IsMigrationSupported exists to guard against additional migration
	// options and features.  It should return nil except if there are new
	// migration features added that haven't been included in all support
	// libraries.
	IsMigrationSupported(*Database, MyLogger, Migration) error

	LoadStatus(context.Context, MyLogger, *Database) ([]MigrationName, error)
}

type MigrationName struct {
	Name    string
	Library string
}

type MigrationOption func(Migration)

// Migration defines a single database defintion update.
type MigrationBase struct {
	Name     MigrationName
	async    bool
	rawAfter []MigrationName
	order    int // overall desired ordring across all libraries, ignores runAfter
	status   MigrationStatus
}

func (m MigrationBase) Copy() MigrationBase {
	if m.rawAfter != nil {
		ra := make([]MigrationName, len(m.rawAfter))
		copy(ra, m.rawAfter)
		m.rawAfter = ra
	}
	return m
}

type Migration interface {
	Base() *MigrationBase
	Copy() Migration
}

// MigrationStatus tracks if a migration is complete or not.
type MigrationStatus struct {
	Done    bool
	Partial string // for Mysql, the string represents the portion of multiple commands that have completed
	Error   string // If an attempt was made but failed, this will be set
}

// Database tracks all of the migrations for a specific database.
type Database struct {
	libraries         []string
	migrations        []Migration // in order of definition
	byLibrary         map[string][]Migration
	migrationIndex    map[MigrationName]Migration
	errors            []error
	db                *sql.DB
	name              string
	driver            Driver
	sequence          []Migration // in order of execution
	status            map[MigrationName]*MigrationStatus
	parent            *Schema
	Options           Options
	Context           context.Context
	log               MyLogger
	asyncInProgress   bool
	unknownMigrations []MigrationName
}

type Options struct {
	// TrackingTable is the name of the table used to track which migrations
	// have been applied
	TrackingTable string

	// SchemaOverride is used to override the default schema.  This is most useful
	// for testing schema migrations
	SchemaOverride string

	MigrationTxOptions *sql.TxOptions

	ErrorOnUnknownMigrations bool
	OnAsyncMigrationFailure  func(n MigrationName, err error)
	OnMigrationsStarted      func()      // only called if migrations are needed
	OnMigrationsComplete     func(error) // called even if no migrations are needed
}

type Schema struct {
	databases     map[string]*Database
	databaseOrder []*Database
	options       Options
	count         int
	context       context.Context
}

// See https://github.com/logur/logur#readme
// This interface definition will not
type MyLogger interface {
	Trace(msg string, fields ...map[string]interface{})
	Debug(msg string, fields ...map[string]interface{})
	Info(msg string, fields ...map[string]interface{})
	Warn(msg string, fields ...map[string]interface{})
	Error(msg string, fields ...map[string]interface{})
}

func New(ctx context.Context, options Options) *Schema {
	if options.TrackingTable == "" {
		options.TrackingTable = DefaultTrackingTable
	}
	return &Schema{
		options:   options,
		context:   ctx,
		databases: make(map[string]*Database),
	}
}

func (s *Schema) NewDatabase(log MyLogger, name string, db *sql.DB, driver Driver) (*Database, error) {
	if _, ok := s.databases[name]; ok {
		return nil, errors.Errorf("Duplicate database '%s'", name)
	}
	database := &Database{
		name:           name,
		db:             db,
		byLibrary:      make(map[string][]Migration),
		migrationIndex: make(map[MigrationName]Migration),
		parent:         s,
		Options:        s.options,
		Context:        s.context,
		driver:         driver,
		log:            log,
	}
	s.databases[name] = database
	s.databaseOrder = append(s.databaseOrder, database)
	return database, nil
}

func Asyncrhronous() func(Migration) {
	return func(m Migration) {
		m.Base().async = true
	}
}

func After(lib, migration string) func(Migration) {
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

func (d *Database) DB() *sql.DB {
	return d.db
}

func (d *Database) Lookup(name MigrationName) (Migration, bool) {
	m, ok := d.migrationIndex[name]
	return m, ok
}

func (d *Database) Migrations(libraryName string, migrations ...Migration) {
	if _, ok := d.byLibrary[libraryName]; ok {
		d.errors = append(d.errors, errors.Errorf("duplicate library '%s' registered with a call to Database.Migrations()", libraryName))
		return
	}
	d.libraries = append(d.libraries, libraryName)
	libList := make([]Migration, len(migrations))
	for i, migration := range migrations {
		migration := migration.Copy()
		migration.Base().Name.Library = libraryName
		d.migrationIndex[migration.Base().Name] = migration
		libList[i] = migration
		migration.Base().order = len(d.migrations)
		d.migrations = append(d.migrations, migration)
	}
	d.byLibrary[libraryName] = libList
}

func (m *MigrationBase) Status() MigrationStatus {
	return m.status
}

func (m *MigrationBase) SetStatus(status MigrationStatus) {
	m.status = status
}

func (n MigrationName) String() string {
	return n.Library + ": " + n.Name
}
