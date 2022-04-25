package libschema

// OverrideOptions define the command line flags and/or configuration block that can be
// provided to control libschema's behavior.  OverrideOptions is designed to be filled
// with https://github.com/muir/nfigure but it doesn't have to be done that way.
type OverrideOptions struct {
	// MigrateOnly causes program to exit when migrations are complete.
	// Asynchronous migrations will be skipped.  If no migrations are need,
	// the program will exit very quickly.  This is accomplished with a call
	// to os.Exit().
	MigrateOnly bool `flag:"migrate-only" help:"Call os.Exit() after completing migrations"`

	// MigrateDatabase specifies that only a specific database should
	// be migrated.  The name must match to a name provided with the schema.NewDatabase() call.
	// For both libschema/lsmysql and libschema/lspostgres, that is the name parameter to
	// New() NOT the database name in the DSN.  This is a logical name.
	MigrateDatabase string `flag:"migrate-database" help:"Migrate only the this database"`

	// MigrateDSN overrides the data source name for a single database.  It must be used in
	// conjunction with MigrateDatabase unless there are only migrations for a single database.
	MigrateDSN string `flag:"migrate-dsn" help:"Override *sql.DB, must combine with --migrate-database"`

	// NoMigrate command line flag / config variable skips all migrations
	NoMigrate bool `flag:"no-migrate" help:"Skip all migrations (except async)"`

	// ExitIfMigrateNeeded command line flag / config variable causes Migrate() to return error if there are
	// migrations required.  Asynchronous migrations do not count as required.
	// In combination with EverythingSynchronous = &true, async migrations will be run before
	// Migrate() returns.  In combination with EverythingSynchronous = &false, async migrations
	// will be run in the background.
	ExitIfMigrateNeeded bool `flag:"exit-if-migrate-needed" help:"Return error if migrations are not current"`

	// TreateAsyncAsRequired command line flag / config variable causes asynchronous migrations to be
	// treated like regular migrations from the point of view of --migrate-only, --no-migrate,
	// and --exit-if-migrate-needed.
	EverythingSynchronous *bool `flag:"migrate-all-synchronously" help:"Run async migrations synchronously"`
}

// DefaultOverrides provides default values for Options.Overrides.  DefaultOverrides
// is only used when Options.Overrides is nil.
//
// DefaultOverrides can be filled by nfigure:
//
//	import "github.com/muir/nfigure"
//	registry := nfigure.NewRegistry()
//	request, err := registry.Request(&libschema.DefaultOverrides)
//	err := registry.Configure()
//
// DefaultOverrides can be filled using the "flag" package:
//
//	import "flag"
//	import "github.com/muir/nfigure"
//	nfigure.MustExportToFlagSet(flag.CommandLine, "flag", &libschema.DefautOverrides)
//
var DefaultOverrides OverrideOptions
