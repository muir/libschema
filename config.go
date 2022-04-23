package libschema

import(
	"github.com/muir/nfigure"
)

// OverrideOptions define the command line flags and/or configuration block that can be
// provided to control libschema's behavior.  OverrideOptions is designed to be filled
// with https://github.com/muir/nfigure but it doesn't have to be done that way.
type OverrideOptions struct {
	// MigrateOnly causes program to exit when migrations are complete.
	// Asynchronous migrations may be skipped.  If no migrations are need,
	// the program will exit very quickly.  This is accomplished with a call
	// to os.Exit().
	MigrateOnly bool `flag:"migrate-only" help:"Call os.Exit() after completing migrations"`

	// MigrateDatabase specifies that only a specific database should
	// be migrated.  The name corresponds to the name provided with the schema.NewDatabase() call
	MigrateDatabase bool `flag:"migrate-database" help:"Migrate only the database named by NewDatabase"`

	// MigrateDSN overrides the data source name for a single database.  It must be used in
	// conjunction with MigrateDatabase.
	MigrateDSN string `flag:"migrate-dsn" help:"Override *sql.DB"`

	// NoMigrate command line flag skips all migrations
	NoMigrate bool `flag:"no-migrate" help:"Skip all migrations (except async)"`

	// ExitIfMigrateNeeded command line flag causes the program to exit instead of running
	// required migrations.  Asynchronous migrations do not count as required.
	ExitIfMigrateNeeded `flag:"exit-if-migrate-needed" help:"Return error if migrations are not current"`

	// TreateAsyncAsRequired command line flag causes asynchronous migrations to be
	// treated like regular migrations from the point of view of --migrate-only, --no-migrate,
	// and --exit-if-migrate-needed.
	EverythingSynchronous `flag:"migrate-all-synchronously" help:"Run async migrations synchronously"`
}

// FillOverridesFromCommandLine turns OverrideOptions into go-style command line flags
// using the standard "flag" package.  Call FillOverridesFromCommandLine() before calling
// "flag.Parse()".  FillOverridesFromCommandLine() may be called during init, simply by
// assigning a file-scoped variable:
//
//	var overrideOptions = FillOverridesFromCommandLine(flag.CommandLine) 
//
func FillOverridesFromCommandLine(fs nfigure.FlagSet) {
}

