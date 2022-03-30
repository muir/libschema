package libschema

import (
	"context"
	"flag"
	"log"
	"os"

	"github.com/muir/libschema/dgorder"
	"github.com/pkg/errors"
)

// MigrateOnly command line flag causes the program to exit when migrations are complete.
// Asynchronous migrations may be skipped.
var MigrateOnly = flag.Bool("migrate-only", false, "Call os.Exit() after completing migrations")

// MigrateDatabase command line flag specifies that only a specific database should
// be migrated.  The name corresponds to the name provided with the schema.NewDatabase() call
var MigrateDatabase = flag.String("migrate-database", "", "Migrate only the database named by NewDatabase")

// MigrateDSN overrides the data source name for a single database.  It must be used in
// conjunction with MigrateDatabase.
var MigrateDSN = flag.String("migrate-dsn", "", "Override *sql.DB")

// NoMigrate command line flag skips all migrations
var NoMigrate = flag.Bool("no-migrate", false, "Skip all migrations (except async)")

// ExitIfMigrateNeeded command line flag causes the program to exit instead of running
// required migrations.  Asynchronous migrations do not count as required.
var ExitIfMigrateNeeded = flag.Bool("exit-if-migrate-needed", false, "Return error if migrations are not current")

// TreateAsyncAsRequired command line flag causes asynchronous migrations to be
// treated like regular migrations from the point of view of --migrate-only, --no-migrate,
// and --exit-if-migrate-needed.
var EverythingSynchronous = flag.Bool("migrate-all-synchronously", false, "Run async migrations synchronously")

// Migrate runs pending migrations that have been registered as long as
// the command line flags support doing migrations.  We all remaining migrations
// are asynchronous then the remaining migrations will be run in the background.
//
// A lock is held while migrations are in progress so that there is no chance of
// double migrations.
func (s *Schema) Migrate(ctx context.Context) (err error) {
	if *MigrateOnly {
		defer func() {
			if err != nil {
				log.Fatalf("Migrations failed: %s", err)
			}
			os.Exit(0)
		}()
	}
	if *NoMigrate {
		return nil
	}
	todo := s.databaseOrder
	if *MigrateDatabase != "" {
		if d, ok := s.databases[*MigrateDatabase]; ok {
			todo = []*Database{d}
		} else {
			return errors.Errorf("database '%s' (from command line) not found", *MigrateDatabase)
		}
	}
	if *MigrateDSN != "" && len(todo) > 1 {
		return errors.Errorf("--migrate-dsn can only be used when there is only one database to migrate")
	}
	for _, d := range todo {
		err := func(d *Database) (finalErr error) {
			if *MigrateDSN != "" {
				var err error
				d.db, err = OpenAnyDB(*MigrateDSN)
				if err != nil {
					return errors.Wrap(err, "Could not open database")
				}
			}
			err = d.prepare(ctx)
			if err != nil {
				return err
			}
			defer func() {
				err := d.unlock()
				if err != nil && finalErr == nil {
					finalErr = err
				}
			}()
			if *ExitIfMigrateNeeded && !d.done() {
				return errors.Errorf("Migrations required for %s", d.name)
			}
			return d.migrate(ctx)
		}(d)
		if err != nil {
			return err
		}
	}
	return
}

func (d *Database) prepare(ctx context.Context) error {
	var err error
	nodes := make([]dgorder.Node, len(d.migrations))
	for i, migration := range d.migrations {
		for _, ref := range migration.Base().rawAfter {
			after, ok := d.migrationIndex[ref]
			if !ok {
				return errors.Errorf("Migration %s for %s is supposed to be after %s for %s but that cannot be found",
					migration.Base().Name.Name, migration.Base().Name.Library, ref.Name, ref.Library)
			}
			nodes[after.Base().order].Blocking = append(nodes[after.Base().order].Blocking, migration.Base().order)
		}
		if i < len(d.migrations)-1 && migration.Base().Name.Library == d.migrations[i+1].Base().Name.Library {
			nodes[i].Blocking = append(nodes[i].Blocking, i+1)
		}

	}
	executionOrder, err := dgorder.Order(nodes, func(i int) string {
		return d.migrations[i].Base().Name.String()
	})
	if err != nil {
		return err
	}

	d.sequence = make([]Migration, len(d.migrations))
	for i, e := range executionOrder {
		m := d.migrations[e]
		d.sequence[i] = m
		if d.Options.DebugLogging {
			d.log.Debug("Migration sequence", map[string]interface{}{
				"database": d.name,
				"library":  m.Base().Name.Library,
				"name":     m.Base().Name.Name,
			})
		}
	}

	err = d.driver.CreateSchemaTableIfNotExists(ctx, d.log, d)
	if err != nil {
		return err
	}

	err = d.driver.LockMigrationsTable(ctx, d.log, d)
	if err != nil {
		return err
	}

	d.unknownMigrations, err = d.driver.LoadStatus(ctx, d.log, d)
	if err != nil {
		return err
	}

	return nil
}

func (d *Database) done() bool {
	lastUnfishedSyncronous := d.lastUnfinishedSynchrnous()
	for i, m := range d.sequence {
		if m.Base().Status().Done {
			continue
		}
		if m.Base().async && i > lastUnfishedSyncronous && !*EverythingSynchronous {
			break
		}
		return false
	}
	if d.Options.ErrorOnUnknownMigrations && len(d.unknownMigrations) > 0 {
		return false
	}
	return true
}

func (d *Database) migrate(ctx context.Context) (err error) {
	if d.Options.ErrorOnUnknownMigrations && len(d.unknownMigrations) > 0 {
		return errors.Errorf("%d unknown migrations, including %s", len(d.unknownMigrations), d.unknownMigrations[0])
	}

	defer func() {
		if !d.asyncInProgress {
			d.allDone(nil, err)
		}
	}()

	if d.done() {
		d.log.Info("No migrations needed", map[string]interface{}{
			"database": d.name,
		})
		return nil
	}

	if d.Options.OnMigrationsStarted != nil {
		d.Options.OnMigrationsStarted()
	}

	d.log.Info("Starting migrations", map[string]interface{}{
		"database": d.name,
	})

	lastUnfishedSyncronous := d.lastUnfinishedSynchrnous()

	for i, m := range d.sequence {
		if m.Base().Status().Done {
			if d.Options.DebugLogging {
				d.log.Trace("Migration already done", map[string]interface{}{
					"database": d.name,
					"library":  m.Base().Name.Library,
					"name":     m.Base().Name.Name,
				})
			}

			continue
		}
		if m.Base().async && i > lastUnfishedSyncronous && !*EverythingSynchronous {
			// This and all following migrations are async
			d.log.Info("The remaining migrations are async starting from", map[string]interface{}{
				"database": d.name,
				"library":  m.Base().Name.Library,
				"name":     m.Base().Name.Name,
			})
			d.asyncInProgress = true
			go d.asyncMigrate(ctx)
			return nil
		}
		var stop bool
		stop, err = d.doOneMigration(ctx, m)
		if err != nil || stop {
			return err
		}
	}
	return nil
}

func (d *Database) doOneMigration(ctx context.Context, m Migration) (bool, error) {
	if d.Options.DebugLogging {
		d.log.Debug("Starting migration", map[string]interface{}{
			"database": d.name,
			"library":  m.Base().Name.Library,
			"name":     m.Base().Name.Name,
		})
	}
	if m.Base().skipIf != nil {
		skip, err := m.Base().skipIf()
		if err != nil {
			return false, errors.Wrapf(err, "SkipIf %s", m.Base().Name)
		}
		if skip {
			return false, nil
		}
	}
	if m.Base().skipRemainingIf != nil {
		skip, err := m.Base().skipRemainingIf()
		if err != nil {
			return false, errors.Wrapf(err, "SkipRemainingIf %s", m.Base().Name)
		}
		if skip {
			return true, nil
		}
	}
	err := d.driver.DoOneMigration(ctx, d.log, d, m)
	if err != nil && d.Options.OnMigrationFailure != nil {
		d.Options.OnMigrationFailure(m.Base().Name, err)
	}
	return false, err
}

func (d *Database) lastUnfinishedSynchrnous() int {
	for i := len(d.sequence) - 1; i >= 0; i-- {
		m := d.sequence[i]
		s, ok := d.status[m.Base().Name]
		if ok && s.Done {
			continue
		}
		if m.Base().async {
			continue
		}
		return i
	}
	return -1
}

func (d *Database) allDone(m Migration, err error) {
	if err != nil && m != nil {
		err = errors.Wrapf(err, "Migration %s", m.Base().Name)
	}
	if d.Options.OnMigrationsComplete != nil {
		d.Options.OnMigrationsComplete(err)
	}
	if err == nil {
		d.log.Info("Migrations complete", map[string]interface{}{
			"database": d.name,
		})
	} else {
		d.log.Info("Migrations failed", map[string]interface{}{
			"database": d.name,
			"error":    err,
		})
	}
}

func (d *Database) asyncMigrate(ctx context.Context) {
	var err error
	var m Migration
	defer func() {
		d.allDone(m, err)
	}()
	for _, m = range d.sequence {
		if m.Base().Status().Done {
			continue
		}
		var stop bool
		stop, err = d.doOneMigration(ctx, m)
		if err != nil || stop {
			return
		}
	}
}

func (d *Database) unlock() error {
	if !d.asyncInProgress {
		return d.driver.UnlockMigrationsTable(d.log)
	}
	return nil
}
