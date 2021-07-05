package libschema

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/pkg/errors"
)

var migrateOnly = flag.Bool("migrate-only", false, "Call os.Exit() after completing migrations")
var migrateDatabase = flag.String("migrate-database", "", "Migrate only the database named by NewDatabase")
var migrateDSN = flag.String("-migrate-dsn", "", "Override *sql.DB")
var noMigrate = flag.Bool("no-migrate", false, "Skip all migrations")
var exitIfMigrateNeeded = flag.Bool("exit-if-migrate-needed", false, "Return error if migrations are not current")

// Migrate runs pending migrations that have been registered as long as
// the command line flags support doing migrations.
func (s *Schema) Migrate(ctx context.Context) (err error) {
	if *migrateOnly {
		defer func() {
			if err != nil {
				log.Fatalf("Migrations failed: %s", err)
			}
			os.Exit(0)
		}()
	}
	if *noMigrate {
		return nil
	}
	todo := s.databaseOrder
	if *migrateDatabase != "" {
		if d, ok := s.databases[*migrateDatabase]; ok {
			todo = []*Database{d}
		} else {
			return errors.Errorf("database '%s' (from command line) not found", *migrateDatabase)
		}
	}
	if *migrateDSN != "" && len(todo) > 1 {
		return errors.Errorf("--migrate-dsn can only be used when there is only one database to migrate")
	}
	for _, d := range todo {
		err := func(d *Database) error {
			if *migrateDSN != "" {
				var err error
				d.db, err = OpenAnyDB(*migrateDSN)
				if err != nil {
					return errors.Wrap(err, "Could not open database")
				}
			}
			err = d.prepare(ctx)
			if err != nil {
				return errors.Wrap(err, "XXX prepare")
			}
			defer d.unlock()
			if *exitIfMigrateNeeded && !d.done() {
				return errors.Errorf("Migrations required for %s", d.name)
			}
			return errors.Wrap(d.migrate(ctx), "XXX migrate")
		}(d)
		if err != nil {
			return err
		}
	}
	return
}

func (d *Database) prepare(ctx context.Context) error {
	var err error

	nodes := make([]Node, len(d.migrations))
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
	executionOrder, err := DependencyOrder(nodes, func(i int) string {
		return d.migrations[i].Base().Name.String()
	})
	if err != nil {
		return err
	}

	d.sequence = make([]Migration, len(d.migrations))
	for i, e := range executionOrder {
		d.sequence[i] = d.migrations[e]
		fmt.Printf("XXX order %s\n", d.migrations[e].Base().Name)
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
	for _, m := range d.migrations {
		s, ok := d.status[m.Base().Name]
		if !ok {
			return false
		}
		if !s.Done {
			return false
		}
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
			if d.Options.OnMigrationsComplete != nil {
				d.Options.OnMigrationsComplete(err)
			}
		}
	}()

	if d.done() {
		return nil
	}

	if d.Options.OnMigrationsStarted != nil {
		d.Options.OnMigrationsStarted()
	}

	lastUnfishedSyncronous := d.lastUnfinishedSynchrnous()

	for i, m := range d.sequence {
		if m.Base().Status().Done {
			continue
		}
		if m.Base().async && i > lastUnfishedSyncronous {
			// This and all following migrations are async
			d.asyncInProgress = true
			go d.asyncMigrate(ctx)
			return nil
		}
		err = d.driver.DoOneMigration(ctx, d.log, d, m)
		if err != nil {
			return err
		}
	}
	return nil
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

func (d *Database) asyncMigrate(ctx context.Context) {
	var err error
	var m Migration
	defer func() {
		if d.Options.OnMigrationsComplete != nil {
			if m != nil {
				d.Options.OnMigrationsComplete(errors.Wrapf(err, "Migration %s", m.Base().Name))
			} else {
				d.Options.OnMigrationsComplete(err)
			}
		}
	}()
	for _, m = range d.sequence {
		if m.Base().Status().Done {
			continue
		}
		err = d.driver.DoOneMigration(ctx, d.log, d, m)
		if err != nil {
			if d.Options.OnAsyncMigrationFailure != nil {
				d.Options.OnAsyncMigrationFailure(m.Base().Name, err)
			}
			return
		}
	}
}

func (d *Database) unlock() {
	if !d.asyncInProgress {
		d.driver.UnlockMigrationsTable(d.log)
	}
}
