package libschema

import (
	"context"
	"log"
	"os"

	"github.com/muir/libschema/internal"

	"github.com/hashicorp/go-multierror"
	"github.com/muir/libschema/dgorder"
	"github.com/pkg/errors"
)

// Migrate runs pending migrations that have been registered as long as
// the command line flags support doing migrations.  We all remaining migrations
// are asynchronous then the remaining migrations will be run in the background.
//
// A lock is held while migrations are in progress so that there is no chance of
// double migrations.
func (s *Schema) Migrate(ctx context.Context) (err error) {
	if s.options.Overrides.MigrateOnly {
		defer func() {
			if err != nil {
				log.Fatalf("Migrations failed: %s", err)
			}
			if internal.TestingMode {
				panic("test exit: migrate only")
			}
			os.Exit(0)
		}()
	}
	if s.options.Overrides.NoMigrate {
		return nil
	}
	todo := s.databaseOrder
	if s.options.Overrides.MigrateDatabase != "" {
		if d, ok := s.databases[s.options.Overrides.MigrateDatabase]; ok {
			todo = []*Database{d}
		} else {
			return errors.Errorf("database '%s' (from command line) not found", s.options.Overrides.MigrateDatabase)
		}
	}
	if s.options.Overrides.MigrateDSN != "" && len(todo) > 1 {
		return errors.Errorf("--migrate-dsn can only be used when there is only one database to migrate")
	}
	for _, d := range todo {
		if len(d.errors) != 0 {
			return multierror.Append(d.errors[0], d.errors[1:]...)
		}
		err := func(d *Database) (finalErr error) {
			if s.options.Overrides.MigrateDSN != "" {
				var err error
				d.db, err = OpenAnyDB(s.options.Overrides.MigrateDSN)
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
			if s.options.Overrides.ErrorIfMigrateNeeded && !d.done(s) {
				return errors.Errorf("Migrations required for %s", d.Name)
			}
			return d.migrate(ctx, s)
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
				"database": d.Name,
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

func (d *Database) done(s *Schema) bool {
	lastUnfishedSyncronous := d.lastUnfinishedSynchrnous()
	for i, m := range d.sequence {
		if m.Base().Status().Done {
			continue
		}
		if m.Base().async && i > lastUnfishedSyncronous && !s.options.Overrides.EverythingSynchronous {
			break
		}
		return false
	}
	if d.Options.ErrorOnUnknownMigrations && len(d.unknownMigrations) > 0 {
		return false
	}
	return true
}

func (d *Database) migrate(ctx context.Context, s *Schema) (err error) {
	if d.Options.ErrorOnUnknownMigrations && len(d.unknownMigrations) > 0 {
		return errors.Errorf("%d unknown migrations, including %s", len(d.unknownMigrations), d.unknownMigrations[0])
	}

	defer func() {
		if !d.asyncInProgress {
			d.allDone(nil, err)
		}
	}()

	if d.done(s) {
		d.log.Info("No migrations needed", map[string]interface{}{
			"database": d.Name,
		})
		return nil
	}

	if d.Options.OnMigrationsStarted != nil {
		d.Options.OnMigrationsStarted(d)
	}

	d.log.Info("Starting migrations", map[string]interface{}{
		"database": d.Name,
	})

	lastUnfishedSyncronous := d.lastUnfinishedSynchrnous()

	for i, m := range d.sequence {
		if m.Base().Status().Done {
			if d.Options.DebugLogging {
				d.log.Trace("Migration already done", map[string]interface{}{
					"database": d.Name,
					"library":  m.Base().Name.Library,
					"name":     m.Base().Name.Name,
				})
			}

			continue
		}
		if m.Base().async && i > lastUnfishedSyncronous && !s.options.Overrides.EverythingSynchronous {
			// This and all following migrations are async
			d.log.Info("The remaining migrations are async starting from", map[string]interface{}{
				"database": d.Name,
				"library":  m.Base().Name.Library,
				"name":     m.Base().Name.Name,
			})
			if !s.options.Overrides.MigrateOnly {
				d.asyncInProgress = true
				go d.asyncMigrate(ctx)
			}
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
			"database": d.Name,
			"library":  m.Base().Name.Library,
			"name":     m.Base().Name.Name,
		})
	}
	if err := d.driver.IsMigrationSupported(d, d.log, m); err != nil {
		d.log.Debug(" migration not supported", map[string]interface{}{
			"error": err.Error(),
		})
		return false, err
	}
	if m.Base().skipIf != nil {
		skip, err := m.Base().skipIf()
		if err != nil {
			return false, errors.Wrapf(err, "SkipIf %s", m.Base().Name)
		}
		if skip {
			d.log.Debug(" skipping migration")
			return false, nil
		}
		d.log.Debug(" not skipping migration")
	}
	if m.Base().skipRemainingIf != nil {
		skip, err := m.Base().skipRemainingIf()
		if err != nil {
			return false, errors.Wrapf(err, "SkipRemainingIf %s", m.Base().Name)
		}
		if skip {
			d.log.Debug(" skipping remaining migrations")
			return true, nil
		}
		d.log.Debug(" not skipping remaining migrations")
	}
	var repeatCount int
	for {
		result, err := d.driver.DoOneMigration(ctx, d.log, d, m)
		if err != nil && d.Options.OnMigrationFailure != nil {
			d.log.Debug(" migration failed", map[string]interface{}{
				"error": err.Error(),
			})
			d.Options.OnMigrationFailure(d, m.Base().Name, err)
		}
		if m.Base().repeatUntilNoOp && err == nil && result != nil {
			ra, err := result.RowsAffected()
			if err != nil {
				return false, err
			}
			if ra == 0 {
				return false, nil
			}
			repeatCount++
			d.log.Info("migration modified rows, repeating", map[string]interface{}{
				"repeatCount":  repeatCount,
				"rowsModified": ra,
			})
			continue
		}
		return false, err
	}
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

// allDone reports status
func (d *Database) allDone(m Migration, err error) {
	if err != nil && m != nil {
		err = errors.Wrapf(err, "Migration %s", m.Base().Name)
	}
	if d.Options.OnMigrationsComplete != nil {
		d.Options.OnMigrationsComplete(d, err)
	}
	if err == nil {
		d.log.Info("Migrations complete", map[string]interface{}{
			"database": d.Name,
		})
	} else {
		d.log.Info("Migrations failed", map[string]interface{}{
			"database": d.Name,
			"error":    err,
		})
	}
}

func (d *Database) asyncMigrate(ctx context.Context) {
	var err error
	var m Migration
	d.log.Info("Starting async migrations")
	defer func() {
		d.asyncInProgress = false
		e := d.unlock()
		if err == nil {
			err = e
		}
		d.allDone(m, err)
		d.log.Info("Done with async migrations")
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
