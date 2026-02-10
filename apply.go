package libschema

import (
	"context"
	"log"
	"os"

	"github.com/hashicorp/go-multierror"
	"github.com/memsql/errors"

	"github.com/muir/libschema/dgorder"
	"github.com/muir/libschema/internal"
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
					return errors.Wrap(err, "could not open database")
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
				return errors.Errorf("migrations required for %s", d.DBName)
			}
			return d.migrate(ctx, s)
		}(d)
		if err != nil {
			return err
		}
	}
	return err
}

func (d *Database) prepare(ctx context.Context) error {
	var err error
	nodes := make([]dgorder.Node, len(d.migrations))
	for i, migration := range d.migrations {
		for _, ref := range migration.Base().rawAfter {
			after, ok := d.migrationIndex[ref]
			if !ok {
				return errors.Errorf("migration %s for %s is supposed to be after %s for %s but that cannot be found",
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
				"database": d.DBName,
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
			"database": d.DBName,
		})
		return nil
	}

	if d.Options.OnMigrationsStarted != nil {
		d.Options.OnMigrationsStarted(d)
	}

	d.log.Info("Starting migrations", map[string]interface{}{
		"database": d.DBName,
	})

	lastUnfishedSyncronous := d.lastUnfinishedSynchrnous()

	syncExecuted := make([]Migration, 0, len(d.sequence))
	for i, m := range d.sequence {
		if m.Base().Status().Done {
			if d.Options.DebugLogging {
				d.log.Trace("Migration already done", map[string]interface{}{
					"database": d.DBName,
					"library":  m.Base().Name.Library,
					"name":     m.Base().Name.Name,
				})
			}

			continue
		}
		if m.Base().async && i > lastUnfishedSyncronous && !s.options.Overrides.EverythingSynchronous {
			// This and all following migrations are async
			d.log.Info("The remaining migrations are async starting from", map[string]interface{}{
				"database": d.DBName,
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
		if err != nil {
			d.reportSequenceError(syncExecuted, err, "sychronous")
		} else {
			syncExecuted = append(syncExecuted, m)
		}
		if err != nil || stop {
			return err
		}
	}
	return nil
}

func (d *Database) reportSequenceError(migrations []Migration, err error, syncOrAsync string) {
	if d.Options.DebugLogging {
		// this output would be redundant
		return
	}
	// report migrations that succeeded
	if len(migrations) > 0 {
		d.log.Info("migrations succeeded before eventual failre", map[string]any{
			"number_succeeded": len(migrations),
		})
		for i := 0; i < len(migrations); i++ {
			m := migrations[i].Base()
			d.log.Info("migration success", map[string]any{
				"name":            string(m.Name.Name),
				"library":         string(m.Name.Library),
				"sequence_number": i + 1,
			},
				m.Notes(),
			)
		}
	}
}

// doOneMigration returns true if migrations should stop
func (d *Database) doOneMigration(ctx context.Context, m Migration) (bool, error) {
	if d.Options.DebugLogging {
		d.log.Debug("Starting migration", map[string]interface{}{
			"database": d.DBName,
			"library":  m.Base().Name.Library,
			"name":     m.Base().Name.Name,
		})
	}
	if err := d.driver.IsMigrationSupported(d, d.log, m); err != nil {
		d.log.Info(" migration not supported", map[string]any{
			"database": d.DBName,
			"library":  m.Base().Name.Library,
			"name":     m.Base().Name.Name,
			"error":    err.Error(),
		})
		m.Base().SetNote("applied", false)
		m.Base().SetNote("reason", "not supported")
		m.Base().SetNote("error", err)
		return false, err
	}
	if m.Base().skipIf != nil {
		skip, err := m.Base().skipIf()
		if err != nil {
			return false, errors.Wrapf(err, "skipIf %s", m.Base().Name)
		}
		if skip {
			d.log.Debug(" skipping migration", map[string]any{
				"database": d.DBName,
				"library":  m.Base().Name.Library,
				"name":     m.Base().Name.Name,
			})
			m.Base().SetNote("applied", false)
			m.Base().SetNote("reason", "skipped with SkipIf")
			return false, nil
		}
		d.log.Debug(" not skipping migration")
	}
	if m.Base().skipRemainingIf != nil {
		skip, err := m.Base().skipRemainingIf()
		if err != nil {
			return false, errors.Wrapf(err, "skipRemainingIf %s", m.Base().Name)
		}
		if skip {
			d.log.Debug(" skipping remaining migrations", map[string]any{
				"database": d.DBName,
				"library":  m.Base().Name.Library,
				"name":     m.Base().Name.Name,
			})
			m.Base().SetNote("applied", false)
			m.Base().SetNote("reason", "skipped with SkipRemainingIf")
			return true, nil
		}
		d.log.Debug(" not skipping remaining migrations")
	}
	var repeatCount int
	var totalRowsModified int64
	for {
		rowsAffected, err := d.driver.DoOneMigration(ctx, d.log, d, m)
		if err != nil {
			d.log.Info(" migration failed", map[string]interface{}{
				"database": d.DBName,
				"name":     m.Base().Name.Name,
				"library":  m.Base().Name.Library,
				"error":    err.Error(),
			})
			if d.Options.OnMigrationFailure != nil {
				d.Options.OnMigrationFailure(d, m.Base().Name, err)
			}
			m.Base().SetNote("applied", false)
			m.Base().SetNote("reason", "tried and failed")
			m.Base().SetNote("error", err)
			return false, errors.Wrapf(err, "migration %s in library %s failed", m.Base().Name.Name, m.Base().Name.Library)
		}

		if m.Base().repeatUntilNoOp && err == nil {
			totalRowsModified += rowsAffected
			m.Base().SetNote("rows_modified", totalRowsModified)
			if rowsAffected == 0 {
				return false, nil
			}
			repeatCount++
			m.Base().SetNote("repeat_count", repeatCount)
			d.log.Info("migration modified rows, repeating", map[string]interface{}{
				"repeatCount":  repeatCount,
				"rowsModified": rowsAffected,
			})
			continue
		}
		return false, nil
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
	if d.Options.OnMigrationsComplete != nil {
		d.Options.OnMigrationsComplete(d, err)
	}
	if err != nil {
		logInfo := map[string]any{
			"database": d.DBName,
			"error":    err,
		}
		if m != nil {
			logInfo["migration"] = m.Base().Name.Name
			logInfo["library"] = m.Base().Name.Library
		}
		d.log.Info("Migrations failed", logInfo)
		return
	}
	d.log.Info("Migrations complete", map[string]interface{}{
		"database": d.DBName,
	})
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
	var i int
	for i, m = range d.sequence {
		if m.Base().Status().Done {
			continue
		}
		var stop bool
		stop, err = d.doOneMigration(ctx, m)
		if err != nil {
			d.reportSequenceError(d.sequence[0:i+1], err, "async")
		}
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
