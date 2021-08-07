
# libschema - schema migration for libraries

[![GoDoc](https://godoc.org/github.com/muir/libschema?status.png)](https://pkg.go.dev/github.com/muir/libschema)
![unit tests](https://github.com/muir/libschema/actions/workflows/Run%20Go%20tests/badge.svg)
![pg tests](https://github.com/muir/libschema/actions/workflows/PostgreSQL%20tests/badge.svg)


Install:

	go get github.com/muir/libschema

---

## Libraries

Libschema provides a way for Go libraries to manage their own migrations.

Trying migrations to libraries supports two things: the first is source code
locality: the migrations can be next to the code that uses the tables that
the migrations address.

The second is support for migrations in third-party libraries.  This is a relatively
unexplored and unsolved problem: how can an open source (or proprietary) library 
specify and maintain a database schema.  Libschema hopes to start solving this problem.

## Register and execute

Migrations are registered:

```go
schema := libschema.NewSchema(ctx, libschema.Options{})

sqlDB, err := sql.Open("postgres", "....")

database, err := lspostgres.New(logger, "main-db", schema, sqlDB)

database.Migrations("MyLibrary",
	lspostgres.Script("createUserTable", `
			CREATE TAGLE users (
				name	text,
				id	bigint
			)`
	}),
	lspostgres.Script("addLastLogin", `
			ALTER TABLE users
				ADD COLUMN last_login timestamp
		`
	}),
)
```

Migrations are then run run later in the order that they were registered.

```go
err := schema.Migrate(context)
```

## Computed Migrations

Migrations may be SQL strings or migrations can be done in Go:

```go
database.Migrations("MyLibrary", 
	lspostgres.Computed("importUsers", func(_ context.Context, _ MyLogger, tx *sql.Tx) error {
		// code to import users here
	}),
)
```

## Asynchronous migrations 

The normal mode for migrations is to run the migrations synchronously when
`schema.Migrate()` is called.  Asynchronous migrations are started when `schema.Migrate()`
is called but they're run in the background in a go-routine.  If there are later
migrations, after the asynchronous migration, they'll force the asynchronous migration
to be synchronous unless they're also asynchronous.

## Version blocking

Migrations can be tied to specific code versions so that they are not run until
conditions are met.  This is done with `SkipRemainingIf`.  This be used to backfill
data.

```go
database.Migrations("MyLibrary",
	...
	lspostgres.Script("addColumn", `
			ALTER TABLE users
				ADD COLUMN rating`,
	libschema.SkipThisAndFollowingIf(func() bool {
		return semver.Compare(version(), "3.11.3") < 1
	})),
	lspostgres.Script("fillInRatings", `
			UPDATE	users
			SET	rating = ...
			WHERE	rating IS NULL;

			ALTER TABLE users
				MODIFY COLUMN rating SET NOT NULL;`,
	libschema.Asychronous),
)
```

## Cross-library dependencies

Although it is best if the schema from one library is independent of the schema for
another, sometimes that's not possible, especially if you want to enforce foriegn key
constraints.

Use `After()` to specify a cross-library dependency.

```go
database.Migrations("users",
	...
	lspostgres.Script("addOrg", `
			ALTER TABLE users
				ADD COLUMN org TEXT,
				ADD ADD CONSTRAINT orgfk FOREIGN KEY (org)
					REFERENCES org (name) `, 
	libschema.After("orgs", "createOrgTable")),
)

database.Migrations("orgs",
	...
	lspostgres.Script("createOrgTable", `
		...
	`),
)
```

## Transactions

For databases that support transactions on metadata, all migrations will be wrapped with
a `BEGIN` and `COMMIT`.  For databases that do not support transactions on metadata, 
migrations will be split into individual commands and run one at a time.  If only some
of the commands succeed, the migration will be marked as partially complete.  If the migration
is revised, then the later parts can be re-tried as long as the earlier parts are not
modified.  This does not apply to `Compute()`ed migrations.

## Command line

Libschema adds command line flags that change the behavior of calling
`schema.Migrate()`
	
	--migrate-only			Call os.Exit() after completing migrations
	--migrate-database		Migrate only the database named by NewDatabase
	--migrate-dsn			Override *sql.DB 
	--no-migrate			Skip all migrations
	--exit-if-migrate-needed	Return error if migrations are not current

## Ordering and pull requests

Migrations are run the order that they're defined.  If the set of migrations is
updated so that there are new migrations that are earlier in the table than migrations
that have already run, this is not considered an error and the new migrations will
be run anyway.  This allows multiple branches of code with migrations to be merged
into a combined branch without hassle.

## Code structure

Registering the migrations before executing them suggests using library singletons.
Library singletons can be supported by using [nserve](https://github.com/muir/nject/nserve) 
or [fx](https://github.com/uber-go/fx).  With nserve, migrations can be given their
own hook.

## Driver inclusion and database support

Like database/sql, libschema requires database-specific drivers.  Include
"github.com/muir/libschema/lspg" for Postgres support and "github.com/muir/libschema/lsmysql"
for Mysql support.

libschema currently supports: PostgreSQL, MySQL.  It is easy to add additional databases.

## Forward only

Libschema does not support reverse migrations.  If you need to fix a migration, fix forward.
The history behind this is that reverse migrations are rarely the right answer for production
systems and the extra work for maintaining reverse migrations is does not have enough of a 
payoff during development to be worth the effort.

One way to get the benefits of reverse migrations for development is to put enough enough
reverse migrations to reverse to the last production schema at the end of the migration 
list but protected by a gateway:

```go
libschema.SkipThisAndRemainingIf(func() bool {
	return os.Getenv("LIBMIGRATE_REVERSE_TO_PROD") != "true"
}),
```

This set of reverse migrations would always be small since it would just be enough to take you
back to the current production release.

