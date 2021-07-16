
# libschema/lsmysql - mysql support for libschema

[![GoDoc](https://godoc.org/github.com/muir/libschema?status.png)](https://pkg.go.dev/github.com/muir/libschema/lsmysql)

Install:

	go get github.com/muir/libschema

---

## DDL Transactions

MySQL and MariaDB do not support DDL (Data Definition Language) transactions like
`CREATE TABLE`.  Such commands cause the current transaction to switch to `autocommit`
mode.

The consequence of this is that it is not possible for a schema migration tool,
like libschema, to track if a migration has been applied or not by tracking the status
of a transaction.

When working with MySQL and MariaDB, schema-changing migrations should be done 
separately from data-changing migrations.  Schema-changing transactions that are 
idempotent are safe and require no special handling.

Schema-changing transactions that are not idempotent need to be guarded with conditionals
so that they're skipped if they've already been applied.

Fortunately, `IF EXISTS` and `IF NOT EXISTS` clauses can be most of the DDL statements.

### Conditionals

The DDL statements missing `IF EXISTS` and `IF NOT EXISTS` include:

```sql
ALTER TABLE ...
	ADD CONSTRAINT
	ALTER COLUMN SET SET DEFAULT
	ALTER COLUMN SET DROP DEFAULT
	ADD FULLTEXT 
	ADD SPATIAL
	ADD PERIOD FOR SYSTEM TIME
	ADD {INDEX|KEY} index_name [NOT] INVISIBLE
	DROP PRIMARY KEY
	RENAME COLUMN
	RENAME INDEX
	RENAME KEY
	DISCARD TABLESPACE
	IMPORT TABLESPACE
	COALESCE PARTITION
	REORGANIZE PARTITION
	EXCHANGE PARTITION
	REMOVE PARTITIONING
	DISABLE KEYS
	ENABLE KEYS
```

To help make these conditional, the lsmysql provides some helper functions to easily
check the current database state.

For example:

```go
schema := libschema.NewSchema(ctx, libschema.Options{})

sqlDB, err := sql.Open("mysql", "....")

database, mysql, err := lsmysql.New(logger, "main-db", schema, sqlDB)

database.Migrations("MyLibrary",
	lsmysql.Script("createUserTable", `
		CREATE TAGLE users (
			name	text,
			id	bigint,
			PRIMARY KEY (id)
		) ENGINE=InnoDB`
	}),
	lsmysql.Script("dropUserPK", `
		ALTAR TABLE users
			DROP PRIMARY KEY`,
		libschema.SkipIf(func() (bool, error) {
			hasPK, err := mysql.HasPrimaryKey("users")
			return !hasPK, err
		})),
	)
```

### Some notes on MySQL

While most identifiers (table names, etc) can be `"`quoted`"`, you cannot use quotes around
a schema (database really) name with `CREATE SCHEMA`.

MySQL does not support schemas.  A schema is just a synonym for `DATABASE` in the MySQL world.
This means that it is easier to put migrations tracking table in the same schema (database) as
the rest of the tables.  It also means that to run migration unit tests, the DSN for testing
has to give access to a user that can create and drop databases.

