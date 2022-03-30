
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
check some of these:
