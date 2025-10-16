# Postgres Idempotent (Non-Transactional) Migrations

Every migration in libschema must fall into one of two safety models:

1. Transactional (atomic) – it runs inside a `BEGIN/COMMIT` so it either fully
    succeeds or is rolled back. Partial progress is never visible.
2. Idempotent (non-transactional) – it must be safe to retry because PostgreSQL
    requires it to run outside an explicit transaction block. Partial progress
    can be visible; therefore the statement (or code) must tolerate being run
    again after a crash.

When you choose (or libschema auto‑detects) the non‑transactional path you are
implicitly asserting: "this migration is idempotent and retry-safe." The
library enforces part of that assertion (single statement scripts; some
idempotency heuristics) and leaves the rest to your careful design.

Libschema supports non-transactional/idempotent migrations in two ways:

1. Automatic detection for common patterns in `Script(...)` (e.g. `CREATE INDEX CONCURRENTLY`).
2. Explicit selection by choosing a generic connection type that does *not* implement
    `Commit()` / `Rollback()` (e.g. `*sql.DB`) when calling `Generate` or `Computed`.

```go
// Auto-detected non-transactional (pattern match on CONCURRENTLY)
lspostgres.Script("add-concurrent-index", `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_users_last_login ON users(last_login)`)

// Explicit non-transactional via generic
lspostgres.Generate[*sql.DB]("add-concurrent-index", func(ctx context.Context, db *sql.DB) string {
    return `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_users_last_login ON users(last_login)`
})

// Transactional (default inference by using *sql.Tx or just omitting the generic):
lspostgres.Generate("create-users", func(ctx context.Context, tx *sql.Tx) string {
    return `CREATE TABLE IF NOT EXISTS users(id bigserial primary key, email text unique)`
})

// Force transactional when you want to override auto-detection by using Generate with *sql.Tx
lspostgres.Generate[*sql.Tx]("alter-type", func(ctx context.Context, tx *sql.Tx) string {
    return `ALTER TYPE my_enum ADD VALUE IF NOT EXISTS 'newval'`
})
```

## Why Some Statements Must Be Non-Transactional (and thus Idempotent)

PostgreSQL rejects certain DDL when executed inside an explicit transaction block
(`BEGIN … COMMIT`) with an error similar to:

> cannot run CREATE INDEX CONCURRENTLY inside a transaction block

These include (PostgreSQL 12+; list not exhaustive):

| Category | Examples |
|----------|----------|
| Concurrent index ops | `CREATE INDEX CONCURRENTLY`, `REINDEX CONCURRENTLY`, `DROP INDEX CONCURRENTLY` |
| Materialized views   | `REFRESH MATERIALIZED VIEW CONCURRENTLY ...` |
| Large maintenance    | `VACUUM FULL`, `CLUSTER`, `REINDEX (without CONCURRENTLY)` (some force commits) |
| Database management  | `CREATE DATABASE`, `ALTER DATABASE ... SET TABLESPACE`, `DROP DATABASE` |
| Replication / slots  | `CREATE SUBSCRIPTION`, `ALTER SUBSCRIPTION` (some restrictions) |
| Some ALTER TYPE ops  | Older Postgres required non-tx for certain `ALTER TYPE ... ADD VALUE` |

Libschema only needs to care about those you reasonably put into application migrations:
usually concurrent index builds and concurrent materialized view refreshes.

## Declaring Idempotent (Non-Transactional) Migrations

Use a generic type argument selecting `*sql.DB` rather than allowing Go to infer `*sql.Tx`:

```go
lspostgres.Computed[*sql.DB]("refresh-mv", func(ctx context.Context, db *sql.DB) error {
    _, err := db.ExecContext(ctx, `REFRESH MATERIALIZED VIEW CONCURRENTLY user_aggregate`)
    return err
})
```

If the generic parameter type implements `Commit()` and `Rollback()` (like `*sql.Tx`),
the migration is considered transactional. Otherwise it is automatically marked non‑transactional.

## Idempotency Requirement (Enforced When Non-Transactional)

Non-transactional migrations must be written so they can be safely retried. Partial application
or process crashes can leave the database in an intermediate state; on restart libschema will
re-attempt the migration if it did not record success.

Guidelines (enforced rules marked *):

1. *Single statement only* for non-transactional script migrations. If you need multiple statements, convert to a `Computed[*sql.DB]` migration and implement your own guards.
2. Prefer IF NOT EXISTS / IF EXISTS modifiers (`CREATE INDEX CONCURRENTLY IF NOT EXISTS`). *For concurrent index create/drop this is enforced.*
3. For materialized views, refreshing concurrently is inherently idempotent; ensure the view definition is stable.
4. For custom computed code, perform explicit existence / state checks before mutating objects.
5. Keep statements retry-safe: a second attempt after partial success must either do nothing or produce the same final state.

Non-idempotent example (bad):
```sql
CREATE INDEX CONCURRENTLY idx_users_email ON users(email); -- no IF NOT EXISTS
```
If this partially fails after creating but before status save, a retry errors out.

Preferred (idempotent):
```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_users_email ON users(email);
```

### Adding a Value to an Enum Type Idempotently

Postgres (>= 12) allows adding enum values with: `ALTER TYPE my_enum ADD VALUE IF NOT EXISTS 'newval';`

If you must schedule this as non-transactional (older Postgres where certain enum additions were non-transactional), write:

```go
lspostgres.Generate[*sql.DB]("add-order-status-canceled", func(ctx context.Context, db *sql.DB) string {
    return `ALTER TYPE order_status ADD VALUE IF NOT EXISTS 'canceled'` // single statement, idempotent
})
```

On very old Postgres versions lacking `IF NOT EXISTS` for enum values, emulate idempotency with a computed migration:

```go
lspostgres.Computed[*sql.DB]("add-order-status-canceled", func(ctx context.Context, db *sql.DB) error {
    const q = `SELECT 1 FROM pg_enum e JOIN pg_type t ON t.oid = e.enumtypid WHERE t.typname = $1 AND e.enumlabel = $2`
    var one int
    err := db.QueryRowContext(ctx, q, "order_status", "canceled").Scan(&one)
    if err == nil { return nil } // already present
    if err != sql.ErrNoRows { return err }
    _, err = db.ExecContext(ctx, `ALTER TYPE order_status ADD VALUE 'canceled'`)
    return err
})
```
This handles existence explicitly and remains safe under retry.

## When NOT to Use Idempotent (Non-Transactional) Mode

Use transactional (default) for:
- DDL that Postgres allows inside a transaction (most CREATE/ALTER TABLE, adding columns, etc.).
- Data backfills or DML batches you want to succeed atomically or to be repeat-until-no-op with row counting.

## Mixing DML and Idempotent (Non-Transactional) DDL

Do not mix data changes that must be atomic with required non-transactional DDL in the same migration.
Separate them into:
1. Transactional migration(s) performing data reshaping.
2. Non-transactional migration for the concurrent index or view refresh.

## RepeatUntilNoOp and Non-Transactional Migrations

`RepeatUntilNoOp` conceptually expects a meaningful `RowsAffected()`. Most non-transactional DDL
returns 0 rows affected; combining them offers no value and may loop unnecessarily.
Avoid mixing these; enforcement may be added.

## Summary Checklist

| Concern | Transactional | Non-Transactional |
|---------|---------------|-------------------|
| Atomic rollback | Yes | No |
| Use case examples | CREATE TABLE, ALTER TABLE ADD COLUMN | CREATE INDEX CONCURRENTLY, REFRESH MV CONCURRENTLY |
| Retry safety needed | Helpful | Critical (must be idempotent) |
| Multi-statement allowed | Yes (still best to keep small) | No (enforced for script form) |

## Future Enhancements
- Broader idempotency heuristics (beyond concurrent index create/drop) or pluggable validators.
- Lint tooling to flag risky patterns (e.g., CREATE INDEX CONCURRENTLY without IF NOT EXISTS inside computed code strings).
- Shared heuristics for other dialects (MySQL / SingleStore) once non-tx support parity is added.

---
Questions or suggestions: open an issue or PR.
