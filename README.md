# pgchronos

Async PostgreSQL client for Nim — [libpq](https://www.postgresql.org/docs/current/libpq.html) + [chronos](https://github.com/status-im/nim-chronos).

libpq handles the wire protocol (auth, SSL, formats); chronos handles fd
scheduling; pgchronos bridges them for non-blocking I/O.

## Install

```
requires "pgchronos >= 0.2.0"
```

Needs a system libpq. Link with `--dynlibOverride:pq --passL:"-L$(pg_config --libdir) -lpq"`.

## Modules

| Module | Purpose |
|---|---|
| `connection` | async connect/close, fd registration, transaction-status introspection (`inTransaction`/`transactionStatus`) |
| `query` | `exec` / `query` / `queryOne` / `queryValue`, `simpleExec` (multi-statement), `forEachRow` streaming |
| `pool` | connection pool: sizing, idle reaping, `stats()`, cancellation-safe guarded acquire |
| `borrow` | `withConn` (autocommit) + `withTxConn` (transaction) borrow templates |
| `prepared` | prepared statement lifecycle |
| `transaction` | conn-level `withTransaction` (**deprecated** — prefer `withTxConn`) |
| `types` | `PgResult`, `Row`, `DbResult[T]`, error types, `classifyPgError` |
| `params` | `toUntypedParam` / `toNullableParam` / `toPgParam[T]` / `getStr` / `isNull` |
| `repository` | `generateRepository(T)` macro — SQL + typed CRUD from annotated types |
| `migrate` | SQL-first migration runner |
| `envinit` | ambient per-thread pool, `initPoolFromEnv` |
| `testing` | test helpers (import `pgchronos/testing` explicitly) |

## Borrowing connections

Two families, all cancellation-safe:

```nim
import pgchronos

let pool = await newPool("postgresql://…")

# Autocommit (each statement commits on its own):
pool.withConn conn:
  discard await conn.exec("INSERT INTO t VALUES ($1)", @[some("x")])

# Transaction (BEGIN → body → COMMIT, auto-rollback on any early exit):
pool.withTxConn conn:
  discard await conn.exec("UPDATE accounts SET bal = bal - 10 WHERE id = $1", @[some("a")])
  discard await conn.exec("UPDATE accounts SET bal = bal + 10 WHERE id = $1", @[some("b")])
```

With an ambient per-thread pool (from `envinit`), drop the `pool.` prefix:

```nim
discard await initPoolFromEnv()   # reads DATABASE_URL, POOL_SIZE
withConn conn: …
withTxConn conn: …
```

### `withTxConn` hazards

* **RETURN ABORTS THE TRANSACTION.** A `return` (or other non-local exit)
  before the implicit COMMIT rolls back everything the body wrote. To persist
  first, call `commitNow conn` before returning.
* An `onBegin(conn)` hook runs inside the transaction right after BEGIN — used
  e.g. for `set_config('app.tenant_id', …)`:
  `pool.withTxConn(conn, myOnBegin): …`.

### Cancellation safety

`withConn` / `withTxConn` and every pool-level helper are safe against a
`CancelledError` delivered at the acquire handoff — a guarded acquire arms a
deferred reclaim so an orphaned slot is returned to the pool. Raw
`pool.acquire()` is **not** handoff-safe; if you must use it, follow the
`acquireGuarded()` + `claim()` pattern, or just use the templates.

## Migrations

`runMigrations(conn, dir)` applies pending `.sql` files in filename order, each
in its own transaction, tracked in `schema_migrations`.

**DIRECT-CONNECTION CONTRACT:** the runner serializes concurrent runs with a
session-scoped `pg_advisory_lock`, which is silently useless through a
transaction-mode pooler (e.g. PgBouncer) — each statement may hit a different
backend. **Run migrations on a direct connection, never the runtime pooler
DSN.** This is a documented contract, not a runtime check (a pooler can't be
reliably detected). A `-- migrate:skip-if-namespace <schema>` leading comment
records a file as applied without executing it when `<schema>` already exists
(for SQL-vendored, non-idempotent extensions).

A `-- migrate:no-transaction` leading comment runs the file's statements
autocommitted (no surrounding BEGIN/COMMIT) so `CREATE INDEX CONCURRENTLY` and
other statements PostgreSQL forbids inside a transaction work. Such migrations
cannot roll back, so every statement must be idempotent (`... IF NOT EXISTS`); a
mid-file failure leaves the file un-recorded and it re-runs.

## Repository macro

```nim
type
  User* {.table: "users".} = object
    id* {.pk.}: string
    email*: string
    firstName* {.column: "first_name".}: string
    note* {.nullable.}: string          # "" ↔ SQL NULL
    createdAt* {.column: "created_at", readOnly.}: string
generateRepository(User)

let r = await conn.dbInsert(User(email: "a@b.c"))   # -> DbResult[User]
if r.isOk: echo r.get.id
```

Add `{.tenantScoped.}` to the type (with a `tenant_id` column) and every CRUD
proc gains a trailing `tenantId` param that stamps inserts/updates and filters
reads/deletes.

## Testing

```
nimble test
```

Requires a local PostgreSQL with a `pgchronos_test` database. Override the DSN
with `PGCHRONOS_TEST_CONNSTR`. CI builds under both `--mm:refc` and `--mm:orc`
with `--threads:on`.
