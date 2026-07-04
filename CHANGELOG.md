# Changelog

## 0.3.0

### Added
- Migration runner: `-- migrate:no-transaction` directive. A migration whose
  header carries it runs each statement as its own autocommitted simple query
  (no BEGIN/COMMIT), enabling `CREATE INDEX CONCURRENTLY` and other statements
  PostgreSQL forbids inside a transaction. Such migrations cannot roll back, so
  their statements must be idempotent (`... IF NOT EXISTS`); a mid-file failure
  leaves the file un-recorded and it re-runs. Exposes `splitSqlStatements` and
  `hasNoTransaction`.

## 0.2.0

DB-layer extraction release.

### Fixed
- Connection pool: a `CancelledError` at the `acquire` handoff no longer orphans
  a pool slot. Library borrowers use a guarded acquire + deferred, lease-guarded
  reclaim; a cancelled fresh-connect is no longer counted as a connect failure.

### Added
- `borrow` — `withConn` (autocommit) and `withTxConn` (BEGIN/onBegin/COMMIT,
  `commitNow`, auto-rollback), ambient and explicit-pool forms.
- `repository` — `generateRepository(T)` macro with `{.tenantScoped.}` and
  `{.nullable.}`, returning `DbResult[T]`.
- `params` — `toUntypedParam`/`toNullableParam`/`toPgParam[T]`/`getStr`/`isNull`.
- `migrate` — SQL-first runner (advisory lock, `version`→`filename` compat,
  `-- migrate:skip-if-namespace`, direct-connection contract).
- `envinit` — ambient per-thread pool + `initPoolFromEnv` (explicit arg beats env).
- `testing` — `asyncTest`, `withRollbackConn`, ephemeral-DB helpers.
- `types` — `DbResult`/`DbError`/`classifyPgError`.
- `connection` — `inTransaction` / `transactionStatus` introspection.
- `PoolStats` — `acquired`/`released`/`reclaimed`/`oldestActiveMs`.
- CI matrix building under `--mm:refc` and `--mm:orc` with `--threads:on`.

### Deprecated
- `transaction.nim`'s `await conn.withTransaction` template (use `withTxConn`).
