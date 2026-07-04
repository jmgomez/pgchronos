## migrate.nim — SQL-first migration runner.
##
## Reads `.sql` files from a directory, applies pending ones in filename order,
## and records them in a `schema_migrations` table. Each file runs in its own
## transaction, so a failure never corrupts the tracking table.
##
## DIRECT-CONNECTION CONTRACT (read this):
##   `pg_advisory_lock` is SESSION-scoped. Through a transaction-mode pooler
##   (e.g. PgBouncer) each statement may land on a different backend, so the
##   lock is silently useless — concurrent runners will NOT serialize. Run
##   migrations on a DIRECT connection (a dedicated non-pooled DSN),
##   never through the runtime pooler DSN. A pooler cannot be reliably detected
##   at runtime, so this is a documented contract, not a runtime check.
##
## Features: an advisory lock with a configurable `lockId`, `version`→`filename`
## column-rename compat, the `-- migrate:skip-if-namespace` (SQL-vendored
## extensions) and `-- migrate:no-transaction` (CREATE INDEX CONCURRENTLY)
## directives, and a dependency only on pgchronos core.

import std/[os, algorithm, strutils, options]
import chronos
import ./types
import ./query

const DefaultMigrationLockId*: int64 = 8675309
  ## Arbitrary advisory-lock key. The id is transient (held only for the run),
  ## so different apps may use different ids harmlessly.

const CreateMigrationsTable = """
  CREATE TABLE IF NOT EXISTS schema_migrations (
    filename    TEXT PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
  )
"""

const SkipIfNamespaceMarker = "-- migrate:skip-if-namespace"

proc parseSkipIfNamespace*(sqlContent: string): string =
  ## A migration may declare, in a leading comment, that it should be skipped
  ## (recorded as applied WITHOUT executing) when a Postgres namespace/schema
  ## already exists. Used for SQL-vendored "extensions" (e.g. pgmq) whose
  ## install DDL is not idempotent against an existing schema. Returns the
  ## namespace, or "" if absent. Directive form:
  ##   -- migrate:skip-if-namespace pgmq
  var checked = 0
  for raw in sqlContent.splitLines():
    let line = raw.strip()
    inc checked
    if line.startsWith(SkipIfNamespaceMarker):
      return line[SkipIfNamespaceMarker.len .. ^1].strip(chars = {' ', '\t', '=', ':'})
    if line.len > 0 and not line.startsWith("--"):
      break
    if checked >= 20:
      break
  return ""

const NoTransactionMarker = "-- migrate:no-transaction"

proc hasNoTransaction*(sqlContent: string): bool =
  ## A migration may declare, in a leading comment, that it must run OUTSIDE a
  ## transaction — needed for statements that PostgreSQL forbids inside a
  ## transaction block, chiefly `CREATE INDEX CONCURRENTLY`. Directive form:
  ##   -- migrate:no-transaction
  ## Such a migration CANNOT be rolled back, so every statement must be
  ## idempotent (`... IF NOT EXISTS`); a mid-file failure leaves the file
  ## un-recorded and it re-runs on the next pass.
  var checked = 0
  for raw in sqlContent.splitLines():
    let line = raw.strip()
    inc checked
    if line.startsWith(NoTransactionMarker):
      return true
    if line.len > 0 and not line.startsWith("--"):
      break
    if checked >= 20:
      break
  return false

proc splitSqlStatements*(sql: string): seq[string] =
  ## Split a SQL string into individual statements on top-level semicolons,
  ## respecting line/block comments, single-quoted strings, and dollar-quoted
  ## strings ($tag$...$tag$). Used by no-transaction migrations, where each
  ## statement must be sent as its own simple query so it autocommits (a
  ## multi-statement simple query runs as ONE implicit transaction, which would
  ## reject CREATE INDEX CONCURRENTLY). Trims whitespace; drops empty pieces.
  result = @[]
  var cur = ""
  var i = 0
  let n = sql.len
  while i < n:
    let c = sql[i]
    if c == '-' and i + 1 < n and sql[i + 1] == '-':
      while i < n and sql[i] != '\n':
        cur.add sql[i]; inc i
    elif c == '/' and i + 1 < n and sql[i + 1] == '*':
      cur.add "/*"; inc i, 2
      while i + 1 < n and not (sql[i] == '*' and sql[i + 1] == '/'):
        cur.add sql[i]; inc i
      if i + 1 < n:
        cur.add "*/"; inc i, 2
    elif c == '\'':
      cur.add c; inc i
      while i < n:
        cur.add sql[i]
        if sql[i] == '\'':
          if i + 1 < n and sql[i + 1] == '\'':
            cur.add sql[i + 1]; inc i, 2
          else:
            inc i; break
        else:
          inc i
    elif c == '$':
      var j = i + 1
      while j < n and (sql[j].isAlphaNumeric or sql[j] == '_'): inc j
      if j < n and sql[j] == '$':
        let tag = sql[i .. j]
        cur.add tag; i = j + 1
        while i < n:
          if sql[i] == '$' and i + tag.len <= n and sql[i ..< i + tag.len] == tag:
            cur.add tag; i += tag.len; break
          cur.add sql[i]; inc i
      else:
        cur.add c; inc i
    elif c == ';':
      let s = cur.strip()
      if s.len > 0: result.add s
      cur = ""; inc i
    else:
      cur.add c; inc i
  let last = cur.strip()
  if last.len > 0: result.add last

proc ensureMigrationsTable(conn: PgConn) {.async.} =
  await conn.simpleExec(CreateMigrationsTable)
  # Compat: an older schema tracked migrations in a `version` column. Rename it
  # to `filename` so both histories converge (harmless if neither is present).
  let hasFilename = await conn.queryValue(
    "SELECT 1 FROM information_schema.columns " &
    "WHERE table_name = 'schema_migrations' AND column_name = 'filename'")
  let hasVersion = await conn.queryValue(
    "SELECT 1 FROM information_schema.columns " &
    "WHERE table_name = 'schema_migrations' AND column_name = 'version'")
  if hasFilename.isNone and hasVersion.isSome:
    discard await conn.exec(
      "ALTER TABLE schema_migrations RENAME COLUMN version TO filename")

proc isApplied(conn: PgConn, filename: string): Future[bool] {.async.} =
  let rowOpt = await conn.queryOne(
    "SELECT 1 FROM schema_migrations WHERE filename = $1", @[some(filename)])
  return rowOpt.isSome

proc recordMigration(conn: PgConn, filename: string) {.async.} =
  discard await conn.exec(
    "INSERT INTO schema_migrations (filename) VALUES ($1)", @[some(filename)])

proc namespaceExists(conn: PgConn, ns: string): Future[bool] {.async.} =
  let r = await conn.queryOne(
    "SELECT 1 FROM pg_namespace WHERE nspname = $1", @[some(ns)])
  return r.isSome

proc runMigrations*(conn: PgConn, migrationsDir: string,
                    lockId: int64 = DefaultMigrationLockId): Future[int] {.async.} =
  ## Apply all pending `.sql` migrations from `migrationsDir` in filename order.
  ## Returns the number newly applied. A session-level advisory lock serializes
  ## concurrent runners (see the DIRECT-CONNECTION CONTRACT above). Each file
  ## runs in its own BEGIN/COMMIT; a failure rolls back that file (not recorded)
  ## and re-raises.
  ##
  ## A file whose header carries `-- migrate:no-transaction` instead runs each
  ## of its statements as a separate autocommitted simple query (no BEGIN/COMMIT)
  ## - required for `CREATE INDEX CONCURRENTLY` and similar. Such a file CANNOT
  ## roll back, so its statements must be idempotent; a mid-file failure leaves
  ## it un-recorded and it re-runs next time.
  discard await conn.exec("SELECT pg_advisory_lock(" & $lockId & ")")
  try:
    await ensureMigrationsTable(conn)

    var files: seq[string] = @[]
    for kind, path in walkDir(migrationsDir):
      if kind == pcFile and path.endsWith(".sql"):
        files.add extractFilename(path)
    files.sort()

    result = 0
    for filename in files:
      if not (await conn.isApplied(filename)):
        let sqlContent = readFile(migrationsDir / filename)
        let skipNs = parseSkipIfNamespace(sqlContent)
        if hasNoTransaction(sqlContent):
          # No-transaction migration: no BEGIN/COMMIT. Each statement is sent as
          # its own autocommitting simple query (a multi-statement simple query
          # would run as one implicit transaction, rejecting CONCURRENTLY). No
          # rollback is possible; a failure propagates and leaves the file
          # un-recorded so it re-runs. Statements must be idempotent.
          if skipNs.len > 0 and (await conn.namespaceExists(skipNs)):
            await conn.recordMigration(filename)
          else:
            for stmt in splitSqlStatements(sqlContent):
              await conn.simpleExec(stmt)
            await conn.recordMigration(filename)
          inc result
        else:
          discard await conn.exec("BEGIN")
          try:
            if skipNs.len > 0 and (await conn.namespaceExists(skipNs)):
              # Schema already present (SQL-vendored extension): record as applied
              # WITHOUT running the non-idempotent install DDL.
              await conn.recordMigration(filename)
            else:
              # Multi-statement migrations require the simple query protocol.
              await conn.simpleExec(sqlContent)
              await conn.recordMigration(filename)
            discard await conn.exec("COMMIT")
            inc result
          except CatchableError as e:
            discard await conn.exec("ROLLBACK")
            raise e
  finally:
    discard await conn.exec("SELECT pg_advisory_unlock(" & $lockId & ")")
