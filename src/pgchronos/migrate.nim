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
## Features: an advisory lock
## with a configurable `lockId`, `version`→`filename` column-rename compat, the
## `-- migrate:skip-if-namespace` directive for SQL-vendored extensions, and a
## dependency only on pgchronos core (no app repository import).

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
