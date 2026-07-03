## testing.nim — test-support helpers for pgchronos-backed apps.
##
## * asyncTest        — `test` that runs an async body via waitFor.
## * withRollbackConn — borrow from the ambient pool, BEGIN, run body, ROLLBACK,
##                      release. Per-test isolation without truncating tables.
## * ephemeral DB     — create/drop a throwaway database (`<prefix>_<pid>`) for a
##                      run, plus conn-string helpers to point at it.
##
## Apps keep their own seeding and truncate lists; this module only provides the
## generic scaffolding.

import std/[os, strutils, unittest]
import chronos
import ./types
import ./connection
import ./query
import ./pool
import ./envinit

export unittest

template asyncTest*(name: string, body: untyped) =
  ## A `test` whose body is awaited. Mirrors the apps' asyncTest.
  test name:
    waitFor (proc() {.async.} = body)()

template withRollbackConn*(conn, body: untyped) =
  ## Borrow from the ambient pool inside BEGIN/…/ROLLBACK so every write in
  ## `body` is discarded — cheap per-test isolation. Cancellation-safe.
  block:
    let p = getPool()
    let conn = await p.acquireGuarded()
    p.claim(conn)
    try:
      discard await conn.exec("BEGIN")
      try:
        body
      finally:
        discard await noCancel(conn.exec("ROLLBACK"))
    finally:
      await noCancel(p.release(conn))

proc withDbName*(connStr, dbName: string): string =
  ## Return `connStr` with its `dbname=` swapped to `dbName` (appended if absent).
  if "dbname=" in connStr:
    var parts: seq[string]
    for tok in connStr.splitWhitespace():
      if tok.startsWith("dbname="):
        parts.add "dbname=" & dbName
      else:
        parts.add tok
    parts.join(" ")
  else:
    connStr.strip() & " dbname=" & dbName

proc ephemeralDbName*(prefix = "pgchronos_test"): string =
  ## A per-process database name, e.g. `pgchronos_test_48213`.
  prefix & "_" & $getCurrentProcessId()

proc quoteIdent(name: string): string =
  ## Minimal identifier quoting for CREATE/DROP DATABASE (test-only, controlled
  ## names). Rejects anything but a safe identifier to avoid surprises.
  for c in name:
    if not (c.isAlphaNumeric or c == '_'):
      raise newException(ValueError, "unsafe database identifier: " & name)
  "\"" & name & "\""

proc createDatabase*(adminConnStr, dbName: string) {.async.} =
  ## CREATE DATABASE `dbName` via a connection to `adminConnStr` (a DSN pointing
  ## at an existing maintenance DB such as `postgres`). Drops any prior copy.
  let conn = await connect(adminConnStr)
  try:
    await conn.simpleExec("DROP DATABASE IF EXISTS " & quoteIdent(dbName))
    await conn.simpleExec("CREATE DATABASE " & quoteIdent(dbName))
  finally:
    conn.close()

proc dropDatabase*(adminConnStr, dbName: string) {.async.} =
  ## DROP DATABASE `dbName` (IF EXISTS) via `adminConnStr`.
  let conn = await connect(adminConnStr)
  try:
    await conn.simpleExec("DROP DATABASE IF EXISTS " & quoteIdent(dbName))
  finally:
    conn.close()
