import std/os
import std/strutils

# Package
version       = "0.2.0"
author        = "jmgomez"
description   = "Async PostgreSQL client for Nim — libpq + chronos"
license       = "MIT"
srcDir        = "src"

# Dependencies
requires "nim >= 2.2.0"
requires "chronos >= 4.0.0"
requires "chronos < 5.0.0"
requires "db_connector >= 0.1.0"

task test, "Run tests":
  let pgConfig = getEnv("PG_CONFIG", "pg_config")
  let (libDirOut, exitCode) = gorgeEx(pgConfig & " --libdir")
  if exitCode != 0:
    quit "Failed to resolve libpq library directory with " & pgConfig
  let libDir = libDirOut.strip()
  exec "nim c -d:test --dynlibOverride:pq " &
    "--passL:\"-L" & libDir & " -Wl,-rpath," & libDir & " -lpq\" " &
    "-r tests/tall.nim"
