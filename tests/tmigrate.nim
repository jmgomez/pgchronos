## tmigrate.nim — Step 4: SQL-first migration runner

import std/[unittest, os]
import chronos
import ../src/pgchronos/types
import ../src/pgchronos/connection
import ../src/pgchronos/query
import ../src/pgchronos/migrate
import ./helpers

const
  FixturesDir = currentSourcePath().parentDir() / "fixtures" / "migrations"
  BadDir = currentSourcePath().parentDir() / "fixtures" / "bad_migrations"
  SkipDir = currentSourcePath().parentDir() / "fixtures" / "skip_migrations"
  MultiDir = currentSourcePath().parentDir() / "fixtures" / "multi_migrations"

proc cleanSlate(conn: PgConn) {.async.} =
  await conn.simpleExec("""
    DROP TABLE IF EXISTS schema_migrations CASCADE;
    DROP TABLE IF EXISTS migration_test CASCADE;
    DROP TABLE IF EXISTS multi_test CASCADE;
    DROP SCHEMA IF EXISTS skipns CASCADE;
  """)

suite "migrate: SQL-first runner":

  test "applies pending migrations in filename order":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      await cleanSlate(conn)
      check (await conn.runMigrations(FixturesDir)) == 2
      let qr = await conn.query(
        "SELECT filename FROM schema_migrations ORDER BY applied_at, filename")
      check qr.rows.len == 2
      check qr.rows[0][0].get == "001_create_test.sql"
      check qr.rows[1][0].get == "002_add_column.sql"
      await cleanSlate(conn)
      conn.close()
    waitFor test()

  test "re-running is a no-op":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      await cleanSlate(conn)
      check (await conn.runMigrations(FixturesDir)) == 2
      check (await conn.runMigrations(FixturesDir)) == 0
      await cleanSlate(conn)
      conn.close()
    waitFor test()

  test "invalid SQL rolls back and is not recorded":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      await cleanSlate(conn)
      var raised = false
      try:
        discard await conn.runMigrations(BadDir)
      except CatchableError:
        raised = true
      check raised
      let cnt = (await conn.queryValue(
        "SELECT COUNT(*) FROM schema_migrations")).get("0")
      check cnt == "0"
      # Connection still usable after the rollback.
      check (await conn.queryValue("SELECT 1")).get("") == "1"
      await cleanSlate(conn)
      conn.close()
    waitFor test()

  test "multi-statement migration applies (table + function + index)":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      await cleanSlate(conn)
      check (await conn.runMigrations(MultiDir)) == 1
      check (await conn.queryValue(
        "SELECT 1 FROM information_schema.tables WHERE table_name='multi_test'")).isSome
      check (await conn.queryValue(
        "SELECT 1 FROM pg_proc WHERE proname='multi_test_noop'")).isSome
      await cleanSlate(conn)
      conn.close()
    waitFor test()

  test "skip-if-namespace: skips DDL when schema already exists":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      await cleanSlate(conn)
      await conn.simpleExec("CREATE SCHEMA skipns")   # pre-existing
      check (await conn.runMigrations(SkipDir)) == 1
      # Recorded as applied...
      check (await conn.queryValue(
        "SELECT COUNT(*) FROM schema_migrations WHERE filename='050_skip_test.sql'")).get("0") == "1"
      # ...but the non-idempotent DDL was skipped (no marker table).
      check (await conn.queryValue(
        "SELECT COUNT(*) FROM information_schema.tables " &
        "WHERE table_schema='skipns' AND table_name='marker'")).get("0") == "0"
      await cleanSlate(conn)
      conn.close()
    waitFor test()

  test "skip-if-namespace: runs normally when schema is absent":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      await cleanSlate(conn)
      check (await conn.runMigrations(SkipDir)) == 1
      check (await conn.queryValue(
        "SELECT COUNT(*) FROM information_schema.tables " &
        "WHERE table_schema='skipns' AND table_name='marker'")).get("0") == "1"
      await cleanSlate(conn)
      conn.close()
    waitFor test()

  test "concurrent runners serialize via the advisory lock":
    proc test() {.async.} =
      let setupConn = await connect(TestConnStr)
      await cleanSlate(setupConn)
      setupConn.close()

      var applied1, applied2: int
      proc runner1() {.async.} =
        let c = await connect(TestConnStr)
        applied1 = await c.runMigrations(FixturesDir)
        c.close()
      proc runner2() {.async.} =
        let c = await connect(TestConnStr)
        applied2 = await c.runMigrations(FixturesDir)
        c.close()
      await allFutures(runner1(), runner2())

      # Exactly one runner applied both; the other saw them already applied.
      check applied1 + applied2 == 2
      check (applied1 == 2 and applied2 == 0) or (applied1 == 0 and applied2 == 2)

      let vc = await connect(TestConnStr)
      check (await vc.queryValue("SELECT COUNT(*) FROM schema_migrations")).get("0") == "2"
      await cleanSlate(vc)
      vc.close()
    waitFor test()

  test "custom lockId is honored":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      await cleanSlate(conn)
      check (await conn.runMigrations(FixturesDir, lockId = 42)) == 2
      await cleanSlate(conn)
      conn.close()
    waitFor test()

  test "version->filename column rename compat":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      await cleanSlate(conn)
      # Simulate an older schema that tracked migrations in a `version` column.
      await conn.simpleExec("""
        CREATE TABLE schema_migrations (
          version TEXT PRIMARY KEY,
          applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      """)
      check (await conn.runMigrations(FixturesDir)) == 2
      # Column was renamed, so the runner could record into `filename`.
      check (await conn.queryValue(
        "SELECT 1 FROM information_schema.columns " &
        "WHERE table_name='schema_migrations' AND column_name='filename'")).isSome
      check (await conn.queryValue(
        "SELECT 1 FROM information_schema.columns " &
        "WHERE table_name='schema_migrations' AND column_name='version'")).isNone
      await cleanSlate(conn)
      conn.close()
    waitFor test()

  test "partial multi-statement migration rolls back statement 1 when statement 2 fails":
    proc test() {.async.} =
      let partialDir = currentSourcePath().parentDir() / "fixtures" / "partial_migrations"
      let conn = await connect(TestConnStr)
      await cleanSlate(conn)
      await conn.simpleExec("DROP TABLE IF EXISTS partial_test CASCADE")
      var raised = false
      try:
        discard await conn.runMigrations(partialDir)
      except CatchableError:
        raised = true
      check raised
      # The earlier CREATE/INSERT in the same file must have rolled back.
      check (await conn.queryValue(
        "SELECT 1 FROM information_schema.tables WHERE table_name='partial_test'")).isNone
      # And nothing was recorded.
      check (await conn.queryValue(
        "SELECT COUNT(*) FROM schema_migrations")).get("0") == "0"
      await cleanSlate(conn)
      conn.close()
    waitFor test()
