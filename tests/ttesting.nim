## ttesting.nim — Step 5: envinit precedence + testing.nim helpers

import std/[unittest, os, options]
import chronos
import ../src/pgchronos/types
import ../src/pgchronos/connection
import ../src/pgchronos/query
import ../src/pgchronos/pool
import ../src/pgchronos/envinit
import ../src/pgchronos/testing
import ./helpers

suite "envinit: pool-size precedence (explicit arg beats env)":

  test "explicit size arg wins over POOL_SIZE env":
    putEnv("PGCHRONOS_TSIZE", "7")
    check resolvePoolSize(3, "PGCHRONOS_TSIZE") == 3   # explicit wins
    delEnv("PGCHRONOS_TSIZE")

  test "env used when no explicit size":
    putEnv("PGCHRONOS_TSIZE", "7")
    check resolvePoolSize(0, "PGCHRONOS_TSIZE") == 7
    delEnv("PGCHRONOS_TSIZE")

  test "default 5 when neither set":
    delEnv("PGCHRONOS_TSIZE")
    check resolvePoolSize(0, "PGCHRONOS_TSIZE") == 5

  test "invalid env falls back to default":
    putEnv("PGCHRONOS_TSIZE", "not-a-number")
    check resolvePoolSize(0, "PGCHRONOS_TSIZE") == 5
    delEnv("PGCHRONOS_TSIZE")

suite "envinit: getPool guard + initPool":

  test "getPool raises Defect when uninitialized":
    proc test() {.async.} =
      await closePool()   # ensure ambient pool cleared
      check not hasPool()
      var raised = false
      try:
        discard getPool()
      except Defect:
        raised = true
      check raised
    waitFor test()

  test "initPool installs the ambient pool with the given size":
    proc test() {.async.} =
      let pool = await initPool(TestConnStr, size = 4, minSize = 1)
      check hasPool()
      check getPool().stats().maxSize == 4
      await closePool()
      check not hasPool()
    waitFor test()

suite "testing.nim helpers":

  test "withDbName swaps dbname":
    check withDbName("host=localhost dbname=old port=5432", "new") ==
      "host=localhost dbname=new port=5432"
    check withDbName("host=localhost", "new") == "host=localhost dbname=new"

  test "withRollbackConn isolates writes":
    proc test() {.async.} =
      let pool = await initPool(TestConnStr, size = 2)
      discard await pool.exec("DROP TABLE IF EXISTS ttesting_rb")
      discard await pool.exec("CREATE TABLE ttesting_rb (v int)")
      withRollbackConn conn:
        discard await conn.exec("INSERT INTO ttesting_rb VALUES (1)")
        discard await conn.exec("INSERT INTO ttesting_rb VALUES (2)")
        let inside = (await conn.queryValue("SELECT count(*) FROM ttesting_rb")).get("0")
        check inside == "2"
      # After ROLLBACK the rows are gone.
      let after = (await pool.queryValue("SELECT count(*) FROM ttesting_rb")).get("0")
      check after == "0"
      discard await pool.exec("DROP TABLE ttesting_rb")
      await closePool()
    waitFor test()

  test "ephemeral DB create/use/drop":
    proc test() {.async.} =
      let admin = withDbName(TestConnStr, "postgres")
      let dbName = ephemeralDbName("pgchronos_ttesting")
      await createDatabase(admin, dbName)
      # Connect to the fresh DB and use it.
      let c = await connect(withDbName(TestConnStr, dbName))
      check (await c.queryValue("SELECT current_database()")).get("") == dbName
      c.close()
      await dropDatabase(admin, dbName)
      # It's gone now.
      let check2 = await connect(admin)
      let exists = (await check2.queryValue(
        "SELECT 1 FROM pg_database WHERE datname = $1", @[some(dbName)]))
      check exists.isNone
      check2.close()
    waitFor test()

suite "testing.nim: asyncTest template + DATABASE_URL path":

  test "initPoolFromEnv reads DATABASE_URL":
    proc test() {.async.} =
      putEnv("DATABASE_URL", TestConnStr)
      delEnv("POOL_SIZE")
      let pool = await initPoolFromEnv()
      check hasPool()
      check (await pool.queryValue("SELECT 1")).get("") == "1"
      check pool.stats().maxSize == 5   # default when neither arg nor POOL_SIZE
      await closePool()
      delEnv("DATABASE_URL")
    waitFor test()

  asyncTest "asyncTest template runs an async body":
    let pool = await initPool(TestConnStr, size = 2)
    check (await pool.queryValue("SELECT 42")).get("") == "42"
    await closePool()
