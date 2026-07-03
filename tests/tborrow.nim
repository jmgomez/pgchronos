## tborrow.nim — Step 1: withConn (autocommit) + withTxConn (transactional)

import std/unittest
import std/options
import std/strutils
import chronos
import ../src/pgchronos/types
import ../src/pgchronos/query
import ../src/pgchronos/pool
import ../src/pgchronos/envinit
import ../src/pgchronos/borrow
import ./helpers

proc setupTable(pool: PgPool) {.async.} =
  discard await pool.exec("DROP TABLE IF EXISTS tborrow_test")
  discard await pool.exec("CREATE TABLE tborrow_test (val text)")

proc rowCount(pool: PgPool): Future[int] {.async.} =
  let v = await pool.queryValue("SELECT count(*) FROM tborrow_test")
  return parseInt(v.get("0"))

suite "borrow: withConn (autocommit) + withTxConn (transactional)":

  test "ambient withConn autocommits each statement":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 3)
      setPool(pool)
      await setupTable(pool)
      withConn conn:
        discard await conn.exec("INSERT INTO tborrow_test VALUES ('a')")
      # Autocommit: visible immediately in a fresh borrow.
      check (await rowCount(pool)) == 1
      await closePool()
    waitFor test()

  test "explicit-pool withConn autocommits":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 3)
      await setupTable(pool)
      pool.withConn conn:
        discard await conn.exec("INSERT INTO tborrow_test VALUES ('b')")
      check (await rowCount(pool)) == 1
      await pool.close()
    waitFor test()

  test "withTxConn commits on success":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 3)
      await setupTable(pool)
      pool.withTxConn conn:
        discard await conn.exec("INSERT INTO tborrow_test VALUES ('c')")
        discard await conn.exec("INSERT INTO tborrow_test VALUES ('d')")
      check (await rowCount(pool)) == 2
      await pool.close()
    waitFor test()

  test "withTxConn rolls back on exception":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 3)
      await setupTable(pool)
      var raised = false
      try:
        pool.withTxConn conn:
          discard await conn.exec("INSERT INTO tborrow_test VALUES ('e')")
          raise newException(ValueError, "boom")
      except ValueError:
        raised = true
      check raised
      check (await rowCount(pool)) == 0
      # Pool healthy afterwards.
      check (await pool.queryValue("SELECT 1")) == some("1")
      await pool.close()
    waitFor test()

  test "withTxConn rolls back on cancel mid-body":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 2)
      await setupTable(pool)
      let fut = (proc(): Future[void] {.async.} =
        pool.withTxConn conn:
          discard await conn.exec("INSERT INTO tborrow_test VALUES ('f')")
          await sleepAsync(seconds(10))
      )()
      await sleepAsync(milliseconds(60))
      await fut.cancelAndWait()
      try: await fut
      except CancelledError: discard
      await sleepAsync(milliseconds(30))
      check (await rowCount(pool)) == 0
      check pool.stats().active == 0
      await pool.close()
    waitFor test()

  test "early return without commitNow rolls back (RETURN-ABORTS hazard)":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 3)
      await setupTable(pool)
      proc handler() {.async.} =
        pool.withTxConn conn:
          discard await conn.exec("INSERT INTO tborrow_test VALUES ('g')")
          return  # aborts the transaction — writes vanish
      await handler()
      check (await rowCount(pool)) == 0
      await pool.close()
    waitFor test()

  test "commitNow then return commits":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 3)
      await setupTable(pool)
      proc handler() {.async.} =
        pool.withTxConn conn:
          discard await conn.exec("INSERT INTO tborrow_test VALUES ('h')")
          commitNow conn
          return
      await handler()
      check (await rowCount(pool)) == 1
      await pool.close()
    waitFor test()

  test "onBegin hook state visible in body and gone after":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 3)
      await setupTable(pool)
      let onBegin = proc(c: PgConn): Future[void] {.async.} =
        discard await c.exec("SELECT set_config('app.borrow_test', 'xyz', true)")
      var seen = ""
      pool.withTxConn(conn, onBegin):
        seen = (await conn.queryValue("SELECT current_setting('app.borrow_test', true)")).get("")
      check seen == "xyz"
      # Transaction-local setting is gone in a new borrow.
      let after = (await pool.queryValue("SELECT current_setting('app.borrow_test', true)")).get("")
      check after == ""
      await pool.close()
    waitFor test()

  test "withTxConn cancelled at acquire handoff — slot not leaked":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 0, maxSize = 1)
      await setupTable(pool)
      for i in 0 ..< 20:
        let fut = (proc(): Future[void] {.async.} =
          pool.withTxConn conn:
            discard await conn.exec("INSERT INTO tborrow_test VALUES ('x')")
            discard await conn.queryValue("SELECT pg_sleep(0.01)")
        )()
        if i mod 2 == 0:
          await sleepAsync(microseconds(120 * i))
        fut.cancelSoon()
        try: await fut
        except CancelledError: discard
        except CatchableError: discard
      await sleepAsync(milliseconds(50))
      check pool.stats().active == 0
      check pool.stats().acquired == pool.stats().released
      # Nothing committed by any cancelled tx.
      check (await rowCount(pool)) == 0
      await pool.close()
    waitFor test()

  test "ambient withTxConn with onBegin hook (4-arg via getPool)":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 2)
      setPool(pool)
      await setupTable(pool)
      let onBegin = proc(c: PgConn): Future[void] {.async.} =
        discard await c.exec("SELECT set_config('app.amb_test', 'zzz', true)")
      var seen = ""
      getPool().withTxConn(conn, onBegin):
        seen = (await conn.queryValue("SELECT current_setting('app.amb_test', true)")).get("")
        discard await conn.exec("INSERT INTO tborrow_test VALUES ('amb')")
      check seen == "zzz"
      check (await rowCount(pool)) == 1
      await closePool()
    waitFor test()
