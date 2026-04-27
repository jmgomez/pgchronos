import std/unittest
import std/options
import std/strutils
import chronos
import db_connector/postgres except PGconn, PGresult
import ../src/pgchronos/types
import ../src/pgchronos/connection
import ../src/pgchronos/query
import ../src/pgchronos/prepared
import ../src/pgchronos/pool
import ./helpers

suite "Connection Pool":
  test "Pool creates minSize connections":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 3, maxSize = 5)
      check pool.idleCount == 3
      await pool.close()
    waitFor test()

  test "Acquire returns a connection":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 5)
      let conn = await pool.acquire()
      check conn.rawConn != nil
      await pool.release(conn)
      await pool.close()
    waitFor test()

  test "Release returns conn to idle":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 5)
      let c = await pool.acquire()
      check pool.idleCount == 0
      await pool.release(c)
      check pool.idleCount == 1
      await pool.close()
    waitFor test()

  test "withConn acquires and releases":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 5)
      pool.withConn conn:
        let val = await conn.queryValue("SELECT 1")
        check val == some("1")
      check pool.idleCount == 1
      await pool.close()
    waitFor test()

  test "withConn releases on exception":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 5)
      try:
        pool.withConn conn:
          raise newException(ValueError, "oops")
      except ValueError:
        discard
      check pool.idleCount == 1
      await pool.close()
    waitFor test()

  test "Pool grows to maxSize under load":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 5)
      var conns: seq[PgConn]
      for i in 0..<5:
        conns.add await pool.acquire()
      check pool.activeCount == 5
      check pool.idleCount == 0
      for c in conns:
        await pool.release(c)
      await pool.close()
    waitFor test()

  test "Acquire timeout raises PgPoolError":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 1,
                               acquireTimeout = milliseconds(100))
      let c = await pool.acquire()
      try:
        let c2 = await pool.acquire()
        await pool.release(c2)
        fail()
      except PgPoolError:
        check true
      await pool.release(c)
      await pool.close()
    waitFor test()

  test "Pool.close rejects new acquires":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 5)
      await pool.close()
      try:
        let c = await pool.acquire()
        c.close()
        fail()
      except PgPoolError:
        check true
    waitFor test()

  test "Pool convenience exec":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 5)
      discard await pool.exec("DROP TABLE IF EXISTS tpool_test")
      discard await pool.exec("CREATE TABLE tpool_test (val text)")
      let affected = await pool.exec("INSERT INTO tpool_test VALUES ('hello')")
      check affected == 1
      discard await pool.exec("DROP TABLE tpool_test")
      await pool.close()
    waitFor test()

  test "Pool convenience query":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 5)
      let res = await pool.query("SELECT generate_series(1, 3)::text AS n")
      check res.rows.len == 3
      await pool.close()
    waitFor test()

  test "Pool convenience queryOne":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 5)
      let row = await pool.queryOne("SELECT 42::text AS answer")
      check row.isSome
      check row.get[0] == some("42")
      await pool.close()
    waitFor test()

  test "Pool convenience queryValue":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 5)
      let val = await pool.queryValue("SELECT 'hello'")
      check val == some("hello")
      await pool.close()
    waitFor test()

  test "Concurrent pool usage":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 2, maxSize = 5)
      var futs: seq[Future[Option[string]]]
      for i in 0..<20:
        futs.add pool.queryValue("SELECT $1::text", @[$i])
      await allFutures(futs)
      for i, f in futs:
        check f.value == some($i)
      await pool.close()
    waitFor test()

  test "Release of closed connection":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 5)
      let c = await pool.acquire()
      c.close()
      await pool.release(c)
      check pool.idleCount == 0
      await pool.close()
    waitFor test()

  test "Pool close closes borrowed connections":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 5)
      let conn = await pool.acquire()
      await pool.close()
      check conn.rawConn == nil
    waitFor test()

  test "Release after pool close does not re-idle connection":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 5)
      let conn = await pool.acquire()
      await pool.close()
      await pool.release(conn)
      check pool.idleCount == 0
    waitFor test()

  test "Acquire skips dead idle connections":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 1)
      let conn = await pool.acquire()
      conn.close()
      await pool.release(conn)
      let conn2 = await pool.acquire()
      check conn2.rawConn != nil
      check pqstatus(conn2.rawConn) == CONNECTION_OK
      await pool.release(conn2)
      await pool.close()
    waitFor test()

  test "Pool reaps extra idle connections above minSize":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 3,
                               idleReapAfter = milliseconds(0))
      let c1 = await pool.acquire()
      let c2 = await pool.acquire()
      await pool.release(c1)
      await pool.release(c2)
      check pool.idleCount == 1
      await pool.close()
    waitFor test()

  test "Pool close fails pending acquires":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 1,
                               acquireTimeout = seconds(5))
      let conn = await pool.acquire()
      let waiter = pool.acquire()
      await sleepAsync(milliseconds(20))
      await pool.close()
      try:
        discard await waiter
        fail()
      except PgPoolError as e:
        check e.msg.contains("closed")
      conn.close()
    waitFor test()

  test "Concurrent acquires do not exceed maxSize":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 0, maxSize = 1)
      let f1 = pool.acquire()
      let f2 = pool.acquire()
      let c1 = await f1
      await sleepAsync(milliseconds(100))
      check not f2.finished
      await pool.release(c1)
      let c2 = await f2
      await pool.release(c2)
      await pool.close()
    waitFor test()

  test "Untracked release does not corrupt pool":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 0, maxSize = 1)
      let foreign = await connect(TestConnStr)
      await pool.release(foreign)
      check pool.idleCount == 0
      let c = await pool.acquire()
      await pool.release(c)
      await pool.close()
    waitFor test()

  test "Pool resets session state on release":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 1)
      pool.withConn conn:
        discard await conn.exec("SET application_name = 'pgchronos_pool_test'")
      pool.withConn conn:
        let val = await conn.queryValue("SELECT current_setting('application_name')")
        check val == some("")
      await pool.close()
    waitFor test()

  test "Prepared statement cannot outlive pool lease":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 1)
      var stmt: PreparedStmt
      pool.withConn conn:
        stmt = await conn.prepare("lease_stmt", "SELECT $1::text")
      try:
        discard await stmt.queryValue(@["hello"])
        fail()
      except PgError as e:
        check e.msg.contains("lease")
      await pool.close()
    waitFor test()

  test "Cancelled pooled query does not poison pool reuse":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 1)
      let fut = pool.query("SELECT pg_sleep(5), 'done'::text AS val")
      await sleepAsync(milliseconds(50))
      await fut.cancelAndWait()
      try:
        discard await fut
        fail()
      except CancelledError:
        discard
      let val = await pool.queryValue("SELECT 'recovered'")
      check val == some("recovered")
      await pool.close()
    waitFor test()
