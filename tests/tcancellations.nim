import std/unittest
import std/options
import std/strutils
import chronos
import ../src/pgchronos/types
import ../src/pgchronos/conn
import ../src/pgchronos/query
import ../src/pgchronos/pool
import ../src/pgchronos/transaction
import ../src/pgchronos/prepared
import ./helpers

suite "Cancellation Safety":

  test "Cancel query mid-flight — connection is closed/tainted":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      let fut = conn.query("SELECT pg_sleep(10)")
      await sleepAsync(milliseconds(50))
      await fut.cancelAndWait()
      try:
        discard await fut
        fail()
      except CancelledError:
        discard
      # Connection should be tainted/closed after cancellation
      check conn.isTainted or conn.rawConn.isNil
      conn.close()
    waitFor test()

  test "Cancel connect mid-flight — no resource leak":
    proc test() {.async.} =
      # Cancel a connection attempt mid-flight; we should be able to create
      # new connections afterward without running out of fds.
      # Note: connecting to localhost may complete before the cancel fires,
      # so we accept either outcome - the key invariant is no fd leak.
      let fut = connect(TestConnStr)
      await sleepAsync(milliseconds(10))
      await fut.cancelAndWait()
      var cancelledOrCompleted = false
      try:
        let conn = await fut
        # Completed before cancel - clean up normally
        conn.close()
        cancelledOrCompleted = true
      except CancelledError:
        cancelledOrCompleted = true
      check cancelledOrCompleted
      # Verify we can still open new connections after (no fd leak)
      let conn2 = await connect(TestConnStr)
      let val = await conn2.queryValue("SELECT 'ok'")
      check val == some("ok")
      conn2.close()
    waitFor test()

  test "Cancel pool.exec — pool remains healthy":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 2)
      let fut = pool.exec("SELECT pg_sleep(10)")
      await sleepAsync(milliseconds(50))
      await fut.cancelAndWait()
      try:
        discard await fut
        fail()
      except CancelledError:
        discard
      # Pool should still be able to serve new queries
      let val = await pool.queryValue("SELECT 'healthy'")
      check val == some("healthy")
      await pool.close()
    waitFor test()

  test "Cancel pool.query — pool remains healthy":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 2)
      let fut = pool.query("SELECT pg_sleep(10)")
      await sleepAsync(milliseconds(50))
      await fut.cancelAndWait()
      try:
        discard await fut
        fail()
      except CancelledError:
        discard
      let val = await pool.queryValue("SELECT 'still_ok'")
      check val == some("still_ok")
      await pool.close()
    waitFor test()

  test "Cancel withConn body — connection released, pool healthy":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 2)
      let fut = (proc(): Future[void] {.async.} =
        pool.withConn conn:
          await sleepAsync(seconds(10))
      )()
      await sleepAsync(milliseconds(50))
      await fut.cancelAndWait()
      try:
        await fut
        fail()
      except CancelledError:
        discard
      # Pool should still be usable
      let val = await pool.queryValue("SELECT 'withconn_ok'")
      check val == some("withconn_ok")
      await pool.close()
    waitFor test()

  test "Cancel transaction body — data not committed, pool still healthy":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 2)
      discard await pool.exec("DROP TABLE IF EXISTS tcancellations_test")
      discard await pool.exec("CREATE TABLE tcancellations_test (val text)")
      let fut = (proc(): Future[void] {.async.} =
        pool.withConn conn:
          await conn.withTransaction:
            discard await conn.exec("INSERT INTO tcancellations_test VALUES ('should_rollback')")
            await sleepAsync(seconds(10))
      )()
      await sleepAsync(milliseconds(50))
      await fut.cancelAndWait()
      try:
        await fut
        fail()
      except CancelledError:
        discard
      # Data should not have been committed
      let count = await pool.queryValue("SELECT count(*) FROM tcancellations_test")
      check count == some("0")
      discard await pool.exec("DROP TABLE tcancellations_test")
      await pool.close()
    waitFor test()

  test "Cancel pool.acquire while waiting — waiter cleaned up, pool healthy":
    proc test() {.async.} =
      # Exhaust the pool with maxSize=1, then cancel a waiting acquire
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 1,
                               acquireTimeout = seconds(30))
      let held = await pool.acquire()
      let waiterFut = pool.acquire()
      await sleepAsync(milliseconds(50))
      await waiterFut.cancelAndWait()
      try:
        discard await waiterFut
        fail()
      except CancelledError:
        discard
      # Release the held connection
      await pool.release(held)
      # Pool should still be able to serve a new acquire
      let c2 = await pool.acquire()
      check c2.rawConn != nil
      await pool.release(c2)
      await pool.close()
    waitFor test()

  test "Pool healthy after multiple cancellations":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 3)
      # Run several cancellations
      for i in 0..<3:
        let fut = pool.query("SELECT pg_sleep(10)")
        await sleepAsync(milliseconds(30))
        await fut.cancelAndWait()
        try:
          discard await fut
          fail()
        except CancelledError:
          discard
      # Pool should still handle concurrent queries
      var futs: seq[Future[Option[string]]]
      for i in 0..<5:
        futs.add pool.queryValue("SELECT $1::text", @[$i])
      await allFutures(futs)
      for i, f in futs:
        check f.value == some($i)
      await pool.close()
    waitFor test()

  test "Cancel prepared statement exec — connection handled":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 2)
      pool.withConn conn:
        let stmt = await conn.prepare("cancel_stmt", "SELECT pg_sleep($1::float)")
        let fut = stmt.exec(@["10"])
        await sleepAsync(milliseconds(50))
        await fut.cancelAndWait()
        try:
          discard await fut
          fail()
        except CancelledError:
          discard
        # Connection should be tainted after cancellation mid-query
        check conn.isTainted or conn.rawConn.isNil
      # Pool should recover and serve new queries
      let val = await pool.queryValue("SELECT 'prepared_ok'")
      check val == some("prepared_ok")
      await pool.close()
    waitFor test()

  test "CancelledError propagates from pool.queryValue":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 2)
      let fut = pool.queryValue("SELECT pg_sleep(10)")
      await sleepAsync(milliseconds(50))
      fut.cancelSoon()
      var gotCancelled = false
      try:
        discard await fut
        fail()
      except CancelledError:
        gotCancelled = true
      check gotCancelled
      # Pool should still work
      let val = await pool.queryValue("SELECT 'propagated'")
      check val == some("propagated")
      await pool.close()
    waitFor test()

  test "Cancel during transaction — no misleading rollback error on tainted conn":
    proc test() {.async.} =
      # When a query is cancelled mid-flight inside a transaction, the
      # connection gets tainted/closed. The transaction error handler should
      # skip the ROLLBACK attempt on a tainted connection rather than
      # producing a misleading "rollback failed" message.
      let conn = await connect(TestConnStr)
      var caughtMsg = ""
      let fut = (proc(): Future[void] {.async.} =
        await conn.withTransaction:
          discard await conn.exec("SELECT pg_sleep(10)")
      )()
      await sleepAsync(milliseconds(50))
      await fut.cancelAndWait()
      try:
        await fut
      except CancelledError as e:
        caughtMsg = e.msg
      except CatchableError as e:
        caughtMsg = e.msg
      # The error message should NOT contain "rollback failed" since
      # rollback should be skipped on a tainted connection
      check not caughtMsg.contains("rollback failed")
      conn.close()
    waitFor test()

  test "Acquire timeout respected during connect backoff":
    proc test() {.async.} =
      # Simulate backoff state by using a pool pointing to localhost
      # on a port that immediately refuses connections (fast failure).
      # After the first connect failure, the pool enters backoff.
      # A second acquire with a short timeout should respect acquireTimeout.
      let pool = await newPool(
        "host=127.0.0.1 port=1 dbname=nope",
        minSize = 0, maxSize = 1,
        acquireTimeout = milliseconds(200))
      # First acquire: fast connect failure (connection refused)
      try:
        let c = await pool.acquire()
        c.close()
        fail()  # should not succeed
      except PgError:
        discard
      # Now pool is in backoff state. Second acquire should respect timeout.
      let start = Moment.now()
      try:
        let c = await pool.acquire()
        c.close()
        fail()
      except PgPoolError:
        discard
      except PgError:
        discard
      let elapsed = Moment.now() - start
      # Should return within acquireTimeout (200ms) + margin, not full backoff
      check elapsed < seconds(1)
      await pool.close()
    waitFor test()
