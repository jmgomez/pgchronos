{.push warning[Deprecated]: off.}
import std/unittest
import std/options
import std/strutils
import chronos
import ../src/pgchronos/types
import ../src/pgchronos/connection
import ../src/pgchronos/query
import ../src/pgchronos/prepared
import ../src/pgchronos/transaction
import ./helpers

suite "Transactions":
  test "Successful transaction commits":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS ttx_test")
        discard await conn.exec("CREATE TABLE ttx_test (val text)")
        await conn.withTransaction:
          discard await conn.exec("INSERT INTO ttx_test VALUES ('committed')")
        let val = await conn.queryValue("SELECT val FROM ttx_test")
        check val == some("committed")
        discard await conn.exec("DROP TABLE ttx_test")
    waitFor test()

  test "Exception causes rollback":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS ttx_test")
        discard await conn.exec("CREATE TABLE ttx_test (val text)")
        try:
          await conn.withTransaction:
            discard await conn.exec("INSERT INTO ttx_test VALUES ('rolled_back')")
            raise newException(ValueError, "oops")
        except ValueError:
          discard
        let val = await conn.queryValue("SELECT count(*) FROM ttx_test")
        check val == some("0")
        discard await conn.exec("DROP TABLE ttx_test")
    waitFor test()

  test "Exception is re-raised":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        var caught = false
        try:
          await conn.withTransaction:
            raise newException(ValueError, "test error")
        except ValueError as e:
          caught = true
          check e.msg == "test error"
        check caught
    waitFor test()

  test "Read-only transaction rejects writes":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS ttx_test")
        discard await conn.exec("CREATE TABLE ttx_test (val text)")
        try:
          await conn.withTransaction(access = taReadOnly):
            discard await conn.exec("INSERT INTO ttx_test VALUES ('nope')")
            fail()
        except PgQueryError as e:
          check e.sqlState == "25006"
        discard await conn.exec("DROP TABLE ttx_test")
    waitFor test()

  test "Serializable isolation":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        await conn.withTransaction(isolation = tiSerializable):
          let val = await conn.queryValue("SELECT 1")
          check val == some("1")
    waitFor test()

  test "Repeatable read isolation":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        await conn.withTransaction(isolation = tiRepeatableRead):
          let val = await conn.queryValue("SELECT 1")
          check val == some("1")
    waitFor test()

  test "Transaction + prepared statement":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS ttx_test")
        discard await conn.exec("CREATE TABLE ttx_test (val text)")
        let stmt = await conn.prepare("tx_ins", "INSERT INTO ttx_test VALUES ($1)")
        await conn.withTransaction:
          discard await stmt.exec(@["hello"])
          discard await stmt.exec(@["world"])
        let count = await conn.queryValue("SELECT count(*) FROM ttx_test")
        check count == some("2")
        await stmt.close()
        discard await conn.exec("DROP TABLE ttx_test")
    waitFor test()

  test "Rollback does not break connection":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        try:
          await conn.withTransaction:
            raise newException(ValueError, "rollback test")
        except ValueError:
          discard
        let val = await conn.queryValue("SELECT 42")
        check val == some("42")
    waitFor test()

  test "Rollback skipped on tainted connection, original error preserved":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      try:
        try:
          await conn.withTransaction:
            conn.close()  # taints the connection
            raise newException(ValueError, "original error")
          fail()
        except ValueError as e:
          # ROLLBACK should be skipped on a tainted conn, so the
          # original error propagates without a misleading "rollback failed" suffix
          check e.msg == "original error"
      finally:
        conn.close()
    waitFor test()

  test "Multiple sequential transactions":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS ttx_test")
        discard await conn.exec("CREATE TABLE ttx_test (val int)")
        for i in 1..3:
          await conn.withTransaction:
            discard await conn.exec("INSERT INTO ttx_test VALUES ($1)", @[$i])
        let count = await conn.queryValue("SELECT count(*) FROM ttx_test")
        check count == some("3")
        discard await conn.exec("DROP TABLE ttx_test")
    waitFor test()
