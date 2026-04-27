import std/unittest
import std/options
import chronos
import db_connector/postgres except PGconn, PGresult
import ../src/pgchronos/types
import ../src/pgchronos/connection
import ./helpers

proc connectTest(): Future[bool] {.async.} =
  let conn = await connect(TestConnStr)
  let ok = pqstatus(conn.rawConn) == CONNECTION_OK
  conn.close()
  return ok

suite "Connection":
  test "Connect with keyword=value DSN":
    let res = waitFor connectTest()
    check res == true

  test "Close releases fd and is safe":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      check conn.rawConn != nil
      conn.close()
      check conn.rawConn == nil
      conn.close()
    waitFor test()

  test "Connection to invalid host raises PgError":
    proc test() {.async.} =
      try:
        let conn = await connect("host=192.0.2.1 port=5432 dbname=nope connect_timeout=1")
        conn.close()
        fail()
      except PgError:
        check true
    waitFor test()

  test "Connection to non-existent database raises PgError":
    proc test() {.async.} =
      try:
        let conn = await connect("host=127.0.0.1 port=5432 dbname=pgchronos_nonexistent_db_xyz")
        conn.close()
        fail()
      except PgError:
        check true
    waitFor test()

  test "Multiple concurrent connections":
    proc test() {.async.} =
      var futs: seq[Future[PgConn]]
      for i in 0..<10:
        futs.add connect(TestConnStr)
      await allFutures(futs)
      for f in futs:
        let c = f.value
        check pqstatus(c.rawConn) == CONNECTION_OK
        c.close()
    waitFor test()

  test "Connection is non-blocking":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      check pqisnonblocking(conn.rawConn) == 1
      conn.close()
    waitFor test()
