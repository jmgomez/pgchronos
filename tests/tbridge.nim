import std/unittest
import std/options
import chronos
import db_connector/postgres except PGconn, PGresult
import ../src/pgchronos/types
import ../src/pgchronos/conn
import ../src/pgchronos/bridge
import ../src/pgchronos/query
import ../src/pgchronos/extract
import ./helpers

suite "Bridge":
  test "waitRead completes when fd is readable":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard pqsendQueryParams(conn.rawConn, "SELECT 1", 0, nil, nil, nil, nil, 0)
        discard pqflush(conn.rawConn)
        await conn.waitRead()
        check pqconsumeInput(conn.rawConn) == 1
        while true:
          let res = pqgetResult(conn.rawConn)
          if res.isNil: break
          pqclear(res)
    waitFor test()

  test "waitWrite completes when fd is writable":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        await conn.waitWrite()
        check true
    waitFor test()

  test "flushSend completes for small query":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard pqsendQueryParams(conn.rawConn, "SELECT 1", 0, nil, nil, nil, nil, 0)
        await conn.flushSend()
        check true
        while true:
          if pqconsumeInput(conn.rawConn) == 0: break
          if pqisBusy(conn.rawConn) == 1:
            await conn.waitRead()
            continue
          let res = pqgetResult(conn.rawConn)
          if res.isNil: break
          pqclear(res)
    waitFor test()

  test "waitResult returns valid PGresult":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard pqsendQueryParams(conn.rawConn, "SELECT 42 AS answer", 0, nil, nil, nil, nil, 0)
        await conn.flushSend()
        let raw = await conn.waitResult()
        check not raw.isNil
        check pqresultStatus(raw) == PGRES_TUPLES_OK
        check $pqgetvalue(raw, 0, 0) == "42"
        pqclear(raw)
        await conn.drainResults()
    waitFor test()

  test "drainResults consumes all results":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard pqsendQueryParams(conn.rawConn, "SELECT 1", 0, nil, nil, nil, nil, 0)
        await conn.flushSend()
        let raw = await conn.waitResult()
        check not raw.isNil
        pqclear(raw)
        await conn.drainResults()
        let val = await conn.queryValue("SELECT 2")
        check val == some("2")
    waitFor test()

  test "Multiple sequential waitRead calls":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        for i in 0..<5:
          let val = await conn.queryValue("SELECT $1::int", @[$i])
          check val == some($i)
    waitFor test()
