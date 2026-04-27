import std/unittest
import std/options
import chronos
import ../src/pgchronos/types
import ../src/pgchronos/connection
import ../src/pgchronos/query
import ../src/pgchronos/pool
import ./helpers

suite "Row lifetime safety":
  test "Row survives subsequent query":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        let rowA = await conn.queryOne("SELECT 'first_query'::text AS val")
        let rowB = await conn.queryOne("SELECT 'second_query'::text AS val")
        check rowA.isSome
        check rowB.isSome
        check rowA.get[0] == some("first_query")
        check rowB.get[0] == some("second_query")
    waitFor test()

  test "Row survives connection close":
    proc test() {.async.} =
      var savedRow: Option[Row]
      block:
        let conn = await connect(TestConnStr)
        savedRow = await conn.queryOne("SELECT 'survive_close'::text AS val")
        conn.close()
      check savedRow.isSome
      check savedRow.get[0] == some("survive_close")
    waitFor test()

  test "Rows survive across await boundaries":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        let row = await conn.queryOne("SELECT 'before_sleep'::text AS val")
        await sleepAsync(milliseconds(50))
        check row.isSome
        check row.get[0] == some("before_sleep")
    waitFor test()

  test "Multiple result sets do not interfere":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        let res1 = await conn.query("SELECT 'set1_' || generate_series(1,3)::text AS val")
        let res2 = await conn.query("SELECT 'set2_' || generate_series(1,3)::text AS val")
        check res1.rows[0][0] == some("set1_1")
        check res1.rows[2][0] == some("set1_3")
        check res2.rows[0][0] == some("set2_1")
        check res2.rows[2][0] == some("set2_3")
    waitFor test()

  test "Row from pool.query survives release":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 2)
      var savedRow: Option[Row]
      pool.withConn conn:
        savedRow = await conn.queryOne("SELECT 'pool_survivor'::text AS val")
      check savedRow.isSome
      check savedRow.get[0] == some("pool_survivor")
      await pool.close()
    waitFor test()

  test "Row stored in seq across many queries":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        var rows: seq[Row]
        for i in 0..<100:
          let row = await conn.queryOne("SELECT $1::text AS val", @[$i])
          if row.isSome:
            rows.add(row.get)
        check rows.len == 100
        for i in 0..<100:
          check rows[i][0] == some($i)
    waitFor test()

  test "Row passed to another proc across await":
    proc test() {.async.} =
      proc processRow(row: Row): Future[string] {.async.} =
        await sleepAsync(milliseconds(10))
        return row[0].get("default")
      await withTestConn proc(conn: PgConn) {.async.} =
        let row = await conn.queryOne("SELECT 'passed_around'::text AS val")
        check row.isSome
        let val = await processRow(row.get)
        check val == "passed_around"
    waitFor test()

  test "Concurrent queries rows do not mix":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 3, maxSize = 10)
      var futs: seq[Future[Option[string]]]
      for i in 0..<10:
        futs.add pool.queryValue("SELECT $1::text", @["val_" & $i])
      await allFutures(futs)
      for i in 0..<10:
        check futs[i].value == some("val_" & $i)
      await pool.close()
    waitFor test()
