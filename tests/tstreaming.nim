import std/unittest
import std/options
import std/strutils
import chronos
import ../src/pgchronos/types
import ../src/pgchronos/connection
import ../src/pgchronos/query
import ../src/pgchronos/pool
import ./helpers

suite "Streaming (forEachRow)":

  test "forEachRow yields each row":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        var rows: seq[Row]
        let count = await conn.forEachRow("SELECT generate_series(1, 5)::text AS n",
          proc(row: Row) {.async.} =
            rows.add(row)
        )
        check count == 5
        check rows.len == 5
        check rows[0][0] == some("1")
        check rows[4][0] == some("5")
    waitFor test()

  test "forEachRow with params":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tstream_test")
        discard await conn.exec("CREATE TABLE tstream_test (id int, name text)")
        discard await conn.exec("INSERT INTO tstream_test VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
        var names: seq[string]
        let count = await conn.forEachRow(
          "SELECT name FROM tstream_test WHERE id > $1 ORDER BY id", @["1"],
          proc(row: Row) {.async.} =
            names.add(row[0].get)
        )
        check count == 2
        check names == @["Bob", "Carol"]
        discard await conn.exec("DROP TABLE tstream_test")
    waitFor test()

  test "forEachRow large result set":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        var sum: int64 = 0
        let count = await conn.forEachRow(
          "SELECT generate_series(1, 50000)::text AS n",
          proc(row: Row) {.async.} =
            sum += parseInt(row[0].get)
        )
        check count == 50000
        check sum == 1250025000'i64
    waitFor test()

  test "forEachRow empty result":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        var called = false
        let count = await conn.forEachRow(
          "SELECT 1 WHERE false",
          proc(row: Row) {.async.} =
            called = true
        )
        check count == 0
        check not called
    waitFor test()

  test "forEachRow SQL error raises PgQueryError":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        try:
          discard await conn.forEachRow("SLECT 1",
            proc(row: Row) {.async.} = discard
          )
          fail()
        except PgQueryError as e:
          check e.sqlState == "42601"
    waitFor test()

  test "forEachRow connection reusable after":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.forEachRow("SELECT generate_series(1, 3)::text",
          proc(row: Row) {.async.} = discard
        )
        let val = await conn.queryValue("SELECT 'after_stream'")
        check val == some("after_stream")
    waitFor test()

  test "forEachRow connection reusable after error":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        try:
          discard await conn.forEachRow("SLECT 1",
            proc(row: Row) {.async.} = discard
          )
        except PgQueryError:
          discard
        let val = await conn.queryValue("SELECT 'recovered'")
        check val == some("recovered")
    waitFor test()

  test "forEachRow with NULL values":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tstream_test")
        discard await conn.exec("CREATE TABLE tstream_test (a int, b text)")
        discard await conn.exec("INSERT INTO tstream_test VALUES (1, NULL), (NULL, 'hello')")
        var rows: seq[Row]
        discard await conn.forEachRow("SELECT a, b FROM tstream_test ORDER BY a NULLS LAST",
          proc(row: Row) {.async.} =
            rows.add(row)
        )
        check rows.len == 2
        check rows[0][0] == some("1")
        check rows[0][1].isNone
        check rows[1][0].isNone
        check rows[1][1] == some("hello")
        discard await conn.exec("DROP TABLE tstream_test")
    waitFor test()

  test "forEachRow via pool":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 3)
      var rows: seq[Row]
      let count = await pool.forEachRow("SELECT generate_series(1, 10)::text AS n",
        proc(row: Row): Future[void] {.async, gcsafe.} =
          {.gcsafe.}:
            rows.add(row)
      )
      check count == 10
      check rows[0][0] == some("1")
      check rows[9][0] == some("10")
      let val = await pool.queryValue("SELECT 'pool_ok'")
      check val == some("pool_ok")
      await pool.close()
    waitFor test()

  test "forEachRow rows survive callback (deep copy)":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        var saved: seq[Row]
        discard await conn.forEachRow("SELECT generate_series(1, 100)::text AS n",
          proc(row: Row) {.async.} =
            saved.add(row)
        )
        check saved.len == 100
        for i in 0..<100:
          check saved[i][0] == some($(i + 1))
    waitFor test()

  test "forEachRow callback exception stops iteration":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        var count = 0
        try:
          discard await conn.forEachRow("SELECT generate_series(1, 100)::text AS n",
            proc(row: Row) {.async.} =
              count.inc
              if count == 5:
                raise newException(ValueError, "stop at 5")
          )
          fail()
        except ValueError as e:
          check e.msg == "stop at 5"
        check count == 5
        # Connection must be tainted — undrained single-row results
        check conn.isTainted or conn.rawConn.isNil
        try:
          discard await conn.queryValue("SELECT 1")
          fail()
        except PgError:
          check true
    waitFor test()

