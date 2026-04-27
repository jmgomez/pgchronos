import std/unittest
import std/options
import chronos
import ../src/pgchronos/types
import ../src/pgchronos/connection
import ../src/pgchronos/query
import ./helpers

suite "NULL handling":
  test "NULL column in query result":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tnull_test")
        discard await conn.exec("CREATE TABLE tnull_test (val text)")
        discard await conn.exec("INSERT INTO tnull_test VALUES (NULL)")
        let row = await conn.queryOne("SELECT val FROM tnull_test")
        check row.isSome
        check row.get[0].isNone
        discard await conn.exec("DROP TABLE tnull_test")
    waitFor test()

  test "NULL param via Option[string]":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tnull_test")
        discard await conn.exec("CREATE TABLE tnull_test (val text)")
        discard await conn.exec("INSERT INTO tnull_test VALUES ($1)",
                                @[none(string)])
        let row = await conn.queryOne("SELECT val FROM tnull_test")
        check row.isSome
        check row.get[0].isNone
        discard await conn.exec("DROP TABLE tnull_test")
    waitFor test()

  test "Mixed NULL and non-NULL in same row":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tnull_test")
        discard await conn.exec("CREATE TABLE tnull_test (a int, b text, c text, d int)")
        discard await conn.exec("INSERT INTO tnull_test VALUES ($1, $2, $3, $4)",
                                @[some("1"), none(string), some("hello"), none(string)])
        let row = await conn.queryOne("SELECT a, b, c, d FROM tnull_test")
        check row.isSome
        let r = row.get
        check r[0] == some("1")
        check r[1].isNone
        check r[2] == some("hello")
        check r[3].isNone
        discard await conn.exec("DROP TABLE tnull_test")
    waitFor test()

  test "All-NULL row":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tnull_test")
        discard await conn.exec("CREATE TABLE tnull_test (a text, b text, c text)")
        discard await conn.exec("INSERT INTO tnull_test VALUES (NULL, NULL, NULL)")
        let row = await conn.queryOne("SELECT a, b, c FROM tnull_test")
        check row.isSome
        for cell in row.get:
          check cell.isNone
        discard await conn.exec("DROP TABLE tnull_test")
    waitFor test()

  test "NULL in different column types":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tnull_test")
        discard await conn.exec("""CREATE TABLE tnull_test (
          i int, t text, b bool, ts timestamp)""")
        discard await conn.exec("INSERT INTO tnull_test VALUES (NULL, NULL, NULL, NULL)")
        let row = await conn.queryOne("SELECT i, t, b, ts FROM tnull_test")
        check row.isSome
        for cell in row.get:
          check cell.isNone
        discard await conn.exec("DROP TABLE tnull_test")
    waitFor test()

  test "COUNT of NULLs":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tnull_test")
        discard await conn.exec("CREATE TABLE tnull_test (val text)")
        discard await conn.exec("INSERT INTO tnull_test VALUES ('a'), (NULL), ('b'), (NULL)")
        let countAll = await conn.queryValue("SELECT count(*) FROM tnull_test")
        check countAll == some("4")
        let countVal = await conn.queryValue("SELECT count(val) FROM tnull_test")
        check countVal == some("2")
        discard await conn.exec("DROP TABLE tnull_test")
    waitFor test()
