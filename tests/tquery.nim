import std/unittest
import std/options
import std/strutils
import chronos
import ../src/pgchronos/types
import ../src/pgchronos/conn
import ../src/pgchronos/query
import ./helpers

suite "Query and Exec":
  test "exec CREATE TABLE":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tquery_test")
        let affected = await conn.exec("""
          CREATE TABLE tquery_test (
            id serial PRIMARY KEY,
            name text NOT NULL,
            age int
          )""")
        check affected == 0
        discard await conn.exec("DROP TABLE tquery_test")
    waitFor test()

  test "exec INSERT returns affected count":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tquery_test")
        discard await conn.exec("CREATE TABLE tquery_test (id serial PRIMARY KEY, name text, age int)")
        let a = await conn.exec("INSERT INTO tquery_test (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Carol', 35)")
        check a == 3
        discard await conn.exec("DROP TABLE tquery_test")
    waitFor test()

  test "exec UPDATE returns affected count":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tquery_test")
        discard await conn.exec("CREATE TABLE tquery_test (id serial PRIMARY KEY, name text, age int)")
        discard await conn.exec("INSERT INTO tquery_test (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Carol', 35), ('Dave', 40), ('Eve', 20)")
        let a = await conn.exec("UPDATE tquery_test SET age = age + 1 WHERE age >= 30")
        check a == 3
        discard await conn.exec("DROP TABLE tquery_test")
    waitFor test()

  test "exec DELETE returns affected count":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tquery_test")
        discard await conn.exec("CREATE TABLE tquery_test (id serial PRIMARY KEY, name text)")
        discard await conn.exec("INSERT INTO tquery_test (name) VALUES ('Alice'), ('Bob')")
        let a = await conn.exec("DELETE FROM tquery_test WHERE name = 'Alice'")
        check a == 1
        discard await conn.exec("DROP TABLE tquery_test")
    waitFor test()

  test "exec with params":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tquery_test")
        discard await conn.exec("CREATE TABLE tquery_test (id serial PRIMARY KEY, name text, age int)")
        let a = await conn.exec("INSERT INTO tquery_test (name, age) VALUES ($1, $2)",
                                @["Alice", "30"])
        check a == 1
        discard await conn.exec("DROP TABLE tquery_test")
    waitFor test()

  test "query SELECT returns rows":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tquery_test")
        discard await conn.exec("CREATE TABLE tquery_test (id serial PRIMARY KEY, name text, age int)")
        discard await conn.exec("INSERT INTO tquery_test (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Carol', 35)")
        let res = await conn.query("SELECT name, age FROM tquery_test ORDER BY name")
        check res.rows.len == 3
        check res.rows[0][0] == some("Alice")
        check res.rows[0][1] == some("30")
        discard await conn.exec("DROP TABLE tquery_test")
    waitFor test()

  test "query returns column names":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tquery_test")
        discard await conn.exec("CREATE TABLE tquery_test (id serial PRIMARY KEY, name text, age int)")
        let res = await conn.query("SELECT id, name, age FROM tquery_test")
        check res.columns == @["id", "name", "age"]
        discard await conn.exec("DROP TABLE tquery_test")
    waitFor test()

  test "query with params":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tquery_test")
        discard await conn.exec("CREATE TABLE tquery_test (id serial PRIMARY KEY, name text, age int)")
        discard await conn.exec("INSERT INTO tquery_test (name, age) VALUES ('Alice', 30), ('Bob', 25)")
        let res = await conn.query("SELECT name, age FROM tquery_test WHERE name = $1", @["Alice"])
        check res.rows.len == 1
        check res.rows[0][0] == some("Alice")
        discard await conn.exec("DROP TABLE tquery_test")
    waitFor test()

  test "query empty result":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tquery_test")
        discard await conn.exec("CREATE TABLE tquery_test (id serial PRIMARY KEY, name text)")
        let res = await conn.query("SELECT name FROM tquery_test WHERE name = 'nobody'")
        check res.rows.len == 0
        discard await conn.exec("DROP TABLE tquery_test")
    waitFor test()

  test "queryOne returns first row":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tquery_test")
        discard await conn.exec("CREATE TABLE tquery_test (id serial PRIMARY KEY, name text)")
        discard await conn.exec("INSERT INTO tquery_test (name) VALUES ('Alice'), ('Bob'), ('Carol')")
        let row = await conn.queryOne("SELECT name FROM tquery_test ORDER BY name")
        check row.isSome
        check row.get[0] == some("Alice")
        discard await conn.exec("DROP TABLE tquery_test")
    waitFor test()

  test "queryOne returns none for empty":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tquery_test")
        discard await conn.exec("CREATE TABLE tquery_test (id serial PRIMARY KEY, name text)")
        let row = await conn.queryOne("SELECT name FROM tquery_test WHERE name = 'nobody'")
        check row.isNone
        discard await conn.exec("DROP TABLE tquery_test")
    waitFor test()

  test "queryValue returns scalar":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tquery_test")
        discard await conn.exec("CREATE TABLE tquery_test (id serial PRIMARY KEY, name text)")
        discard await conn.exec("INSERT INTO tquery_test (name) VALUES ('Alice'), ('Bob'), ('Carol')")
        let val = await conn.queryValue("SELECT count(*) FROM tquery_test")
        check val == some("3")
        discard await conn.exec("DROP TABLE tquery_test")
    waitFor test()

  test "queryValue returns none for empty":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tquery_test")
        discard await conn.exec("CREATE TABLE tquery_test (id serial PRIMARY KEY, name text)")
        let val = await conn.queryValue("SELECT name FROM tquery_test WHERE name = 'nobody'")
        check val.isNone
        discard await conn.exec("DROP TABLE tquery_test")
    waitFor test()

  test "exec invalid SQL raises PgQueryError":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        try:
          discard await conn.exec("SLECT 1")
          fail()
        except PgQueryError as e:
          check e.sqlState == "42601"
    waitFor test()

  test "query invalid SQL raises PgQueryError":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        try:
          discard await conn.query("SLECT 1")
          fail()
        except PgQueryError as e:
          check e.sqlState == "42601"
    waitFor test()

  test "Connection reusable after error":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        try:
          discard await conn.exec("SLECT 1")
        except PgQueryError:
          discard
        let val = await conn.queryValue("SELECT 1")
        check val == some("1")
    waitFor test()

  test "query large result set":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        let res = await conn.query("SELECT generate_series(1, 10000)::text AS n")
        check res.rows.len == 10000
        check res.rows[0][0] == some("1")
        check res.rows[9999][0] == some("10000")
    waitFor test()

  test "Sequential queries on same connection":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        for i in 0..<100:
          let val = await conn.queryValue("SELECT $1::int", @[$i])
          check val == some($i)
    waitFor test()

  test "exec DROP TABLE":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tquery_test")
    waitFor test()

  test "Concurrent ad hoc use on same connection fails fast":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        let slow = conn.query("SELECT pg_sleep(0.2), 'slow'::text AS val")
        await sleepAsync(milliseconds(50))
        try:
          discard await conn.queryValue("SELECT 'fast'")
          fail()
        except PgError as e:
          check e.msg.contains("already in progress")
        let res = await slow
        check res.rows.len == 1
        check res.rows[0][1] == some("slow")
    waitFor test()

  test "Cancelled in-flight query closes direct connection":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      let fut = conn.query("SELECT pg_sleep(5), 'done'::text AS val")
      await sleepAsync(milliseconds(50))
      await fut.cancelAndWait()
      try:
        discard await fut
        fail()
      except CancelledError:
        check conn.rawConn == nil
      try:
        discard await conn.queryValue("SELECT 1")
        fail()
      except PgError as e:
        check e.msg.contains("closed")
    waitFor test()

  test "Multiple statements are rejected or connection remains reusable":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        try:
          discard await conn.query("SELECT 1; SELECT 2")
          fail()
        except PgError:
          discard
        let val = await conn.queryValue("SELECT 'recovered'")
        check val == some("recovered")
    waitFor test()
