import std/unittest
import std/options
import std/strutils
import chronos
import ../src/pgchronos/types
import ../src/pgchronos/connection
import ../src/pgchronos/query
import ../src/pgchronos/prepared
import ./helpers

suite "Prepared Statements":
  test "Prepare + execute INSERT":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tprep_test")
        discard await conn.exec("CREATE TABLE tprep_test (id serial PRIMARY KEY, name text)")
        let stmt = await conn.prepare("ins_test", "INSERT INTO tprep_test (name) VALUES ($1)")
        let affected = await stmt.exec(@["Alice"])
        check affected == 1
        let val = await conn.queryValue("SELECT name FROM tprep_test")
        check val == some("Alice")
        await stmt.close()
        discard await conn.exec("DROP TABLE tprep_test")
    waitFor test()

  test "Execute same stmt multiple times":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tprep_test")
        discard await conn.exec("CREATE TABLE tprep_test (id serial PRIMARY KEY, name text)")
        let stmt = await conn.prepare("multi_ins", "INSERT INTO tprep_test (name) VALUES ($1)")
        for name in ["Alice", "Bob", "Carol", "Dave", "Eve"]:
          discard await stmt.exec(@[name])
        let count = await conn.queryValue("SELECT count(*) FROM tprep_test")
        check count == some("5")
        await stmt.close()
        discard await conn.exec("DROP TABLE tprep_test")
    waitFor test()

  test "Prepare + query SELECT":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tprep_test")
        discard await conn.exec("CREATE TABLE tprep_test (id serial PRIMARY KEY, name text, age int)")
        discard await conn.exec("INSERT INTO tprep_test (name, age) VALUES ('Alice', 30), ('Bob', 25)")
        let stmt = await conn.prepare("sel_test", "SELECT name, age FROM tprep_test WHERE age > $1")
        let res = await stmt.query(@["26"])
        check res.rows.len == 1
        check res.rows[0][0] == some("Alice")
        await stmt.close()
        discard await conn.exec("DROP TABLE tprep_test")
    waitFor test()

  test "Close deallocates on server":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        let stmt = await conn.prepare("reuse_name", "SELECT 1")
        await stmt.close()
        let stmt2 = await conn.prepare("reuse_name", "SELECT 2")
        let val = await stmt2.queryValue()
        check val == some("2")
        await stmt2.close()
    waitFor test()

  test "Prepare invalid SQL raises PgQueryError":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        try:
          discard await conn.prepare("bad_sql", "SLECT 1")
          fail()
        except PgQueryError:
          check true
    waitFor test()

  test "Prepare invalid SQL leaves connection reusable":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        try:
          discard await conn.prepare("bad_sql_reuse", "SLECT 1")
          fail()
        except PgQueryError:
          discard
        let val = await conn.queryValue("SELECT 'still_ok'")
        check val == some("still_ok")
    waitFor test()

  test "Prepared statement rejects use after connection close":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      let stmt = await conn.prepare("closed_conn_stmt", "SELECT 1")
      conn.close()
      try:
        discard await stmt.queryValue()
        fail()
      except PgError as e:
        check e.msg.contains("closed")
    waitFor test()

  test "Prepared statement close quotes identifier safely":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        let stmt = await conn.prepare("quote\"semi;name", "SELECT 1")
        await stmt.close()
        let val = await conn.queryValue("SELECT 2")
        check val == some("2")
    waitFor test()

  test "Prepared query fails fast when another operation is in progress":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        let stmt = await conn.prepare("ov_stmt", "SELECT $1::text")
        let slow = conn.query("SELECT pg_sleep(0.2), 'slow'::text AS val")
        await sleepAsync(milliseconds(50))
        try:
          discard await stmt.queryValue(@["fast"])
          fail()
        except PgError as e:
          check e.msg.contains("already in progress")
        let res = await slow
        check res.rows[0][1] == some("slow")
        await stmt.close()
    waitFor test()

  test "Multiple prepared statements on same conn":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tprep_test")
        discard await conn.exec("CREATE TABLE tprep_test (id serial PRIMARY KEY, name text, age int)")
        let ins = await conn.prepare("p_ins", "INSERT INTO tprep_test (name, age) VALUES ($1, $2)")
        let sel = await conn.prepare("p_sel", "SELECT name FROM tprep_test WHERE age = $1")
        let cnt = await conn.prepare("p_cnt", "SELECT count(*) FROM tprep_test")
        discard await ins.exec(@["Alice", "30"])
        discard await ins.exec(@["Bob", "25"])
        let res = await sel.query(@["30"])
        check res.rows.len == 1
        check res.rows[0][0] == some("Alice")
        let count = await cnt.queryValue()
        check count == some("2")
        await ins.close()
        await sel.close()
        await cnt.close()
        discard await conn.exec("DROP TABLE tprep_test")
    waitFor test()

  test "Prepared stmt survives other queries":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        let stmt = await conn.prepare("survivor", "SELECT $1::text")
        discard await conn.queryValue("SELECT 'adhoc1'")
        let val1 = await stmt.queryValue(@["prepared1"])
        check val1 == some("prepared1")
        discard await conn.queryValue("SELECT 'adhoc2'")
        let val2 = await stmt.queryValue(@["prepared2"])
        check val2 == some("prepared2")
        await stmt.close()
    waitFor test()
