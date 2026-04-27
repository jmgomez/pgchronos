import std/unittest
import std/options
import std/strutils
import chronos
import ../src/pgchronos/types
import ../src/pgchronos/connection
import ../src/pgchronos/query
import ../src/pgchronos/prepared
import ../src/pgchronos/transaction
import ../src/pgchronos/pool
import ../src/pgchronos/extract
import ./helpers

suite "Integration":
  test "CRUD cycle":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tint_crud")
        discard await conn.exec("CREATE TABLE tint_crud (id serial PRIMARY KEY, name text, age int)")
        discard await conn.exec("INSERT INTO tint_crud (name, age) VALUES ($1, $2)", @["Alice", "30"])
        let row = await conn.queryOne("SELECT name, age FROM tint_crud WHERE name = $1", @["Alice"])
        check row.isSome
        check row.get[0] == some("Alice")
        check row.get[1] == some("30")
        let updated = await conn.exec("UPDATE tint_crud SET age = $1 WHERE name = $2", @["31", "Alice"])
        check updated == 1
        let row2 = await conn.queryOne("SELECT age FROM tint_crud WHERE name = $1", @["Alice"])
        check row2.get[0] == some("31")
        let deleted = await conn.exec("DELETE FROM tint_crud WHERE name = $1", @["Alice"])
        check deleted == 1
        let count = await conn.queryValue("SELECT count(*) FROM tint_crud")
        check count == some("0")
        discard await conn.exec("DROP TABLE tint_crud")
    waitFor test()

  test "Migration pattern":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tint_migrate")
        discard await conn.exec("CREATE TABLE tint_migrate (id serial PRIMARY KEY, name text)")
        discard await conn.exec("ALTER TABLE tint_migrate ADD COLUMN email text")
        discard await conn.exec("CREATE INDEX idx_tint_migrate_email ON tint_migrate (email)")
        discard await conn.exec("INSERT INTO tint_migrate (name, email) VALUES ($1, $2)",
                                @["Alice", "alice@example.com"])
        let row = await conn.queryOne("SELECT name, email FROM tint_migrate")
        check row.isSome
        check row.get[1] == some("alice@example.com")
        discard await conn.exec("DROP TABLE tint_migrate")
    waitFor test()

  test "Pool + transaction":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 2, maxSize = 5)
      discard await pool.exec("DROP TABLE IF EXISTS tint_ptx")
      discard await pool.exec("CREATE TABLE tint_ptx (val int)")
      pool.withConn conn:
        await conn.withTransaction:
          discard await conn.exec("INSERT INTO tint_ptx VALUES (1)")
          discard await conn.exec("INSERT INTO tint_ptx VALUES (2)")
      let count = await pool.queryValue("SELECT count(*) FROM tint_ptx")
      check count == some("2")
      discard await pool.exec("DROP TABLE tint_ptx")
      await pool.close()
    waitFor test()

  test "JSON column roundtrip":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tint_json")
        discard await conn.exec("CREATE TABLE tint_json (data jsonb)")
        let jsonStr = """{"name": "Alice", "tags": ["nim", "pg"]}"""
        discard await conn.exec("INSERT INTO tint_json VALUES ($1::jsonb)", @[jsonStr])
        let val = await conn.queryValue("SELECT data::text FROM tint_json")
        check val.isSome
        check val.get.contains("Alice")
        discard await conn.exec("DROP TABLE tint_json")
    waitFor test()

  test "Boolean column roundtrip":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tint_bool")
        discard await conn.exec("CREATE TABLE tint_bool (val bool)")
        discard await conn.exec("INSERT INTO tint_bool VALUES ($1)", @["true"])
        discard await conn.exec("INSERT INTO tint_bool VALUES ($1)", @["false"])
        let res = await conn.query("SELECT val FROM tint_bool ORDER BY val")
        check res.getBool(0, "val") == some(false)
        check res.getBool(1, "val") == some(true)
        discard await conn.exec("DROP TABLE tint_bool")
    waitFor test()

  test "Integer types roundtrip":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tint_ints")
        discard await conn.exec("CREATE TABLE tint_ints (s smallint, m int, b bigint)")
        discard await conn.exec("INSERT INTO tint_ints VALUES ($1, $2, $3)",
                                @["32000", "2000000000", "9000000000000000000"])
        let res = await conn.query("SELECT s, m, b FROM tint_ints")
        check res.getInt(0, "s") == some(32000'i64)
        check res.getInt(0, "m") == some(2000000000'i64)
        check res.getInt(0, "b") == some(9000000000000000000'i64)
        discard await conn.exec("DROP TABLE tint_ints")
    waitFor test()

  test "Float types roundtrip":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tint_floats")
        discard await conn.exec("CREATE TABLE tint_floats (f4 float4, f8 float8)")
        discard await conn.exec("INSERT INTO tint_floats VALUES ($1, $2)",
                                @["3.14", "2.718281828459045"])
        let res = await conn.query("SELECT f4, f8 FROM tint_floats")
        let f4 = res.getFloat(0, "f4")
        check f4.isSome
        check abs(f4.get - 3.14) < 0.01
        let f8 = res.getFloat(0, "f8")
        check f8.isSome
        check abs(f8.get - 2.718281828459045) < 0.000001
        discard await conn.exec("DROP TABLE tint_floats")
    waitFor test()

  test "Large batch insert":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS tint_batch")
        discard await conn.exec("CREATE TABLE tint_batch (id int, val text)")
        await conn.withTransaction:
          for i in 0..<1000:
            discard await conn.exec("INSERT INTO tint_batch VALUES ($1, $2)",
                                    @[$i, "row_" & $i])
        let count = await conn.queryValue("SELECT count(*) FROM tint_batch")
        check count == some("1000")
        discard await conn.exec("DROP TABLE tint_batch")
    waitFor test()

  test "Concurrent handler simulation":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 3, maxSize = 10)
      discard await pool.exec("DROP TABLE IF EXISTS tint_conc")
      discard await pool.exec("CREATE TABLE tint_conc (id serial PRIMARY KEY, val text)")
      var futs: seq[Future[void]]
      for i in 0..<50:
        proc handler(idx: int) {.async.} =
          pool.withConn conn:
            discard await conn.exec("INSERT INTO tint_conc (val) VALUES ($1)", @["handler_" & $idx])
            let row = await conn.queryOne("SELECT val FROM tint_conc WHERE val = $1",
                                          @["handler_" & $idx])
            check row.isSome
        futs.add handler(i)
      await allFutures(futs)
      let count = await pool.queryValue("SELECT count(*) FROM tint_conc")
      check count == some("50")
      discard await pool.exec("DROP TABLE tint_conc")
      await pool.close()
    waitFor test()

  test "Error recovery in pool context":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 5)
      try:
        pool.withConn conn:
          discard await conn.exec("SLECT 1")
      except PgQueryError:
        discard
      let val = await pool.queryValue("SELECT 'recovered'")
      check val == some("recovered")
      await pool.close()
    waitFor test()

  test "Prepared statement in pool context":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 2)
      pool.withConn conn:
        let stmt = await conn.prepare("pool_prep", "SELECT $1::text")
        let val = await stmt.queryValue(@["hello"])
        check val == some("hello")
        await stmt.close()
      pool.withConn conn:
        let stmt = await conn.prepare("pool_prep", "SELECT $1::text || '!'")
        let val = await stmt.queryValue(@["hi"])
        check val == some("hi!")
        await stmt.close()
      await pool.close()
    waitFor test()

  test "Unicode strings round-trip":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        let val = await conn.queryValue("SELECT $1::text", @["こんにちは世界"])
        check val == some("こんにちは世界")
    waitFor test()

  test "Special characters in strings":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        let special = "back\\slash 'quote' new\nline\ttab"
        let val = await conn.queryValue("SELECT $1::text", @[special])
        check val == some(special)
    waitFor test()
