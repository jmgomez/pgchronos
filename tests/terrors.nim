import std/unittest
import std/options
import chronos
import ../src/pgchronos/types
import ../src/pgchronos/conn
import ../src/pgchronos/query
import ./helpers

suite "Error handling":
  test "Syntax error with SQLSTATE 42601":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        try:
          discard await conn.exec("SLECT 1")
          fail()
        except PgQueryError as e:
          check e.sqlState == "42601"
          check e.severity == "ERROR"
    waitFor test()

  test "Unique violation SQLSTATE 23505":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS terr_test")
        discard await conn.exec("CREATE TABLE terr_test (id int UNIQUE)")
        discard await conn.exec("INSERT INTO terr_test VALUES (1)")
        try:
          discard await conn.exec("INSERT INTO terr_test VALUES (1)")
          fail()
        except PgQueryError as e:
          check e.sqlState == "23505"
          check e.detail.len > 0
        discard await conn.exec("DROP TABLE terr_test")
    waitFor test()

  test "Not-null violation SQLSTATE 23502":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS terr_test")
        discard await conn.exec("CREATE TABLE terr_test (val text NOT NULL)")
        try:
          discard await conn.exec("INSERT INTO terr_test VALUES (NULL)")
          fail()
        except PgQueryError as e:
          check e.sqlState == "23502"
        discard await conn.exec("DROP TABLE terr_test")
    waitFor test()

  test "Foreign key violation SQLSTATE 23503":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS terr_child")
        discard await conn.exec("DROP TABLE IF EXISTS terr_parent")
        discard await conn.exec("CREATE TABLE terr_parent (id int PRIMARY KEY)")
        discard await conn.exec("CREATE TABLE terr_child (parent_id int REFERENCES terr_parent(id))")
        try:
          discard await conn.exec("INSERT INTO terr_child VALUES (999)")
          fail()
        except PgQueryError as e:
          check e.sqlState == "23503"
        discard await conn.exec("DROP TABLE terr_child")
        discard await conn.exec("DROP TABLE terr_parent")
    waitFor test()

  test "Check constraint SQLSTATE 23514":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS terr_test")
        discard await conn.exec("CREATE TABLE terr_test (age int CHECK (age > 0))")
        try:
          discard await conn.exec("INSERT INTO terr_test VALUES (-1)")
          fail()
        except PgQueryError as e:
          check e.sqlState == "23514"
        discard await conn.exec("DROP TABLE terr_test")
    waitFor test()

  test "Division by zero SQLSTATE 22012":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        try:
          discard await conn.query("SELECT 1/0")
          fail()
        except PgQueryError as e:
          check e.sqlState == "22012"
    waitFor test()

  test "Undefined table SQLSTATE 42P01":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        try:
          discard await conn.query("SELECT * FROM terr_nonexistent_table_xyz")
          fail()
        except PgQueryError as e:
          check e.sqlState == "42P01"
    waitFor test()

  test "Undefined column SQLSTATE 42703":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS terr_test")
        discard await conn.exec("CREATE TABLE terr_test (val text)")
        try:
          discard await conn.query("SELECT nonexistent_col FROM terr_test")
          fail()
        except PgQueryError as e:
          check e.sqlState == "42703"
        discard await conn.exec("DROP TABLE terr_test")
    waitFor test()

  test "Error severity is ERROR":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        try:
          discard await conn.exec("SLECT 1")
        except PgQueryError as e:
          check e.severity == "ERROR"
    waitFor test()

  test "Connection usable after query error":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        try:
          discard await conn.exec("SLECT 1")
        except PgQueryError:
          discard
        let val = await conn.queryValue("SELECT 'recovered'")
        check val == some("recovered")
    waitFor test()

  test "PgError base type catches all":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        try:
          discard await conn.exec("SLECT 1")
        except PgError:
          check true
    waitFor test()

  test "Error detail and hint populated":
    proc test() {.async.} =
      await withTestConn proc(conn: PgConn) {.async.} =
        discard await conn.exec("DROP TABLE IF EXISTS terr_test")
        discard await conn.exec("CREATE TABLE terr_test (id int UNIQUE)")
        discard await conn.exec("INSERT INTO terr_test VALUES (1)")
        try:
          discard await conn.exec("INSERT INTO terr_test VALUES (1)")
        except PgQueryError as e:
          check e.detail.len > 0
        discard await conn.exec("DROP TABLE terr_test")
    waitFor test()
