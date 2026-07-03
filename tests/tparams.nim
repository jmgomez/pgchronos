## tparams.nim — Step 2: params helpers, DbResult, classifyPgError

import std/unittest
import std/options
import chronos
import ../src/pgchronos/types
import ../src/pgchronos/connection
import ../src/pgchronos/query
import ../src/pgchronos/params
import ./helpers

suite "params + DbResult":

  test "toUntypedParam / toNullableParam / toPgParam":
    check toUntypedParam("x") == some("x")
    check toNullableParam("") == none(string)
    check toNullableParam("y") == some("y")
    check toPgParam(42) == some("42")
    check toPgParam(true) == some("true")
    check toPgParam(3.5) == some("3.5")

  test "getStr / isNull on a Row":
    let row: Row = @[some("a"), none(string)]
    check row.getStr(0) == "a"
    check row.getStr(1) == ""
    check row.getStr(5) == ""       # out of range
    check not row.isNull(0)
    check row.isNull(1)
    check row.isNull(5)

  test "DbResult ok/err/get/getOr/isNotFound":
    let okR = DbResult[int].ok(7)
    check okR.isOk
    check okR.get == 7
    check okR.getOr(0) == 7
    check not okR.isNotFound

    let errR = DbResult[int].err(dekNotFound, "missing")
    check not errR.isOk
    check errR.getOr(-1) == -1
    check errR.isNotFound
    check errR.error.kind == dekNotFound

  test "DbResult[void] ok":
    let v = DbResult[void].ok()
    check v.isOk

suite "classifyPgError against real PG error states":

  test "unique violation -> dekDuplicate":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      discard await conn.exec("DROP TABLE IF EXISTS tclass_u")
      discard await conn.exec("CREATE TABLE tclass_u (id int PRIMARY KEY)")
      discard await conn.exec("INSERT INTO tclass_u VALUES (1)")
      var kind: DbErrorKind
      try:
        discard await conn.exec("INSERT INTO tclass_u VALUES (1)")
      except CatchableError as e:
        kind = classifyPgError(e)
      check kind == dekDuplicate
      discard await conn.exec("DROP TABLE tclass_u")
      conn.close()
    waitFor test()

  test "foreign key violation -> dekConstraint":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      discard await conn.exec("DROP TABLE IF EXISTS tclass_c CASCADE")
      discard await conn.exec("DROP TABLE IF EXISTS tclass_p CASCADE")
      discard await conn.exec("CREATE TABLE tclass_p (id int PRIMARY KEY)")
      discard await conn.exec(
        "CREATE TABLE tclass_c (pid int REFERENCES tclass_p(id))")
      var kind: DbErrorKind
      try:
        discard await conn.exec("INSERT INTO tclass_c VALUES (999)")
      except CatchableError as e:
        kind = classifyPgError(e)
      check kind == dekConstraint
      discard await conn.exec("DROP TABLE tclass_c")
      discard await conn.exec("DROP TABLE tclass_p")
      conn.close()
    waitFor test()

  test "check constraint violation -> dekConstraint":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      discard await conn.exec("DROP TABLE IF EXISTS tclass_ck")
      discard await conn.exec("CREATE TABLE tclass_ck (n int CHECK (n > 0))")
      var kind: DbErrorKind
      try:
        discard await conn.exec("INSERT INTO tclass_ck VALUES (-1)")
      except CatchableError as e:
        kind = classifyPgError(e)
      check kind == dekConstraint
      discard await conn.exec("DROP TABLE tclass_ck")
      conn.close()
    waitFor test()

  test "syntax error -> dekQuery":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      var kind: DbErrorKind
      try:
        discard await conn.exec("SELECT FROM WHERE")
      except CatchableError as e:
        kind = classifyPgError(e)
      check kind == dekQuery
      conn.close()
    waitFor test()

  test "not-null violation (23502) classified and sqlState captured":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      discard await conn.exec("DROP TABLE IF EXISTS tclass_nn")
      discard await conn.exec("CREATE TABLE tclass_nn (id int NOT NULL)")
      var state = ""
      var kind: DbErrorKind
      try:
        discard await conn.exec("INSERT INTO tclass_nn (id) VALUES (NULL)")
      except PgQueryError as e:
        state = e.sqlState
        kind = classifyPgError(e)
      except CatchableError as e:
        kind = classifyPgError(e)
      check state == "23502"
      # Faithful hoisted classifyPgError maps 23502 to dekQuery (not special-cased).
      check kind == dekQuery
      discard await conn.exec("DROP TABLE tclass_nn")
      conn.close()
    waitFor test()

  test "serialization failure (40001) surfaces sqlState when it occurs":
    proc test() {.async.} =
      let a = await connect(TestConnStr)
      let b = await connect(TestConnStr)
      discard await a.exec("DROP TABLE IF EXISTS tclass_ser")
      discard await a.exec("CREATE TABLE tclass_ser (id int PRIMARY KEY, val int)")
      discard await a.exec("INSERT INTO tclass_ser VALUES (1, 10), (2, 10)")

      # Non-blocking write-skew: each txn READS the row the other WRITES (SSI
      # predicate locks, no row-lock blocking), then writes a DIFFERENT row.
      # SSI detects the read/write cycle at commit and aborts the later txn.
      discard await a.exec("BEGIN ISOLATION LEVEL SERIALIZABLE")
      discard await b.exec("BEGIN ISOLATION LEVEL SERIALIZABLE")
      discard await a.queryValue("SELECT val FROM tclass_ser WHERE id = 2")
      discard await b.queryValue("SELECT val FROM tclass_ser WHERE id = 1")
      discard await a.exec("UPDATE tclass_ser SET val = 0 WHERE id = 1")
      discard await b.exec("UPDATE tclass_ser SET val = 0 WHERE id = 2")
      discard await a.exec("COMMIT")

      var sawSerFailure = false
      try:
        discard await b.exec("COMMIT")
      except PgQueryError as e:
        if e.sqlState == "40001":
          sawSerFailure = true
          check classifyPgError(e) == dekQuery   # faithful: not special-cased
      except CatchableError:
        discard
      check sawSerFailure   # the later committer must fail with 40001
      discard await a.exec("DROP TABLE IF EXISTS tclass_ser")
      a.close()
      b.close()
    waitFor test()
