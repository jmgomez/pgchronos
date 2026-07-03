## tintrospect.nim — TODO 16: inTransaction / transactionStatus introspection

import std/unittest
import chronos
import ../src/pgchronos/types
import ../src/pgchronos/connection
import ../src/pgchronos/query
import ./helpers

suite "connection: transaction-status introspection":

  test "autocommit -> not in a transaction (tsIdle)":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      check conn.transactionStatus() == tsIdle
      check not conn.inTransaction()
      conn.close()
    waitFor test()

  test "inside BEGIN -> in a transaction (tsInTrans)":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      discard await conn.exec("BEGIN")
      check conn.transactionStatus() == tsInTrans
      check conn.inTransaction()
      discard await conn.exec("COMMIT")
      check conn.transactionStatus() == tsIdle
      check not conn.inTransaction()
      conn.close()
    waitFor test()

  test "aborted transaction -> tsInError, inTransaction is false":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      discard await conn.exec("BEGIN")
      # Provoke an error inside the transaction → it enters the failed state.
      try:
        discard await conn.exec("SELECT * FROM definitely_missing_table_xyz")
      except CatchableError:
        discard
      check conn.transactionStatus() == tsInError
      check not conn.inTransaction()   # doomed block: cannot accept more work
      discard await conn.exec("ROLLBACK")
      check conn.transactionStatus() == tsIdle
      conn.close()
    waitFor test()

  test "closed connection -> tsUnknown":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      conn.close()
      check conn.transactionStatus() == tsUnknown
      check not conn.inTransaction()
    waitFor test()
