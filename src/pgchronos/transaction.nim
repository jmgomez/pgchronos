import chronos
import ./types
import ./connection
import ./query

proc beginSql(isolation: TransactionIsolation, access: TransactionAccess): string =
  result = "BEGIN"
  case isolation
  of tiDefault: discard
  of tiReadCommitted: result.add " ISOLATION LEVEL READ COMMITTED"
  of tiRepeatableRead: result.add " ISOLATION LEVEL REPEATABLE READ"
  of tiSerializable: result.add " ISOLATION LEVEL SERIALIZABLE"
  case access
  of taDefault: discard
  of taReadWrite: result.add " READ WRITE"
  of taReadOnly: result.add " READ ONLY"

type Transaction* = object
  conn: PgConn
  isolation: TransactionIsolation
  access: TransactionAccess

proc transactionImpl*(conn: PgConn,
                      isolation: TransactionIsolation,
                      access: TransactionAccess,
                      body: proc(): Future[void] {.async.}): Future[void] {.async.} =
  discard await conn.exec(beginSql(isolation, access))
  try:
    await body()
    discard await conn.exec("COMMIT")
  except CatchableError as e:
    if not conn.isTainted and not conn.rawConn.isNil:
      try:
        discard await noCancel(conn.exec("ROLLBACK"))
      except CatchableError as rollbackErr:
        e.msg = e.msg & " (rollback failed: " & rollbackErr.msg & ")"
        conn.close()
    raise e

proc withTransaction*(conn: PgConn,
                      isolation: TransactionIsolation = tiDefault,
                      access: TransactionAccess = taDefault): Transaction =
  ## Returns a Transaction that, when used with ``await conn.withTransaction: body``,
  ## executes body inside a database transaction.
  Transaction(conn: conn, isolation: isolation, access: access)

template await*(tx: Transaction, body: untyped) {.deprecated:
    "use withTxConn from pgchronos/borrow (removes the chronos <5.0.0 pin)".} =
  ## Executes ``body`` inside a database transaction.
  ## Usage: ``await conn.withTransaction: body``
  ## or:   ``await conn.withTransaction(isolation = tiSerializable): body``
  ## DEPRECATED: superseded by ``withTxConn`` in ``pgchronos/borrow``. This
  ## template depends on chronos async internals (``chronosInternalRetFuture``),
  ## which is the sole remaining reason for the ``chronos < 5.0.0`` pin.
  let fut = transactionImpl(tx.conn, tx.isolation, tx.access,
                             proc(): Future[void] {.async.} = body)
  when declared(chronosInternalRetFuture):
    chronosInternalRetFuture.internalChild = fut
    yield chronosInternalRetFuture.internalChild
    cast[type(fut)](chronosInternalRetFuture.internalChild).internalRaiseIfError(fut)
