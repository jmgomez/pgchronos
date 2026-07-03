import chronos
import db_connector/postgres except PGconn, PGresult
import ./types
import ./bridge

proc close*(conn: PgConn) =
  if conn.rawConn != nil:
    if conn.isRegistered:
      discard unregister2(conn.asyncFd)
      conn.markUnregistered()
    pqfinish(conn.rawConn)
    conn.clearRawConn()
  conn.markTainted()
  conn.endOperation()

proc connect*(connStr: string): Future[PgConn] {.async.} =
  let raw = pqconnectStart(connStr.cstring)
  if raw.isNil:
    raise newException(PgError, "PQconnectStart returned nil")

  if pqstatus(raw) == CONNECTION_BAD:
    let msg = $pqerrorMessage(raw)
    pqfinish(raw)
    raise newException(PgError, "Connection failed: " & msg)

  let sockFd = pqsocket(raw)
  if sockFd < 0:
    pqfinish(raw)
    raise newException(PgError, "PQsocket returned invalid fd")

  let asyncFd = AsyncFD(sockFd)
  register2(asyncFd).isOkOr:
    pqfinish(raw)
    raise newException(PgError, "Failed to register fd with chronos")

  var conn = newPgConn(raw, asyncFd, true)

  try:
    while true:
      let pollStatus = pqconnectPoll(raw)
      case pollStatus
      of PGRES_POLLING_OK:
        break
      of PGRES_POLLING_READING:
        await conn.waitRead()
      of PGRES_POLLING_WRITING:
        await conn.waitWrite()
      of PGRES_POLLING_FAILED:
        let msg = $pqerrorMessage(raw)
        conn.close()
        raise newException(PgError, "Connection failed: " & msg)
      else:
        let msg = $pqerrorMessage(raw)
        conn.close()
        raise newException(PgError, "Unexpected poll status: " & msg)

    if pqsetnonblocking(raw, 1) != 0:
      let msg = $pqerrorMessage(raw)
      conn.close()
      raise newException(PgError, "Failed to set non-blocking: " & msg)
  except CancelledError:
    conn.close()
    raise

  return conn

type
  TransactionStatus* = enum
    tsUnknown   ## connection is closed/bad or the status is indeterminate
    tsIdle      ## not in a transaction (autocommit)
    tsActive    ## a command is currently in progress
    tsInTrans   ## inside a valid transaction block
    tsInError   ## inside a FAILED transaction block (must ROLLBACK to recover)

proc transactionStatus*(conn: PgConn): TransactionStatus =
  ## The connection's transaction state, read from libpq's client-side cache
  ## (zero round-trips). `tsInError` is useful for pool-release hygiene.
  if conn.isNil or conn.rawConn.isNil:
    return tsUnknown
  case pqtransactionStatus(conn.rawConn)
  of PQTRANS_IDLE: tsIdle
  of PQTRANS_ACTIVE: tsActive
  of PQTRANS_INTRANS: tsInTrans
  of PQTRANS_INERROR: tsInError
  else: tsUnknown

proc inTransaction*(conn: PgConn): bool =
  ## True when the connection is inside a valid transaction block. Zero
  ## round-trips. Use to guard operations that must run within the caller's
  ## transaction (e.g. enqueueing to an outbox table). Returns false for a
  ## FAILED transaction (see `transactionStatus`/`tsInError`), since a doomed
  ## block cannot accept further work until it is rolled back.
  conn.transactionStatus() == tsInTrans
