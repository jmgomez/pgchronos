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
