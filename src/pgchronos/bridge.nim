import chronos
import db_connector/postgres except PGconn, PGresult
import ./types

proc waitRead*(conn: PgConn): Future[void] =
  ## Wait indefinitely for libpq socket readability.
  ## Callers that need bounded latency should wrap higher-level operations
  ## with ``withTimeout()`` and treat timeout/cancellation as connection-fatal.
  conn.ensureOpen()
  var retFuture = newFuture[void]("pgchronos.waitRead")
  proc cb(udata: pointer) =
    if not retFuture.finished:
      removeReader2(conn.asyncFd).isOkOr:
        if not retFuture.finished:
          retFuture.fail(newException(PgError, "removeReader2 failed"))
        return
      retFuture.complete()
  addReader2(conn.asyncFd, cb).isOkOr:
    retFuture.fail(newException(PgError, "addReader2 failed"))
  return retFuture

proc waitWrite*(conn: PgConn): Future[void] =
  ## Wait indefinitely for libpq socket writability.
  ## Callers that need bounded latency should wrap higher-level operations
  ## with ``withTimeout()`` and treat timeout/cancellation as connection-fatal.
  conn.ensureOpen()
  var retFuture = newFuture[void]("pgchronos.waitWrite")
  proc cb(udata: pointer) =
    if not retFuture.finished:
      removeWriter2(conn.asyncFd).isOkOr:
        if not retFuture.finished:
          retFuture.fail(newException(PgError, "removeWriter2 failed"))
        return
      retFuture.complete()
  addWriter2(conn.asyncFd, cb).isOkOr:
    retFuture.fail(newException(PgError, "addWriter2 failed"))
  return retFuture

proc flushSend*(conn: PgConn) {.async.} =
  ## Flush the libpq output buffer until fully sent.
  ## This inherits the indefinite wait contract of ``waitWrite()``.
  conn.ensureOpen()
  while true:
    let rc = pqflush(conn.rawConn)
    if rc == 0:
      return
    if rc == -1:
      raise newException(PgError, "PQflush error: " & $pqerrorMessage(conn.rawConn))
    await conn.waitWrite()

proc waitResult*(conn: PgConn): Future[PPGresult] {.async.} =
  ## Wait until a ``PGresult`` is available.
  ## This inherits the indefinite wait contract of ``waitRead()``.
  conn.ensureOpen()
  while true:
    if pqconsumeInput(conn.rawConn) == 0:
      raise newException(PgError, "PQconsumeInput error: " & $pqerrorMessage(conn.rawConn))
    if pqisBusy(conn.rawConn) == 0:
      return pqgetResult(conn.rawConn)
    await conn.waitRead()

proc drainResults*(conn: PgConn) {.async.} =
  ## Drain all remaining ``PGresult`` values for the current command.
  ## This inherits the indefinite wait contract of ``waitResult()``.
  conn.ensureOpen()
  while true:
    let res = await conn.waitResult()
    if res.isNil:
      break
    pqclear(res)
