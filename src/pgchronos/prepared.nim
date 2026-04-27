import std/options
import chronos
import db_connector/postgres except PGconn, PGresult
import ./types
import ./connection
import ./libpq_extra
import ./bridge
import ./extract
import ./query

proc quoteIdentifier(name: string): string =
  result = "\""
  for ch in name:
    if ch == '"':
      result.add "\"\""
    else:
      result.add ch
  result.add '"'

proc ensureValid(stmt: PreparedStmt) =
  if stmt.stmtConn.isNil:
    raise newException(PgError, "Prepared statement is detached from any connection")
  stmt.stmtConn.ensureUsable()
  if stmt.stmtLeaseId != stmt.stmtConn.currentLeaseId:
    raise newException(PgError,
      "Prepared statement is no longer valid for the current pool lease")

proc prepare*(conn: PgConn, name: string, sql: string): Future[PreparedStmt] {.async.} =
  try:
    conn.beginOperation()
    if pqsendPrepare(conn.rawConn, name.cstring, sql.cstring, 0, nil) == 0:
      raise newException(PgError, "PQsendPrepare failed: " & $pqerrorMessage(conn.rawConn))
    await conn.flushSend()
    let raw = await conn.waitResult()
    if raw.isNil:
      raise newException(PgError, "PQgetResult returned nil for prepare")
    let status = pqresultStatus(raw)
    if status == PGRES_FATAL_ERROR:
      let err = extractError(raw)
      pqclear(raw)
      await conn.drainResults()
      raise err
    pqclear(raw)
    await conn.ensureNoExtraResults()
    return newPreparedStmt(name, conn)
  except CancelledError:
    conn.close()
    raise
  finally:
    conn.endOperation()

proc execPreparedInternal(stmt: PreparedStmt,
                          params: seq[Option[string]]): Future[PgResult] {.async.} =
  stmt.ensureValid()
  let conn = stmt.stmtConn
  try:
    conn.beginOperation()
    let nParams = params.len.int32
    if nParams == 0:
      if pqsendQueryPrepared(conn.rawConn, stmt.stmtName.cstring, 0, nil, nil, nil, 0) == 0:
        raise newException(PgError, "PQsendQueryPrepared failed: " & $pqerrorMessage(conn.rawConn))
    elif nParams <= 8:
      var paramVals: array[8, cstring]
      var paramStrs: array[8, string]
      for i in 0..<nParams:
        if params[i].isSome:
          paramStrs[i] = params[i].get
          paramVals[i] = paramStrs[i].cstring
        else:
          paramVals[i] = nil
      if pqsendQueryPrepared(conn.rawConn, stmt.stmtName.cstring, nParams,
                             cast[cstringArray](addr paramVals[0]),
                             nil, nil, 0) == 0:
        raise newException(PgError, "PQsendQueryPrepared failed: " & $pqerrorMessage(conn.rawConn))
    else:
      var paramVals = newSeq[cstring](nParams)
      var paramStrs = newSeq[string](nParams)
      for i in 0..<nParams:
        if params[i].isSome:
          paramStrs[i] = params[i].get
          paramVals[i] = paramStrs[i].cstring
        else:
          paramVals[i] = nil
      if pqsendQueryPrepared(conn.rawConn, stmt.stmtName.cstring, nParams,
                             cast[cstringArray](addr paramVals[0]),
                             nil, nil, 0) == 0:
        raise newException(PgError, "PQsendQueryPrepared failed: " & $pqerrorMessage(conn.rawConn))
    await conn.flushSend()
    let raw = await conn.waitResult()
    if raw.isNil:
      raise newException(PgError, "PQgetResult returned nil")
    let res = try:
      extractResult(raw)
    except PgQueryError:
      await conn.drainResults()
      raise
    await conn.ensureNoExtraResults()
    return res
  except CancelledError:
    conn.close()
    raise
  finally:
    conn.endOperation()

proc exec*(stmt: PreparedStmt,
           params: seq[Option[string]] = @[]): Future[int64] {.async.} =
  let res = await stmt.execPreparedInternal(params)
  return res.affected

proc query*(stmt: PreparedStmt,
            params: seq[Option[string]] = @[]): Future[PgResult] {.async.} =
  return await stmt.execPreparedInternal(params)

proc queryOne*(stmt: PreparedStmt,
               params: seq[Option[string]] = @[]): Future[Option[Row]] {.async.} =
  let res = await stmt.execPreparedInternal(params)
  if res.rows.len == 0:
    return none(Row)
  return some(res.rows[0])

proc queryValue*(stmt: PreparedStmt,
                 params: seq[Option[string]] = @[]): Future[Option[string]] {.async.} =
  let res = await stmt.execPreparedInternal(params)
  if res.rows.len == 0 or res.rows[0].len == 0:
    return none(string)
  return res.rows[0][0]

proc close*(stmt: PreparedStmt) {.async.} =
  stmt.ensureValid()
  discard await stmt.stmtConn.exec("DEALLOCATE " & quoteIdentifier(stmt.stmtName))

proc exec*(stmt: PreparedStmt, params: openArray[string]): Future[int64] =
  return stmt.exec(params.toOptSeq)

proc query*(stmt: PreparedStmt, params: openArray[string]): Future[PgResult] =
  return stmt.query(params.toOptSeq)

proc queryOne*(stmt: PreparedStmt, params: openArray[string]): Future[Option[Row]] =
  return stmt.queryOne(params.toOptSeq)

proc queryValue*(stmt: PreparedStmt, params: openArray[string]): Future[Option[string]] =
  return stmt.queryValue(params.toOptSeq)
