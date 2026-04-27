import std/options
import chronos
import db_connector/postgres except PGconn, PGresult
import ./types
import ./conn
import ./bridge
import ./extract

proc sendQueryParamsImpl(conn: PgConn, sql: cstring, nParams: int32,
                         paramVals: ptr cstring) =
  if pqsendQueryParams(conn.rawConn, sql, nParams, nil,
                       cast[cstringArray](paramVals),
                       nil, nil, 0) == 0:
    raise newException(PgError, "PQsendQueryParams failed: " & $pqerrorMessage(conn.rawConn))

proc sendQueryParams(conn: PgConn, sql: string,
                     params: seq[Option[string]]) =
  conn.ensureUsable()
  let nParams = params.len.int32
  if nParams == 0:
    sendQueryParamsImpl(conn, sql.cstring, 0, nil)
  elif nParams <= 8:
    # Stack-allocated fast path for common 1-8 param queries
    var paramVals: array[8, cstring]
    var paramStrs: array[8, string]
    for i in 0..<nParams:
      if params[i].isSome:
        paramStrs[i] = params[i].get
        paramVals[i] = paramStrs[i].cstring
      else:
        paramVals[i] = nil
    sendQueryParamsImpl(conn, sql.cstring, nParams, addr paramVals[0])
  else:
    var paramVals = newSeq[cstring](nParams)
    var paramStrs = newSeq[string](nParams)
    for i in 0..<nParams:
      if params[i].isSome:
        paramStrs[i] = params[i].get
        paramVals[i] = paramStrs[i].cstring
      else:
        paramVals[i] = nil
    sendQueryParamsImpl(conn, sql.cstring, nParams, addr paramVals[0])

proc ensureNoExtraResults*(conn: PgConn) {.async.} =
  var raw = await conn.waitResult()
  if raw.isNil:
    return

  var trailingError: ref PgQueryError
  while not raw.isNil:
    if trailingError.isNil and pqresultStatus(raw) == PGRES_FATAL_ERROR:
      trailingError = extractError(raw)
    pqclear(raw)
    raw = await conn.waitResult()

  if not trailingError.isNil:
    raise trailingError

  raise newException(PgError,
    "Multiple result sets are not supported on a single query call")

proc execInternal(conn: PgConn, sql: string,
                  params: seq[Option[string]]): Future[PgResult] {.async.} =
  try:
    conn.beginOperation()
    sendQueryParams(conn, sql, params)
    await conn.flushSend()
    let raw = await conn.waitResult()
    if raw.isNil:
      raise newException(PgError, "PQgetResult returned nil")
    var res: PgResult
    try:
      res = extractResult(raw)
    except PgQueryError as e:
      await conn.drainResults()
      raise e
    await conn.ensureNoExtraResults()
    return res
  except CancelledError:
    conn.close()
    raise
  finally:
    conn.endOperation()

proc exec*(conn: PgConn, sql: string,
           params: seq[Option[string]]): Future[int64] {.async.} =
  let res = await conn.execInternal(sql, params)
  return res.affected

proc exec*(conn: PgConn, sql: string,
           params: openArray[Option[string]] = []): Future[int64] =
  return conn.exec(sql, @params)

proc query*(conn: PgConn, sql: string,
            params: seq[Option[string]]): Future[PgResult] {.async.} =
  return await conn.execInternal(sql, params)

proc query*(conn: PgConn, sql: string,
            params: openArray[Option[string]] = []): Future[PgResult] =
  return conn.query(sql, @params)

proc queryOne*(conn: PgConn, sql: string,
               params: seq[Option[string]]): Future[Option[Row]] {.async.} =
  let res = await conn.execInternal(sql, params)
  if res.rows.len == 0:
    return none(Row)
  return some(res.rows[0])

proc queryOne*(conn: PgConn, sql: string,
               params: openArray[Option[string]] = []): Future[Option[Row]] =
  return conn.queryOne(sql, @params)

proc queryValue*(conn: PgConn, sql: string,
                 params: seq[Option[string]]): Future[Option[string]] {.async.} =
  let res = await conn.execInternal(sql, params)
  if res.rows.len == 0 or res.rows[0].len == 0:
    return none(string)
  return res.rows[0][0]

proc queryValue*(conn: PgConn, sql: string,
                 params: openArray[Option[string]] = []): Future[Option[string]] =
  return conn.queryValue(sql, @params)

# Streaming: single-row mode via pqSetSingleRowMode

proc forEachRow*(conn: PgConn, sql: string,
                 params: seq[Option[string]],
                 cb: proc(row: Row): Future[void] {.gcsafe.}): Future[int64] {.async.} =
  ## Stream rows one at a time using libpq's single-row mode.
  ## Calls cb for each row. Returns total row count.
  ## Much lower memory than query() for large result sets — only one row
  ## of C data is held at a time, deep-copied into a Nim Row, then cleared.
  try:
    conn.beginOperation()
    sendQueryParams(conn, sql, params)
    if pqSetSingleRowMode(conn.rawConn) != 1:
      raise newException(PgError, "pqSetSingleRowMode failed: " & $pqerrorMessage(conn.rawConn))
    await conn.flushSend()
    var count: int64 = 0
    while true:
      let raw = await conn.waitResult()
      if raw.isNil:
        break
      let status = pqresultStatus(raw)
      if status == PGRES_SINGLE_TUPLE:
        let row = extractRow(raw)
        pqclear(raw)
        try:
          await cb(row)
        except CancelledError:
          raise
        except CatchableError:
          conn.markTainted()
          raise
        count.inc
      elif status == PGRES_TUPLES_OK:
        # End-of-data marker
        pqclear(raw)
      elif status == PGRES_FATAL_ERROR:
        let err = extractError(raw)
        pqclear(raw)
        await conn.drainResults()
        raise err
      else:
        pqclear(raw)
    return count
  except CancelledError:
    conn.close()
    raise
  finally:
    conn.endOperation()

proc forEachRow*(conn: PgConn, sql: string,
                 params: openArray[Option[string]],
                 cb: proc(row: Row): Future[void] {.gcsafe.}): Future[int64] =
  return conn.forEachRow(sql, @params, cb)

proc forEachRow*(conn: PgConn, sql: string,
                 params: openArray[string],
                 cb: proc(row: Row): Future[void] {.gcsafe.}): Future[int64] =
  return conn.forEachRow(sql, params.toOptSeq, cb)

proc forEachRow*(conn: PgConn, sql: string,
                 cb: proc(row: Row): Future[void] {.gcsafe.}): Future[int64] =
  return conn.forEachRow(sql, newSeq[Option[string]](), cb)

# Synchronous callback overloads (no Future allocation per row)

proc forEachRow*(conn: PgConn, sql: string,
                 params: seq[Option[string]],
                 cb: proc(row: Row) {.gcsafe.}): Future[int64] =
  conn.forEachRow(sql, params, proc(row: Row): Future[void] {.async, gcsafe.} =
    {.gcsafe.}: cb(row)
  )

proc forEachRow*(conn: PgConn, sql: string,
                 params: openArray[string],
                 cb: proc(row: Row) {.gcsafe.}): Future[int64] =
  conn.forEachRow(sql, params.toOptSeq, proc(row: Row): Future[void] {.async, gcsafe.} =
    {.gcsafe.}: cb(row)
  )

proc forEachRow*(conn: PgConn, sql: string,
                 cb: proc(row: Row) {.gcsafe.}): Future[int64] =
  conn.forEachRow(sql, newSeq[Option[string]](), proc(row: Row): Future[void] {.async, gcsafe.} =
    {.gcsafe.}: cb(row)
  )

# String convenience overloads

proc exec*(conn: PgConn, sql: string,
           params: varargs[string]): Future[int64] =
  return conn.exec(sql, toOptSeq(@params))

proc query*(conn: PgConn, sql: string,
            params: varargs[string]): Future[PgResult] =
  return conn.query(sql, toOptSeq(@params))

proc queryOne*(conn: PgConn, sql: string,
               params: varargs[string]): Future[Option[Row]] =
  return conn.queryOne(sql, toOptSeq(@params))

proc queryValue*(conn: PgConn, sql: string,
                 params: varargs[string]): Future[Option[string]] =
  return conn.queryValue(sql, toOptSeq(@params))
