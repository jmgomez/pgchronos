import std/options
import std/strutils
import db_connector/postgres except PGconn, PGresult
import ./types
import ./libpq_extra

proc colIndex*(res: PgResult, col: string): int =
  for i, c in res.columns:
    if c == col:
      return i
  raise newException(KeyError, "Unknown column: " & col)

proc getStr*(res: PgResult, row: int, col: string): Option[string] =
  let idx = res.colIndex(col)
  res.rows[row][idx]

proc getInt*(res: PgResult, row: int, col: string): Option[int64] =
  let val = res.getStr(row, col)
  if val.isNone:
    return none(int64)
  return some(parseBiggestInt(val.get))

proc getFloat*(res: PgResult, row: int, col: string): Option[float64] =
  let val = res.getStr(row, col)
  if val.isNone:
    return none(float64)
  return some(parseFloat(val.get))

proc getBool*(res: PgResult, row: int, col: string): Option[bool] =
  let val = res.getStr(row, col)
  if val.isNone:
    return none(bool)
  return some(val.get == "t")

proc extractError*(raw: PPGresult): ref PgQueryError =
  let err = newException(PgQueryError,
    $pqresultErrorMessage(raw))
  let sqlstate = pqresultErrorField(raw, PG_DIAG_SQLSTATE)
  if not sqlstate.isNil:
    err.sqlState = $sqlstate
  let severity = pqresultErrorField(raw, PG_DIAG_SEVERITY)
  if not severity.isNil:
    err.severity = $severity
  let detail = pqresultErrorField(raw, PG_DIAG_MESSAGE_DETAIL)
  if not detail.isNil:
    err.detail = $detail
  let hint = pqresultErrorField(raw, PG_DIAG_MESSAGE_HINT)
  if not hint.isNil:
    err.hint = $hint
  return err

proc extractRow*(raw: PPGresult): Row =
  let nfields = pqnfields(raw)
  result = newSeq[Option[string]](nfields)
  for c in 0..<nfields:
    if pqgetisnull(raw, 0, c.int32) == 1:
      result[c] = none(string)
    else:
      result[c] = some($pqgetvalue(raw, 0, c.int32))

proc extractColumns*(raw: PPGresult): seq[string] =
  let nfields = pqnfields(raw)
  result = newSeq[string](nfields)
  for i in 0..<nfields:
    result[i] = $pqfname(raw, i.int32)

proc extractResult*(raw: PPGresult): PgResult =
  let status = pqresultStatus(raw)
  if status == PGRES_FATAL_ERROR:
    let err = extractError(raw)
    pqclear(raw)
    raise err

  var res = PgResult()

  let nfields = pqnfields(raw)
  res.columns = newSeq[string](nfields)
  for i in 0..<nfields:
    res.columns[i] = $pqfname(raw, i.int32)

  let nrows = pqntuples(raw)
  res.rows = newSeq[Row](nrows)
  for r in 0..<nrows:
    res.rows[r] = newSeq[Option[string]](nfields)
    for c in 0..<nfields:
      if pqgetisnull(raw, r.int32, c.int32) == 1:
        res.rows[r][c] = none(string)
      else:
        res.rows[r][c] = some($pqgetvalue(raw, r.int32, c.int32))

  let cmdTuples = pqcmdTuples(raw)
  if not cmdTuples.isNil and cmdTuples[0] != '\0':
    try:
      res.affected = parseBiggestInt($cmdTuples)
    except ValueError as e:
      pqclear(raw)
      raise newException(PgError, "Invalid affected-row count from libpq: " & e.msg)

  pqclear(raw)
  return res
