import std/options
import std/deques
import chronos
import db_connector/postgres except PGconn, PGresult
import ./types
import ./connection
import ./query

proc markIdle(pool: PgPool, conn: PgConn) =
  pool.addIdleConn(conn, Moment.now())

proc activeIndex(pool: PgPool, conn: PgConn): int =
  pool.activeConnIndex(conn)

proc removeActive(pool: PgPool, conn: PgConn): bool =
  let idx = pool.activeIndex(conn)
  if idx < 0:
    return false
  pool.removeActiveConnAt(idx)
  return true

proc wakeWaiter(pool: PgPool) =
  while pool.waitersLen > 0:
    let waiter = pool.popFirstWaiter()
    if not waiter.finished:
      waiter.complete()
      return
    pool.consumeDeadWaiter()

proc compactWaiters(pool: PgPool) =
  var live = initDeque[Future[void]]()
  while pool.waitersLen > 0:
    let waiter = pool.popFirstWaiter()
    if not waiter.finished:
      live.addLast(waiter)
  pool.clearDeadWaiters()
  while live.len > 0:
    pool.addWaiter(live.popFirst())

proc currentBackoff(pool: PgPool): Duration =
  let shift = min(pool.connectFailureCount, 5)
  let ms = 50 * (1 shl shift)  # 50ms, 100ms, 200ms, ... capped at 1600ms
  if ms > 1600: milliseconds(1600) else: milliseconds(ms)

proc resetSession(pool: PgPool, conn: PgConn): Future[void] {.async.} =
  if pqtransactionStatus(conn.rawConn) != PQTRANS_IDLE:
    raise newException(PgError, "Connection returned to pool with an open transaction")
  case pool.resetMode
  of rmDiscardAll:
    discard await conn.exec("DISCARD ALL")
  of rmTransactionCheck:
    discard  # pqtransactionStatus check above is sufficient
  of rmNone:
    discard

proc reapIdle(pool: PgPool) =
  let now = Moment.now()
  # Throttle: skip if checked recently (min of 10s or the reap interval itself)
  let throttle = if pool.idleReapAfter < seconds(10): pool.idleReapAfter
                 else: seconds(10)
  if now - pool.lastReapAt < throttle:
    return
  pool.setLastReapAt(now)
  var i = pool.idleCount - 1
  while i >= pool.minSize and i >= 0:
    if now - pool.idleAtAt(i) >= pool.idleReapAfter:
      let conn = pool.idleConnAt(i)
      pool.removeIdleConnAt(i)
      conn.close()
    dec i

proc newPool*(connStr: string, minSize: int = 1, maxSize: int = 10,
              acquireTimeout: Duration = seconds(30),
              idleReapAfter: Duration = minutes(5),
              resetMode: ResetMode = rmDiscardAll): Future[PgPool] {.async.} =
  if minSize < 0:
    raise newException(PgPoolError, "minSize must be >= 0, got " & $minSize)
  if maxSize < 1:
    raise newException(PgPoolError, "maxSize must be >= 1, got " & $maxSize)
  if minSize > maxSize:
    raise newException(PgPoolError, "minSize (" & $minSize & ") must be <= maxSize (" & $maxSize & ")")
  if acquireTimeout <= Duration.default:
    raise newException(PgPoolError, "acquireTimeout must be positive")
  var pool = initPgPoolState(connStr, minSize, maxSize, acquireTimeout, idleReapAfter, resetMode)
  try:
    for i in 0..<minSize:
      let c = await connect(connStr)
      pool.markIdle(c)
  except CatchableError:
    for c in pool.idleConns:
      c.close()
    raise
  return pool

proc acquire*(pool: PgPool): Future[PgConn] {.async.} =
  let deadline = Moment.now() + pool.acquireTimeout
  while true:
    if pool.isClosed:
      raise newException(PgPoolError, "Pool is closed")
    pool.reapIdle()
    # Try an idle connection
    while pool.idleCount > 0:
      let idx = pool.idleCount - 1
      let c = pool.idleConnAt(idx)
      pool.trimIdleTo(idx)
      if c.rawConn.isNil or c.isTainted or pqstatus(c.rawConn) != CONNECTION_OK:
        c.close()
        continue
      discard c.bumpLeaseId()
      c.clearTainted()
      pool.addActiveConn(c)
      return c
    # Respect backoff, but cap to remaining timeout
    let now = Moment.now()
    let retryAt = pool.nextConnectAttemptAt
    if retryAt > now and retryAt > Moment.default:
      let remaining = deadline - now
      if remaining <= Duration.default:
        raise newException(PgPoolError, "Acquire timeout")
      let backoffWait = retryAt - now
      let sleepDur = if backoffWait < remaining: backoffWait else: remaining
      await sleepAsync(sleepDur)
      continue
    # Try to open a new connection
    if pool.activeCount + pool.pendingConnectCount + pool.idleCount < pool.maxSize:
      pool.reservePendingConnect()
      try:
        let c = await connect(pool.connStr)
        pool.releasePendingConnect()
        pool.noteConnectSuccess()
        discard c.bumpLeaseId()
        pool.addActiveConn(c)
        return c
      except CatchableError:
        pool.releasePendingConnect()
        pool.noteConnectFailure(Moment.now() + pool.currentBackoff())
        pool.wakeWaiter()
        raise
    # Wait for a connection to be released
    let remaining = deadline - Moment.now()
    if remaining <= Duration.default:
      raise newException(PgPoolError, "Acquire timeout")
    let waiter = newFuture[void]("pgchronos.pool.acquire")
    pool.addWaiter(waiter)
    try:
      let completed = await withTimeout(waiter, remaining)
      if completed:
        await waiter
        continue
      if not waiter.finished:
        waiter.fail(newException(PgPoolError, "Acquire timeout"))
        pool.noteDeadWaiter()
        if pool.deadWaiterCount * 2 >= pool.waitersLen:
          pool.compactWaiters()
      raise newException(PgPoolError, "Acquire timeout")
    except CancelledError:
      if not waiter.finished:
        waiter.fail(newException(PgPoolError, "Acquire cancelled"))
        pool.noteDeadWaiter()
        if pool.deadWaiterCount * 2 >= pool.waitersLen:
          pool.compactWaiters()
      raise

proc release*(pool: PgPool, conn: PgConn): Future[void] {.async.} =
  if not pool.removeActive(conn):
    if not conn.isNil:
      conn.close()
    return
  if conn.isNil:
    pool.wakeWaiter()
    return
  discard conn.bumpLeaseId()
  if pool.isClosed or conn.rawConn.isNil or conn.isTainted or pqstatus(conn.rawConn) != CONNECTION_OK:
    conn.close()
    pool.wakeWaiter()
    return
  try:
    await noCancel(pool.resetSession(conn))
  except CatchableError:
    conn.close()
    pool.wakeWaiter()
    return
  pool.markIdle(conn)
  pool.reapIdle()
  pool.wakeWaiter()

proc close*(pool: PgPool) {.async.} =
  pool.setClosed(true)
  while pool.waitersLen > 0:
    let waiter = pool.popFirstWaiter()
    if not waiter.finished:
      waiter.fail(newException(PgPoolError, "Pool closed"))
  for c in pool.idleConns:
    c.close()
  for c in pool.activeConns:
    c.close()
  pool.clearIdle()
  pool.clearActiveConns()

template withConn*(pool: PgPool, conn, body: untyped) =
  block:
    let conn = await pool.acquire()
    try:
      body
    finally:
      await noCancel(pool.release(conn))

proc simpleExec*(pool: PgPool, sql: string): Future[void] {.async.} =
  let conn = await pool.acquire()
  try:
    await conn.simpleExec(sql)
  finally:
    await noCancel(pool.release(conn))

proc exec*(pool: PgPool, sql: string,
           params: seq[Option[string]] = @[]): Future[int64] {.async.} =
  let conn = await pool.acquire()
  try:
    return await conn.exec(sql, params)
  finally:
    await noCancel(pool.release(conn))

proc query*(pool: PgPool, sql: string,
            params: seq[Option[string]] = @[]): Future[PgResult] {.async.} =
  let conn = await pool.acquire()
  try:
    return await conn.query(sql, params)
  finally:
    await noCancel(pool.release(conn))

proc queryOne*(pool: PgPool, sql: string,
               params: seq[Option[string]] = @[]): Future[Option[Row]] {.async.} =
  let conn = await pool.acquire()
  try:
    return await conn.queryOne(sql, params)
  finally:
    await noCancel(pool.release(conn))

proc queryValue*(pool: PgPool, sql: string,
                 params: seq[Option[string]] = @[]): Future[Option[string]] {.async.} =
  let conn = await pool.acquire()
  try:
    return await conn.queryValue(sql, params)
  finally:
    await noCancel(pool.release(conn))

proc forEachRow*(pool: PgPool, sql: string,
                 params: seq[Option[string]],
                 cb: proc(row: Row): Future[void] {.gcsafe.}): Future[int64] {.async.} =
  let conn = await pool.acquire()
  try:
    return await conn.forEachRow(sql, params, cb)
  finally:
    await noCancel(pool.release(conn))

proc forEachRow*(pool: PgPool, sql: string,
                 cb: proc(row: Row): Future[void] {.gcsafe.}): Future[int64] {.async.} =
  let conn = await pool.acquire()
  try:
    return await conn.forEachRow(sql, cb)
  finally:
    await noCancel(pool.release(conn))

proc exec*(pool: PgPool, sql: string, params: varargs[string]): Future[int64] =
  return pool.exec(sql, toOptSeq(@params))

proc query*(pool: PgPool, sql: string, params: varargs[string]): Future[PgResult] =
  return pool.query(sql, toOptSeq(@params))

proc queryOne*(pool: PgPool, sql: string, params: varargs[string]): Future[Option[Row]] =
  return pool.queryOne(sql, toOptSeq(@params))

proc queryValue*(pool: PgPool, sql: string, params: varargs[string]): Future[Option[string]] =
  return pool.queryValue(sql, toOptSeq(@params))

proc forEachRow*(pool: PgPool, sql: string, params: openArray[string],
                 cb: proc(row: Row): Future[void] {.gcsafe.}): Future[int64] =
  return pool.forEachRow(sql, params.toOptSeq, cb)
