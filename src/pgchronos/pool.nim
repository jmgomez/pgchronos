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

proc release*(pool: PgPool, conn: PgConn): Future[void] {.async.} =
  if not pool.removeActive(conn):
    if not conn.isNil:
      conn.close()
    return
  pool.noteReleased()
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

# --- Cancellation-safe handoff (AUDITOR F1 fix) ---
#
# `acquire` registers the connection as active before returning it. A chronos
# `CancelledError` delivered at the borrower's `await pool.acquire...()`
# await-resume raises BEFORE the returned conn is bound, so the borrower's
# `try/finally` never runs and `release` is never called — the slot is
# orphaned in `activeConns` forever (the idle reaper only touches idle conns).
# This class of leak caused a real production pool-exhaustion outage.
#
# Fix: a *guarded* acquire marks the conn unclaimed and arms a deferred check.
# Every library borrower flips `claimed` true synchronously the instant it
# resumes (before any further await, so no cancellation can intervene). The
# deferred check runs on a later loop tick — after the borrower would have
# resumed. If the borrower was cancelled at the handoff it never claimed, and
# the check returns the orphaned slot to the pool. Raw `acquire()` callers own
# their conn (claimed=true, no reclaim) and must pair it with `release` in a
# `finally`; prefer `withConn`/the pool helpers, which are safe by construction.

proc reclaimRelease(pool: PgPool, conn: PgConn) {.async: (raises: []).} =
  # release() is not raises:[]; a raise inside asyncSpawn becomes an unhandled
  # FutureDefect. This is best-effort reclaim with no caller to propagate to.
  try:
    await pool.release(conn)
  except CatchableError:
    discard

proc reclaimIfOrphaned(pool: PgPool, conn: PgConn, lease: uint64) =
  # Guard on the lease snapshot: if the conn was claimed → released → re-borrowed
  # before this deferred check fired, its leaseId has advanced and we must NOT
  # touch it (that would double-release the *new* borrow). Only reclaim a conn
  # that is still on the same borrow, unclaimed, and active.
  if conn.currentLeaseId == lease and not conn.isClaimed and
     pool.activeConnIndex(conn) >= 0:
    pool.noteReclaimed()
    asyncSpawn reclaimRelease(pool, conn)

proc reclaimLater(pool: PgPool, conn: PgConn, lease: uint64) {.async: (raises: []).} =
  # Yield the event loop so the borrower's synchronous claim() (which runs the
  # instant it resumes from `await acquireGuarded`) executes first. If the
  # borrower was cancelled at that handoff it never claims, and we reclaim the
  # orphaned slot. Two ticks make the ordering robust across chronos versions.
  try:
    await sleepAsync(ZeroDuration)
    await sleepAsync(ZeroDuration)
  except CancelledError:
    return
  reclaimIfOrphaned(pool, conn, lease)

proc armReclaim(pool: PgPool, conn: PgConn) =
  # Snapshot the lease NOW (finishAcquire has already bumped it for this borrow).
  asyncSpawn reclaimLater(pool, conn, conn.currentLeaseId)

proc finishAcquire(pool: PgPool, conn: PgConn, guarded: bool) =
  conn.setClaimed(not guarded)
  pool.addActiveConn(conn, Moment.now())
  pool.noteAcquired()
  if guarded:
    pool.armReclaim(conn)

proc acquireImpl(pool: PgPool, guarded: bool): Future[PgConn] {.async.} =
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
      pool.finishAcquire(c, guarded)
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
        pool.finishAcquire(c, guarded)
        return c
      except CancelledError:
        # A cancelled acquire is NOT a connect failure — do not impose backoff on
        # the other waiters. Just undo the pending-connect bookkeeping and wake
        # one so a real slot isn't left unclaimed.
        pool.releasePendingConnect()
        pool.wakeWaiter()
        raise
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

proc acquire*(pool: PgPool): Future[PgConn] =
  ## Borrow a connection. The caller OWNS it and MUST return it with `release`
  ## in a `finally`.
  ##
  ## WARNING — NOT cancellation-safe at the handoff. If a `CancelledError` is
  ## delivered at the `await pool.acquire()` resume, the conn is never bound and
  ## its slot leaks forever. Only use this when you fully control cancellation.
  ## For anything else prefer `withConn` / the pool-level helpers (all safe by
  ## construction), or the explicit safe pattern: `let c = await pool.acquireGuarded()`
  ## then `pool.claim(c)` as the very next synchronous statement.
  acquireImpl(pool, guarded = false)

proc acquireGuarded*(pool: PgPool): Future[PgConn] =
  ## Cancellation-safe borrow: the returned conn is unclaimed and armed for
  ## reclaim. The caller MUST call `pool.claim(conn)` as its first synchronous
  ## action after the await. Used by every library borrower.
  acquireImpl(pool, guarded = true)

proc claim*(pool: PgPool, conn: PgConn) {.inline.} =
  ## Mark a guarded-acquired conn as owned by the resumed borrower, disarming
  ## the deferred reclaim. Must run synchronously right after
  ## `await pool.acquireGuarded()`, before any further await.
  conn.setClaimed(true)

proc close*(pool: PgPool) {.async.} =
  pool.setClosed(true)
  while pool.waitersLen > 0:
    let waiter = pool.popFirstWaiter()
    if not waiter.finished:
      waiter.fail(newException(PgPoolError, "Pool closed"))
  for c in pool.idleConns:
    c.close()
  for c in pool.activeConns:
    # Outstanding borrows are force-closed at shutdown. Credit them as released
    # so the acquired == released invariant survives closing a pool with active
    # conns (otherwise stats() drifts permanently).
    c.close()
    pool.noteReleased()
  pool.clearIdle()
  pool.clearActiveConns()

proc stats*(pool: PgPool): PoolStats =
  ## Snapshot of pool state for monitoring/diagnostics.
  var oldestActiveMs: int64 = 0
  if pool.activeCount > 0:
    oldestActiveMs = (Moment.now() - pool.oldestActiveAt()).milliseconds
  PoolStats(
    minSize: pool.minSize,
    maxSize: pool.maxSize,
    active: pool.activeCount,
    idle: pool.idleCount,
    pending: pool.pendingConnectCount,
    waiters: pool.waitersLen,
    connectFailures: pool.connectFailureCount,
    closed: pool.isClosed,
    acquired: pool.acquiredCount,
    released: pool.releasedCount,
    reclaimed: pool.reclaimedCount,
    oldestActiveMs: oldestActiveMs,
  )

proc reap*(pool: PgPool) =
  ## Force idle reaping now, bypassing the throttle window. Use this when
  ## you want to shrink the pool above minSize without waiting for the next
  ## acquire/release cycle. Idle connections older than idleReapAfter close.
  pool.setLastReapAt(Moment.default)
  pool.reapIdle()

template withConn*(pool, conn, body: untyped) =
  ## Explicit-pool autocommit borrow. `pool` is `untyped` (not `PgPool`) so
  ## this 3-arg form is distinguished from the 2-arg ambient `withConn` in
  ## borrow.nim purely by arity — a typed first param makes Nim eagerly
  ## type-check the fresh conn identifier of a 2-arg command-syntax call.
  block:
    let conn = await pool.acquireGuarded()
    pool.claim(conn)
    try:
      body
    finally:
      await noCancel(pool.release(conn))

proc simpleExec*(pool: PgPool, sql: string): Future[void] {.async.} =
  let conn = await pool.acquireGuarded()
  pool.claim(conn)
  try:
    await conn.simpleExec(sql)
  finally:
    await noCancel(pool.release(conn))

proc exec*(pool: PgPool, sql: string,
           params: seq[Option[string]] = @[]): Future[int64] {.async.} =
  let conn = await pool.acquireGuarded()
  pool.claim(conn)
  try:
    return await conn.exec(sql, params)
  finally:
    await noCancel(pool.release(conn))

proc query*(pool: PgPool, sql: string,
            params: seq[Option[string]] = @[]): Future[PgResult] {.async.} =
  let conn = await pool.acquireGuarded()
  pool.claim(conn)
  try:
    return await conn.query(sql, params)
  finally:
    await noCancel(pool.release(conn))

proc queryOne*(pool: PgPool, sql: string,
               params: seq[Option[string]] = @[]): Future[Option[Row]] {.async.} =
  let conn = await pool.acquireGuarded()
  pool.claim(conn)
  try:
    return await conn.queryOne(sql, params)
  finally:
    await noCancel(pool.release(conn))

proc queryValue*(pool: PgPool, sql: string,
                 params: seq[Option[string]] = @[]): Future[Option[string]] {.async.} =
  let conn = await pool.acquireGuarded()
  pool.claim(conn)
  try:
    return await conn.queryValue(sql, params)
  finally:
    await noCancel(pool.release(conn))

proc forEachRow*(pool: PgPool, sql: string,
                 params: seq[Option[string]],
                 cb: proc(row: Row): Future[void] {.gcsafe.}): Future[int64] {.async.} =
  let conn = await pool.acquireGuarded()
  pool.claim(conn)
  try:
    return await conn.forEachRow(sql, params, cb)
  finally:
    await noCancel(pool.release(conn))

proc forEachRow*(pool: PgPool, sql: string,
                 cb: proc(row: Row): Future[void] {.gcsafe.}): Future[int64] {.async.} =
  let conn = await pool.acquireGuarded()
  pool.claim(conn)
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
