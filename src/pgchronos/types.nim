import std/options
import std/deques
import std/strutils
import chronos
import db_connector/postgres

export options, chronos

type
  PgConn* = ref object
    raw: PPGconn
    fd: AsyncFD
    registered: bool
    busy: bool
    tainted: bool
    leaseId: uint64
    claimed: bool
      ## Handoff guard for cancellation-safe borrowing. A guarded acquire
      ## (used by every library borrower) sets this false and arms a deferred
      ## reclaim; the borrower flips it true synchronously the instant it
      ## resumes. If the borrower is cancelled at the acquire await-resume it
      ## never claims, and the reclaim returns the orphaned slot to the pool.

  PgPool* = ref object
    connStr: string
    minSize, maxSize: int
    idle: seq[PgConn]
    idleAt: seq[Moment]
    activeConns: seq[PgConn]
    activeAt: seq[Moment]
    acquiredCount: int64
    releasedCount: int64
    reclaimedCount: int64
    pendingConnects: int
    waiters: Deque[Future[void]]
    deadWaiters: int
    closed: bool
    acquireTimeout: Duration
    idleReapAfter: Duration
    nextConnectAttemptAt: Moment
    connectFailureCount: int
    resetMode: ResetMode
    lastReapAt: Moment

  PgResult* = object
    rows*: seq[Row]
    columns*: seq[string]
    affected*: int64

  Row* = seq[Option[string]]

  PgError* = object of CatchableError

  PgQueryError* = object of PgError
    sqlState*: string
    severity*: string
    detail*: string
    hint*: string

  PgPoolError* = object of PgError

  TransactionIsolation* = enum
    tiDefault
    tiReadCommitted
    tiRepeatableRead
    tiSerializable

  TransactionAccess* = enum
    taDefault
    taReadWrite
    taReadOnly

  ResetMode* = enum
    rmDiscardAll    ## DISCARD ALL on every release (safest, 1 extra round-trip)
    rmTransactionCheck  ## Only check pqtransactionStatus, no DISCARD ALL (fast, no extra round-trip)
    rmNone          ## No reset at all (fastest, caller is responsible for clean state)

  PreparedStmt* = object
    name: string
    conn: PgConn
    leaseId: uint64

  PoolStats* = object
    ## Snapshot of pool state for observability.
    minSize*: int
    maxSize*: int
    active*: int        ## connections currently borrowed
    idle*: int          ## connections sitting in the pool ready to use
    pending*: int       ## connect() calls in flight
    waiters*: int       ## acquire() callers blocked waiting for a slot
    connectFailures*: int  ## consecutive connect failures (resets on success)
    closed*: bool
    acquired*: int64    ## total connections handed to a borrower (lifetime)
    released*: int64    ## total connections returned to the pool (lifetime)
    reclaimed*: int64   ## slots reclaimed after a cancelled-at-handoff borrow
    oldestActiveMs*: int64  ## age of the longest-held active connection, in ms (0 if none)

# --- Internal helpers (cross-module, not part of the public API) ---
# These procs are exported (*) so that conn.nim, pool.nim, query.nim, etc.
# can access private fields. Nim has no package-private visibility.
# Users should treat everything below this line as internal.

proc newPgConn*(raw: PPGconn, fd: AsyncFD, registered: bool): PgConn =
  PgConn(raw: raw, fd: fd, registered: registered, busy: false, tainted: false,
         leaseId: 0'u64, claimed: true)

proc rawConn*(conn: PgConn): PPGconn = conn.raw
proc clearRawConn*(conn: PgConn) = conn.raw = nil
proc asyncFd*(conn: PgConn): AsyncFD = conn.fd
proc isRegistered*(conn: PgConn): bool = conn.registered
proc markUnregistered*(conn: PgConn) = conn.registered = false
proc isBusy*(conn: PgConn): bool = conn.busy
proc setBusy*(conn: PgConn, busy: bool) = conn.busy = busy
proc isTainted*(conn: PgConn): bool = conn.tainted
proc markTainted*(conn: PgConn) = conn.tainted = true
proc clearTainted*(conn: PgConn) = conn.tainted = false
proc isClaimed*(conn: PgConn): bool = conn.claimed
proc setClaimed*(conn: PgConn, claimed: bool) = conn.claimed = claimed
proc currentLeaseId*(conn: PgConn): uint64 = conn.leaseId
proc bumpLeaseId*(conn: PgConn): uint64 =
  conn.leaseId = conn.leaseId + 1'u64
  conn.leaseId

proc initPgPoolState*(connStr: string, minSize: int, maxSize: int,
                      acquireTimeout: Duration, idleReapAfter: Duration,
                      resetMode: ResetMode): PgPool =
  PgPool(
    connStr: connStr,
    minSize: minSize,
    maxSize: maxSize,
    idle: @[],
    idleAt: @[],
    activeConns: @[],
    activeAt: @[],
    acquiredCount: 0,
    releasedCount: 0,
    reclaimedCount: 0,
    pendingConnects: 0,
    waiters: initDeque[Future[void]](),
    deadWaiters: 0,
    closed: false,
    acquireTimeout: acquireTimeout,
    idleReapAfter: idleReapAfter,
    nextConnectAttemptAt: Moment.default,
    connectFailureCount: 0,
    resetMode: resetMode,
    lastReapAt: Moment.default,
  )

proc connStr*(pool: PgPool): string = pool.connStr
proc minSize*(pool: PgPool): int = pool.minSize
proc maxSize*(pool: PgPool): int = pool.maxSize
proc activeCount*(pool: PgPool): int = pool.activeConns.len
proc idleCount*(pool: PgPool): int = pool.idle.len
proc isClosed*(pool: PgPool): bool = pool.closed
proc acquireTimeout*(pool: PgPool): Duration = pool.acquireTimeout
proc idleReapAfter*(pool: PgPool): Duration = pool.idleReapAfter
proc pendingConnectCount*(pool: PgPool): int = pool.pendingConnects
proc nextConnectAttemptAt*(pool: PgPool): Moment = pool.nextConnectAttemptAt
proc connectFailureCount*(pool: PgPool): int = pool.connectFailureCount
proc resetMode*(pool: PgPool): ResetMode = pool.resetMode
proc lastReapAt*(pool: PgPool): Moment = pool.lastReapAt
proc setLastReapAt*(pool: PgPool, t: Moment) = pool.lastReapAt = t

proc addIdleConn*(pool: PgPool, conn: PgConn, idleAt: Moment) =
  pool.idle.add(conn)
  pool.idleAt.add(idleAt)

proc idleConnAt*(pool: PgPool, idx: int): PgConn = pool.idle[idx]
proc idleAtAt*(pool: PgPool, idx: int): Moment = pool.idleAt[idx]
proc removeIdleConnAt*(pool: PgPool, idx: int) =
  pool.idle.del(idx)
  pool.idleAt.del(idx)
proc trimIdleTo*(pool: PgPool, len: int) =
  pool.idle.setLen(len)
  pool.idleAt.setLen(len)
proc idleConns*(pool: PgPool): seq[PgConn] = pool.idle
proc clearIdle*(pool: PgPool) =
  pool.idle.setLen(0)
  pool.idleAt.setLen(0)

proc addActiveConn*(pool: PgPool, conn: PgConn, at: Moment) =
  pool.activeConns.add(conn)
  pool.activeAt.add(at)
proc activeConnIndex*(pool: PgPool, conn: PgConn): int =
  for i, c in pool.activeConns:
    if c == conn:
      return i
  return -1
proc removeActiveConnAt*(pool: PgPool, idx: int) =
  pool.activeConns.del(idx)
  pool.activeAt.del(idx)
proc activeConns*(pool: PgPool): seq[PgConn] = pool.activeConns
proc clearActiveConns*(pool: PgPool) =
  pool.activeConns.setLen(0)
  pool.activeAt.setLen(0)
proc oldestActiveAt*(pool: PgPool): Moment =
  ## Timestamp of the longest-held active connection (Moment.high if none).
  result = Moment.high
  for t in pool.activeAt:
    if t < result:
      result = t

proc noteAcquired*(pool: PgPool) = pool.acquiredCount.inc
proc noteReleased*(pool: PgPool) = pool.releasedCount.inc
proc noteReclaimed*(pool: PgPool) = pool.reclaimedCount.inc
proc acquiredCount*(pool: PgPool): int64 = pool.acquiredCount
proc releasedCount*(pool: PgPool): int64 = pool.releasedCount
proc reclaimedCount*(pool: PgPool): int64 = pool.reclaimedCount

proc reservePendingConnect*(pool: PgPool) = pool.pendingConnects.inc
proc releasePendingConnect*(pool: PgPool) =
  if pool.pendingConnects > 0:
    pool.pendingConnects.dec
proc noteConnectSuccess*(pool: PgPool) =
  pool.connectFailureCount = 0
  pool.nextConnectAttemptAt = Moment.default
proc noteConnectFailure*(pool: PgPool, retryAt: Moment) =
  pool.connectFailureCount.inc
  pool.nextConnectAttemptAt = retryAt

proc waitersLen*(pool: PgPool): int = pool.waiters.len
proc addWaiter*(pool: PgPool, waiter: Future[void]) = pool.waiters.addLast(waiter)
proc popFirstWaiter*(pool: PgPool): Future[void] = pool.waiters.popFirst()
proc noteDeadWaiter*(pool: PgPool) = pool.deadWaiters.inc
proc deadWaiterCount*(pool: PgPool): int = pool.deadWaiters
proc clearDeadWaiters*(pool: PgPool) = pool.deadWaiters = 0
proc consumeDeadWaiter*(pool: PgPool) =
  if pool.deadWaiters > 0:
    pool.deadWaiters.dec
proc setClosed*(pool: PgPool, closed: bool) = pool.closed = closed

proc newPreparedStmt*(name: string, conn: PgConn): PreparedStmt =
  PreparedStmt(name: name, conn: conn, leaseId: conn.currentLeaseId)

proc stmtName*(stmt: PreparedStmt): string = stmt.name
proc stmtConn*(stmt: PreparedStmt): PgConn = stmt.conn
proc stmtLeaseId*(stmt: PreparedStmt): uint64 = stmt.leaseId

proc ensureOpen*(conn: PgConn) =
  if conn.isNil or conn.raw.isNil:
    raise newException(PgError, "Connection is closed")

proc ensureUsable*(conn: PgConn) =
  conn.ensureOpen()
  if conn.isTainted:
    raise newException(PgError, "Connection is not reusable after a cancelled or failed operation")

proc beginOperation*(conn: PgConn) =
  conn.ensureUsable()
  if conn.isBusy:
    raise newException(PgError, "Another operation is already in progress on this connection")
  conn.setBusy(true)

proc endOperation*(conn: PgConn) =
  conn.setBusy(false)

proc toOptSeq*(params: openArray[string]): seq[Option[string]] =
  result = newSeq[Option[string]](params.len)
  for i, p in params:
    result[i] = some(p)

# =============================================================================
# DbResult — Result/Either type for the repository layer
# =============================================================================
# Hoisted byte-identical from pepetraining/hermes repository.nim. A typed CRUD
# call returns DbResult[T] instead of raising, so callers branch on isOk and
# get a classified DbErrorKind (unique / constraint / connection / ...).

type
  DbErrorKind* = enum
    dekNotFound       ## No matching row
    dekDuplicate      ## Unique constraint violation
    dekConstraint     ## Foreign key or check constraint
    dekConnection     ## Connection lost or pool exhausted
    dekQuery          ## SQL syntax or execution error
    dekMapping        ## Row-to-object conversion error
    dekUnexpected     ## Catch-all

  DbError* = object
    kind*: DbErrorKind
    message*: string
    detail*: string

  DbResult*[T] = object
    case isOk*: bool
    of true:
      value*: T
    of false:
      error*: DbError

# -- DbResult constructors --

proc ok*[T](R: typedesc[DbResult[T]], val: T): DbResult[T] =
  DbResult[T](isOk: true, value: val)

proc ok*(R: typedesc[DbResult[void]]): DbResult[void] =
  DbResult[void](isOk: true)

proc err*[T](R: typedesc[DbResult[T]], kind: DbErrorKind, msg: string,
    detail: string = ""): DbResult[T] =
  DbResult[T](isOk: false, error: DbError(kind: kind, message: msg,
      detail: detail))

# -- DbResult accessors --

proc get*[T](r: DbResult[T]): T =
  if not r.isOk:
    raise newException(Defect,
        "Attempted to unwrap error DbResult: " & r.error.message)
  r.value

proc getOr*[T](r: DbResult[T], fallback: T): T =
  if r.isOk: r.value else: fallback

proc isNotFound*[T](r: DbResult[T]): bool =
  not r.isOk and r.error.kind == dekNotFound

proc errorStr*(e: DbError): string =
  system.`$`(e.kind) & ": " & e.message

proc str*[T](r: DbResult[T]): string =
  if r.isOk:
    when T is void: "Ok()"
    else: "Ok(" & system.`$`(r.value) & ")"
  else:
    "Err(" & r.error.errorStr & ")"

# =============================================================================
# classifyPgError — map a PostgreSQL error to a DbErrorKind
# =============================================================================

proc classifyPgError*(e: ref CatchableError): DbErrorKind =
  let msg = e.msg
  when compiles(e of PgQueryError):
    if e of PgQueryError:
      let pgErr = (ref PgQueryError)(e)
      let state = pgErr.sqlState
      # 23505 = unique_violation
      if state == "23505":
        return dekDuplicate
      # 23503 = foreign_key_violation, 23514 = check_violation
      elif state == "23503" or state == "23514":
        return dekConstraint
      # 08xxx = connection exceptions
      elif state.len >= 2 and state[0..1] == "08":
        return dekConnection
  if "duplicate key" in msg or "unique constraint" in msg:
    dekDuplicate
  elif "violates foreign key" in msg or "violates check constraint" in msg:
    dekConstraint
  elif "connection" in msg.toLowerAscii or "server closed" in msg.toLowerAscii:
    dekConnection
  else:
    dekQuery
