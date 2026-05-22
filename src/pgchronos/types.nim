import std/options
import std/deques
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

  PgPool* = ref object
    connStr: string
    minSize, maxSize: int
    idle: seq[PgConn]
    idleAt: seq[Moment]
    activeConns: seq[PgConn]
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

# --- Internal helpers (cross-module, not part of the public API) ---
# These procs are exported (*) so that conn.nim, pool.nim, query.nim, etc.
# can access private fields. Nim has no package-private visibility.
# Users should treat everything below this line as internal.

proc newPgConn*(raw: PPGconn, fd: AsyncFD, registered: bool): PgConn =
  PgConn(raw: raw, fd: fd, registered: registered, busy: false, tainted: false,
         leaseId: 0'u64)

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

proc addActiveConn*(pool: PgPool, conn: PgConn) =
  pool.activeConns.add(conn)
proc activeConnIndex*(pool: PgPool, conn: PgConn): int =
  for i, c in pool.activeConns:
    if c == conn:
      return i
  return -1
proc removeActiveConnAt*(pool: PgPool, idx: int) =
  pool.activeConns.del(idx)
proc activeConns*(pool: PgPool): seq[PgConn] = pool.activeConns
proc clearActiveConns*(pool: PgPool) =
  pool.activeConns.setLen(0)

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
