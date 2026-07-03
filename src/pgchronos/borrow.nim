## borrow.nim — connection-borrow templates.
##
## Two families, both cancellation-safe (guarded acquire + shielded cleanup):
##
## * `withConn`   — AUTOCOMMIT borrow. No BEGIN/COMMIT; each statement commits
##                  on its own. What pgchronos and Hermes have always meant by
##                  `withConn`.
## * `withTxConn` — TRANSACTIONAL borrow. BEGIN -> optional `onBegin(conn)` hook
##                  -> body -> COMMIT, with automatic ROLLBACK on any exit that
##                  isn't a completed commit. `commitNow conn` commits early.
##                  Palestrum's old `withConn` semantics, renamed so
##                  transactional vs autocommit is explicit at the call site.
##
## Forms (all params `untyped`, so overloads resolve purely by arity — a typed
## first param would make Nim eagerly type-check the fresh conn identifier of a
## command-syntax call):
##
##   withConn conn:                 body   # ambient autocommit
##   pool.withConn conn:            body   # explicit autocommit (defined in pool.nim)
##   withTxConn conn:               body   # ambient transaction
##   pool.withTxConn conn:          body   # explicit transaction
##   pool.withTxConn(conn, onBegin): body  # explicit transaction + onBegin hook
##
## For an AMBIENT transaction with an onBegin hook, call the explicit 4-arg form
## with `getPool()`:  getPool().withTxConn(conn, onBegin): body

import chronos
import ./types
import ./query
import ./pool
import ./envinit

export pool.withConn  # re-export the explicit-pool autocommit template

type
  OnBeginProc* = proc(conn: PgConn): Future[void] {.gcsafe.}
    ## Convenience type for annotating an onBegin hook. Passed inside the
    ## transaction right after BEGIN. Palestrum uses it for
    ## `set_config('app.tenant_id', ...)` / row-security — the transaction
    ## *skeleton* is library code, the *policy* stays in the app.

# ---------------------------------------------------------------------------
# Autocommit — ambient
# ---------------------------------------------------------------------------

template withConn*(conn, body: untyped) =
  ## Ambient autocommit borrow from this thread's pool (see envinit.getPool).
  block:
    let p = getPool()
    let conn = await p.acquireGuarded()
    p.claim(conn)
    try:
      body
    finally:
      await noCancel(p.release(conn))

# ---------------------------------------------------------------------------
# Transactional — shared cleanup
# ---------------------------------------------------------------------------

proc txReleaseCleanup*(pool: PgPool, conn: PgConn, committed: bool) {.async.} =
  ## Roll back an uncommitted transaction (best-effort) then release the conn.
  ## MUST be awaited through `noCancel`: a bare `await` in a `finally` fires the
  ## pending CancelledError at the await call site BEFORE this body runs, so the
  ## release is skipped and the slot leaks permanently (Palestrum's authoritative
  ## analysis of the failure mode).
  if not committed and not conn.isNil and not conn.isTainted and
     not conn.rawConn.isNil:
    try:
      discard await noCancel(conn.exec("ROLLBACK"))
    except CatchableError:
      conn.markTainted()  # release() will close a tainted conn
  await pool.release(conn)

# ---------------------------------------------------------------------------
# Transactional — explicit pool
# ---------------------------------------------------------------------------

template withTxConn*(pool, connVar, body: untyped) {.dirty.} =
  ## BEGIN -> body -> COMMIT on `pool`. Any exit before the commit (exception,
  ## cancellation, or an early `return`) rolls back.
  ##
  ## RETURN-ABORTS-TRANSACTION HAZARD: a `return` (or other non-local exit) in
  ## `body` before the implicit COMMIT rolls back every write the body made.
  ## Intentional for validation error paths, but a footgun for a handler that
  ## writes and *then* returns early. SAFE PATTERN: `commitNow connVar` after
  ## the last write you must persist, before any early return.
  block:
    let connVar = await pool.acquireGuarded()
    pool.claim(connVar)
    var connCommitted {.inject.} = false
    template commitNow(_: untyped) {.used.} =
      if not connCommitted:
        discard await connVar.exec("COMMIT")
        connCommitted = true
    try:
      discard await connVar.exec("BEGIN")
      body
      if not connCommitted:
        discard await connVar.exec("COMMIT")
        connCommitted = true
    finally:
      await noCancel(txReleaseCleanup(pool, connVar, connCommitted))

template withTxConn*(pool, connVar, onBegin, body: untyped) {.dirty.} =
  ## As the 3-arg form, plus an `onBegin(conn)` hook run inside the transaction
  ## right after BEGIN (before the body). If `onBegin` is nil it is skipped.
  block:
    let connVar = await pool.acquireGuarded()
    pool.claim(connVar)
    var connCommitted {.inject.} = false
    template commitNow(_: untyped) {.used.} =
      if not connCommitted:
        discard await connVar.exec("COMMIT")
        connCommitted = true
    try:
      discard await connVar.exec("BEGIN")
      if not onBegin.isNil:
        await onBegin(connVar)
      body
      if not connCommitted:
        discard await connVar.exec("COMMIT")
        connCommitted = true
    finally:
      await noCancel(txReleaseCleanup(pool, connVar, connCommitted))

# ---------------------------------------------------------------------------
# Transactional — ambient
# ---------------------------------------------------------------------------

template withTxConn*(connVar, body: untyped) {.dirty.} =
  ## Ambient BEGIN/COMMIT borrow from this thread's pool.
  withTxConn(getPool(), connVar, body)
