## tpoolleak.nim — Step 0: acquire-cancellation slot-leak fix
##
## Verifies the guarded-acquire handoff: a borrower cancelled at the
## `await pool.acquire...()` resume must not orphan a pool slot, and the
## leak-accounting counters in stats() reconcile.

import std/unittest
import std/options
import chronos
import ../src/pgchronos/types
import ../src/pgchronos/query
import ../src/pgchronos/pool
import ./helpers

suite "Pool leak / cancellation-safe handoff":

  test "unclaimed guarded acquire is reclaimed on next tick":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 2)
      # Simulate a borrower that acquires but never claims (as if cancelled at
      # the handoff): the deferred reclaim must return the slot to the pool.
      let c = await pool.acquireGuarded()
      check pool.stats().active == 1
      # Do NOT claim. Let the event loop run the deferred reclaim.
      await sleepAsync(milliseconds(20))
      check pool.stats().active == 0
      check pool.stats().reclaimed == 1
      # Pool is still usable.
      let v = await pool.queryValue("SELECT 'ok'")
      check v == some("ok")
      await pool.close()
    waitFor test()

  test "claimed guarded acquire is NOT reclaimed":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 2)
      let c = await pool.acquireGuarded()
      pool.claim(c)
      await sleepAsync(milliseconds(20))
      # Still held by us — not reclaimed.
      check pool.stats().active == 1
      check pool.stats().reclaimed == 0
      await pool.release(c)
      check pool.stats().active == 0
      await pool.close()
    waitFor test()

  test "cancel withConn at acquire-resume — slot not leaked":
    proc test() {.async.} =
      # maxSize=1: exercise many cancel/borrow cycles and assert the single
      # slot never leaks. Cancels land at various points, including the handoff.
      let pool = await newPool(TestConnStr, minSize = 0, maxSize = 1,
                               acquireTimeout = seconds(5))
      for i in 0 ..< 25:
        let fut = (proc(): Future[void] {.async.} =
          pool.withConn conn:
            discard await conn.queryValue("SELECT pg_sleep(0.02)")
        )()
        # Cancel at a spread of ticks so some land at the acquire handoff.
        if i mod 2 == 0:
          await sleepAsync(microseconds(200 * i))
        fut.cancelSoon()
        try: await fut
        except CancelledError: discard
        except CatchableError: discard
      # Drain any pending deferred reclaims.
      await sleepAsync(milliseconds(50))
      check pool.stats().active == 0
      # Pool must still serve work.
      let v = await pool.queryValue("SELECT 'healthy'")
      check v == some("healthy")
      check pool.stats().active == 0
      await pool.close()
    waitFor test()

  test "acquired/released counters reconcile after a soak":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 3)
      for i in 0 ..< 40:
        discard await pool.queryValue("SELECT 1")
      # Every completed borrow acquired then released.
      let s = pool.stats()
      check s.active == 0
      check s.acquired == s.released
      check s.acquired >= 40
      await pool.close()
    waitFor test()

  test "raw acquire still works and counts":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 2)
      let c = await pool.acquire()
      check pool.stats().active == 1
      # Raw acquire is claimed by default — no reclaim.
      await sleepAsync(milliseconds(20))
      check pool.stats().active == 1
      check pool.stats().reclaimed == 0
      await pool.release(c)
      check pool.stats().active == 0
      check pool.stats().acquired == pool.stats().released
      await pool.close()
    waitFor test()

  test "conn reuse across borrows never spuriously reclaims (leaseId guard)":
    proc test() {.async.} =
      # maxSize=1 forces the same conn to be reused each cycle. A stale reclaim
      # armed for an earlier borrow must not touch a later re-borrow.
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 1)
      for i in 0 ..< 30:
        let c = await pool.acquireGuarded()
        pool.claim(c)
        discard await c.queryValue("SELECT 1")
        await pool.release(c)
        await sleepAsync(ZeroDuration)   # let any armed reclaim fire between cycles
      let s = pool.stats()
      check s.reclaimed == 0            # every borrow was claimed → nothing reclaimed
      check s.active == 0
      check s.acquired == s.released
      await pool.close()
    waitFor test()

  test "cancellation soak: acquired == released and active == 0":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 0, maxSize = 3)
      for i in 0 ..< 40:
        let fut = (proc(): Future[void] {.async.} =
          pool.withConn conn:
            discard await conn.queryValue("SELECT pg_sleep(0.01)")
        )()
        if i mod 3 == 0:
          await sleepAsync(microseconds(150 * (i mod 7)))
          fut.cancelSoon()
        try: await fut
        except CancelledError: discard
        except CatchableError: discard
      await sleepAsync(milliseconds(60))   # drain deferred reclaims
      let s = pool.stats()
      check s.active == 0
      check s.acquired == s.released
      await pool.close()
    waitFor test()

  test "cancelled acquire is not counted as a connect failure":
    proc test() {.async.} =
      # A cancelled fresh-connect must not bump connectFailureCount or impose
      # backoff on subsequent acquires (localhost connects fast, so the acquire
      # may also just complete — either way there must be no failure/backoff).
      let pool = await newPool(TestConnStr, minSize = 0, maxSize = 1)
      let fut = pool.acquire()
      fut.cancelSoon()
      try:
        let c = await fut
        await pool.release(c)
      except CancelledError: discard
      check pool.stats().connectFailures == 0
      # Next acquire must not be delayed by backoff.
      let start = Moment.now()
      let c2 = await pool.acquire()
      check (Moment.now() - start) < seconds(1)
      await pool.release(c2)
      await pool.close()
    waitFor test()

  test "close() keeps acquired == released even with an outstanding borrow":
    proc test() {.async.} =
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 2)
      let c = await pool.acquire()   # borrowed, never released
      check pool.stats().active == 1
      await pool.close()             # force-closes the active conn
      let s = pool.stats()
      check s.acquired == s.released
      discard c
    waitFor test()

  test "leaseId advances every borrow (foundation of the reclaim guard)":
    proc test() {.async.} =
      # The reclaim guard compares a snapshotted leaseId against the live one.
      # It is only sound if a conn's leaseId strictly advances across borrows,
      # so a stale reclaim can never match a re-borrowed conn. Lock that here.
      let pool = await newPool(TestConnStr, minSize = 1, maxSize = 1)
      let c1 = await pool.acquireGuarded()
      pool.claim(c1)
      let lease1 = c1.currentLeaseId
      await pool.release(c1)
      let c2 = await pool.acquireGuarded()   # same physical conn, reused
      pool.claim(c2)
      let lease2 = c2.currentLeaseId
      check lease2 != lease1
      check lease2 > lease1
      await pool.release(c2)
      await pool.close()
    waitFor test()
