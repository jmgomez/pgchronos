## envinit.nim — ambient (per-thread) pool wiring + env-driven initialization.
##
## Both downstream apps run a per-thread pool and borrow from an *ambient*
## pool via 2-arg `withConn`/`withTxConn` (see borrow.nim). This module owns
## that threadvar and the env-topology helpers.
##
## `initPoolFromEnv` and friends are the env-driven constructors; the raw
## ambient accessors (`getPool`/`setPool`/`hasPool`) let apps wire their own.

import std/[os, strutils]
import chronos
import ./types
import ./pool

var ambientPool {.threadvar.}: PgPool

proc setPool*(pool: PgPool) =
  ## Install `pool` as this thread's ambient pool (used by 2-arg borrows).
  {.cast(gcsafe).}:
    ambientPool = pool

proc hasPool*(): bool =
  ## True if an ambient pool is installed on this thread.
  {.cast(gcsafe).}:
    not ambientPool.isNil

proc getPool*(): PgPool =
  ## The ambient pool for this thread. Raises Defect if uninitialized — a
  ## missing pool is a programming error (wiring bug), not a recoverable
  ## condition, so we fail loud rather than returning nil.
  {.cast(gcsafe).}:
    result = ambientPool
    if result.isNil:
      raise newException(Defect,
        "pgchronos: no pool initialized on this thread " &
        "(call initPool/initPoolFromEnv or setPool first)")

proc resolvePoolSize*(size: int, sizeEnv: string): int =
  ## Precedence: explicit `size` arg (> 0) beats the env var, which beats the
  ## default of 5. Explicit configuration must beat ambient env vars;
  ## the reverse would let POOL_SIZE silently shadow the explicit argument.
  if size > 0:
    return size
  let envStr = getEnv(sizeEnv, "")
  if envStr != "":
    try:
      return parseInt(envStr)
    except ValueError:
      discard
  return 5

proc initPool*(connString: string, size: int = 5, minSize: int = 1,
               resetMode: ResetMode = rmDiscardAll): Future[PgPool] {.async.} =
  ## Create a pool and install it as this thread's ambient pool. Returns it too.
  let pool = await newPool(connString, minSize = minSize, maxSize = size,
                           acquireTimeout = seconds(30), resetMode = resetMode)
  setPool(pool)
  return pool

proc initPoolFromEnv*(size: int = 0, envVar = "DATABASE_URL",
                      sizeEnv = "POOL_SIZE", minSize = 1,
                      resetMode: ResetMode = rmDiscardAll,
                      default = "host=localhost port=5432 dbname=postgres"):
                      Future[PgPool] {.async.} =
  ## Initialize the ambient pool from the environment.
  ## Connection string: `envVar` (default `DATABASE_URL`), falling back to `default`.
  ## Size: explicit `size` arg (if > 0) beats `sizeEnv` (default `POOL_SIZE`)
  ## beats the built-in default of 5 — EXPLICIT ARG WINS.
  let connStr = getEnv(envVar, default)
  let poolSize = resolvePoolSize(size, sizeEnv)
  return await initPool(connStr, size = poolSize, minSize = minSize,
                        resetMode = resetMode)

proc closePool*() {.async.} =
  ## Close and clear this thread's ambient pool.
  {.cast(gcsafe).}:
    if not ambientPool.isNil:
      await ambientPool.close()
      ambientPool = nil
