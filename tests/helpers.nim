import std/os
import std/options
import chronos
import ../src/pgchronos/types
import ../src/pgchronos/conn
import ../src/pgchronos/pool

const TestConnStr* = getEnv("PGCHRONOS_TEST_CONNSTR",
  "host=127.0.0.1 port=5432 user=postgres dbname=pgchronos_test")

proc withTestConn*(body: proc(conn: PgConn): Future[void] {.async.}): Future[void] {.async.} =
  let conn = await connect(TestConnStr)
  try:
    await body(conn)
  finally:
    conn.close()

proc withTestPool*(body: proc(pool: PgPool): Future[void] {.async.},
                   minSize = 1, maxSize = 5): Future[void] {.async.} =
  let pool = await newPool(TestConnStr, minSize = minSize, maxSize = maxSize)
  try:
    await body(pool)
  finally:
    await pool.close()
