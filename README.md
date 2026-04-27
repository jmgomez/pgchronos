# pgchronos

Async PostgreSQL client for Nim. Wraps libpq with [chronos](https://github.com/status-im/nim-chronos) for non-blocking I/O.

libpq handles the protocol (auth, SSL, wire format). chronos handles fd scheduling. pgchronos bridges them 

## Install

```
nimble install pgchronos
```

Requires `libpq` (PostgreSQL client library) installed on the system.

## Usage

```nim
import pgchronos

proc main() {.async.} =
  let conn = await connect("host=127.0.0.1 dbname=mydb user=postgres")

  discard await conn.exec("CREATE TABLE users (id serial PRIMARY KEY, name text, age int)")
  discard await conn.exec("INSERT INTO users (name, age) VALUES ($1, $2)", "Alice", "30")

  let res = await conn.query("SELECT name, age FROM users WHERE age > $1", "25")
  for row in res.rows:
    echo row[0].get, " is ", row[1].get

  let name = await conn.queryValue("SELECT name FROM users WHERE id = $1", "1")
  echo name  # some("Alice")

  conn.close()

waitFor main()
```

## Pool

```nim
proc main() {.async.} =
  let pool = await newPool("host=127.0.0.1 dbname=mydb", minSize = 2, maxSize = 10)

  # Automatic acquire/release
  pool.withConn conn:
    discard await conn.exec("INSERT INTO users (name) VALUES ($1)", "Bob")

  # Convenience methods
  let count = await pool.queryValue("SELECT count(*) FROM users")

  # Concurrent -- pool hands each coroutine its own connection
  var futs: seq[Future[Option[string]]]
  for i in 0..<20:
    futs.add pool.queryValue("SELECT $1::text", $i)
  await allFutures(futs)

  await pool.close()

waitFor main()
```

## Transactions

```nim
await conn.withTransaction:
  discard await conn.exec("INSERT INTO accounts (id, balance) VALUES ($1, $2)", "1", "100")
  discard await conn.exec("UPDATE accounts SET balance = balance - $1 WHERE id = $2", "50", "1")
  # COMMIT on success, ROLLBACK on exception

await conn.withTransaction(isolation = tiSerializable, access = taReadOnly):
  let balance = await conn.queryValue("SELECT balance FROM accounts WHERE id = $1", "1")
```

## Prepared Statements

```nim
let stmt = await conn.prepare("get_user", "SELECT name, age FROM users WHERE id = $1")
let row = await stmt.queryOne("1")
echo row.get[0]  # some("Alice")
await stmt.close()
```

## Streaming

For large result sets, `forEachRow` uses libpq's single-row mode -- only one row in memory at a time:

```nim
var total = 0
let count = await conn.forEachRow("SELECT amount FROM transactions",
  proc(row: Row): Future[void] {.async.} =
    {.cast(gcsafe).}:
      total += parseInt(row[0].get)
)
```

Sync callback (no Future allocation per row):

```nim
await conn.forEachRow("SELECT id FROM big_table",
  proc(row: Row) {.gcsafe.} =
    echo row[0].get
)
```

## NULL Handling

Parameters and results use `Option[string]`. Pass `none(string)` for SQL NULL:

```nim
discard await conn.exec("INSERT INTO t VALUES ($1, $2)",
                        @[some("value"), none(string)])

let row = await conn.queryOne("SELECT nullable_col FROM t")
if row.get[0].isNone:
  echo "NULL"
```

## Errors

```nim
try:
  discard await conn.exec("INSERT INTO users (id) VALUES (1)")  # duplicate
except PgQueryError as e:
  echo e.sqlState  # "23505" (unique_violation)
  echo e.severity  # "ERROR"
  echo e.detail    # "Key (id)=(1) already exists."
```

## Pool Configuration

```nim
let pool = await newPool(connStr,
  minSize = 2,
  maxSize = 20,
  acquireTimeout = seconds(5),
  idleReapAfter = minutes(5),
  resetMode = rmTransactionCheck,  # skip DISCARD ALL for lower latency
)
```

`ResetMode` options:
- `rmDiscardAll` (default) -- `DISCARD ALL` on every release, safest
- `rmTransactionCheck` -- only verify no open transaction, no extra round-trip
- `rmNone` -- no reset, caller responsible for clean state

## Row Helpers

```nim
let res = await conn.query("SELECT id, name, score, active FROM users")
res.getInt(0, "id")       # some(1'i64)
res.getStr(0, "name")     # some("Alice")
res.getFloat(0, "score")  # some(3.14)
res.getBool(0, "active")  # some(true)
```

## Testing

Requires a local PostgreSQL with a `pgchronos_test` database:

```sql
CREATE DATABASE pgchronos_test;
```

```
nimble test
```

## License

MIT
