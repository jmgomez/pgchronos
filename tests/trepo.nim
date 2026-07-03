## trepo.nim — Step 3: merged repository macro (non-tenant + tenantScoped)

import std/unittest
import std/options
import chronos
import ../src/pgchronos/types
import ../src/pgchronos/connection
import ../src/pgchronos/query
import ../src/pgchronos/repository
import ./helpers

# --- Non-tenant model ---
type
  Widget* {.table: "repo_widgets".} = object
    id* {.pk.}: string
    name*: string
    qty*: int
    price*: float
    active*: bool
    note* {.nullable.}: string
    createdAt* {.column: "created_at", readOnly.}: string

generateRepository(Widget)

# --- Tenant-scoped model ---
type
  Gadget* {.table: "repo_gadgets", tenantScoped.} = object
    id* {.pk.}: string
    tenantId* {.column: "tenant_id".}: string
    label*: string
    createdAt* {.column: "created_at", readOnly.}: string

generateRepository(Gadget)

const
  TenantA = "11111111-1111-1111-1111-111111111111"
  TenantB = "22222222-2222-2222-2222-222222222222"

proc resetSchema(conn: PgConn) {.async.} =
  await conn.simpleExec("""
    DROP TABLE IF EXISTS repo_widgets;
    DROP TABLE IF EXISTS repo_gadgets;
    CREATE TABLE repo_widgets (
      id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
      name text NOT NULL UNIQUE,
      qty int NOT NULL DEFAULT 0,
      price double precision NOT NULL DEFAULT 0,
      active boolean NOT NULL DEFAULT false,
      note text,
      created_at timestamptz NOT NULL DEFAULT now()
    );
    CREATE TABLE repo_gadgets (
      id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
      tenant_id uuid NOT NULL,
      label text NOT NULL,
      created_at timestamptz NOT NULL DEFAULT now()
    );
  """)

suite "repository: metadata + SQL generation":

  test "non-tenant metadata + SQL":
    check Widget.tableName() == "repo_widgets"
    check Widget.columnNames() == @["id", "name", "qty", "price", "active",
        "note", "created_at"]
    check Widget.writableColumns() == @["name", "qty", "price", "active", "note"]
    check Widget.sqlSelectById() ==
      "SELECT id, name, qty, price, active, note, created_at FROM repo_widgets WHERE id = $1"
    check Widget.sqlCount() == "SELECT COUNT(*) FROM repo_widgets"

  test "tenant-scoped SQL injects tenant filters":
    check Gadget.writableColumns() == @["tenant_id", "label"]
    check Gadget.sqlSelectById() ==
      "SELECT id, tenant_id, label, created_at FROM repo_gadgets WHERE id = $1 AND tenant_id = $2"
    check Gadget.sqlSelectAll() ==
      "SELECT id, tenant_id, label, created_at FROM repo_gadgets WHERE tenant_id = $1"
    check Gadget.sqlCount() == "SELECT COUNT(*) FROM repo_gadgets WHERE tenant_id = $1"
    check Gadget.sqlExists() ==
      "SELECT EXISTS(SELECT 1 FROM repo_gadgets WHERE id = $1 AND tenant_id = $2)"
    check Gadget.sqlUpdate() ==
      "UPDATE repo_gadgets SET tenant_id = $1, label = $2 WHERE id = $3 AND tenant_id = $4 RETURNING id, tenant_id, label, created_at"

suite "repository: non-tenant CRUD":

  test "insert / getById / update / delete / count / exists":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      await resetSchema(conn)
      let ins = await conn.dbInsert(Widget(name: "w1", qty: 3, price: 1.5,
          active: true, note: "hi"))
      check ins.isOk
      let w = ins.get
      check w.id.len > 0
      check w.name == "w1"
      check w.qty == 3
      check w.price == 1.5
      check w.active == true
      check w.note == "hi"

      let got = await conn.dbGetById(Widget, w.id)
      check got.isOk
      check got.get.name == "w1"

      var upd = w
      upd.qty = 9
      let ur = await conn.dbUpdate(upd)
      check ur.isOk
      check ur.get.qty == 9

      check (await conn.dbCount(Widget)).get == 1
      check (await conn.dbExists(Widget, w.id)).get == true

      let del = await conn.dbDelete(Widget, w.id)
      check del.isOk
      check (await conn.dbCount(Widget)).get == 0
      conn.close()
    waitFor test()

  test "nullable field: empty string round-trips as NULL":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      await resetSchema(conn)
      let ins = await conn.dbInsert(Widget(name: "nn", note: ""))
      check ins.isOk
      check ins.get.note == ""
      # Verify it's actually NULL in the DB, not empty string.
      let isNullVal = (await conn.queryValue(
        "SELECT note IS NULL FROM repo_widgets WHERE id = $1",
        @[some(ins.get.id)])).get("")
      check isNullVal == "t"
      let got = await conn.dbGetById(Widget, ins.get.id)
      check got.get.note == ""
      conn.close()
    waitFor test()

  test "getById missing -> dekNotFound":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      await resetSchema(conn)
      let r = await conn.dbGetById(Widget, "00000000-0000-0000-0000-000000000000")
      check not r.isOk
      check r.error.kind == dekNotFound
      check r.isNotFound
      conn.close()
    waitFor test()

  test "duplicate unique -> dekDuplicate":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      await resetSchema(conn)
      check (await conn.dbInsert(Widget(name: "dup"))).isOk
      let r2 = await conn.dbInsert(Widget(name: "dup"))
      check not r2.isOk
      check r2.error.kind == dekDuplicate
      conn.close()
    waitFor test()

  test "paged fetch":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      await resetSchema(conn)
      for i in 0 ..< 5:
        check (await conn.dbInsert(Widget(name: "p" & $i, qty: i))).isOk
      let page = await conn.dbGetAllPaged(Widget, 2, 1)
      check page.isOk
      check page.get.len == 2
      check (await conn.dbGetAll(Widget)).get.len == 5
      conn.close()
    waitFor test()

suite "repository: tenant-scoped CRUD + isolation":

  test "insert stamps tenant_id; reads are tenant-isolated":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      await resetSchema(conn)
      let ins = await conn.dbInsert(Gadget(label: "gA"), TenantA)
      check ins.isOk
      check ins.get.tenantId == TenantA
      let gid = ins.get.id

      # Same tenant sees it.
      check (await conn.dbGetById(Gadget, gid, TenantA)).isOk
      check (await conn.dbExists(Gadget, gid, TenantA)).get == true
      # Other tenant does not.
      let other = await conn.dbGetById(Gadget, gid, TenantB)
      check not other.isOk
      check other.error.kind == dekNotFound
      check (await conn.dbExists(Gadget, gid, TenantB)).get == false
      conn.close()
    waitFor test()

  test "getAll / count only return the active tenant's rows":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      await resetSchema(conn)
      check (await conn.dbInsert(Gadget(label: "a1"), TenantA)).isOk
      check (await conn.dbInsert(Gadget(label: "a2"), TenantA)).isOk
      check (await conn.dbInsert(Gadget(label: "b1"), TenantB)).isOk

      let aAll = await conn.dbGetAll(Gadget, TenantA)
      check aAll.get.len == 2
      check (await conn.dbCount(Gadget, TenantA)).get == 2
      let bAll = await conn.dbGetAll(Gadget, TenantB)
      check bAll.get.len == 1
      check bAll.get[0].label == "b1"
      conn.close()
    waitFor test()

  test "cross-tenant update and delete are blocked":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      await resetSchema(conn)
      let ins = await conn.dbInsert(Gadget(label: "orig"), TenantA)
      let gid = ins.get.id

      var upd = ins.get
      upd.label = "hacked"
      let ur = await conn.dbUpdate(upd, TenantB)   # wrong tenant
      check not ur.isOk
      check ur.error.kind == dekNotFound

      let dr = await conn.dbDelete(Gadget, gid, TenantB)   # wrong tenant
      check not dr.isOk

      # Still intact under the right tenant.
      let still = await conn.dbGetById(Gadget, gid, TenantA)
      check still.isOk
      check still.get.label == "orig"
      conn.close()
    waitFor test()

suite "repository: helpers + savepoint":

  test "dbInsertWithId preserves the caller-supplied id":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      await resetSchema(conn)
      let myId = "550e8400-e29b-41d4-a716-446655440000"
      var w = Widget(name: "withid")
      w.id = myId
      let r = await conn.dbInsertWithId(w)
      check r.isOk
      check r.get.id == myId
      conn.close()
    waitFor test()

  test "dbQueryOne / dbQueryMany":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      await resetSchema(conn)
      check (await conn.dbInsert(Widget(name: "q1", qty: 1))).isOk
      check (await conn.dbInsert(Widget(name: "q2", qty: 2))).isOk
      let one = await conn.dbQueryOne(Widget,
        "SELECT id, name, qty, price, active, note, created_at FROM repo_widgets WHERE name = $1",
        @[some("q1")])
      check one.isOk
      check one.get.name == "q1"
      let many = await conn.dbQueryMany(Widget,
        "SELECT id, name, qty, price, active, note, created_at FROM repo_widgets ORDER BY qty")
      check many.isOk
      check many.get.len == 2
      # not-found custom query -> dekNotFound
      let none = await conn.dbQueryOne(Widget,
        "SELECT id, name, qty, price, active, note, created_at FROM repo_widgets WHERE name = $1",
        @[some("nope")])
      check none.error.kind == dekNotFound
      conn.close()
    waitFor test()

  test "dbExec / dbExecAffected / dbScalar / dbScalarSeq":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      await resetSchema(conn)
      check (await conn.dbInsert(Widget(name: "s1"))).isOk
      check (await conn.dbInsert(Widget(name: "s2"))).isOk

      let ex = await conn.dbExec("UPDATE repo_widgets SET qty = 5")
      check ex.isOk
      let aff = await conn.dbExecAffected("UPDATE repo_widgets SET qty = 6")
      check aff.isOk
      check aff.get == 2
      let sc = await conn.dbScalar("SELECT count(*) FROM repo_widgets")
      check sc.isOk
      check sc.get == "2"
      let ss = await conn.dbScalarSeq("SELECT name FROM repo_widgets ORDER BY name")
      check ss.isOk
      check ss.get == @["s1", "s2"]
      # error path is classified, not raised
      let bad = await conn.dbExec("UPDATE nonexistent_table SET x = 1")
      check not bad.isOk
      conn.close()
    waitFor test()

  test "withSavepoint: rollback to savepoint on error, keep outer tx":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      await resetSchema(conn)
      discard await conn.exec("BEGIN")
      check (await conn.dbInsert(Widget(name: "keep"))).isOk
      var raised = false
      try:
        conn.withSavepoint:
          discard await conn.exec("INSERT INTO repo_widgets (name) VALUES ('sp')")
          raise newException(ValueError, "boom")
      except ValueError:
        raised = true
      check raised
      # Outer tx still alive and 'keep' survives; 'sp' rolled back.
      let names = await conn.dbScalarSeq("SELECT name FROM repo_widgets ORDER BY name")
      check names.get == @["keep"]
      discard await conn.exec("COMMIT")
      check (await conn.dbCount(Widget)).get == 1
      conn.close()
    waitFor test()

  test "withSavepoint: commits nested work on success":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      await resetSchema(conn)
      discard await conn.exec("BEGIN")
      conn.withSavepoint:
        discard await conn.exec("INSERT INTO repo_widgets (name) VALUES ('ok')")
      discard await conn.exec("COMMIT")
      check (await conn.dbCount(Widget)).get == 1
      conn.close()
    waitFor test()

  test "tenant-scoped dbGetAllPaged is tenant-filtered":
    proc test() {.async.} =
      let conn = await connect(TestConnStr)
      await resetSchema(conn)
      for i in 0 ..< 4:
        check (await conn.dbInsert(Gadget(label: "a" & $i), TenantA)).isOk
      check (await conn.dbInsert(Gadget(label: "b0"), TenantB)).isOk
      let page = await conn.dbGetAllPaged(Gadget, 2, 0, TenantA)
      check page.isOk
      check page.get.len == 2
      for g in page.get:
        check g.tenantId == TenantA
      conn.close()
    waitFor test()
