## repository.nim — generic repository pattern for pgchronos.
##
## `generateRepository(T)` reads an annotated object type at compile time and
## emits metadata procs, SQL statements, row conversion, and typed async CRUD
## returning `DbResult[T]`.
##
##   type
##     User* {.table: "users".} = object
##       id* {.pk.}: string
##       email*: string
##       firstName* {.column: "first_name".}: string
##       createdAt* {.column: "created_at", readOnly.}: string
##   generateRepository(User)
##
## Features:
##   * direct queryOne/queryValue, generic toPgParam, PgConn/seq[Option[string]]
##     signatures.
##   * {.tenantScoped.} — multi-tenant infra: tenant SQL variants and a trailing
##     `tenantId: string` param on every CRUD proc. Costs nothing for non-tenant
##     models (a compile-time `when` picks the plain signature).
##   * {.nullable.} + toNullableParam — ""→NULL column mapping.
##   * NULL-guarded fromRow — every field guarded via isNull.
##   * dbScalarSeq.
##   * withSavepoint — a SAVEPOINT template (named to avoid clashing with
##     transaction.nim).

import std/[macros, genasts, strutils, options]
import ./types
import ./query
import ./params
import chronos

export types, query, params, strutils, options, chronos

# =============================================================================
# Custom Pragmas
# =============================================================================

template table*(name: string) {.pragma.}
  ## SQL table name. If omitted, snake_case(typeName) & "s".

template column*(name: string) {.pragma.}
  ## Override the SQL column name (default: snake_case of the field name).

template pk*() {.pragma.}
  ## Marks the primary key field. Exactly one per entity.

template readOnly*() {.pragma.}
  ## Read from the DB but never written in INSERT/UPDATE.

template skipField*() {.pragma.}
  ## Excluded from all SQL operations entirely.

template nullable*() {.pragma.}
  ## String field maps to a nullable column: "" → SQL NULL.

template tenantScoped*() {.pragma.}
  ## Marks a type as tenant-scoped: CRUD procs gain a trailing `tenantId` param,
  ## inserts/updates stamp `tenant_id`, and reads/deletes filter by it.

# =============================================================================
# Compile-Time Helpers
# =============================================================================

proc camelToSnake(s: string): string {.compileTime.} =
  result = ""
  for i, c in s:
    if c.isUpperAscii:
      if i > 0:
        result.add '_'
      result.add c.toLowerAscii
    else:
      result.add c

type
  FieldInfo = object
    nimName: string
    sqlColumn: string
    isPk: bool
    isReadOnly: bool
    isSkipped: bool
    isNullable: bool
    nimType: NimNode

proc analyzeFields(recList: NimNode): seq[FieldInfo] {.compileTime.} =
  result = @[]
  for field in recList:
    if field.kind != nnkIdentDefs:
      continue
    let nameNode = field[0]
    var info: FieldInfo
    info.nimType = field[^2]

    if nameNode.kind == nnkPragmaExpr:
      let identNode = nameNode[0]
      info.nimName = if identNode.kind == nnkPostfix: $identNode[1]
                     else: $identNode
      let pragmas = nameNode[1]
      for p in pragmas:
        if p.kind == nnkExprColonExpr and p[0].eqIdent("column"):
          info.sqlColumn = p[1].strVal
        elif p.eqIdent("pk"):
          info.isPk = true
        elif p.eqIdent("readOnly"):
          info.isReadOnly = true
        elif p.eqIdent("skipField"):
          info.isSkipped = true
        elif p.eqIdent("nullable"):
          info.isNullable = true
    elif nameNode.kind == nnkPostfix:
      info.nimName = $nameNode[1]
    else:
      info.nimName = $nameNode

    if info.sqlColumn == "":
      info.sqlColumn = camelToSnake(info.nimName)

    if not info.isSkipped:
      result.add info

# =============================================================================
# The Core Macro
# =============================================================================

macro generateRepository*(T: typedesc): untyped =
  let impl = T.getType[1].getImpl

  # -- Extract type name --
  let typeNameNode = impl[0]
  let typeName = if typeNameNode.kind == nnkPragmaExpr:
    let inner = typeNameNode[0]
    if inner.kind == nnkPostfix: inner[1] else: inner
  elif typeNameNode.kind == nnkPostfix:
    typeNameNode[1]
  else:
    typeNameNode
  let typeIdent = ident($typeName)

  # -- Extract table name + tenantScoped pragma --
  var tableName = ""
  var isTenantScoped = false
  if typeNameNode.kind == nnkPragmaExpr:
    let pragmaList = typeNameNode[1]
    for p in pragmaList:
      if p.kind == nnkExprColonExpr and p[0].eqIdent("table"):
        tableName = p[1].strVal
      elif p.eqIdent("tenantScoped"):
        isTenantScoped = true
  if tableName == "":
    tableName = camelToSnake($typeName) & "s"

  # -- Extract object fields --
  var recList: NimNode
  let objectTy = impl[2]
  if objectTy.kind == nnkObjectTy:
    recList = objectTy[2]
  elif objectTy.kind == nnkRefTy:
    recList = objectTy[0][2]
  else:
    error("generateRepository expects an object type, got " & $objectTy.kind, T)

  let fields = analyzeFields(recList)

  # -- Find PK field --
  var pkIdx = -1
  for i, f in fields:
    if f.isPk:
      pkIdx = i
      break
  if pkIdx < 0:
    error($typeName & " must have exactly one field annotated with {.pk.}", T)
  let pkField = fields[pkIdx]
  let pkIdent = ident(pkField.nimName)

  # -- Build column lists --
  var allColumns, writableColumns: seq[string]
  var writableFieldNames: seq[string]
  for f in fields:
    allColumns.add f.sqlColumn
    if not f.isPk and not f.isReadOnly:
      writableColumns.add f.sqlColumn
      writableFieldNames.add f.nimName

  # -- Build SQL statements with numbered $N placeholders --
  let allColStr = allColumns.join(", ")
  let writableColStr = writableColumns.join(", ")

  let insertPlaceholders = block:
    var parts: seq[string]
    for i in 1..writableColumns.len:
      parts.add "$" & $i
    parts.join(", ")

  let updateSets = block:
    var parts: seq[string]
    for i, col in writableColumns:
      parts.add col & " = $" & $(i + 1)
    parts.join(", ")

  let updateWhereParam = "$" & $(writableColumns.len + 1)

  let sqlInsertStr = "INSERT INTO " & tableName & " (" & writableColStr &
      ") VALUES (" & insertPlaceholders & ") RETURNING " & allColStr

  let insertWithPkCols = pkField.sqlColumn & ", " & writableColStr
  let insertWithPkPlaceholders = block:
    var parts: seq[string]
    for i in 1..(writableColumns.len + 1):
      parts.add "$" & $i
    parts.join(", ")
  let sqlInsertWithPkStr = "INSERT INTO " & tableName & " (" & insertWithPkCols &
      ") VALUES (" & insertWithPkPlaceholders & ") RETURNING " & allColStr

  var sqlSelectByIdStr = "SELECT " & allColStr & " FROM " & tableName &
      " WHERE " & pkField.sqlColumn & " = $1"
  var sqlSelectAllStr = "SELECT " & allColStr & " FROM " & tableName
  var sqlSelectAllPagedStr = "SELECT " & allColStr & " FROM " & tableName &
      " ORDER BY " & pkField.sqlColumn & " LIMIT $1 OFFSET $2"
  let sqlUpdateStr = "UPDATE " & tableName & " SET " & updateSets &
      " WHERE " & pkField.sqlColumn & " = " & updateWhereParam & " RETURNING " & allColStr
  var sqlUpdateTenantStr = sqlUpdateStr
  var sqlDeleteStr = "DELETE FROM " & tableName & " WHERE " &
      pkField.sqlColumn & " = $1"
  var sqlCountStr = "SELECT COUNT(*) FROM " & tableName
  var sqlExistsStr = "SELECT EXISTS(SELECT 1 FROM " & tableName &
      " WHERE " & pkField.sqlColumn & " = $1)"

  # -- Tenant-scoped SQL variants --
  if isTenantScoped:
    sqlSelectByIdStr &= " AND tenant_id = $2"
    sqlSelectAllStr &= " WHERE tenant_id = $1"
    sqlSelectAllPagedStr = "SELECT " & allColStr & " FROM " & tableName &
        " WHERE tenant_id = $1 ORDER BY " & pkField.sqlColumn & " LIMIT $2 OFFSET $3"
    let updateTenantWhereParam = "$" & $(writableColumns.len + 2)
    sqlUpdateTenantStr = "UPDATE " & tableName & " SET " & updateSets &
        " WHERE " & pkField.sqlColumn & " = " & updateWhereParam &
        " AND tenant_id = " & updateTenantWhereParam & " RETURNING " & allColStr
    sqlDeleteStr &= " AND tenant_id = $2"
    sqlCountStr &= " WHERE tenant_id = $1"
    sqlExistsStr = "SELECT EXISTS(SELECT 1 FROM " & tableName &
        " WHERE " & pkField.sqlColumn & " = $1 AND tenant_id = $2)"

  # -- Build toRow (writable field values) --
  let entityIdent = ident("entity")
  var toRowItems = newNimNode(nnkBracket)
  for fname in writableFieldNames:
    let fld = ident(fname)
    var ft: NimNode
    var isNull = false
    for f in fields:
      if f.nimName == fname:
        ft = f.nimType
        isNull = f.isNullable
        break
    if ft != nil and ft.eqIdent("string"):
      if isNull:
        toRowItems.add newCall(ident("toNullableParam"),
          newDotExpr(entityIdent, fld))
      else:
        toRowItems.add newCall(ident("toUntypedParam"),
          newCall(ident("$"), newDotExpr(entityIdent, fld)))
    else:
      toRowItems.add newCall(ident("toPgParam"), newDotExpr(entityIdent, fld))

  # -- Build fromRow: NULL-guard EVERY field --
  let rowIdent = ident("row")
  var fromRowBody = newNimNode(nnkStmtList)
  for i, f in fields:
    let fld = ident(f.nimName)
    let idx = newLit(i)
    let ft = f.nimType
    let resultDot = newDotExpr(ident("result"), fld)
    let notNull = prefix(newCall(ident("isNull"), rowIdent, idx), "not")

    let innerAssign =
      if ft.eqIdent("int"):
        newAssignment(resultDot, newCall(ident("parseInt"),
            newCall(ident("getStr"), rowIdent, idx)))
      elif ft.eqIdent("int64"):
        newAssignment(resultDot, newCall(ident("parseBiggestInt"),
            newCall(ident("getStr"), rowIdent, idx)))
      elif ft.eqIdent("float") or ft.eqIdent("float64"):
        newAssignment(resultDot, newCall(ident("parseFloat"),
            newCall(ident("getStr"), rowIdent, idx)))
      elif ft.eqIdent("bool"):
        newAssignment(resultDot,
          infix(
            infix(newCall(ident("getStr"), rowIdent, idx), "==", newLit("t")),
            "or",
            infix(newCall(ident("getStr"), rowIdent, idx), "==", newLit("true"))
          )
        )
      else:
        newAssignment(resultDot, newCall(ident("getStr"), rowIdent, idx))

    fromRowBody.add newIfStmt((notNull, newStmtList(innerAssign)))

  # -- Literal nodes for genAst --
  let tableNameLit = newLit(tableName)
  let pkColumnLit = newLit(pkField.sqlColumn)
  let allColumnsLit = newLit(allColumns)
  let writableColumnsLit = newLit(writableColumns)
  let sqlInsertLit = newLit(sqlInsertStr)
  let sqlInsertWithPkLit = newLit(sqlInsertWithPkStr)
  let sqlSelectByIdLit = newLit(sqlSelectByIdStr)
  let sqlSelectAllLit = newLit(sqlSelectAllStr)
  let sqlSelectAllPagedLit = newLit(sqlSelectAllPagedStr)
  let sqlUpdateLit = if isTenantScoped: newLit(sqlUpdateTenantStr)
                     else: newLit(sqlUpdateStr)
  let sqlDeleteLit = newLit(sqlDeleteStr)
  let sqlCountLit = newLit(sqlCountStr)
  let sqlExistsLit = newLit(sqlExistsStr)
  let isTenantScopedLit = newLit(isTenantScoped)

  result = genAst(typeIdent, tableNameLit, pkColumnLit, allColumnsLit,
      writableColumnsLit, sqlInsertLit, sqlInsertWithPkLit, sqlSelectByIdLit,
      sqlSelectAllLit, sqlSelectAllPagedLit, sqlUpdateLit, sqlDeleteLit,
      sqlCountLit, sqlExistsLit, toRowItems, fromRowBody, pkIdent,
      isTenantScopedLit, entity = entityIdent, row = rowIdent):

    # -- Metadata --
    proc tableName*(T: typedesc[typeIdent]): string {.inline.} = tableNameLit
    proc pkColumn*(T: typedesc[typeIdent]): string {.inline.} = pkColumnLit
    proc columnNames*(T: typedesc[typeIdent]): seq[string] {.inline.} = allColumnsLit
    proc writableColumns*(T: typedesc[typeIdent]): seq[string] {.inline.} = writableColumnsLit

    # -- SQL statement accessors --
    proc sqlInsert*(T: typedesc[typeIdent]): string {.inline.} = sqlInsertLit
    proc sqlInsertWithPk*(T: typedesc[typeIdent]): string {.inline.} = sqlInsertWithPkLit
    proc sqlSelectById*(T: typedesc[typeIdent]): string {.inline.} = sqlSelectByIdLit
    proc sqlSelectAll*(T: typedesc[typeIdent]): string {.inline.} = sqlSelectAllLit
    proc sqlSelectAllPaged*(T: typedesc[typeIdent]): string {.inline.} = sqlSelectAllPagedLit
    proc sqlUpdate*(T: typedesc[typeIdent]): string {.inline.} = sqlUpdateLit
    proc sqlDelete*(T: typedesc[typeIdent]): string {.inline.} = sqlDeleteLit
    proc sqlCount*(T: typedesc[typeIdent]): string {.inline.} = sqlCountLit
    proc sqlExists*(T: typedesc[typeIdent]): string {.inline.} = sqlExistsLit

    # -- Row conversion --
    proc toRow*(entity: typeIdent): seq[Option[string]] = @toRowItems
    proc fromRow*(row: Row, T: typedesc[typeIdent]): typeIdent = fromRowBody
    proc pkValue*(entity: typeIdent): string {.inline.} = $entity.pkIdent

    # -- Typed CRUD (async). Tenant-scoped types get a trailing tenantId param;
    #    the `when` picks the exact signature at compile time. --
    when isTenantScopedLit:

      proc dbInsert*(conn: PgConn, entity: typeIdent,
          tenantId: string): Future[DbResult[typeIdent]] {.async.} =
        try:
          var e = entity
          e.tenantId = tenantId
          let params = e.toRow()
          let rowOpt = await conn.queryOne(sqlInsertLit, params)
          if rowOpt.isNone:
            return DbResult[typeIdent].err(dekQuery,
                "INSERT into " & tableNameLit & " returned empty row")
          return DbResult[typeIdent].ok(fromRow(rowOpt.get, typeIdent))
        except CatchableError as e:
          return DbResult[typeIdent].err(classifyPgError(e),
              "Insert into " & tableNameLit & " failed", e.msg)

      proc dbInsertWithId*(conn: PgConn, entity: typeIdent,
          tenantId: string): Future[DbResult[typeIdent]] {.async.} =
        try:
          var e = entity
          e.tenantId = tenantId
          var params = @[toUntypedParam(pkValue(e))]
          params.add(e.toRow())
          let rowOpt = await conn.queryOne(sqlInsertWithPkLit, params)
          if rowOpt.isNone:
            return DbResult[typeIdent].err(dekQuery,
                "INSERT into " & tableNameLit & " returned empty row")
          return DbResult[typeIdent].ok(fromRow(rowOpt.get, typeIdent))
        except CatchableError as e:
          return DbResult[typeIdent].err(classifyPgError(e),
              "Insert into " & tableNameLit & " failed", e.msg)

      proc dbGetById*(conn: PgConn, T: typedesc[typeIdent], id: string,
          tenantId: string): Future[DbResult[typeIdent]] {.async.} =
        try:
          let rowOpt = await conn.queryOne(sqlSelectByIdLit,
              @[toUntypedParam(id), toUntypedParam(tenantId)])
          if rowOpt.isNone:
            return DbResult[typeIdent].err(dekNotFound,
                tableNameLit & " with " & pkColumnLit & " = '" & id & "' not found")
          return DbResult[typeIdent].ok(fromRow(rowOpt.get, typeIdent))
        except CatchableError as e:
          return DbResult[typeIdent].err(classifyPgError(e),
              "Select from " & tableNameLit & " failed", e.msg)

      proc dbGetAll*(conn: PgConn, T: typedesc[typeIdent],
          tenantId: string): Future[DbResult[seq[typeIdent]]] {.async.} =
        try:
          let qr = await conn.query(sqlSelectAllLit, @[toUntypedParam(tenantId)])
          var entities = newSeq[typeIdent](qr.rows.len)
          for i, row in qr.rows:
            entities[i] = fromRow(row, typeIdent)
          return DbResult[seq[typeIdent]].ok(entities)
        except CatchableError as e:
          return DbResult[seq[typeIdent]].err(classifyPgError(e),
              "Select all from " & tableNameLit & " failed", e.msg)

      proc dbGetAllPaged*(conn: PgConn, T: typedesc[typeIdent],
          limit: int, offset: int,
          tenantId: string): Future[DbResult[seq[typeIdent]]] {.async.} =
        try:
          let qr = await conn.query(sqlSelectAllPagedLit,
              @[toUntypedParam(tenantId), toPgParam(limit), toPgParam(offset)])
          var entities = newSeq[typeIdent](qr.rows.len)
          for i, row in qr.rows:
            entities[i] = fromRow(row, typeIdent)
          return DbResult[seq[typeIdent]].ok(entities)
        except CatchableError as e:
          return DbResult[seq[typeIdent]].err(classifyPgError(e),
              "Select paged from " & tableNameLit & " failed", e.msg)

      proc dbUpdate*(conn: PgConn, entity: typeIdent,
          tenantId: string): Future[DbResult[typeIdent]] {.async.} =
        try:
          var e = entity
          e.tenantId = tenantId
          var params = e.toRow()
          params.add(toUntypedParam(pkValue(e)))
          params.add(toUntypedParam(tenantId))
          let rowOpt = await conn.queryOne(sqlUpdateLit, params)
          if rowOpt.isNone:
            return DbResult[typeIdent].err(dekNotFound,
                tableNameLit & " with " & pkColumnLit & " = '" &
                pkValue(entity) & "' not found for update")
          return DbResult[typeIdent].ok(fromRow(rowOpt.get, typeIdent))
        except CatchableError as e:
          return DbResult[typeIdent].err(classifyPgError(e),
              "Update " & tableNameLit & " failed", e.msg)

      proc dbDelete*(conn: PgConn, T: typedesc[typeIdent], id: string,
          tenantId: string): Future[DbResult[void]] {.async.} =
        try:
          let affected = await conn.exec(sqlDeleteLit,
              @[toUntypedParam(id), toUntypedParam(tenantId)])
          if affected == 0:
            return DbResult[void].err(dekNotFound,
                tableNameLit & " with " & pkColumnLit & " = '" & id &
                "' not found for deletion")
          return DbResult[void].ok()
        except CatchableError as e:
          return DbResult[void].err(classifyPgError(e),
              "Delete from " & tableNameLit & " failed", e.msg)

      proc dbCount*(conn: PgConn, T: typedesc[typeIdent],
          tenantId: string): Future[DbResult[int64]] {.async.} =
        try:
          let val = (await conn.queryValue(sqlCountLit,
              @[toUntypedParam(tenantId)])).get("0")
          return DbResult[int64].ok(parseBiggestInt(val))
        except CatchableError as e:
          return DbResult[int64].err(classifyPgError(e),
              "Count " & tableNameLit & " failed", e.msg)

      proc dbExists*(conn: PgConn, T: typedesc[typeIdent], id: string,
          tenantId: string): Future[DbResult[bool]] {.async.} =
        try:
          let val = (await conn.queryValue(sqlExistsLit,
              @[toUntypedParam(id), toUntypedParam(tenantId)])).get("")
          return DbResult[bool].ok(val == "t" or val == "true")
        except CatchableError as e:
          return DbResult[bool].err(classifyPgError(e),
              "Exists check on " & tableNameLit & " failed", e.msg)

    else:

      proc dbInsert*(conn: PgConn,
          entity: typeIdent): Future[DbResult[typeIdent]] {.async.} =
        try:
          let params = entity.toRow()
          let rowOpt = await conn.queryOne(sqlInsertLit, params)
          if rowOpt.isNone:
            return DbResult[typeIdent].err(dekQuery,
                "INSERT into " & tableNameLit & " returned empty row")
          return DbResult[typeIdent].ok(fromRow(rowOpt.get, typeIdent))
        except CatchableError as e:
          return DbResult[typeIdent].err(classifyPgError(e),
              "Insert into " & tableNameLit & " failed", e.msg)

      proc dbInsertWithId*(conn: PgConn,
          entity: typeIdent): Future[DbResult[typeIdent]] {.async.} =
        try:
          var params = @[toUntypedParam(pkValue(entity))]
          params.add(entity.toRow())
          let rowOpt = await conn.queryOne(sqlInsertWithPkLit, params)
          if rowOpt.isNone:
            return DbResult[typeIdent].err(dekQuery,
                "INSERT into " & tableNameLit & " returned empty row")
          return DbResult[typeIdent].ok(fromRow(rowOpt.get, typeIdent))
        except CatchableError as e:
          return DbResult[typeIdent].err(classifyPgError(e),
              "Insert into " & tableNameLit & " failed", e.msg)

      proc dbGetById*(conn: PgConn, T: typedesc[typeIdent],
          id: string): Future[DbResult[typeIdent]] {.async.} =
        try:
          let rowOpt = await conn.queryOne(sqlSelectByIdLit, @[toUntypedParam(id)])
          if rowOpt.isNone:
            return DbResult[typeIdent].err(dekNotFound,
                tableNameLit & " with " & pkColumnLit & " = '" & id & "' not found")
          return DbResult[typeIdent].ok(fromRow(rowOpt.get, typeIdent))
        except CatchableError as e:
          return DbResult[typeIdent].err(classifyPgError(e),
              "Select from " & tableNameLit & " failed", e.msg)

      proc dbGetAll*(conn: PgConn,
          T: typedesc[typeIdent]): Future[DbResult[seq[typeIdent]]] {.async.} =
        try:
          let qr = await conn.query(sqlSelectAllLit, @[])
          var entities = newSeq[typeIdent](qr.rows.len)
          for i, row in qr.rows:
            entities[i] = fromRow(row, typeIdent)
          return DbResult[seq[typeIdent]].ok(entities)
        except CatchableError as e:
          return DbResult[seq[typeIdent]].err(classifyPgError(e),
              "Select all from " & tableNameLit & " failed", e.msg)

      proc dbGetAllPaged*(conn: PgConn, T: typedesc[typeIdent],
          limit: int, offset: int): Future[DbResult[seq[typeIdent]]] {.async.} =
        try:
          let qr = await conn.query(sqlSelectAllPagedLit,
              @[toPgParam(limit), toPgParam(offset)])
          var entities = newSeq[typeIdent](qr.rows.len)
          for i, row in qr.rows:
            entities[i] = fromRow(row, typeIdent)
          return DbResult[seq[typeIdent]].ok(entities)
        except CatchableError as e:
          return DbResult[seq[typeIdent]].err(classifyPgError(e),
              "Select paged from " & tableNameLit & " failed", e.msg)

      proc dbUpdate*(conn: PgConn,
          entity: typeIdent): Future[DbResult[typeIdent]] {.async.} =
        try:
          var params = entity.toRow()
          params.add(toUntypedParam(pkValue(entity)))
          let rowOpt = await conn.queryOne(sqlUpdateLit, params)
          if rowOpt.isNone:
            return DbResult[typeIdent].err(dekNotFound,
                tableNameLit & " with " & pkColumnLit & " = '" &
                pkValue(entity) & "' not found for update")
          return DbResult[typeIdent].ok(fromRow(rowOpt.get, typeIdent))
        except CatchableError as e:
          return DbResult[typeIdent].err(classifyPgError(e),
              "Update " & tableNameLit & " failed", e.msg)

      proc dbDelete*(conn: PgConn, T: typedesc[typeIdent],
          id: string): Future[DbResult[void]] {.async.} =
        try:
          let affected = await conn.exec(sqlDeleteLit, @[toUntypedParam(id)])
          if affected == 0:
            return DbResult[void].err(dekNotFound,
                tableNameLit & " with " & pkColumnLit & " = '" & id &
                "' not found for deletion")
          return DbResult[void].ok()
        except CatchableError as e:
          return DbResult[void].err(classifyPgError(e),
              "Delete from " & tableNameLit & " failed", e.msg)

      proc dbCount*(conn: PgConn,
          T: typedesc[typeIdent]): Future[DbResult[int64]] {.async.} =
        try:
          let val = (await conn.queryValue(sqlCountLit)).get("0")
          return DbResult[int64].ok(parseBiggestInt(val))
        except CatchableError as e:
          return DbResult[int64].err(classifyPgError(e),
              "Count " & tableNameLit & " failed", e.msg)

      proc dbExists*(conn: PgConn, T: typedesc[typeIdent],
          id: string): Future[DbResult[bool]] {.async.} =
        try:
          let val = (await conn.queryValue(sqlExistsLit,
              @[toUntypedParam(id)])).get("")
          return DbResult[bool].ok(val == "t" or val == "true")
        except CatchableError as e:
          return DbResult[bool].err(classifyPgError(e),
              "Exists check on " & tableNameLit & " failed", e.msg)

    # -- Custom entity-typed query helpers (no tenant param) --
    proc dbQueryOne*(conn: PgConn, T: typedesc[typeIdent], query: string,
        params: seq[Option[string]] = @[]): Future[DbResult[typeIdent]] {.async.} =
      try:
        let rowOpt = await conn.queryOne(query, params)
        if rowOpt.isNone:
          return DbResult[typeIdent].err(dekNotFound,
              "No matching " & tableNameLit & " found")
        return DbResult[typeIdent].ok(fromRow(rowOpt.get, typeIdent))
      except CatchableError as e:
        return DbResult[typeIdent].err(classifyPgError(e),
            "Custom query on " & tableNameLit & " failed", e.msg)

    proc dbQueryMany*(conn: PgConn, T: typedesc[typeIdent], query: string,
        params: seq[Option[string]] = @[]): Future[DbResult[seq[typeIdent]]] {.async.} =
      try:
        let qr = await conn.query(query, params)
        var entities = newSeq[typeIdent](qr.rows.len)
        for i, row in qr.rows:
          entities[i] = fromRow(row, typeIdent)
        return DbResult[seq[typeIdent]].ok(entities)
      except CatchableError as e:
        return DbResult[seq[typeIdent]].err(classifyPgError(e),
            "Custom query on " & tableNameLit & " failed", e.msg)

# =============================================================================
# Untyped Query Helpers (not per-entity, always available, async)
# =============================================================================

proc dbExec*(conn: PgConn, query: string,
    params: seq[Option[string]] = @[]): Future[DbResult[void]] {.async.} =
  try:
    discard await conn.exec(query, params)
    return DbResult[void].ok()
  except CatchableError as e:
    return DbResult[void].err(classifyPgError(e), "Exec failed", e.msg)

proc dbExecAffected*(conn: PgConn, query: string,
    params: seq[Option[string]] = @[]): Future[DbResult[int64]] {.async.} =
  try:
    let affected = await conn.exec(query, params)
    return DbResult[int64].ok(affected)
  except CatchableError as e:
    return DbResult[int64].err(classifyPgError(e), "Exec failed", e.msg)

proc dbScalar*(conn: PgConn, query: string,
    params: seq[Option[string]] = @[]): Future[DbResult[string]] {.async.} =
  try:
    let val = (await conn.queryValue(query, params)).get("")
    return DbResult[string].ok(val)
  except CatchableError as e:
    return DbResult[string].err(classifyPgError(e), "Scalar query failed", e.msg)

proc dbScalarSeq*(conn: PgConn, query: string,
    params: seq[Option[string]] = @[]): Future[DbResult[seq[string]]] {.async.} =
  ## Column 0 of every row as a seq[string]; err on DB failure.
  try:
    let qr = await conn.query(query, params)
    var vals = newSeq[string](qr.rows.len)
    for i, row in qr.rows:
      vals[i] = row.getStr(0)
    return DbResult[seq[string]].ok(vals)
  except CatchableError as e:
    return DbResult[seq[string]].err(classifyPgError(e),
        "Scalar-seq query failed", e.msg)

# =============================================================================
# SAVEPOINT helper (named to avoid the clash with transaction.nim). Nests a
# savepoint inside an open transaction.
# =============================================================================

template withSavepoint*(conn: PgConn, body: untyped) =
  discard await conn.exec("SAVEPOINT repo_tx")
  try:
    body
    discard await conn.exec("RELEASE SAVEPOINT repo_tx")
  except CatchableError:
    discard await conn.exec("ROLLBACK TO SAVEPOINT repo_tx")
    raise
