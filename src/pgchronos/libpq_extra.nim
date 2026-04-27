import db_connector/postgres

const
  PG_DIAG_SEVERITY* = cint('S')
  PG_DIAG_SQLSTATE* = cint('C')
  PG_DIAG_MESSAGE_PRIMARY* = cint('M')
  PG_DIAG_MESSAGE_DETAIL* = cint('D')
  PG_DIAG_MESSAGE_HINT* = cint('H')

when defined(windows):
  const dllName = "libpq.dll"
elif defined(macosx):
  const dllName = "libpq.dylib"
else:
  const dllName = "libpq.so(.5|)"

proc pqsendPrepare*(conn: PPGconn, stmtName, query: cstring,
                    nParams: int32, paramTypes: POid): int32
  {.cdecl, dynlib: dllName, importc: "PQsendPrepare".}
