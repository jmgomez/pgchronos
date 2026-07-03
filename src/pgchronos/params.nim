## params.nim — query-parameter and row helpers for the repository layer.
##
## pgchronos passes parameters as `seq[Option[string]]` (none = SQL NULL) and
## returns rows as `seq[Option[string]]`. These helpers bridge ordinary Nim
## values to that representation. Helpers:
##   * toUntypedParam — string -> param, letting PostgreSQL infer the type
##     (avoids uuid-vs-text mismatches on FK columns).
##   * toNullableParam — "" -> SQL NULL (for nullable columns).
##   * toPgParam[T]    — any value -> its `$` text.
##   * getStr/isNull   — read a Row cell as string / test for NULL.

import std/options
import ./types

proc toUntypedParam*(v: string): Option[string] {.inline.} =
  ## A string parameter whose SQL type PostgreSQL infers from context.
  some(v)

proc toNullableParam*(v: string): Option[string] {.inline.} =
  ## For nullable columns: an empty string maps to SQL NULL (none).
  if v.len > 0: some(v) else: none(string)

proc toPgParam*[T](v: T): Option[string] {.inline.} =
  ## Generic: convert any value to its text representation as a parameter.
  some($v)

proc getStr*(row: Row, idx: int): string {.inline.} =
  ## Read cell `idx` of a Row as a string; "" for out-of-range or NULL.
  if idx < row.len and row[idx].isSome: row[idx].get
  else: ""

proc isNull*(row: Row, idx: int): bool {.inline.} =
  ## True if cell `idx` is SQL NULL (or out of range).
  idx >= row.len or row[idx].isNone
