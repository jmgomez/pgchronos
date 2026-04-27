import std/unittest
import std/options
import ../src/pgchronos/types
import ../src/pgchronos/extract

suite "PgResult row access helpers":
  let testResult = PgResult(
    columns: @["id", "name", "age", "score", "active"],
    rows: @[
      @[some("1"), some("Alice"), some("30"), some("3.14"), some("t")],
      @[some("2"), none(string), some("25"), none(string), some("f")],
      @[some("3"), some(""), some("abc"), some("0.0"), none(string)],
    ],
    affected: 0
  )

  test "getStr returns value by column name":
    check testResult.getStr(0, "name") == some("Alice")

  test "getStr returns None for NULL":
    check testResult.getStr(1, "name") == none(string)

  test "getStr empty string is Some":
    check testResult.getStr(2, "name") == some("")

  test "getStr unknown column raises":
    expect KeyError:
      discard testResult.getStr(0, "nonexistent")

  test "getInt parses integer":
    check testResult.getInt(0, "id") == some(1'i64)

  test "getInt returns None for NULL":
    check testResult.getInt(1, "score") == none(int64)

  test "getInt non-numeric raises ValueError":
    expect ValueError:
      discard testResult.getInt(2, "age")

  test "getFloat parses float":
    check testResult.getFloat(0, "score") == some(3.14)

  test "getFloat returns None for NULL":
    check testResult.getFloat(1, "score") == none(float64)

  test "getBool parses PG booleans":
    check testResult.getBool(0, "active") == some(true)
    check testResult.getBool(1, "active") == some(false)

  test "getBool returns None for NULL":
    check testResult.getBool(2, "active") == none(bool)

  test "column names preserved in order":
    check testResult.columns == @["id", "name", "age", "score", "active"]

  test "Row indexed access":
    check testResult.rows[0][0] == some("1")

  test "Row NULL is None":
    check testResult.rows[1][1].isNone
