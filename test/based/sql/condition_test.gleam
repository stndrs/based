import based/sql/condition
import based/sql/internal/fmt
import based/sql/internal/value
import gleam/list
import gleam/option.{None, Some}

fn mock_comparable(
  node: condition.Node,
  values: List(v),
) -> fn() -> condition.Comparable(a, v) {
  fn() { condition.comparable(fn(_) { #(node, values) }) }
}

pub fn exists_to_string_test() {
  let fmt = fmt.new()

  let exists =
    condition.exists("SELECT 1")
    |> condition.to_string(fmt)

  assert "EXISTS (SELECT 1)" == exists
}

pub fn raw_to_string_test() {
  let fmt = fmt.new()

  let raw =
    condition.raw("a = b")
    |> condition.to_string(fmt)

  assert "a = b" == raw
}

pub fn not_to_string_test() {
  let fmt = fmt.new()

  let not =
    condition.raw("a = b")
    |> condition.not
    |> condition.to_string(fmt)

  assert "NOT a = b" == not
}

pub fn operators_to_string_test() {
  let fmt = fmt.new()
  let left = condition.column("a", None, None)
  let right = condition.column("b", None, None)
  let mock_left = mock_comparable(left, [])
  let mock_right = mock_comparable(right, [])

  let #(cond, _) = condition.eq("a", "b", of: mock_left, and: mock_right)
  assert "a = b" == condition.to_string(cond, fmt)

  let #(cond, _) = condition.gt("a", "b", of: mock_left, and: mock_right)
  assert "a > b" == condition.to_string(cond, fmt)

  let #(cond, _) = condition.lt("a", "b", of: mock_left, and: mock_right)
  assert "a < b" == condition.to_string(cond, fmt)

  let #(cond, _) = condition.gt_eq("a", "b", of: mock_left, and: mock_right)
  assert "a >= b" == condition.to_string(cond, fmt)

  let #(cond, _) = condition.lt_eq("a", "b", of: mock_left, and: mock_right)
  assert "a <= b" == condition.to_string(cond, fmt)

  let #(cond, _) = condition.not_eq("a", "b", of: mock_left, and: mock_right)
  assert "a <> b" == condition.to_string(cond, fmt)

  let #(cond, _) = condition.in("a", "b", of: mock_left, and: mock_right)
  assert "a IN b" == condition.to_string(cond, fmt)

  assert "a LIKE b" == condition.like(left, right) |> condition.to_string(fmt)
  assert "a NOT LIKE b"
    == condition.not_like(left, right) |> condition.to_string(fmt)
}

pub fn between_to_string_test() {
  let fmt = fmt.new()
  let a = condition.column("a", None, None)
  let b = condition.column("b", None, None)

  let #(cond, _) =
    condition.between(
      "a",
      "b",
      "b",
      of: mock_comparable(a, []),
      and: mock_comparable(b, []),
    )
  assert "a BETWEEN b AND b" == condition.to_string(cond, fmt)
}

pub fn logical_to_string_test() {
  let fmt = fmt.new()
  let a = condition.raw("a")
  let b = condition.raw("b")

  assert "a OR b" == condition.or(a, b) |> condition.to_string(fmt)
  assert "NOT a OR b"
    == condition.or(a, b) |> condition.not |> condition.to_string(fmt)
}

pub fn is_to_string_test() {
  let fmt = fmt.new()
  let a = condition.column("a", None, None)

  assert "a IS TRUE" == condition.is(a, True) |> condition.to_string(fmt)
  assert "a IS FALSE" == condition.is(a, False) |> condition.to_string(fmt)
  assert "a IS NULL" == condition.is_null(a, True) |> condition.to_string(fmt)
  assert "a IS NOT NULL"
    == condition.is_null(a, False) |> condition.to_string(fmt)
}

pub fn node_to_string_test() {
  let fmt = fmt.new()

  assert "col IS NULL"
    == condition.column("col", None, None)
    |> condition.is_null(True)
    |> condition.to_string(fmt)
  assert "tab.col IS NULL"
    == condition.column("col", None, Some("tab"))
    |> condition.is_null(True)
    |> condition.to_string(fmt)
  assert "tab.col AS ali IS NULL"
    == condition.column("col", Some("ali"), Some("tab"))
    |> condition.is_null(True)
    |> condition.to_string(fmt)

  assert "COUNT(col) IS NULL"
    == condition.aggregate(fmt.count, "col", None, None)
    |> condition.is_null(True)
    |> condition.to_string(fmt)
  assert "COUNT(tab.col) IS NULL"
    == condition.aggregate(fmt.count, "col", Some("tab"), None)
    |> condition.is_null(True)
    |> condition.to_string(fmt)
  assert "COUNT(tab.col) AS ali IS NULL"
    == condition.aggregate(fmt.count, "col", Some("tab"), Some("ali"))
    |> condition.is_null(True)
    |> condition.to_string(fmt)

  assert "(SELECT 1) IS NULL"
    == condition.subquery("SELECT 1")
    |> condition.is_null(True)
    |> condition.to_string(fmt)
  assert "ANY (SELECT 1) IS NULL"
    == condition.any("SELECT 1")
    |> condition.is_null(True)
    |> condition.to_string(fmt)
  assert "ALL (SELECT 1) IS NULL"
    == condition.all("SELECT 1")
    |> condition.is_null(True)
    |> condition.to_string(fmt)

  assert ":param IS NULL"
    == condition.value |> condition.is_null(True) |> condition.to_string(fmt)
  assert ":param IS NULL"
    == condition.text("val")
    |> condition.is_null(True)
    |> condition.to_string(fmt)
  assert "(:param, :param, :param) IS NULL"
    == condition.values(3)
    |> condition.is_null(True)
    |> condition.to_string(fmt)
  assert "NULL IS NULL"
    == condition.null |> condition.is_null(True) |> condition.to_string(fmt)
}

pub fn split_test() {
  let mapper = value.mapper() |> value.on_text(fn(s) { s })

  let left = condition.column("a", None, None)
  let right = condition.text("val")
  let mock_left = mock_comparable(left, ["left_val"])
  let mock_right = mock_comparable(right, ["right_val"])

  let cond_val =
    condition.eq("ignored", "ignored", of: mock_left, and: mock_right)

  let #(conditions, values) = condition.split([cond_val], mapper)

  assert 1 == list.length(conditions)
  assert ["left_val", "right_val", "val"] == values
}
