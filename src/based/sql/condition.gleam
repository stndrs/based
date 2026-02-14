import based/sql/internal/fmt
import based/sql/internal/value
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/string

pub opaque type Node {
  Aggregate(
    fun: fn(String) -> String,
    column: String,
    table: Option(String),
    alias: Option(String),
  )
  Subquery(sql: String, values: Int)
  Column(name: String, alias: Option(String), table: Option(String))
  Text(value: String)
  Value
  Values(count: Int)
  Null
}

pub fn text(value: String) -> Node {
  Text(value:)
}

pub const value = Value

pub fn values(count: Int) -> Node {
  Values(count:)
}

pub fn subquery(sql: String, values: Int) -> Node {
  Subquery(sql:, values:)
}

pub fn column(
  name: String,
  alias: Option(String),
  table: Option(String),
) -> Node {
  Column(name:, alias:, table:)
}

pub fn aggregate(
  fun: fn(String) -> String,
  column: String,
  table: Option(String),
  alias: Option(String),
) -> Node {
  Aggregate(fun:, column:, table:, alias:)
}

pub const null = Null

@internal
pub fn node_to_string(node: Node, fmt: fmt.Fmt(v)) -> String {
  case node {
    Aggregate(fun:, column:, table:, alias:) -> {
      let col = case table {
        Some(table) -> {
          fmt.to_identifier(fmt, table)
          |> string.append(".")
          |> string.append(fmt.to_identifier(fmt, column))
        }
        None -> fmt.to_identifier(fmt, column)
      }

      let agg = fun(col)

      case alias {
        Some(a) -> fmt.alias(agg, a)
        None -> agg
      }
    }
    Column(name:, alias:, table:) -> {
      let col = case table {
        Some(table) -> {
          fmt.to_identifier(fmt, table)
          |> string.append(".")
          |> string.append(fmt.to_identifier(fmt, name))
        }
        None -> fmt.to_identifier(fmt, name)
      }

      case alias {
        Some(a) -> fmt.alias(col, a)
        None -> col
      }
    }
    Subquery(sql:, values: _) -> fmt.enclose(sql)
    Text(..) -> fmt.placeholder
    Value(..) -> fmt.placeholder
    Values(count:) -> {
      list.repeat("", times: count)
      |> list.map(fn(_) { fmt.placeholder })
      |> string.join(", ")
      |> fmt.enclose
    }
    Null -> fmt.null
  }
}

fn node_to_values(node: Node, text_to_value: fn(String) -> v) -> List(v) {
  case node {
    Text(value:) -> [text_to_value(value)]
    _ -> []
  }
}

pub opaque type Condition {
  Compare(left: Node, right: Node, operator: Operator)
  Between(left: Node, start: Node, end: Node)
  Is(left: Node, right: Bool)
  IsNull(left: Node, right: Bool)
  Or(left: Condition, right: Condition)
  Not(condition: Condition)
  Exists(subquery: Node)
  Raw(sql: String)
}

@internal
pub fn to_values(
  condition: Condition,
  text_to_value: fn(String) -> v,
) -> List(v) {
  case condition {
    Compare(left:, right:, operator: _) -> {
      [
        node_to_values(left, text_to_value),
        node_to_values(right, text_to_value),
      ]
      |> list.flatten
    }
    Between(left:, start:, end:) -> {
      [
        node_to_values(left, text_to_value),
        node_to_values(start, text_to_value),
        node_to_values(end, text_to_value),
      ]
      |> list.flatten
    }
    Is(left:, right: _) -> {
      node_to_values(left, text_to_value)
    }
    IsNull(left:, right: _) -> {
      node_to_values(left, text_to_value)
    }
    Or(left:, right:) -> {
      [to_values(left, text_to_value), to_values(right, text_to_value)]
      |> list.flatten
    }
    Not(condition:) -> to_values(condition, text_to_value)
    Exists(subquery:) -> node_to_values(subquery, text_to_value)
    Raw(sql: _) -> []
  }
}

type Operator {
  Eq
  Gt
  Lt
  GtEq
  LtEq
  NotEq
  In
  Like
  NotLike
}

pub opaque type Comparable(a, v) {
  Comparable(function: fn(a) -> #(Node, List(v)))
}

pub fn comparable(function: fn(a) -> #(Node, List(v))) -> Comparable(a, v) {
  Comparable(function:)
}

@internal
pub fn to_node_and_values(
  comparable: Comparable(a, v),
  value: a,
) -> #(Node, List(v)) {
  comparable.function(value)
}

pub fn eq(
  left: a,
  right: b,
  of left_comparable: fn() -> Comparable(a, v),
  and right_comparable: fn() -> Comparable(b, v),
) -> #(Condition, List(v)) {
  let #(left, left_values) = left_comparable().function(left)
  let #(right, right_values) = right_comparable().function(right)

  let condition = Compare(left:, right:, operator: Eq)
  let values = list.append(left_values, right_values)

  #(condition, values)
}

pub fn gt(
  left: a,
  right: b,
  of left_comparable: fn() -> Comparable(a, v),
  and right_comparable: fn() -> Comparable(b, v),
) -> #(Condition, List(v)) {
  let #(left, left_values) = left_comparable().function(left)
  let #(right, right_values) = right_comparable().function(right)

  let condition = Compare(left:, right:, operator: Gt)
  let values = list.append(left_values, right_values)

  #(condition, values)
}

pub fn lt(
  left: a,
  right: b,
  of left_comparable: fn() -> Comparable(a, v),
  and right_comparable: fn() -> Comparable(b, v),
) -> #(Condition, List(v)) {
  let #(left, left_values) = left_comparable().function(left)
  let #(right, right_values) = right_comparable().function(right)

  let condition = Compare(left:, right:, operator: Lt)
  let values = list.append(left_values, right_values)

  #(condition, values)
}

pub fn gt_eq(
  left: a,
  right: b,
  of left_comparable: fn() -> Comparable(a, v),
  and right_comparable: fn() -> Comparable(b, v),
) -> #(Condition, List(v)) {
  let #(left, left_values) = left_comparable().function(left)
  let #(right, right_values) = right_comparable().function(right)

  let condition = Compare(left:, right:, operator: GtEq)
  let values = list.append(left_values, right_values)

  #(condition, values)
}

pub fn lt_eq(
  left: a,
  right: b,
  of left_comparable: fn() -> Comparable(a, v),
  and right_comparable: fn() -> Comparable(b, v),
) -> #(Condition, List(v)) {
  let #(left, left_values) = left_comparable().function(left)
  let #(right, right_values) = right_comparable().function(right)

  let condition = Compare(left:, right:, operator: LtEq)
  let values = list.append(left_values, right_values)

  #(condition, values)
}

pub fn not_eq(
  left: a,
  right: b,
  of left_comparable: fn() -> Comparable(a, v),
  and right_comparable: fn() -> Comparable(b, v),
) -> #(Condition, List(v)) {
  let #(left, left_values) = left_comparable().function(left)
  let #(right, right_values) = right_comparable().function(right)

  let condition = Compare(left:, right:, operator: NotEq)
  let values = list.append(left_values, right_values)

  #(condition, values)
}

pub fn between(
  left: a,
  start: b,
  end: b,
  of left_comparable: fn() -> Comparable(a, v),
  and between_comparable: fn() -> Comparable(b, v),
) -> #(Condition, List(v)) {
  let #(left, left_values) = left_comparable().function(left)
  let #(start, start_values) = between_comparable().function(start)
  let #(end, end_values) = between_comparable().function(end)

  let condition = Between(left:, start:, end:)

  let values = list.flatten([left_values, start_values, end_values])

  #(condition, values)
}

pub fn in(
  left: a,
  right: b,
  of left_comparable: fn() -> Comparable(a, v),
  and right_comparable: fn() -> Comparable(b, v),
) -> #(Condition, List(v)) {
  let #(left, left_values) = left_comparable().function(left)
  let #(right, right_values) = right_comparable().function(right)

  let condition = Compare(left:, right:, operator: In)
  let values = list.append(left_values, right_values)

  #(condition, values)
}

pub fn like(left: Node, right: Node) -> Condition {
  Compare(left:, right:, operator: Like)
}

pub fn not_like(left: Node, right: Node) -> Condition {
  Compare(left:, right:, operator: NotLike)
}

pub fn is(left: Node, right: Bool) -> Condition {
  Is(left:, right:)
}

pub fn is_null(left: Node, right: Bool) -> Condition {
  IsNull(left:, right:)
}

pub fn or(left: Condition, right: Condition) -> Condition {
  Or(left:, right:)
}

pub fn not(condition: Condition) -> Condition {
  Not(condition:)
}

pub fn exists(sql: String, values: Int) -> Condition {
  subquery(sql, values) |> Exists
}

pub fn raw(sql: String) -> Condition {
  Raw(sql:)
}

@internal
pub fn split(
  conditions: List(#(Condition, List(v))),
  value_mapper: value.Mapper(v),
) -> #(List(Condition), List(v)) {
  let empty: #(List(Condition), List(List(v))) = #([], [])

  let #(conditions, values) =
    conditions
    |> list.fold(from: empty, with: fn(acc, condition) {
      let #(next_condition, next_values) = condition

      let #(acc_conditions, acc_values) = acc
      let condition_values =
        to_values(next_condition, value.from_text(_, value_mapper))

      #(
        list.prepend(acc_conditions, next_condition),
        list.prepend(acc_values, next_values)
          |> list.prepend(condition_values),
      )
    })

  #(list.reverse(conditions), list.flatten(list.reverse(values)))
}

pub fn to_string(cond: Condition, fmt: fmt.Fmt(v)) -> String {
  case cond {
    Compare(left:, right:, operator:) -> {
      let left = node_to_string(left, fmt)
      let right = node_to_string(right, fmt)

      let fmt = operator_to_fmt(operator)

      fmt(left, right)
    }
    Between(left:, start:, end:) -> {
      let left = node_to_string(left, fmt)
      let start = node_to_string(start, fmt)
      let end = node_to_string(end, fmt)

      fmt.between(left, start, end)
    }
    Is(left:, right:) -> {
      let left = node_to_string(left, fmt)

      case right {
        True -> fmt.is(left, fmt.true)
        False -> fmt.is(left, fmt.false)
      }
    }
    IsNull(left:, right:) -> {
      let left = node_to_string(left, fmt)

      case right {
        True -> fmt.is(left, fmt.null)
        False -> fmt.is_not(left, fmt.null)
      }
    }
    Or(left:, right:) -> {
      left
      |> to_string(fmt)
      |> fmt.or(to_string(right, fmt))
    }
    Not(condition:) -> {
      condition
      |> to_string(fmt)
      |> fmt.not
    }
    Exists(subquery:) -> {
      subquery
      |> node_to_string(fmt)
      |> fmt.exists
    }
    Raw(sql:) -> sql
  }
}

fn operator_to_fmt(operator: Operator) -> fn(String, String) -> String {
  case operator {
    Eq -> fmt.eq
    Gt -> fmt.gt
    Lt -> fmt.lt
    GtEq -> fmt.gt_eq
    LtEq -> fmt.lt_eq
    NotEq -> fmt.not_eq
    Like -> fmt.like
    In -> fmt.in
    NotLike -> fmt.not_like
  }
}
