import based/sql/internal/fmt
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/string

pub opaque type Node(v) {
  Aggregate(
    fun: fn(String) -> String,
    column: String,
    table: Option(String),
    alias: Option(String),
  )
  Subquery(sql: String, values: List(v))
  Column(name: String, alias: Option(String), table: Option(String))
  Text(value: String)
  Value(value: v)
  Null
}

@internal
pub fn text(value: String) -> Node(v) {
  Text(value:)
}

@internal
pub fn value(value: v) -> Node(v) {
  Value(value:)
}

@internal
pub fn subquery(sql: String, values: List(v)) -> Node(v) {
  Subquery(sql:, values:)
}

@internal
pub fn column(
  name: String,
  alias: Option(String),
  table: Option(String),
) -> Node(v) {
  Column(name:, alias:, table:)
}

@internal
pub fn aggregate(
  fun: fn(String) -> String,
  column: String,
  table: Option(String),
  alias: Option(String),
) -> Node(v) {
  Aggregate(fun:, column:, table:, alias:)
}

@internal
pub const null = Null

@internal
pub fn node_to_string(node: Node(v), fmt: fmt.Fmt(v)) -> String {
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
    Null -> fmt.null
  }
}

@internal
pub fn node_to_values(node: Node(v), text_to_value: fn(String) -> v) -> List(v) {
  case node {
    Aggregate(..) -> []
    Column(..) -> []
    Text(value:) -> [text_to_value(value)]
    Subquery(sql: _, values:) -> values
    Value(value:) -> [value]
    Null -> []
  }
}

pub opaque type Condition(v) {
  Compare(left: Node(v), right: Node(v), operator: Operator(v))
  Is(left: Node(v), right: Bool)
  IsNull(left: Node(v), right: Bool)
  Or(left: Condition(v), right: Condition(v))
  Not(condition: Condition(v))
  Raw(sql: String)
}

@internal
pub fn to_values(
  condition: Condition(v),
  text_to_value: fn(String) -> v,
) -> List(v) {
  case condition {
    Compare(left:, right:, operator:) -> {
      [
        node_to_values(left, text_to_value),
        node_to_values(right, text_to_value),
        operator_to_value(operator, text_to_value),
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
    Raw(sql: _) -> []
  }
}

type Operator(v) {
  Eq
  Gt
  Lt
  GtEq
  LtEq
  NotEq
  Between(Node(v))
  In
  Like
  NotLike
}

pub fn eq(left: Node(v), right: Node(v)) -> Condition(v) {
  Compare(left:, right:, operator: Eq)
}

pub fn gt(left: Node(v), right: Node(v)) -> Condition(v) {
  Compare(left:, right:, operator: Gt)
}

pub fn lt(left: Node(v), right: Node(v)) -> Condition(v) {
  Compare(left:, right:, operator: Lt)
}

pub fn gt_eq(left: Node(v), right: Node(v)) -> Condition(v) {
  Compare(left:, right:, operator: GtEq)
}

pub fn lt_eq(left: Node(v), right: Node(v)) -> Condition(v) {
  Compare(left:, right:, operator: LtEq)
}

pub fn not_eq(left: Node(v), right: Node(v)) -> Condition(v) {
  Compare(left:, right:, operator: NotEq)
}

pub fn between(left: Node(v), start: Node(v), end: Node(v)) -> Condition(v) {
  Compare(left:, right: start, operator: Between(end))
}

pub fn in(left: Node(v), right: Node(v)) -> Condition(v) {
  Compare(left:, right:, operator: In)
}

pub fn like(left: Node(v), right: Node(v)) -> Condition(v) {
  Compare(left:, right:, operator: Like)
}

pub fn not_like(left: Node(v), right: Node(v)) -> Condition(v) {
  Compare(left:, right:, operator: NotLike)
}

pub fn is(left: Node(v), right: Bool) -> Condition(v) {
  Is(left:, right:)
}

pub fn is_null(left: Node(v), right: Bool) -> Condition(v) {
  IsNull(left:, right:)
}

pub fn or(left: Condition(v), right: Condition(v)) -> Condition(v) {
  Or(left:, right:)
}

pub fn not(condition: Condition(v)) -> Condition(v) {
  Not(condition:)
}

pub fn raw(sql: String) -> Condition(v) {
  Raw(sql:)
}

pub fn to_string(cond: Condition(v), fmt: fmt.Fmt(v)) -> String {
  case cond {
    Compare(left:, right:, operator:) -> {
      let left = node_to_string(left, fmt)
      let right = node_to_string(right, fmt)

      let fmt = operator_to_fmt(operator, fmt)

      fmt(left, right)
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
    Raw(sql:) -> sql
  }
}

fn operator_to_fmt(
  operator: Operator(v),
  fmt: fmt.Fmt(v),
) -> fn(String, String) -> String {
  case operator {
    Eq -> fmt.eq
    Gt -> fmt.gt
    Lt -> fmt.lt
    GtEq -> fmt.gt_eq
    LtEq -> fmt.lt_eq
    NotEq -> fmt.not_eq
    Like -> fmt.like
    In -> fmt.in
    Between(end) -> fn(left, start) {
      let end = node_to_string(end, fmt)

      fmt.between(left, start, end)
    }
    NotLike -> fmt.not_like
  }
}

fn operator_to_value(
  operator: Operator(v),
  text_to_value: fn(String) -> v,
) -> List(v) {
  case operator {
    Between(end) -> node_to_values(end, text_to_value)
    _ -> []
  }
}
