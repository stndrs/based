import based
import based/sql/condition.{type Condition}
import based/sql/internal/fmt
import based/sql/table
import gleam/bool
import gleam/option.{type Option, None, Some}
import gleam/string

type Function {
  Avg
  Count
  Max
  Min
  Sum
}

pub opaque type Column {
  Column(
    name: String,
    alias: Option(String),
    table: Option(String),
    func: Option(Function),
  )
}

pub fn new(name: String) -> Column {
  Column(name:, alias: None, table: None, func: None)
}

pub fn alias(column: Column, alias: String) -> Column {
  Column(..column, alias: Some(alias))
}

pub fn for(column: Column, table: table.Table) -> Column {
  let table = case table.alias {
    Some(a) -> Some(a)
    None -> Some(table.name)
  }

  Column(..column, table:)
}

pub fn avg(name: String) -> Column {
  Column(name:, alias: None, table: None, func: Some(Avg))
}

pub fn count(name: String) -> Column {
  Column(name:, alias: None, table: None, func: Some(Count))
}

pub fn max(name: String) -> Column {
  Column(name:, alias: None, table: None, func: Some(Max))
}

pub fn min(name: String) -> Column {
  Column(name:, alias: None, table: None, func: Some(Min))
}

pub fn sum(name: String) -> Column {
  Column(name:, alias: None, table: None, func: Some(Sum))
}

const asterisk = "*"

pub const all = Column(name: asterisk, alias: None, table: None, func: None)

@internal
pub fn to_string(column: Column, repo: based.Repo(v)) -> String {
  let Column(name:, alias:, table:, func:) = column

  let col = case func {
    Some(func) -> aggregate_to_string(func, name, table, repo.fmt)
    None -> column_to_string(name, table, repo.fmt)
  }

  case alias {
    Some(a) -> fmt.alias(col, a)
    None -> col
  }
}

fn column_to_string(
  name: String,
  table: Option(String),
  fmt: fmt.Fmt(v),
) -> String {
  use <- bool.guard(when: name == asterisk, return: asterisk)

  case table {
    Some(table) -> {
      fmt.to_identifier(fmt, table)
      |> string.append(".")
      |> string.append(fmt.to_identifier(fmt, name))
    }
    None -> fmt.to_identifier(fmt, name)
  }
}

fn aggregate_to_string(
  func: Function,
  name: String,
  table: Option(String),
  fmt: fmt.Fmt(v),
) -> String {
  let col = case table {
    Some(table) -> {
      fmt.to_identifier(fmt, table)
      |> string.append(".")
      |> string.append({
        use <- bool.guard(when: name == asterisk, return: asterisk)

        fmt.to_identifier(fmt, name)
      })
    }
    None -> {
      use <- bool.guard(when: name == asterisk, return: asterisk)

      fmt.to_identifier(fmt, name)
    }
  }

  case func {
    Avg -> fmt.avg(col)
    Count -> fmt.count(col)
    Max -> fmt.max(col)
    Min -> fmt.min(col)
    Sum -> fmt.sum(col)
  }
}

pub fn value(column: Column) -> condition.Node {
  case column.func {
    Some(func) -> {
      let formatter = case func {
        Avg -> fmt.avg
        Count -> fmt.count
        Max -> fmt.max
        Min -> fmt.min
        Sum -> fmt.sum
      }

      condition.aggregate(formatter, column.name, column.table, column.alias)
    }
    None -> condition.column(column.name, column.alias, column.table)
  }
}

@internal
pub fn name(column: Column) -> String {
  column.name
}

@internal
pub fn table(column: Column) -> Option(String) {
  column.table
}

pub fn comparable() -> condition.Comparable(Column, v) {
  condition.comparable(fn(col) {
    let node = value(col)

    #(node, [])
  })
}

pub fn eq(
  column: Column,
  right: a,
  of right_comparable: fn() -> condition.Comparable(a, v),
) -> #(Condition, List(v)) {
  condition.eq(column, right, of: comparable, and: right_comparable)
}

pub fn gt(
  column: Column,
  right: a,
  of right_comparable: fn() -> condition.Comparable(a, v),
) -> #(Condition, List(v)) {
  condition.gt(column, right, of: comparable, and: right_comparable)
}

pub fn lt(
  column: Column,
  right: a,
  of right_comparable: fn() -> condition.Comparable(a, v),
) -> #(Condition, List(v)) {
  condition.lt(column, right, of: comparable, and: right_comparable)
}

pub fn gt_eq(
  column: Column,
  right: a,
  of right_comparable: fn() -> condition.Comparable(a, v),
) -> #(Condition, List(v)) {
  condition.gt_eq(column, right, of: comparable, and: right_comparable)
}

pub fn lt_eq(
  column: Column,
  right: a,
  of right_comparable: fn() -> condition.Comparable(a, v),
) -> #(Condition, List(v)) {
  condition.lt_eq(column, right, of: comparable, and: right_comparable)
}

pub fn not_eq(
  column: Column,
  right: a,
  of right_comparable: fn() -> condition.Comparable(a, v),
) -> #(Condition, List(v)) {
  condition.not_eq(column, right, of: comparable, and: right_comparable)
}

pub fn between(
  column: Column,
  start: a,
  end: a,
  of right_comparable: fn() -> condition.Comparable(a, v),
) -> #(Condition, List(v)) {
  condition.between(column, start, end, of: comparable, and: right_comparable)
}

pub fn like(column: Column, val: String) -> #(Condition, List(v)) {
  let right = condition.text(val)

  let condition =
    column
    |> value
    |> condition.like(right)

  #(condition, [])
}

pub fn not_like(column: Column, val: String) -> #(Condition, List(v)) {
  let right = condition.text(val)

  let condition =
    column
    |> value
    |> condition.not_like(right)

  #(condition, [])
}

pub fn in(
  column: Column,
  right: a,
  of right_comparable: fn() -> condition.Comparable(a, v),
) -> #(Condition, List(v)) {
  condition.in(column, right, of: comparable, and: right_comparable)
}

pub fn is(column: Column, right: Bool) -> #(Condition, List(v)) {
  let condition =
    column
    |> value
    |> condition.is(right)

  #(condition, [])
}

pub fn is_null(column: Column) -> #(Condition, List(v)) {
  let condition =
    column
    |> value
    |> condition.is_null(True)

  #(condition, [])
}

pub fn is_not_null(column: Column) -> #(Condition, List(v)) {
  let condition =
    column
    |> value
    |> condition.is_null(False)

  #(condition, [])
}
