import based/sql/column.{type Column}
import based/sql/condition.{type Condition}
import based/sql/table
import gleam/list
import gleam/option.{type Option, None, Some}

// ---------- Table ---------- //

pub fn table(name: String) -> table.Table {
  table.new(name)
}

// ---------- Column ---------- //

pub fn column(name: String) -> Column {
  column.new(name)
}

pub const all = column.all

pub fn avg(name: String) -> Column {
  column.avg(name)
}

pub fn count(name: String) -> Column {
  column.count(name)
}

pub fn max(name: String) -> Column {
  column.max(name)
}

pub fn min(name: String) -> Column {
  column.min(name)
}

pub fn sum(name: String) -> Column {
  column.sum(name)
}

// ---------- Conditions ---------- //

pub type Comparable(a, v) {
  Comparable(to_node: fn(a) -> condition.Node(v))
}

pub const col = Comparable(to_node: column.value)

pub const value = Comparable(to_node: condition.value)

pub fn eq(
  column: Column,
  right: a,
  of comparable: Comparable(a, v),
) -> Condition(v) {
  let right = comparable.to_node(right)

  column
  |> column.value
  |> condition.eq(right)
}

pub fn gt(
  column: Column,
  right: a,
  of comparable: Comparable(a, v),
) -> Condition(v) {
  let right = comparable.to_node(right)

  column
  |> column.value
  |> condition.gt(right)
}

pub fn lt(
  column: Column,
  right: a,
  of comparable: Comparable(a, v),
) -> Condition(v) {
  let right = comparable.to_node(right)

  column
  |> column.value
  |> condition.lt(right)
}

pub fn gt_eq(
  column: Column,
  right: a,
  of comparable: Comparable(a, v),
) -> Condition(v) {
  let right = comparable.to_node(right)

  column
  |> column.value
  |> condition.gt_eq(right)
}

pub fn lt_eq(
  column: Column,
  right: a,
  of comparable: Comparable(a, v),
) -> Condition(v) {
  let right = comparable.to_node(right)

  column
  |> column.value
  |> condition.lt_eq(right)
}

pub fn not_eq(
  column: Column,
  right: a,
  of comparable: Comparable(a, v),
) -> Condition(v) {
  let right = comparable.to_node(right)

  column
  |> column.value
  |> condition.not_eq(right)
}

pub fn between(
  column: Column,
  start: a,
  end: a,
  of comparable: Comparable(a, v),
) -> Condition(v) {
  let end = comparable.to_node(end)
  let start = comparable.to_node(start)

  column
  |> column.value
  |> condition.between(start, end)
}

pub fn like(column: Column, val: String) -> Condition(v) {
  let right = condition.text(val)

  column
  |> column.value
  |> condition.like(right)
}

pub fn not_like(column: Column, val: String) -> Condition(v) {
  let right = condition.text(val)

  column
  |> column.value
  |> condition.not_like(right)
}

pub fn in(
  column: Column,
  right: a,
  of kind: fn(a) -> condition.Node(v),
) -> Condition(v) {
  let right = kind(right)

  column
  |> column.value
  |> condition.in(right)
}

pub fn is(column: Column, right: Bool) -> Condition(v) {
  column
  |> column.value
  |> condition.is(right)
}

pub fn is_null(column: Column) -> Condition(v) {
  column
  |> column.value
  |> condition.is_null(True)
}

pub fn is_not_null(column: Column) -> Condition(v) {
  column
  |> column.value
  |> condition.is_null(False)
}

pub fn or(left: Condition(v), right: Condition(v)) -> Condition(v) {
  condition.or(left, right)
}

pub fn not(condition: Condition(v)) -> Condition(v) {
  condition.not(condition)
}

pub fn raw(sql: String) -> Condition(v) {
  condition.raw(sql)
}

// ---------- Join ---------- //

pub type JoinType {
  InnerJoin
  LeftJoin
  RightJoin
  FullJoin
}

pub type Join(v) {
  Join(type_: JoinType, table: table.Table, exprs: List(Condition(v)))
}

pub fn inner_join(table: table.Table, exprs: List(Condition(v))) -> Join(v) {
  Join(InnerJoin, table, exprs)
}

pub fn left_join(table: table.Table, exprs: List(Condition(v))) -> Join(v) {
  Join(LeftJoin, table, exprs)
}

pub fn right_join(table: table.Table, exprs: List(Condition(v))) -> Join(v) {
  Join(RightJoin, table, exprs)
}

pub fn full_join(table: table.Table, exprs: List(Condition(v))) -> Join(v) {
  Join(FullJoin, table, exprs)
}

// -------------------------- //

pub type Order {
  Asc
  Desc
}

pub fn list(vals: List(a), of kind: fn(a) -> v) -> condition.Node(v) {
  vals
  |> list.map(kind)
  |> condition.values
}

pub fn nullable(
  value: Option(a),
  inner_type: fn(a) -> condition.Node(v),
) -> condition.Node(v) {
  case value {
    Some(term) -> inner_type(term)
    None -> condition.null
  }
}
