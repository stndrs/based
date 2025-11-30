import based/db
import based/format.{type Format}
import based/sql/expr.{type Expr}
import based/sql/internal/builder
import based/sql/internal/fmt
import based/sql/join.{type Join}
import based/sql/node.{type Node}
import based/sql/table.{type Table}
import gleam/function
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/string_tree.{type StringTree}

type For {
  Update
}

pub opaque type Select(v) {
  Select(
    table: Option(Table(v)),
    columns: List(String),
    distinct: Bool,
    join: List(Join(v)),
    where: List(List(Expr(v))),
    group_by: List(String),
    having: List(List(Expr(v))),
    order_by: List(String),
    order: Option(node.Order),
    limit: Option(Int),
    offset: Option(Int),
    for: Option(For),
    values: List(List(v)),
  )
}

pub fn new(columns: List(String)) -> Select(v) {
  Select(
    table: None,
    columns:,
    distinct: False,
    join: [],
    where: [],
    group_by: [],
    having: [],
    order_by: [],
    order: None,
    limit: None,
    offset: None,
    for: None,
    values: [],
  )
}

fn prepend_values(select: Select(v), values: List(v)) -> Select(v) {
  let values = list.prepend(select.values, values)
  Select(..select, values:)
}

pub fn distinct(select: Select(v)) -> Select(v) {
  Select(..select, distinct: True)
}

// From

pub fn from(select: Select(v), table: Table(v)) -> Select(v) {
  let values = table.to_values(table)

  Select(..select, table: Some(table), values: [values])
}

// Where

pub fn where(select: Select(v), exprs: List(Expr(v))) -> Select(v) {
  let values = list.flat_map(exprs, expr.to_values)
  let where = list.prepend(select.where, exprs)

  Select(..select, where:)
  |> prepend_values(values)
}

pub fn where_not(select: Select(v), exprs: List(Expr(v))) -> Select(v) {
  list.map(exprs, expr.not) |> where(select, _)
}

// Joins

pub fn join(
  select: Select(v),
  table: Table(v),
  exprs: List(Expr(v)),
) -> Select(v) {
  do_join(select, table, exprs, join.inner)
}

pub fn left_join(
  select: Select(v),
  table: Table(v),
  exprs: List(Expr(v)),
) -> Select(v) {
  do_join(select, table, exprs, join.left)
}

pub fn right_join(
  select: Select(v),
  table: Table(v),
  exprs: List(Expr(v)),
) -> Select(v) {
  do_join(select, table, exprs, join.right)
}

pub fn full_join(
  select: Select(v),
  table: Table(v),
  exprs: List(Expr(v)),
) -> Select(v) {
  do_join(select, table, exprs, join.full)
}

fn do_join(
  select: Select(v),
  table: Table(v),
  exprs: List(Expr(v)),
  joiner: fn(Table(v), List(Expr(v))) -> Join(v),
) -> Select(v) {
  let values = list.flat_map(exprs, expr.to_values)
  let join_clause = joiner(table, exprs)
  let join = list.prepend(select.join, join_clause)

  Select(..select, join:) |> prepend_values(values)
}

// Group By

pub fn group_by(qb: Select(v), group_by: List(String)) -> Select(v) {
  Select(..qb, group_by:)
}

pub fn having(select: Select(v), having: List(Expr(v))) -> Select(v) {
  let values = list.flat_map(having, expr.to_values)
  let having = list.prepend(select.having, having)

  Select(..select, having:)
  |> prepend_values(values)
}

// Order By

pub fn order_by(select: Select(v), order_by: List(String)) -> Select(v) {
  Select(..select, order_by:)
}

pub fn asc(select: Select(v)) -> Select(v) {
  Select(..select, order: Some(node.Asc))
}

pub fn desc(select: Select(v)) -> Select(v) {
  Select(..select, order: Some(node.Desc))
}

// Limit

pub fn limit(select: Select(v), count: Int, of outer: fn(Int) -> v) -> Select(v) {
  Select(..select, limit: Some(count))
  |> prepend_values([outer(count)])
}

pub fn offset(
  select: Select(v),
  count: Int,
  of outer: fn(Int) -> v,
) -> Select(v) {
  Select(..select, offset: Some(count))
  |> prepend_values([outer(count)])
}

// For Update

pub fn for_update(select: Select(v)) -> Select(v) {
  Select(..select, for: Some(Update))
}

// Query String Building

pub fn to_query(select: Select(v), format: Format(v)) -> db.Query(v) {
  let values = select.values |> list.reverse |> list.flatten

  let to_placeholder = format.to_placeholder(format, _)

  build(select, format)
  |> builder.placeholders(on: fmt.placeholder, with: to_placeholder)
  |> string_tree.to_string
  |> db.sql
  |> db.values(values)
}

pub fn to_subquery(select: Select(v), format: Format(v)) -> Node(v) {
  to_query(select, format) |> node.subquery
}

pub fn to_table(select: Select(v), format: Format(v)) -> Table(v) {
  to_query(select, format) |> table.from_query
}

pub fn to_string(select: Select(v), format: Format(v)) -> String {
  let values = select.values |> list.reverse |> list.flatten

  build(select, format)
  |> string_tree.to_string
  |> builder.to_string(values, format)
}

// Builders

fn build(select: Select(v), format: Format(v)) -> StringTree {
  let select_fmt = case select.distinct {
    True -> fmt.select_distinct
    False -> fmt.select
  }

  let from_fmt = case select.table {
    Some(table) -> fn(st) { fmt.from(st, table.to_string(table, format)) }
    None -> function.identity
  }

  string_tree.new()
  |> select_fmt(select.columns)
  |> from_fmt
  |> builder.append_joins(select.join, format)
  |> builder.append_where(select.where, format)
  |> builder.append_group_by(select.group_by)
  |> builder.append_having(select.having, format)
  |> builder.append_order_by(select.order_by, select.order)
  |> builder.append_limit(select.limit, select.offset)
  |> append_for(select.for)
}

fn append_for(st: StringTree, for: Option(For)) -> StringTree {
  builder.append_optional(st, for, fn(f) {
    case f {
      Update -> fmt.for_update(st)
    }
  })
}
