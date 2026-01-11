import based/db
import based/sql
import based/sql/internal/builder
import based/sql/internal/fmt
import based/sql/internal/join.{type Join}
import based/sql/internal/node.{type Node}
import gleam/function
import gleam/list
import gleam/option.{type Option, None, Some}

type For {
  Update
}

pub opaque type Select(v) {
  Select(
    sql: sql.Sql(v),
    table: Option(sql.Table(v)),
    columns: List(String),
    distinct: Bool,
    join: List(Join(v)),
    where: List(List(sql.Expr(v))),
    group_by: List(String),
    having: List(List(sql.Expr(v))),
    order_by: List(String),
    order: Option(sql.Order),
    limit: Option(Int),
    offset: Option(Int),
    for: Option(For),
    values: List(List(v)),
  )
}

fn prepend_values(select: Select(v), values: List(v)) -> Select(v) {
  let values = list.prepend(select.values, values)
  Select(..select, values:)
}

pub fn distinct(select: Select(v)) -> Select(v) {
  Select(..select, distinct: True)
}

pub fn new(sql: sql.Sql(v)) -> Select(v) {
  Select(
    sql:,
    table: None,
    columns: [],
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

// From

pub fn from(
  sql: sql.Sql(v),
  source: a,
  of kind: fn(a) -> sql.Table(v),
) -> Select(v) {
  let table = kind(source)

  let values = sql.table_to_values(table)

  Select(
    sql:,
    table: Some(table),
    columns: ["*"],
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
    values: [values],
  )
}

pub fn columns(select: Select(v), columns: List(String)) -> Select(v) {
  Select(..select, columns:)
}

// Where

pub fn where(select: Select(v), exprs: List(sql.Expr(v))) -> Select(v) {
  let values = list.flat_map(exprs, sql.expr_to_values)
  let where = list.prepend(select.where, exprs)

  Select(..select, where:)
  |> prepend_values(values)
}

pub fn where_not(select: Select(v), exprs: List(sql.Expr(v))) -> Select(v) {
  list.map(exprs, sql.not) |> where(select, _)
}

// Joins

pub fn join(
  select: Select(v),
  source: a,
  of kind: fn(a) -> sql.Table(v),
  on exprs: List(sql.Expr(v)),
) -> Select(v) {
  let table = kind(source)

  do_join(select, table, exprs, join.inner)
}

pub fn left_join(
  select: Select(v),
  source: a,
  of kind: fn(a) -> sql.Table(v),
  on exprs: List(sql.Expr(v)),
) -> Select(v) {
  let table = kind(source)

  do_join(select, table, exprs, join.left)
}

pub fn right_join(
  select: Select(v),
  source: a,
  of kind: fn(a) -> sql.Table(v),
  on exprs: List(sql.Expr(v)),
) -> Select(v) {
  let table = kind(source)

  do_join(select, table, exprs, join.right)
}

pub fn full_join(
  select: Select(v),
  source: a,
  of kind: fn(a) -> sql.Table(v),
  on exprs: List(sql.Expr(v)),
) -> Select(v) {
  let table = kind(source)

  do_join(select, table, exprs, join.full)
}

fn do_join(
  select: Select(v),
  table: sql.Table(v),
  exprs: List(sql.Expr(v)),
  joiner: fn(sql.Table(v), List(sql.Expr(v))) -> join.Join(v),
) -> Select(v) {
  let values = list.flat_map(exprs, sql.expr_to_values)
  let join_clause = joiner(table, exprs)
  let join = list.prepend(select.join, join_clause)

  Select(..select, join:) |> prepend_values(values)
}

// Group By

pub fn group_by(qb: Select(v), group_by: List(String)) -> Select(v) {
  Select(..qb, group_by:)
}

pub fn having(select: Select(v), having: List(sql.Expr(v))) -> Select(v) {
  let values = list.flat_map(having, sql.expr_to_values)
  let having = list.prepend(select.having, having)

  Select(..select, having:)
  |> prepend_values(values)
}

// Order By

pub fn order_by(select: Select(v), order_by: List(String)) -> Select(v) {
  Select(..select, order_by:)
}

pub fn asc(select: Select(v)) -> Select(v) {
  Select(..select, order: Some(sql.Asc))
}

pub fn desc(select: Select(v)) -> Select(v) {
  Select(..select, order: Some(sql.Desc))
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

pub fn to_query(select: Select(v)) -> db.Query(v) {
  let values = select.values |> list.reverse |> list.flatten

  let to_placeholder = sql.to_placeholder(select.sql, _)

  build(select)
  |> builder.placeholders(on: fmt.placeholder, with: to_placeholder)
  |> db.sql
  |> db.params(values)
}

pub fn subquery(select: Select(v)) -> sql.Table(v) {
  select
  |> to_query
  |> sql.from_query
}

pub fn to_subquery(select: Select(v)) -> Node(v) {
  to_query(select) |> sql.subquery
}

pub fn to_table(select: Select(v)) -> sql.Table(v) {
  to_query(select) |> sql.from_query
}

pub fn to_string(select: Select(v)) -> String {
  let values = select.values |> list.reverse |> list.flatten

  build(select)
  |> builder.to_string(values, select.sql)
}

// Builders

fn build(select: Select(v)) -> String {
  let select_sql = case select.distinct {
    True -> fmt.select_distinct
    False -> fmt.select
  }

  let from_sql = case select.table {
    Some(table) -> {
      let to_string =
        table
        |> sql.table_to_node
        |> node.to_string(sql.to_identifier(select.sql, _))

      fmt.from(_, to_string)
    }
    None -> function.identity
  }

  select_sql(select.columns)
  |> from_sql
  |> builder.append_joins(select.join, select.sql)
  |> builder.append_where(select.where, select.sql)
  |> builder.append_group_by(select.group_by)
  |> builder.append_having(select.having, select.sql)
  |> builder.append_order_by(select.order_by, select.order)
  |> builder.append_limit(select.limit, select.offset)
  |> append_for(select.for)
}

fn append_for(st: String, for: Option(For)) -> String {
  case for {
    Some(_) -> fmt.for_update(st)
    None -> st
  }
}
