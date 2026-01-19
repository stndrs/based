import based
import based/db
import based/sql
import based/sql/expression.{type Expression}
import based/sql/internal/builder
import based/sql/internal/fmt
import based/sql/join.{type Join}
import based/sql/node.{type Node}
import based/sql/table
import gleam/function
import gleam/list
import gleam/option.{type Option, None, Some}

type For {
  Update
}

type TableOrSubquery(v) {
  Table(table.Table)
  Subquery(db.Query(v))
}

pub opaque type Select(v) {
  Select(
    fmt: fmt.Fmt(v),
    table: Option(TableOrSubquery(v)),
    columns: List(String),
    distinct: Bool,
    join: List(Join(v)),
    where: List(List(Expression(v))),
    group_by: List(String),
    having: List(List(Expression(v))),
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

pub fn new(repo: based.Repo(v)) -> Select(v) {
  Select(
    fmt: repo.fmt,
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

pub fn from(repo: based.Repo(v), table: table.Table) -> Select(v) {
  Select(
    fmt: repo.fmt,
    table: Some(Table(table)),
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
    values: [],
  )
}

pub fn from_query(repo: based.Repo(v), query: db.Query(v)) -> Select(v) {
  Select(
    fmt: repo.fmt,
    table: Some(Subquery(query)),
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
    values: [query.values],
  )
}

pub fn columns(select: Select(v), columns: List(String)) -> Select(v) {
  Select(..select, columns:)
}

// Where

pub fn where(select: Select(v), exprs: List(Expression(v))) -> Select(v) {
  let values =
    list.flat_map(exprs, expression.to_values(_, fmt.to_value(select.fmt, _)))
  let where = list.prepend(select.where, exprs)

  Select(..select, where:)
  |> prepend_values(values)
}

pub fn where_not(select: Select(v), exprs: List(Expression(v))) -> Select(v) {
  list.map(exprs, sql.not) |> where(select, _)
}

// Joins

pub fn join(
  select: Select(v),
  table: table.Table,
  on exprs: List(Expression(v)),
) -> Select(v) {
  do_join(select, table, exprs, join.inner)
}

pub fn left_join(
  select: Select(v),
  table: table.Table,
  on exprs: List(Expression(v)),
) -> Select(v) {
  do_join(select, table, exprs, join.left)
}

pub fn right_join(
  select: Select(v),
  table: table.Table,
  on exprs: List(Expression(v)),
) -> Select(v) {
  do_join(select, table, exprs, join.right)
}

pub fn full_join(
  select: Select(v),
  table: table.Table,
  on exprs: List(Expression(v)),
) -> Select(v) {
  do_join(select, table, exprs, join.full)
}

fn do_join(
  select: Select(v),
  table: table.Table,
  exprs: List(Expression(v)),
  joiner: fn(table.Table, List(Expression(v))) -> join.Join(v),
) -> Select(v) {
  let values =
    list.flat_map(exprs, expression.to_values(_, fmt.to_value(select.fmt, _)))
  let join_clause = joiner(table, exprs)
  let join = list.prepend(select.join, join_clause)

  Select(..select, join:) |> prepend_values(values)
}

// Group By

pub fn group_by(qb: Select(v), group_by: List(String)) -> Select(v) {
  Select(..qb, group_by:)
}

pub fn having(select: Select(v), having: List(Expression(v))) -> Select(v) {
  let values =
    list.flat_map(having, expression.to_values(_, fmt.to_value(select.fmt, _)))
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

  let to_placeholder = fmt.to_placeholder(select.fmt, _)

  build(select)
  |> builder.placeholders(on: fmt.placeholder, with: to_placeholder)
  |> db.sql
  |> db.params(values)
}

pub fn to_subquery(select: Select(v)) -> Node(v) {
  to_query(select)
  |> node.query(None)
}

pub fn to_string(select: Select(v)) -> String {
  let values = select.values |> list.reverse |> list.flatten

  build(select)
  |> builder.to_string(values, select.fmt)
}

// Builders

fn build(select: Select(v)) -> String {
  let select_sql = case select.distinct {
    True -> fmt.select_distinct
    False -> fmt.select
  }

  let from_sql = case select.table {
    Some(Table(table)) -> {
      let to_string =
        table
        |> table.to_string(fmt.to_identifier(select.fmt, _))

      fmt.from(_, to_string)
    }
    Some(Subquery(query)) -> {
      let to_string =
        query.sql
        |> fmt.enclose

      fmt.from(_, to_string)
    }
    None -> function.identity
  }

  select_sql(select.columns)
  |> from_sql
  |> builder.append_joins(select.join, select.fmt)
  |> builder.append_where(select.where, select.fmt)
  |> builder.append_group_by(select.group_by)
  |> builder.append_having(select.having, select.fmt)
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
