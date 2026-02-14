import based/db
import based/repo.{type Repo}
import based/sql
import based/sql/column.{type Column}
import based/sql/condition.{type Condition}
import based/sql/internal/builder
import based/sql/internal/fmt
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
    repo: Repo(v),
    table: Option(TableOrSubquery(v)),
    columns: List(Column),
    distinct: Bool,
    join: List(sql.Join),
    where: List(List(Condition)),
    group_by: List(String),
    having: List(List(Condition)),
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

pub fn new(repo: Repo(v)) -> Select(v) {
  Select(
    repo:,
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

pub fn from(repo: Repo(v), table: table.Table) -> Select(v) {
  Select(
    repo:,
    table: Some(Table(table)),
    columns: [column.all],
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

pub fn from_query(repo: Repo(v), query: db.Query(v)) -> Select(v) {
  Select(
    repo:,
    table: Some(Subquery(query)),
    columns: [column.all],
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

pub fn columns(select: Select(v), columns: List(Column)) -> Select(v) {
  Select(..select, columns:)
}

// Where

pub fn where(
  select: Select(v),
  conditions: List(#(Condition, List(v))),
) -> Select(v) {
  let #(conditions, values) =
    condition.split(conditions, select.repo.value_mapper)

  let where = list.prepend(select.where, conditions)

  Select(..select, where:) |> prepend_values(values)
}

pub fn where_not(
  select: Select(v),
  conditions: List(#(Condition, List(v))),
) -> Select(v) {
  conditions
  |> list.map(sql.not)
  |> where(select, _)
}

pub fn where_exists(select: Select(v), subquery: Select(v)) -> Select(v) {
  let query = to_query(subquery)
  let count = list.length(query.values)
  let exists = condition.exists(query.sql, count)

  let where = list.prepend(select.where, [exists])

  Select(..select, where:) |> prepend_values(query.values)
}

// Joins

pub fn join(
  select: Select(v),
  table: table.Table,
  on conditions: List(#(Condition, List(v))),
) -> Select(v) {
  do_join(select, table, conditions, sql.inner_join)
}

pub fn left_join(
  select: Select(v),
  table: table.Table,
  on conditions: List(#(Condition, List(v))),
) -> Select(v) {
  do_join(select, table, conditions, sql.left_join)
}

pub fn right_join(
  select: Select(v),
  table: table.Table,
  on conditions: List(#(Condition, List(v))),
) -> Select(v) {
  do_join(select, table, conditions, sql.right_join)
}

pub fn full_join(
  select: Select(v),
  table: table.Table,
  on conditions: List(#(Condition, List(v))),
) -> Select(v) {
  do_join(select, table, conditions, sql.full_join)
}

fn do_join(
  select: Select(v),
  table: table.Table,
  conditions: List(#(Condition, List(v))),
  joiner: fn(table.Table, List(Condition)) -> sql.Join,
) -> Select(v) {
  let #(conditions, values) =
    condition.split(conditions, select.repo.value_mapper)

  let join = joiner(table, conditions)
  let join = list.prepend(select.join, join)

  Select(..select, join:) |> prepend_values(values)
}

// Group By

pub fn group_by(qb: Select(v), group_by: List(String)) -> Select(v) {
  Select(..qb, group_by:)
}

pub fn having(
  select: Select(v),
  having: List(#(Condition, List(v))),
) -> Select(v) {
  let #(conditions, values) = condition.split(having, select.repo.value_mapper)

  let having = list.prepend(select.having, conditions)

  Select(..select, having:) |> prepend_values(values)
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

  let to_placeholder = fmt.to_placeholder(select.repo.fmt, _)

  build(select)
  |> builder.placeholders(on: fmt.placeholder, with: to_placeholder)
  |> db.sql
  |> db.params(values)
}

pub const subquery = sql.Kind(comparable: subquery_comp)

fn subquery_comp() -> condition.Comparable(Select(v), v) {
  condition.comparable(fn(select: Select(v)) {
    let query = to_query(select)

    let count = list.length(query.values)

    let node = condition.subquery(query.sql, count)

    #(node, query.values)
  })
}

pub const any = sql.Kind(comparable: any_comp)

pub const all = sql.Kind(comparable: all_comp)

fn any_comp() -> condition.Comparable(Select(v), v) {
  condition.comparable(fn(select: Select(v)) {
    let query = to_query(select)

    let count = list.length(query.values)

    let node = condition.any(query.sql, count)

    #(node, query.values)
  })
}

fn all_comp() -> condition.Comparable(Select(v), v) {
  condition.comparable(fn(select: Select(v)) {
    let query = to_query(select)

    let count = list.length(query.values)

    let node = condition.all(query.sql, count)

    #(node, query.values)
  })
}

pub fn to_string(select: Select(v)) -> String {
  let values = select.values |> list.reverse |> list.flatten

  build(select)
  |> builder.to_string(values, select.repo.fmt)
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
        |> table.to_string(fmt.to_identifier(select.repo.fmt, _))

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

  select.columns
  |> list.map(column.to_string(_, select.repo))
  |> select_sql
  |> from_sql
  |> builder.append_joins(select.join, select.repo.fmt)
  |> builder.append_where(select.where, select.repo.fmt)
  |> builder.append_group_by(select.group_by)
  |> builder.append_having(select.having, select.repo.fmt)
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
