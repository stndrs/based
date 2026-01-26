//// A builder for constructing SQL UPDATE statements.
////
//// ## Usage
////
//// ```gleam
//// import based/sql/update
//// import database/value
////
//// let users = sql.table("users")
//// let sql = sql.new()
////
//// let query = update.table(sql, users)
////   |> update.set("name", sql.value("John", of: value.text))
////   |> update.where([
////     sql.name("id") |> sql.column |> expression.eq(sql.value(value.int(123)))
////   ])
////   |> update.returning(["id", "name"])
////   |> update.to_query
//// ```

import based
import based/db
import based/sql
import based/sql/condition.{type Condition}
import based/sql/internal/builder
import based/sql/internal/fmt
import based/sql/table
import gleam/list
import gleam/option.{type Option, None}

pub opaque type Update(v) {
  Update(
    repo: based.Repo(v),
    table: table.Table,
    sets: List(#(String, condition.Node)),
    where: List(List(Condition)),
    order_by: List(String),
    order: Option(sql.Order),
    limit: Option(Int),
    offset: Option(Int),
    returning: List(String),
    values: List(List(v)),
  )
}

/// Create a new UPDATE query for the specified table
pub fn table(repo: based.Repo(v), table: table.Table) -> Update(v) {
  Update(
    repo:,
    table:,
    sets: [],
    where: [],
    order_by: [],
    order: None,
    limit: None,
    offset: None,
    returning: [],
    values: [],
  )
}

/// Add a column assignment to the UPDATE statement
pub fn set(update: Update(v), column: String, value: a, of kind: sql.Kind(a, v)) {
  let #(node, values) = condition.to_node_and_values(kind.comparable(), value)
  let sets = update.sets |> list.prepend(#(column, node))

  Update(..update, sets:) |> prepend_values(values)
}

/// Add WHERE conditions to the UPDATE statement
pub fn where(
  update: Update(v),
  conditions: List(#(Condition, List(v))),
) -> Update(v) {
  let conds =
    conditions
    |> list.map(fn(cond) { cond.0 })

  let vals =
    conditions
    |> list.flat_map(fn(cond) { cond.1 })

  Update(..update, where: [conds]) |> prepend_values(vals)
}

/// Add WHERE NOT conditions to the UPDATE statement
pub fn where_not(
  update: Update(v),
  conditions: List(#(Condition, List(v))),
) -> Update(v) {
  let negated_exprs = list.map(conditions, sql.not)

  where(update, negated_exprs)
}

/// Specify columns to return after the update. Only applies to adapter
/// packages that support RETURNING.
pub fn returning(update: Update(v), columns: List(String)) -> Update(v) {
  Update(..update, returning: columns)
}

/// Convert the UPDATE query to a database query with parameters
pub fn to_query(update: Update(v)) -> db.Query(v) {
  let values = update.values |> list.reverse |> list.flatten

  let to_placeholder = fmt.to_placeholder(update.repo.fmt, _)

  build(update)
  |> builder.placeholders(on: fmt.placeholder, with: to_placeholder)
  |> db.sql
  |> db.params(values)
}

/// Convert the UPDATE query to a formatted SQL string
pub fn to_string(update: Update(v)) -> String {
  let values = update.values |> list.reverse |> list.flatten

  build(update)
  |> builder.to_string(values, update.repo.fmt)
}

fn build(update: Update(v)) -> String {
  let sets = update.sets |> list.reverse

  let updates = {
    use #(column, value) <- list.map(sets)

    let right = condition.node_to_string(value, update.repo.fmt)

    column
    |> fmt.eq(right)
  }

  let table =
    update.table
    |> table.to_string(fmt.to_identifier(update.repo.fmt, _))

  fmt.update(table)
  |> fmt.set(updates)
  |> builder.append_where(update.where, update.repo.fmt)
  |> builder.append_returning(update.returning)
  |> builder.append_order_by(update.order_by, update.order)
  |> builder.append_limit(update.limit, update.offset)
}

fn prepend_values(update: Update(v), values: List(v)) -> Update(v) {
  let values = list.prepend(update.values, values)
  Update(..update, values:)
}
