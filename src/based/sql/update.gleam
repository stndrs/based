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
////     sql.name("id") |> sql.column |> expr.eq(sql.value(value.int(123)))
////   ])
////   |> update.returning(["id", "name"])
////   |> update.to_query
//// ```

import based/db
import based/sql
import based/sql/internal/builder
import based/sql/internal/expr
import based/sql/internal/fmt
import based/sql/internal/node.{type Node}
import based/sql/internal/table
import gleam/list
import gleam/option.{type Option, None}

pub opaque type Update(v) {
  Update(
    sql: sql.Sql(v),
    table: sql.Table(v),
    sets: List(#(String, Node(v))),
    where: List(List(sql.Expr(v))),
    order_by: List(String),
    order: Option(sql.Order),
    limit: Option(Int),
    offset: Option(Int),
    returning: List(String),
    values: List(List(v)),
  )
}

/// Create a new UPDATE query for the specified table
pub fn table(sql: sql.Sql(v), table: sql.Table(v)) -> Update(v) {
  Update(
    sql:,
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
pub fn set(update: Update(v), column: String, value: Node(v)) {
  let sets = update.sets |> list.prepend(#(column, value))
  let values = node.unwrap(value)

  Update(..update, sets:) |> prepend_values(values)
}

/// Add WHERE conditions to the UPDATE statement
pub fn where(update: Update(v), exprs: List(sql.Expr(v))) -> Update(v) {
  let values = list.flat_map(exprs, expr.to_values)

  Update(..update, where: [exprs])
  |> prepend_values(values)
}

/// Add WHERE NOT conditions to the UPDATE statement
pub fn where_not(update: Update(v), exprs: List(sql.Expr(v))) -> Update(v) {
  let negated_exprs = list.map(exprs, sql.not)
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

  let to_placeholder = sql.to_placeholder(update.sql, _)

  build(update)
  |> builder.placeholders(on: fmt.placeholder, with: to_placeholder)
  |> db.sql
  |> db.params(values)
}

/// Convert the UPDATE query to a formatted SQL string
pub fn to_string(update: Update(v)) -> String {
  let values = update.values |> list.reverse |> list.flatten

  build(update)
  |> builder.to_string(values, update.sql)
}

fn build(update: Update(v)) -> String {
  let sets = update.sets |> list.reverse

  let updates = {
    use #(column, value) <- list.map(sets)

    let right =
      value
      |> node.to_string(sql.to_identifier(update.sql, _))

    column
    |> fmt.eq(right)
  }

  let table =
    update.table
    |> table.to_node
    |> node.to_string(sql.to_identifier(update.sql, _))

  fmt.update(table)
  |> fmt.set(updates)
  |> builder.append_where(update.where, update.sql)
  |> builder.append_returning(update.returning)
  |> builder.append_order_by(update.order_by, update.order)
  |> builder.append_limit(update.limit, update.offset)
}

fn prepend_values(update: Update(v), values: List(v)) -> Update(v) {
  let values = list.prepend(update.values, values)
  Update(..update, values:)
}
