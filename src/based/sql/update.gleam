//// A builder for constructing SQL UPDATE statements.
////
//// ## Usage
////
//// ```gleam
//// import based/db
//// import based/format
//// import based/sql/update
//// import based/sql/table
////
//// let users = table.new("users")
//// let format = format.new()
////
//// let update = update.new(users)
////   |> update.set("name", node.string("John"))
////   |> update.where([
////     column.new("id") |> expr.eq(node.int(123))
////   ])
////   |> update.returning(["id", "name"])
////   |> update.to_query(format)
//// ```

import based/db
import based/sql
import based/sql/internal/builder
import based/sql/internal/fmt
import gleam/list
import gleam/option.{type Option, None}
import gleam/string_tree.{type StringTree}

pub opaque type Update(v) {
  Update(
    table: sql.Table(v),
    sets: List(#(String, sql.Node(v))),
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
pub fn table(table: sql.Table(v)) -> Update(v) {
  Update(
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
pub fn set(update: Update(v), column: String, value: sql.Node(v)) {
  let sets = update.sets |> list.prepend(#(column, value))
  let values = sql.unwrap(value)

  Update(..update, sets:) |> prepend_values(values)
}

/// Add WHERE conditions to the UPDATE statement
pub fn where(update: Update(v), exprs: List(sql.Expr(v))) -> Update(v) {
  let values = list.flat_map(exprs, sql.expr_to_values)

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
pub fn to_query(update: Update(v), format: sql.Format(v)) -> db.Query(v) {
  let values = update.values |> list.reverse |> list.flatten

  let to_placeholder = sql.to_placeholder(format, _)

  build(update, format)
  |> builder.placeholders(on: fmt.placeholder, with: to_placeholder)
  |> string_tree.to_string
  |> db.sql
  |> db.values(values)
}

/// Convert the UPDATE query to a formatted SQL string
pub fn to_string(update: Update(v), format: sql.Format(v)) -> String {
  let values = update.values |> list.reverse |> list.flatten

  build(update, format)
  |> string_tree.to_string
  |> builder.to_string(values, format)
}

fn build(update: Update(v), format: sql.Format(v)) -> StringTree {
  let sets = update.sets |> list.reverse

  let updates =
    sets
    |> list.map(with: fn(col_with_val) {
      let right = sql.node_to_string_tree(col_with_val.1, format)

      col_with_val.0
      |> string_tree.from_string
      |> fmt.eq(right)
    })
    |> string_tree.join(", ")

  let table = sql.table_to_string(update.table, format)

  string_tree.new()
  |> fmt.update(table)
  |> fmt.set(updates)
  |> builder.append_where(update.where, format)
  |> builder.append_returning(update.returning)
  |> builder.append_order_by(update.order_by, update.order)
  |> builder.append_limit(update.limit, update.offset)
}

fn prepend_values(update: Update(v), values: List(v)) -> Update(v) {
  let values = list.prepend(update.values, values)
  Update(..update, values:)
}
