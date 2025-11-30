//// A builder for constructing SQL DELETE statements.
////
//// ## Usage
////
//// ```gleam
//// import based/db
//// import based/format
//// import based/sql/delete
//// import based/sql/table
////
//// let table = table.new("users")
//// let format = format.new()
////
//// let query = delete.new()
////   |> delete.from(table)
////   |> delete.where([
////     column.new("id") |> expr.eq(node.int(123))
////   ])
////   |> delete.returning(["id", "name"])
////   |> delete.to_query(format)
//// ```

import based/db
import based/format.{type Format}
import based/sql/expr.{type Expr}
import based/sql/internal/builder
import based/sql/internal/fmt
import based/sql/table.{type Table}
import gleam/list
import gleam/string_tree.{type StringTree}

/// A DELETE query with table, WHERE conditions, RETURNING columns, and values.
pub opaque type Delete(v) {
  Delete(
    table: Table(v),
    where: List(List(Expr(v))),
    returning: List(String),
    values: List(List(v)),
  )
}

/// Create a new DELETE query.
pub fn new() -> Delete(v) {
  let table = table.new("")
  Delete(table:, where: [], returning: [], values: [])
}

/// Set the table for a DELETE query.
pub fn from(delete: Delete(v), table: Table(v)) -> Delete(v) {
  Delete(..delete, table:)
}

/// Add WHERE conditions to a DELETE query.
pub fn where(delete: Delete(v), exprs: List(Expr(v))) -> Delete(v) {
  let values = list.flat_map(exprs, expr.to_values)

  Delete(..delete, where: [exprs]) |> prepend_values(values)
}

/// Add negated WHERE conditions to a DELETE query.
pub fn where_not(delete: Delete(v), exprs: List(Expr(v))) -> Delete(v) {
  let negated_exprs = list.map(exprs, expr.not)
  where(delete, negated_exprs)
}

/// Set RETURNING columns for a DELETE query.
pub fn returning(delete: Delete(v), columns: List(String)) -> Delete(v) {
  Delete(..delete, returning: columns)
}

/// Convert a DELETE query to a database query using the given format.
pub fn to_query(del: Delete(v), format: Format(v)) -> db.Query(v) {
  let values = del.values |> list.reverse |> list.flatten

  let to_placeholder = format.to_placeholder(format, _)

  build(del, format)
  |> builder.placeholders(on: fmt.placeholder, with: to_placeholder)
  |> string_tree.to_string
  |> db.sql
  |> db.values(values)
}

/// Convert a DELETE query to a string representation using the given format.
pub fn to_string(delete: Delete(v), format: Format(v)) -> String {
  let values = delete.values |> list.reverse |> list.flatten

  build(delete, format)
  |> string_tree.to_string
  |> builder.to_string(values, format)
}

/// Build a DELETE query's SQL string tree using the given format.
fn build(delete: Delete(v), format: Format(v)) -> StringTree {
  let from = table.to_string(delete.table, format)

  string_tree.new()
  |> fmt.delete
  |> fmt.from(from)
  |> builder.append_where(delete.where, format)
  |> builder.append_returning(delete.returning)
}

/// Prepend values to a DELETE query.
fn prepend_values(delete: Delete(v), values: List(v)) -> Delete(v) {
  let values = list.prepend(delete.values, values)
  Delete(..delete, values:)
}
