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
import based/sql
import based/sql/internal/builder
import based/sql/internal/fmt
import gleam/list
import gleam/string_tree.{type StringTree}

/// A DELETE query with table, WHERE conditions, RETURNING columns, and values.
pub opaque type Delete(v) {
  Delete(
    table: sql.Table(v),
    where: List(List(sql.Expr(v))),
    returning: List(String),
    values: List(List(v)),
  )
}

/// Set the table for a DELETE query.
pub fn from(table: sql.Table(v)) -> Delete(v) {
  Delete(table:, where: [], returning: [], values: [])
}

/// Add WHERE conditions to a DELETE query.
pub fn where(delete: Delete(v), exprs: List(sql.Expr(v))) -> Delete(v) {
  let values = list.flat_map(exprs, sql.expr_to_values)

  Delete(..delete, where: [exprs]) |> prepend_values(values)
}

/// Add negated WHERE conditions to a DELETE query.
pub fn where_not(delete: Delete(v), exprs: List(sql.Expr(v))) -> Delete(v) {
  let negated_exprs = list.map(exprs, sql.not)
  where(delete, negated_exprs)
}

/// Set RETURNING columns for a DELETE query.
pub fn returning(delete: Delete(v), columns: List(String)) -> Delete(v) {
  Delete(..delete, returning: columns)
}

/// Convert a DELETE query to a database query using the given format.
pub fn to_query(del: Delete(v), format: sql.Format(v)) -> db.Query(v) {
  let values = del.values |> list.reverse |> list.flatten

  let to_placeholder = sql.to_placeholder(format, _)

  build(del, format)
  |> builder.placeholders(on: fmt.placeholder, with: to_placeholder)
  |> string_tree.to_string
  |> db.sql
  |> db.values(values)
}

/// Convert a DELETE query to a string representation using the given format.
pub fn to_string(delete: Delete(v), format: sql.Format(v)) -> String {
  let values = delete.values |> list.reverse |> list.flatten

  build(delete, format)
  |> string_tree.to_string
  |> builder.to_string(values, format)
}

/// Build a DELETE query's SQL string tree using the given format.
fn build(delete: Delete(v), format: sql.Format(v)) -> StringTree {
  let from = sql.table_to_string(delete.table, format)

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
