//// A builder for constructing SQL DELETE statements.
////
//// ## Usage
////
//// ```gleam
//// import based/sql/delete
//// import database/value
////
//// let users = sql.table("users")
//// let sql = sql.new()
////
//// let query =
////   delete.from(sql, users)
////   |> delete.where([
////     sql.name("id") |> sql.column |> expr.eq(sql.value(value.int(123)))
////   ])
////   |> delete.returning(["id", "name"])
////   |> delete.to_query
//// ```

import based
import based/db
import based/sql
import based/sql/internal/builder
import based/sql/internal/expr
import based/sql/internal/fmt
import based/sql/table
import gleam/list

/// A DELETE query with table, WHERE conditions, RETURNING columns, and values.
pub opaque type Delete(v) {
  Delete(
    fmt: fmt.Fmt(v),
    table: table.Table,
    where: List(List(sql.Expr(v))),
    returning: List(String),
    values: List(List(v)),
  )
}

/// Set the table for a DELETE query.
pub fn from(repo: based.Repo(v), table: table.Table) -> Delete(v) {
  Delete(fmt: repo.fmt, table:, where: [], returning: [], values: [])
}

/// Add WHERE conditions to a DELETE query.
pub fn where(delete: Delete(v), exprs: List(sql.Expr(v))) -> Delete(v) {
  let values =
    list.flat_map(exprs, expr.to_values(_, fmt.to_value(delete.fmt, _)))

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

/// Convert a DELETE query to a database query using the given fmt.
pub fn to_query(del: Delete(v)) -> db.Query(v) {
  let values = del.values |> list.reverse |> list.flatten

  let to_placeholder = fmt.to_placeholder(del.fmt, _)

  build(del)
  |> builder.placeholders(on: fmt.placeholder, with: to_placeholder)
  |> db.sql
  |> db.params(values)
}

/// Convert a DELETE query to a string representation using the given fmt.
pub fn to_string(delete: Delete(v)) -> String {
  let values = delete.values |> list.reverse |> list.flatten

  build(delete)
  |> builder.to_string(values, delete.fmt)
}

/// Build a DELETE query's SQL string tree using the given fmt.
fn build(delete: Delete(v)) -> String {
  let from =
    delete.table
    |> table.to_string(fmt.to_identifier(delete.fmt, _))

  fmt.delete
  |> fmt.from(from)
  |> builder.append_where(delete.where, delete.fmt)
  |> builder.append_returning(delete.returning)
}

/// Prepend values to a DELETE query.
fn prepend_values(delete: Delete(v), values: List(v)) -> Delete(v) {
  let values = list.prepend(delete.values, values)
  Delete(..delete, values:)
}
