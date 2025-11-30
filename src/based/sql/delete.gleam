import based/db
import based/format.{type Format}
import based/sql/expr.{type Expr}
import based/sql/internal/builder
import based/sql/internal/fmt
import based/sql/table.{type Table}
import gleam/list
import gleam/string_tree.{type StringTree}

pub opaque type Delete(v) {
  Delete(
    table: Table(v),
    where: List(List(Expr(v))),
    returning: List(String),
    values: List(List(v)),
  )
}

pub fn new() -> Delete(v) {
  let table = table.new("")
  Delete(table:, where: [], returning: [], values: [])
}

pub fn from(delete: Delete(v), table: Table(v)) -> Delete(v) {
  Delete(..delete, table:)
}

pub fn where(delete: Delete(v), exprs: List(Expr(v))) -> Delete(v) {
  let values = list.flat_map(exprs, expr.to_values)

  Delete(..delete, where: [exprs]) |> prepend_values(values)
}

pub fn where_not(delete: Delete(v), exprs: List(Expr(v))) -> Delete(v) {
  let negated_exprs = list.map(exprs, expr.not)
  where(delete, negated_exprs)
}

pub fn returning(delete: Delete(v), columns: List(String)) -> Delete(v) {
  Delete(..delete, returning: columns)
}

pub fn to_query(del: Delete(v), format: Format(v)) -> db.Query(v) {
  let values = del.values |> list.reverse |> list.flatten

  let to_placeholder = format.to_placeholder(format, _)

  build(del, format)
  |> builder.placeholders(on: fmt.placeholder, with: to_placeholder)
  |> string_tree.to_string
  |> db.sql
  |> db.values(values)
}

pub fn to_string(delete: Delete(v), format: Format(v)) -> String {
  let values = delete.values |> list.reverse |> list.flatten

  build(delete, format)
  |> string_tree.to_string
  |> builder.to_string(values, format)
}

fn build(delete: Delete(v), format: Format(v)) -> StringTree {
  let from = table.to_string(delete.table, format)

  string_tree.new()
  |> fmt.delete
  |> fmt.from(from)
  |> builder.append_where(delete.where, format)
  |> builder.append_returning(delete.returning)
}

fn prepend_values(delete: Delete(v), values: List(v)) -> Delete(v) {
  let values = list.prepend(delete.values, values)
  Delete(..delete, values:)
}
