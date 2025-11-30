import based/db
import based/sql
import based/sql/internal/builder
import based/sql/internal/fmt
import gleam/list
import gleam/string_tree.{type StringTree}

pub opaque type Insert(v) {
  Insert(
    table: sql.Node(v),
    columns: List(String),
    returning: List(String),
    values: List(v),
  )
}

pub fn into(table: sql.Table(v)) -> Insert(v) {
  let table = sql.table_to_node(table)
  Insert(table:, columns: [], returning: [], values: [])
}

pub fn columns(insert: Insert(v), cols: List(String)) -> Insert(v) {
  Insert(..insert, columns: cols)
}

pub fn values(insert: Insert(v), vals: List(List(sql.Node(v)))) -> Insert(v) {
  let values = list.flat_map(vals, list.flat_map(_, sql.unwrap))

  Insert(..insert, values:)
}

pub fn returning(insert: Insert(v), cols: List(String)) -> Insert(v) {
  Insert(..insert, returning: cols)
}

pub fn to_query(insert: Insert(v), format: sql.SqlFmt(v)) -> db.Query(v) {
  let to_placeholder = sql.to_placeholder(format, _)

  build(insert, format)
  |> builder.placeholders(on: fmt.placeholder, with: to_placeholder)
  |> string_tree.to_string
  |> db.sql
  |> db.values(insert.values)
}

pub fn to_string(insert: Insert(v), format: sql.SqlFmt(v)) -> String {
  build(insert, format)
  |> string_tree.to_string
  |> builder.to_string(insert.values, format)
}

fn build(insert: Insert(v), format: sql.SqlFmt(v)) -> StringTree {
  let values =
    insert.values
    |> list.map(fn(_) { string_tree.from_string(fmt.placeholder) })
    |> list.sized_chunk(into: list.length(insert.columns))
    |> list.map(fn(vals) {
      vals
      |> string_tree.join(with: ", ")
      |> fmt.enclose_tree
    })

  let into = sql.node_to_string(insert.table, format)

  string_tree.new()
  |> fmt.insert(insert.columns, into:, values:)
  |> builder.append_returning(insert.returning)
}
