import based/db
import based/format.{type Format}
import based/sql/internal/builder
import based/sql/internal/fmt
import based/sql/node.{type Node}
import based/sql/table.{type Table}
import gleam/list
import gleam/string_tree.{type StringTree}

pub opaque type Insert(v) {
  Insert(
    table: Table(v),
    columns: List(String),
    returning: List(String),
    values: List(v),
  )
}

pub fn new(table: Table(v)) -> Insert(v) {
  Insert(table:, columns: [], returning: [], values: [])
}

pub fn columns(insert: Insert(v), cols: List(String)) -> Insert(v) {
  Insert(..insert, columns: cols)
}

pub fn values(insert: Insert(v), vals: List(List(Node(v)))) -> Insert(v) {
  let values = list.flat_map(vals, list.flat_map(_, node.unwrap))

  Insert(..insert, values:)
}

pub fn returning(insert: Insert(v), cols: List(String)) -> Insert(v) {
  Insert(..insert, returning: cols)
}

pub fn to_query(insert: Insert(v), format: Format(v)) -> db.Query(v) {
  let to_placeholder = format.to_placeholder(format, _)

  build(insert, format)
  |> builder.placeholders(on: fmt.placeholder, with: to_placeholder)
  |> string_tree.to_string
  |> db.sql
  |> db.values(insert.values)
}

pub fn to_string(insert: Insert(v), format: Format(v)) -> String {
  build(insert, format)
  |> string_tree.to_string
  |> builder.to_string(insert.values, format)
}

fn build(insert: Insert(v), format: Format(v)) -> StringTree {
  let values =
    insert.values
    |> list.map(fn(_) { string_tree.from_string(fmt.placeholder) })
    |> list.sized_chunk(into: list.length(insert.columns))
    |> list.map(fn(vals) {
      string_tree.join(vals, with: ", ")
      |> fmt.enclose_tree
    })

  let into = table.to_string(insert.table, format)

  string_tree.new()
  |> fmt.insert(insert.columns, into:, values:)
  |> builder.append_returning(insert.returning)
}
