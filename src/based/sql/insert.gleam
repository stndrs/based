import based/db
import based/sql
import based/sql/internal/builder
import based/sql/internal/fmt
import based/sql/internal/node
import gleam/list
import gleam/string

pub opaque type Insert(v) {
  Insert(
    sql: sql.Sql(v),
    table: sql.Table(v),
    columns: List(String),
    returning: List(String),
    values: List(v),
  )
}

pub fn into(sql: sql.Sql(v), table: sql.Table(v)) -> Insert(v) {
  Insert(sql:, table:, columns: [], returning: [], values: [])
}

pub fn columns(insert: Insert(v), cols: List(String)) -> Insert(v) {
  Insert(..insert, columns: cols)
}

pub fn values(insert: Insert(v), vals: List(List(v))) -> Insert(v) {
  let values = list.flatten(vals)

  Insert(..insert, values:)
}

pub fn returning(insert: Insert(v), cols: List(String)) -> Insert(v) {
  Insert(..insert, returning: cols)
}

pub fn to_query(insert: Insert(v)) -> db.Query(v) {
  let to_placeholder = sql.to_placeholder(insert.sql, _)

  build(insert)
  |> builder.placeholders(on: fmt.placeholder, with: to_placeholder)
  |> db.sql
  |> db.params(insert.values)
}

pub fn to_string(insert: Insert(v)) -> String {
  build(insert)
  |> builder.to_string(insert.values, insert.sql)
}

fn build(insert: Insert(v)) -> String {
  let values =
    insert.values
    |> list.map(fn(_) { fmt.placeholder })
    |> list.sized_chunk(into: list.length(insert.columns))
    |> list.map(fn(vals) {
      vals
      |> string.join(with: ", ")
      |> fmt.enclose
    })

  let into =
    insert.table
    |> sql.table_to_node
    |> node.to_string(sql.to_identifier(insert.sql, _))

  fmt.insert(insert.columns, into:, values:)
  |> builder.append_returning(insert.returning)
}
