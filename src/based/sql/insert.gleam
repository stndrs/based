import based/db
import based/repo.{type Repo}
import based/sql/column.{type Column}
import based/sql/internal/builder
import based/sql/internal/fmt
import based/sql/table
import gleam/list
import gleam/string

pub opaque type Insert(v) {
  Insert(
    repo: Repo(v),
    table: table.Table,
    columns: List(String),
    returning: List(Column),
    values: List(v),
  )
}

pub fn into(repo: Repo(v), table: table.Table) -> Insert(v) {
  Insert(repo:, table:, columns: [], returning: [], values: [])
}

pub fn columns(insert: Insert(v), cols: List(String)) -> Insert(v) {
  Insert(..insert, columns: cols)
}

pub fn values(insert: Insert(v), vals: List(List(v))) -> Insert(v) {
  let values = list.flatten(vals)

  Insert(..insert, values:)
}

pub fn returning(insert: Insert(v), cols: List(Column)) -> Insert(v) {
  Insert(..insert, returning: cols)
}

pub fn to_query(insert: Insert(v)) -> db.Query(v) {
  let to_placeholder = fmt.to_placeholder(insert.repo.fmt, _)

  build(insert)
  |> builder.placeholders(on: fmt.placeholder, with: to_placeholder)
  |> db.sql
  |> db.params(insert.values)
}

pub fn to_string(insert: Insert(v)) -> String {
  build(insert)
  |> builder.to_string(insert.values, insert.repo.fmt)
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
    |> table.to_string(fmt.to_identifier(insert.repo.fmt, _))

  let returning = list.map(insert.returning, column.to_string(_, insert.repo))

  fmt.insert(insert.columns, into:, values:)
  |> builder.append_returning(returning)
}
