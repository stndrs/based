import based/db
import based/sql/internal/fmt
import based/sql/internal/table
import gleam/bool
import gleam/list
import gleam/option.{type Option}
import gleam/string

pub type Node(v) {
  Column(table.Identifier)
  Columns(List(String))
  Value(v)
  Values(List(v))
  Tuples(List(List(v)))
  Query(query: db.Query(v), alias: Option(String))
  Null(Bool)
}

pub fn unwrap(node: Node(v)) -> List(v) {
  case node {
    Value(val) -> [val]
    Values(vals) -> vals
    Tuples(vals) -> list.flatten(vals)
    Query(query:, alias: _) -> query.values
    _ -> []
  }
}

pub fn to_string(
  node: Node(v),
  with handle_identifier: fn(String) -> String,
) -> String {
  case node {
    Column(identifier) -> {
      identifier
      |> table.identifier_to_string(handle_identifier)
    }
    Columns(columns) ->
      columns
      |> string.join(", ")
      |> fmt.enclose
    Value(_) -> fmt.placeholder
    Values(values) ->
      values
      |> list.map(fn(_) { fmt.placeholder })
      |> string.join(", ")
      |> fmt.enclose
    Tuples(tuples) ->
      tuples
      |> list.map(fn(vals) {
        list.map(vals, fn(_) { fmt.placeholder })
        |> string.join(", ")
        |> fmt.enclose
      })
      |> string.join(", ")
      |> fmt.enclose
    Query(query:, alias: _) -> fmt.enclose(query.sql)
    Null(val) -> {
      use <- bool.guard(when: val, return: fmt.null)

      fmt.not(fmt.null)
    }
  }
}
