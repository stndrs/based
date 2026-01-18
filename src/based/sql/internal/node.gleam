import based/db
import based/sql/internal/fmt
import gleam/bool
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/string

pub type Node(v) {
  Column(name: String, alias: Option(String), table: Option(String))
  Columns(List(String))
  Text(String)
  Value(v)
  List(List(Node(v)))
  Tuples(List(List(v)))
  Query(query: db.Query(v), alias: Option(String))
  Null(Bool)
}

pub fn unwrap(node: Node(v), with handle_text: fn(String) -> v) -> List(v) {
  case node {
    Value(val) -> [val]
    List(vals) -> list.flat_map(vals, unwrap(_, handle_text))
    Tuples(vals) -> list.flatten(vals)
    Query(query:, alias: _) -> query.values
    Text(val) -> [handle_text(val)]
    _ -> []
  }
}

pub fn to_string(
  node: Node(v),
  with handle_identifier: fn(String) -> String,
) -> String {
  case node {
    Column(name:, alias:, table:) -> {
      let ident = case table {
        Some(table) -> {
          table
          |> handle_identifier
          |> string.append(".")
          |> string.append(handle_identifier(name))
        }
        None -> handle_identifier(name)
      }

      case alias {
        Some(a) -> fmt.alias(ident, a)
        None -> ident
      }
    }
    Columns(columns) ->
      columns
      |> string.join(", ")
      |> fmt.enclose
    Text(_) -> fmt.placeholder
    Value(_) -> fmt.placeholder
    List(values) ->
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
