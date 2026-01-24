import based/db
import based/sql/internal/fmt
import gleam/bool
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/string

pub opaque type Node(v) {
  Aggregate(
    name: String,
    alias: Option(String),
    table: Option(String),
    to_string: fn(String) -> String,
  )
  Column(name: String, alias: Option(String), table: Option(String))
  Columns(List(String))
  Text(String)
  Value(v)
  List(List(Node(v)))
  Tuples(List(List(v)))
  Query(query: db.Query(v), alias: Option(String))
  Null(Bool)
}

@internal
pub fn query(query: db.Query(v), alias: Option(String)) -> Node(v) {
  Query(query:, alias:)
}

@internal
pub fn text(value: String) -> Node(v) {
  Text(value)
}

@internal
pub fn list(value: List(Node(v))) -> Node(v) {
  List(value)
}

@internal
pub fn value(value: v) -> Node(v) {
  Value(value)
}

@internal
pub fn null(value: Bool) -> Node(v) {
  Null(value)
}

@internal
pub fn column(
  name: String,
  alias: Option(String),
  table: Option(String),
) -> Node(v) {
  Column(name:, alias:, table:)
}

@internal
pub fn aggregate(
  name: String,
  alias: Option(String),
  table: Option(String),
  to_string: fn(String) -> String,
) -> Node(v) {
  Aggregate(name:, alias:, table:, to_string:)
}

@internal
pub fn to_values(node: Node(v), with handle_text: fn(String) -> v) -> List(v) {
  case node {
    Value(val) -> [val]
    List(vals) -> list.flat_map(vals, to_values(_, handle_text))
    Tuples(vals) -> list.flatten(vals)
    Query(query:, alias: _) -> query.values
    Text(val) -> [handle_text(val)]
    _ -> []
  }
}

@internal
pub fn to_string(
  node: Node(v),
  with handle_identifier: fn(String) -> String,
) -> String {
  case node {
    Aggregate(name:, alias:, table:, to_string:) -> {
      let func =
        case table {
          Some(table) -> {
            table
            |> handle_identifier
            |> string.append(".")
            |> string.append(handle_identifier(name))
          }
          None -> handle_identifier(name)
        }
        |> to_string

      case alias {
        Some(a) -> fmt.alias(func, a)
        None -> func
      }
    }
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
