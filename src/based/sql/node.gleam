import based/db
import based/format.{type Format}
import based/sql/column.{type Column}
import based/sql/internal/fmt
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/string
import gleam/string_tree.{type StringTree}

pub type Order {
  Asc
  Desc
}

pub type Node(v) {
  Table(name: String, alias: Option(String))
  ColumnRef(Column(v))
  Columns(List(Column(v)))
  Value(v)
  Values(List(v))
  Tuples(List(List(v)))
  Subquery(query: db.Query(v))
}

pub fn column(col: Column(v)) -> Node(v) {
  ColumnRef(col)
}

pub fn columns(cols: List(Column(v))) -> Node(v) {
  Columns(cols)
}

pub fn subquery(query: db.Query(v)) -> Node(v) {
  Subquery(query)
}

pub fn value(value: v) -> Node(v) {
  Value(value)
}

pub fn values(values: List(v)) -> Node(v) {
  Values(values)
}

pub fn tuples(tups: List(List(v))) -> Node(v) {
  Tuples(tups)
}

pub fn unwrap(node: Node(v)) -> List(v) {
  case node {
    Value(val) -> [val]
    Values(vals) -> vals
    Tuples(vals) -> list.flatten(vals)
    Subquery(query) -> query.values
    _ -> []
  }
}

pub fn to_string(node: Node(v), format: Format(v)) -> String {
  case node {
    Table(name, alias) -> {
      let escaped_name = format.to_identifier(format, name)

      case alias {
        Some(a) -> escaped_name <> " AS " <> a
        None -> escaped_name
      }
    }
    ColumnRef(col) -> {
      column.to_string(col, format)
      |> format.to_identifier(format, _)
    }
    Columns(columns) ->
      list.map(columns, fn(col) {
        col
        |> column.to_string(format)
        |> format.to_identifier(format, _)
      })
      |> string.join(", ")
      |> fmt.enclose
    Value(_val) -> fmt.placeholder
    Values(values) -> {
      values
      |> list.map(fn(_value) { fmt.placeholder })
      |> string.join(", ")
      |> fmt.enclose
    }
    Tuples(tuples) -> {
      tuples
      |> list.map(fn(vals) {
        list.map(vals, fn(_) { fmt.placeholder })
        |> string.join(", ")
        |> fmt.enclose
      })
      |> string.join(", ")
      |> fmt.enclose
    }
    Subquery(query) -> fmt.enclose(query.sql)
  }
}

pub fn to_string_tree(node: Node(v), format: Format(v)) -> StringTree {
  to_string(node, format) |> string_tree.from_string
}
